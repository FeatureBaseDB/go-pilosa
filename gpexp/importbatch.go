package gpexp

import (
	"github.com/pilosa/go-pilosa"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// TODO if using column translation, column ids might get way out of
// order. Could be worth sorting everything after translation (as an
// option?). Instead of sorting all simultaneously, it might be faster
// (more cache friendly) to sort ids and save the swap ops to apply to
// everything else that needs to be sorted.

// TODO support clearing values? nil values in records are ignored,
// but perhaps we could have a special type indicating that a bit or
// value should explicitly be cleared?

// RecordBatch is a Pilosa ingest interface designed to allow for
// maximum throughput on common workloads. Users should call Add()
// with a Row object until it returns ErrBatchNowFull, at which time
// they should call Import(), and then repeat.
//
// Add will not modify or otherwise retain the Row once it returns, so
// it is recommended that callers reuse the same Row with repeated
// calls to Add, just modifying its values appropriately in between
// calls. This avoids allocating a new slice of Values for each
// inserted Row.
//
// The supported types of the values in Row.Values are implementation
// defined. Similarly, the supported types for Row.ID are
// implementation defined.
type RecordBatch interface {
	Add(Row) error
	Import() error
}

// Batch implements RecordBatch.
//
// It supports Values of type string, uint64, int64, or nil. The
// following table describes what Pilosa field each type of value must
// map to. Fields are set up when calling "NewBatch".
//
// | type   | pilosa field type | options   |
// |--------+-------------------+-----------|
// | string | set               | keys=true |
// | uint64 | set               | any       |
// | int64  | int               | any       |
// | nil    | any               |           |
//
// nil values are ignored.
type Batch struct {
	client    *pilosa.Client
	index     *pilosa.Index
	header    []*pilosa.Field
	headerMap map[string]*pilosa.Field

	// ids is a slice of length batchSize of record IDs
	ids []uint64

	// rowIDs is a slice of length len(Batch.header) which contains slices of length batchSize
	rowIDs map[string][]uint64

	// values holds the values for each record of an int field
	values map[string][]int64

	// clearValues holds a slice of indices into b.ids for each
	// integer field which has nil values. After translation, these
	// slices will be filled out with the actual column IDs those
	// indices pertain to so that they can be cleared.
	clearValues map[string][]uint64

	// TODO, support timestamps, set fields with more than one value per record, mutex, and bool.

	// for each field, keep a map of key to which record indexes that key mapped to
	toTranslate map[string]map[string][]int

	// for string ids which we weren't able to immediately translate,
	// keep a map of which record(s) each string id maps to.
	//
	// TODO:
	// this is probably super inefficient in the (common) case where
	// each record has a different string ID. In that case, a simple
	// slice of strings would probably work better.
	toTranslateID map[string][]int

	transCache Translator
}

type BatchOption func(b *Batch) error

func OptTranslator(t Translator) BatchOption {
	return func(b *Batch) error {
		b.transCache = t
		return nil
	}
}

// NewBatch initializes a new Batch object which will use the given
// Pilosa client, index, set of fields, and will take "size" records
// before returning ErrBatchNowFull. The positions of the Fields in
// 'fields' correspond to the positions of values in the Row's Values
// passed to Batch.Add().
func NewBatch(client *pilosa.Client, size int, index *pilosa.Index, fields []*pilosa.Field, opts ...BatchOption) (*Batch, error) {
	if len(fields) == 0 || size == 0 {
		return nil, errors.New("can't batch with no fields or batch size")
	}
	headerMap := make(map[string]*pilosa.Field, len(fields))
	rowIDs := make(map[string][]uint64)
	values := make(map[string][]int64)
	tt := make(map[string]map[string][]int)
	for _, field := range fields {
		headerMap[field.Name()] = field
		opts := field.Opts()
		switch opts.Type() {
		case pilosa.FieldTypeDefault, pilosa.FieldTypeSet:
			if opts.Keys() {
				tt[field.Name()] = make(map[string][]int)
			}
			rowIDs[field.Name()] = make([]uint64, 0, size)
		case pilosa.FieldTypeInt:
			values[field.Name()] = make([]int64, 0, size)
		}
	}
	b := &Batch{
		client:        client,
		header:        fields,
		headerMap:     headerMap,
		index:         index,
		ids:           make([]uint64, 0, size),
		rowIDs:        rowIDs,
		values:        values,
		clearValues:   make(map[string][]uint64),
		toTranslate:   tt,
		toTranslateID: make(map[string][]int),
		transCache:    NewMapTranslator(),
	}
	for _, opt := range opts {
		err := opt(b)
		if err != nil {
			return nil, errors.Wrap(err, "applying options")
		}
	}
	return b, nil
}

// Row represents a single record which can be added to a RecordBatch.
//
// Note: it is not named "Record" because there is a conflict with
// another type in this package. This may be rectified by deprecating
// something or splitting packages in the future.
type Row struct {
	ID     interface{}
	Values []interface{}
}

// Add adds a record to the batch. Performance will be best if record
// IDs are shard-sorted. That is, all records which belong to the same
// Pilosa shard are added adjacent to each other. If the records are
// also in-order within a shard this will likely help as well.
func (b *Batch) Add(rec Row) error {
	if len(b.ids) == cap(b.ids) {
		return ErrBatchAlreadyFull
	}
	if len(rec.Values) != len(b.header) {
		return errors.Errorf("record needs to match up with batch fields, got %d fields and %d record", len(b.header), len(rec.Values))
	}

	switch rid := rec.ID.(type) {
	case uint64:
		b.ids = append(b.ids, rid)
	case string:
		if colID, ok, err := b.transCache.GetCol(b.index.Name(), rid); err != nil {
			return errors.Wrap(err, "translating column")
		} else if ok {
			b.ids = append(b.ids, colID)
		} else {
			ints, ok := b.toTranslateID[rid]
			if !ok {
				ints = make([]int, 0)
			}
			ints = append(ints, len(b.ids))
			b.toTranslateID[rid] = ints
			b.ids = append(b.ids, 0)
		}
	default:
		return errors.Errorf("unsupported id type %T value %v", rid, rid)
	}

	for i := 0; i < len(rec.Values); i++ {
		field := b.header[i]
		switch val := rec.Values[i].(type) {
		case string:
			rowIDs := b.rowIDs[field.Name()]
			// translate val and append to b.rowIDs[i]
			if rowID, ok, err := b.transCache.GetRow(b.index.Name(), field.Name(), val); err != nil {
				return errors.Wrap(err, "translating row")
			} else if ok {
				b.rowIDs[field.Name()] = append(rowIDs, rowID)
			} else {
				ints, ok := b.toTranslate[field.Name()][val]
				if !ok {
					ints = make([]int, 0)
				}
				ints = append(ints, len(rowIDs))
				b.toTranslate[field.Name()][val] = ints
				b.rowIDs[field.Name()] = append(rowIDs, 0)
			}
		case uint64:
			b.rowIDs[field.Name()] = append(b.rowIDs[field.Name()], val)
		case int64:
			b.values[field.Name()] = append(b.values[field.Name()], val)
		case nil:
			if field.Opts().Type() == pilosa.FieldTypeInt {
				b.values[field.Name()] = append(b.values[field.Name()], 0)
				clearIndexes, ok := b.clearValues[field.Name()]
				if !ok {
					clearIndexes = make([]uint64, 0)
				}
				clearIndexes = append(clearIndexes, uint64(len(b.ids)-1))
				b.clearValues[field.Name()] = clearIndexes

			} else {
				b.rowIDs[field.Name()] = append(b.rowIDs[field.Name()], nilSentinel)
			}
		default:
			return errors.Errorf("Val %v Type %[1]T is not currently supported. Use string, uint64 (row id), or int64 (integer value)", val)
		}
	}
	if len(b.ids) == cap(b.ids) {
		return ErrBatchNowFull
	}
	return nil
}

// ErrBatchNowFull, similar to io.EOF, is a marker error to notify the
// user of a batch that it is time to call Import.
var ErrBatchNowFull = errors.New("batch is now full - you cannot add any more records (though the one you just added was accepted)")

// ErrBatchAlreadyFull is a real error saying that Batch.Add did not
// complete because the batch was full.
var ErrBatchAlreadyFull = errors.New("batch was already full, record was rejected")

// Import does all necessary key translation and then imports the
// batch data into Pilosa. It readies itself for the next set of
// records by clearing internal structures without releasing the
// associated memory.
func (b *Batch) Import() error {
	// first we need to translate the toTranslate, then fill out the missing row IDs
	err := b.doTranslation()
	if err != nil {
		return errors.Wrap(err, "doing Translation")
	}

	// create bitmaps out of each field in b.rowIDs and import. Also
	// import int data.
	err = b.doImport()
	if err != nil {
		return errors.Wrap(err, "doing import")
	}

	// clear existing structures without reclaiming the memory
	b.reset()
	return nil
}

func (b *Batch) doTranslation() error {
	var keys []string

	// translate column keys if there are any
	if len(b.toTranslateID) > 0 {
		keys = make([]string, 0, len(b.toTranslateID))
		for k := range b.toTranslateID {
			keys = append(keys, k)
		}
		ids, err := b.client.TranslateColumnKeys(b.index, keys)
		if err != nil {
			return errors.Wrap(err, "translating col keys")
		}
		if err := b.transCache.AddCols(b.index.Name(), keys, ids); err != nil {
			return errors.Wrap(err, "adding cols to cache")
		}
		for j, key := range keys {
			id := ids[j]
			for _, recordIdx := range b.toTranslateID[key] {
				b.ids[recordIdx] = id
			}
		}
	} else {
		keys = make([]string, 0)
	}

	// translate row keys
	for fieldName, tt := range b.toTranslate {
		keys = keys[:0]

		// make a slice of keys
		for k := range tt {
			keys = append(keys, k)
		}

		if len(keys) == 0 {
			continue
		}

		// translate keys from Pilosa
		ids, err := b.client.TranslateRowKeys(b.headerMap[fieldName], keys)
		if err != nil {
			return errors.Wrap(err, "translating row keys")
		}
		if err := b.transCache.AddRows(b.index.Name(), fieldName, keys, ids); err != nil {
			return errors.Wrap(err, "adding rows to cache")
		}

		// fill out missing IDs in local batch records with translated IDs
		rows := b.rowIDs[fieldName]
		for j, key := range keys {
			id := ids[j]
			for _, recordIdx := range tt[key] {
				rows[recordIdx] = id
			}
		}
	}

	for _, idIndexes := range b.clearValues {
		for i, index := range idIndexes {
			idIndexes[i] = b.ids[index]
		}
	}
	return nil
}

func (b *Batch) doImport() error {
	eg := errgroup.Group{}

	frags := b.makeFragments()
	for shard, viewMap := range frags {
		for fieldView, bitmap := range viewMap {
			fieldView := fieldView
			bitmap := bitmap
			eg.Go(func() error {
				err := b.client.ImportRoaringBitmap(b.index.Field(fieldView.field), shard, map[string]*roaring.Bitmap{"": bitmap}, false)
				return errors.Wrapf(err, "importing data for %s", fieldView.field)
			})
		}
	}
	eg.Go(func() error {
		return b.importValueData()
	})
	return eg.Wait()
}

// this is kind of bad as it means we can never import column id
// ^uint64(0) which is a valid column ID. I think it's unlikely to
// matter much in practice (we could maybe special case it somewhere
// if needed though).
var nilSentinel = ^uint64(0)

func (b *Batch) makeFragments() fragments {
	shardWidth := b.index.ShardWidth()
	if shardWidth == 0 {
		shardWidth = pilosa.DefaultShardWidth
	}
	frags := make(fragments)
	for fname, rowIDs := range b.rowIDs {
		curShard := ^uint64(0) // impossible sentinel value for shard.
		var curBM *roaring.Bitmap
		for j := range b.ids {
			col, row := b.ids[j], rowIDs[j]
			if row == nilSentinel {
				continue
			}
			if col/shardWidth != curShard {
				curShard = col / shardWidth
				curBM = frags.GetOrCreate(curShard, fname, "")
			}
			curBM.DirectAdd(row*shardWidth + (col % shardWidth))
		}
	}
	return frags
}

func (b *Batch) importValueData() error {
	shardWidth := b.index.ShardWidth()
	if shardWidth == 0 {
		shardWidth = pilosa.DefaultShardWidth
	}

	eg := errgroup.Group{}
	curShard := b.ids[0] / shardWidth
	startIdx := 0
	for i := 1; i <= len(b.ids); i++ {
		// when i==len(b.ids) we ensure that the import logic gets run
		// by making a fake shard once we're past the last ID
		recordID := (curShard + 2) * shardWidth
		if i < len(b.ids) {
			recordID = b.ids[i]
		}
		if recordID/shardWidth != curShard {
			endIdx := i
			ids := b.ids[startIdx:endIdx]
			for field, values := range b.values {
				field := field
				shard := curShard
				vslice := values[startIdx:endIdx]
				eg.Go(func() error {
					err := b.client.ImportValues(b.index.Name(), field, shard, vslice, ids, false)
					return errors.Wrapf(err, "importing values for %s", field)
				})
			}
			startIdx = i
			curShard = recordID / shardWidth
		}
	}

	err := eg.Wait()
	if err != nil {
		return errors.Wrap(err, "importing value data")
	}

	// Now we clear any values for which we got a nil.
	//
	// TODO we need an endpoint which lets us set and clear
	// transactionally... this is kind of a hack.
	maxLen := 0
	for _, ids := range b.clearValues {
		if len(ids) > maxLen {
			maxLen = len(ids)
		}
	}
	eg = errgroup.Group{}
	values := make([]int64, 0, maxLen)
	for field, ids := range b.clearValues {
		// TODO maybe sort ids here
		curShard := b.ids[0] / shardWidth
		startIdx := 0
		for i := 1; i <= len(ids); i++ {
			recordID := (curShard + 2) * shardWidth
			if i < len(ids) {
				recordID = b.ids[i]
			}
			if recordID/shardWidth != curShard {
				endIdx := i
				idSlice := ids[startIdx:endIdx]
				values := values[:len(idSlice)]
				field := field
				shard := curShard
				eg.Go(func() error {
					err := b.client.ImportValues(b.index.Name(), field, shard, values, idSlice, true)
					return errors.Wrap(err, "clearing values")
				})
				startIdx = i
				curShard = recordID / shardWidth
			}
		}
	}

	return errors.Wrap(eg.Wait(), "importing clear value data")
}

// reset is called at the end of importing to ready the batch for the
// next round. Where possible it does not re-allocate memory.
func (b *Batch) reset() {
	b.ids = b.ids[:0]
	for fieldName, rowIDs := range b.rowIDs {
		b.rowIDs[fieldName] = rowIDs[:0]
		m := b.toTranslate[fieldName]
		for k := range m {
			delete(m, k) // TODO pool these slices
		}
	}
	for k := range b.toTranslateID {
		delete(b.toTranslateID, k) // TODO pool these slices
	}
	for k := range b.values {
		delete(b.values, k) // TODO pool these slices
	}
	for k := range b.clearValues {
		delete(b.clearValues, k) // TODO pool these slices
	}
}

type fieldView struct {
	field string
	view  string
}

// map[shard][fieldview]fragmentData
type fragments map[uint64]map[fieldView]*roaring.Bitmap

func (f fragments) GetOrCreate(shard uint64, field, view string) *roaring.Bitmap {
	viewMap, ok := f[shard]
	if !ok {
		viewMap = make(map[fieldView]*roaring.Bitmap)
	}
	bm, ok := viewMap[fieldView{field: field, view: view}]
	if !ok {
		bm = roaring.NewBTreeBitmap()
		viewMap[fieldView{field: field, view: view}] = bm
	}
	f[shard] = viewMap
	return bm
}
