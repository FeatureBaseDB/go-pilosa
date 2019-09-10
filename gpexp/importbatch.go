package gpexp

import (
	"time"

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

	// rowIDs is a map of field names to slices of length batchSize
	// which contain row IDs.
	rowIDs map[string][]uint64

	// values holds the values for each record of an int field
	values map[string][]int64

	// times holds a time for each record. (if any of the fields are time fields)
	times []QuantizedTime

	// nullIndices holds a slice of indices into b.ids for each
	// integer field which has nil values.
	nullIndices map[string][]uint64

	// TODO support mutex and bool fields.

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

// BatchOption is a functional option for Batch objects.
type BatchOption func(b *Batch) error

// OptTranslator allows one to pass in a custom Translator
// implementation for mapping keys to IDs.
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
	hasTime := false
	for _, field := range fields {
		headerMap[field.Name()] = field
		opts := field.Opts()
		switch typ := opts.Type(); typ {
		case pilosa.FieldTypeDefault, pilosa.FieldTypeSet, pilosa.FieldTypeTime:
			if opts.Keys() {
				tt[field.Name()] = make(map[string][]int)
			}
			rowIDs[field.Name()] = make([]uint64, 0, size)
			hasTime = typ == pilosa.FieldTypeTime || hasTime
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
		nullIndices:   make(map[string][]uint64),
		toTranslate:   tt,
		toTranslateID: make(map[string][]int),
		transCache:    NewMapTranslator(),
	}
	if hasTime {
		b.times = make([]QuantizedTime, 0, size)
	}
	for _, opt := range opts {
		err := opt(b)
		if err != nil {
			return nil, errors.Wrap(err, "applying options")
		}
	}
	return b, nil
}

// Row represents a single record which can be added to a Batch.
type Row struct {
	ID     interface{}
	Values []interface{}
	Time   QuantizedTime
}

// QuantizedTime represents a moment in time down to some granularity
// (year, month, day, or hour).
type QuantizedTime struct {
	ymdh [10]byte
}

// Set sets the Quantized time to the given timestamp (down to hour
// granularity).
func (qt *QuantizedTime) Set(t time.Time) {
	copy(qt.ymdh[:], t.Format("2006010215"))
}

// SetYear sets the quantized time's year, but leaves month, day, and
// hour untouched.
func (qt *QuantizedTime) SetYear(year string) {
	copy(qt.ymdh[:4], year)
}

// SetMonth sets the QuantizedTime's month, but leaves year, day, and
// hour untouched.
func (qt *QuantizedTime) SetMonth(month string) {
	copy(qt.ymdh[4:6], month)
}

// SetDay sets the QuantizedTime's day, but leaves year, month, and
// hour untouched.
func (qt *QuantizedTime) SetDay(day string) {
	copy(qt.ymdh[6:8], day)
}

// SetHour sets the QuantizedTime's hour, but leaves year, month, and
// day untouched.
func (qt *QuantizedTime) SetHour(hour string) {
	copy(qt.ymdh[8:10], hour)
}

// Reset sets the time to the zero value which generates no time views.
func (qt *QuantizedTime) Reset() {
	for i := range qt.ymdh {
		qt.ymdh[i] = 0
	}
}

// views builds the list of Pilosa views for this particular time,
// given a quantum.
func (qt *QuantizedTime) views(q pilosa.TimeQuantum) ([]string, error) {
	zero := QuantizedTime{}
	if *qt == zero {
		return nil, nil
	}
	views := make([]string, 0, len(q))
	for _, unit := range q {
		switch unit {
		case 'Y':
			if qt.ymdh[0] == 0 {
				return nil, errors.New("no data set for year")
			}
			views = append(views, string(qt.ymdh[:4]))
		case 'M':
			if qt.ymdh[4] == 0 {
				return nil, errors.New("no data set for month")
			}
			views = append(views, string(qt.ymdh[:6]))
		case 'D':
			if qt.ymdh[6] == 0 {
				return nil, errors.New("no data set for day")
			}
			views = append(views, string(qt.ymdh[:8]))
		case 'H':
			if qt.ymdh[8] == 0 {
				return nil, errors.New("no data set for hour")
			}
			views = append(views, string(qt.ymdh[:10]))
		}
	}
	return views, nil
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
	default: // TODO support nil ID as being auto-allocated.
		return errors.Errorf("unsupported id type %T value %v", rid, rid)
	}

	if b.times != nil {
		b.times = append(b.times, rec.Time)
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
				nullIndices, ok := b.nullIndices[field.Name()]
				if !ok {
					nullIndices = make([]uint64, 0)
				}
				nullIndices = append(nullIndices, uint64(len(b.ids)-1))
				b.nullIndices[field.Name()] = nullIndices

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

	return nil
}

func (b *Batch) doImport() error {
	eg := errgroup.Group{}

	frags, err := b.makeFragments()
	if err != nil {
		return errors.Wrap(err, "making fragments")
	}
	for shard, fieldMap := range frags {
		for field, viewMap := range fieldMap {
			field := field
			viewMap := viewMap
			eg.Go(func() error {
				err := b.client.ImportRoaringBitmap(b.index.Field(field), shard, viewMap, false)
				return errors.Wrapf(err, "importing data for %s", field)
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

func (b *Batch) makeFragments() (fragments, error) {
	shardWidth := b.index.ShardWidth()
	if shardWidth == 0 {
		shardWidth = pilosa.DefaultShardWidth
	}
	frags := make(fragments)
	for fname, rowIDs := range b.rowIDs {
		field := b.headerMap[fname]
		opts := field.Opts()
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
			// TODO this is super ugly, but we want to avoid setting
			// bits on the standard view in the specific case when
			// there isn't one. Should probably refactor this whole
			// loop to be more general w.r.t. views. Also... tests for
			// the NoStandardView case would be great.
			if !(opts.Type() == pilosa.FieldTypeTime && opts.NoStandardView()) {
				curBM.DirectAdd(row*shardWidth + (col % shardWidth))
			}
			if opts.Type() == pilosa.FieldTypeTime {
				views, err := b.times[j].views(opts.TimeQuantum())
				if err != nil {
					return nil, errors.Wrap(err, "calculating views")
				}
				for _, view := range views {
					tbm := frags.GetOrCreate(curShard, fname, view)
					tbm.DirectAdd(row*shardWidth + (col % shardWidth))
				}
			}
		}
	}
	return frags, nil
}

// importValueData imports data for int fields.
func (b *Batch) importValueData() error {
	shardWidth := b.index.ShardWidth()
	if shardWidth == 0 {
		shardWidth = pilosa.DefaultShardWidth
	}
	eg := errgroup.Group{}

	ids := make([]uint64, len(b.ids))
	for field, values := range b.values {
		// grow our temp ids slice to full length
		ids = ids[:len(b.ids)]
		// copy orig ids back in
		copy(ids, b.ids)

		// trim out null values from ids and values.
		nullIndices := b.nullIndices[field]
		for i, nullIndex := range nullIndices {
			nullIndex -= uint64(i) // offset the index by the number of items removed so far
			ids = append(ids[:nullIndex], ids[nullIndex+1:]...)
			values = append(values[:nullIndex], values[nullIndex+1:]...)
		}

		// now do imports by shard
		curShard := ids[0] / shardWidth
		startIdx := 0
		for i := 1; i <= len(ids); i++ {
			var recordID uint64
			if i < len(ids) {
				recordID = ids[i]
			} else {
				recordID = (curShard + 2) * shardWidth
			}

			if recordID/shardWidth != curShard {
				endIdx := i
				shard := curShard
				field := field
				path, data, err := b.client.EncodeImportValues(b.index.Name(), field, shard, values[startIdx:endIdx], ids[startIdx:endIdx], false)
				if err != nil {
					return errors.Wrap(err, "encoding import values")
				}
				eg.Go(func() error {
					err := b.client.DoImportValues(b.index.Name(), shard, path, data)
					return errors.Wrapf(err, "importing values for %s", field)
				})
				startIdx = i
				curShard = recordID / shardWidth
			}
		}
	}
	err := eg.Wait()
	return errors.Wrap(err, "importing value data")
}

// reset is called at the end of importing to ready the batch for the
// next round. Where possible it does not re-allocate memory.
func (b *Batch) reset() {
	b.ids = b.ids[:0]
	b.times = b.times[:0]
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
	for k := range b.nullIndices {
		delete(b.nullIndices, k) // TODO pool these slices
	}
}

// map[shard][field][view]fragmentData
type fragments map[uint64]map[string]map[string]*roaring.Bitmap

func (f fragments) GetOrCreate(shard uint64, field, view string) *roaring.Bitmap {
	fieldMap, ok := f[shard]
	if !ok {
		fieldMap = make(map[string]map[string]*roaring.Bitmap)
	}
	viewMap, ok := fieldMap[field]
	if !ok {
		viewMap = make(map[string]*roaring.Bitmap)
	}
	bm, ok := viewMap[view]
	if !ok {
		bm = roaring.NewBTreeBitmap()
		viewMap[view] = bm
	}
	fieldMap[field] = viewMap
	f[shard] = fieldMap
	return bm
}
