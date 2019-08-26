package pilosa

import (
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Batch struct {
	client    *Client
	index     *Index
	header    []*Field
	headerMap map[string]*Field

	// ids is a slice of length batchSize of record IDs
	ids []uint64

	// rowIDs is a slice of length len(Batch.header) which contains slices of length batchSize
	rowIDs map[string][]uint64

	// values holds the values for each record of an int field
	values map[string][]int64

	// TODO, support set fields without translation, timestamps, set fields with more than one value per record, mutex, and bool.
	// also null values

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
}

func NewBatch(client *Client, size int, index *Index, fields []*Field) *Batch {
	if len(fields) == 0 || size == 0 {
		panic("can't batch with no fields or batch size")
	}
	headerMap := make(map[string]*Field, len(fields))
	rowIDs := make(map[string][]uint64)
	values := make(map[string][]int64)
	tt := make(map[string]map[string][]int)
	for _, field := range fields {
		headerMap[field.Name()] = field
		opts := field.Opts()
		switch opts.Type() {
		case FieldTypeDefault, FieldTypeSet:
			if opts.Keys() {
				tt[field.Name()] = make(map[string][]int)
			}
			rowIDs[field.Name()] = make([]uint64, 0, size)
		case FieldTypeInt:
			values[field.Name()] = make([]int64, 0, size)
		}
	}
	return &Batch{
		client:        client,
		header:        fields,
		headerMap:     headerMap,
		index:         index,
		ids:           make([]uint64, 0, size),
		rowIDs:        rowIDs,
		values:        values,
		toTranslate:   tt,
		toTranslateID: make(map[string][]int),
	}
}

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
		if colID, ok := b.client.translator.GetCol(b.index.Name(), rid); ok {
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
			if rowID, ok := b.client.translator.GetRow(b.index.Name(), field.Name(), val); ok {
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
		default:
			return errors.Errorf("Val %v Type %[1]T is not currently supported. Use string, uint64 (row id), or int64 (integer value)", val)
		}
	}
	if len(b.ids) == cap(b.ids) {
		return ErrBatchNowFull
	}
	return nil
}

var ErrBatchNowFull = errors.New("batch is now full - you cannot add any more records (though the one you just added was accepted)")
var ErrBatchAlreadyFull = errors.New("batch was already full, record was rejected")

func (b *Batch) Import() error {
	// first we need to translate the toTranslate, then fill out the missing row IDs
	err := b.doTranslation()
	if err != nil {
		return errors.Wrap(err, "doing Translation")
	}

	// create bitmaps out of each field in b.rowIDs and import
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
		for k, _ := range b.toTranslateID {
			keys = append(keys, k)
		}
		ids, err := b.client.translateColumnKeys(b.index, keys)
		if err != nil {
			return errors.Wrap(err, "translating col keys")
		}
		for j, key := range keys {
			id := ids[j]
			for _, recordIdx := range b.toTranslateID[key] {
				b.ids[recordIdx] = id
			}
			b.client.translator.AddCol(b.index.Name(), key, id)
		}
	} else {
		keys = make([]string, 0)
	}

	// translate row keys
	for fieldName, tt := range b.toTranslate {
		keys = keys[:0]

		// make a slice of keys
		for k, _ := range tt {
			keys = append(keys, k)
		}

		if len(keys) == 0 {
			continue
		}

		// translate keys from Pilosa
		ids, err := b.client.translateRowKeys(b.headerMap[fieldName], keys)
		if err != nil {
			return errors.Wrap(err, "translating row keys")
		}

		// fill out missing IDs in local batch records with translated IDs
		rows := b.rowIDs[fieldName]
		for j, key := range keys {
			id := ids[j]
			for _, recordIdx := range tt[key] {
				rows[recordIdx] = id
			}
			b.client.translator.AddRow(b.index.Name(), fieldName, key, id)
		}
	}
	return nil
}

func (b *Batch) doImport() error {
	eg := errgroup.Group{}

	frags := b.makeFragments()
	for shard, viewMap := range frags {
		uris, err := b.client.GetURIsForShard(b.index.Name(), shard)
		uri := uris[0]
		if err != nil {
			return errors.Wrap(err, "getting uris for shard")
		}
		for fieldView, bitmap := range viewMap {
			fieldView := fieldView
			bitmap := bitmap
			eg.Go(func() error {
				err := b.client.importRoaringBitmap(uri, b.index.Field(fieldView.field), shard, map[string]*roaring.Bitmap{"": bitmap}, &ImportOptions{})
				return errors.Wrapf(err, "importing data for %s", fieldView.field)
			})
		}
	}
	eg.Go(func() error {
		return b.importValueData()
	})
	return eg.Wait()
}

func (b *Batch) makeFragments() fragments {
	shardWidth := b.index.shardWidth
	if shardWidth == 0 {
		shardWidth = DefaultShardWidth
	}
	frags := make(fragments)
	for fname, rowIDs := range b.rowIDs {
		curShard := ^uint64(0) // impossible sentinel value.
		var curBM *roaring.Bitmap
		for j, _ := range b.ids {
			col, row := b.ids[j], rowIDs[j]
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
	shardWidth := b.index.shardWidth
	if shardWidth == 0 {
		shardWidth = DefaultShardWidth
	}

	eg := errgroup.Group{}
	curShard := b.ids[0] / shardWidth
	startIdx := 0
	for i := 1; i <= len(b.ids); i++ {
		// when i==len(b.ids) we ensure that the import logic gets run
		// by making a fake shard once we're past the last ID
		recordID := ^uint64(0)
		if i < len(b.ids) {
			recordID = b.ids[i]
		}
		if recordID/shardWidth != curShard {
			endIdx := i
			ids := b.ids[startIdx:endIdx]
			for field, values := range b.values {
				vslice := values[startIdx:endIdx]
				eg.Go(func() error {
					err := b.client.ImportValues(b.index.Name(), field, curShard, vslice, ids)
					return errors.Wrapf(err, "importing values for %s", field)
				})
			}
			startIdx = i
			curShard = recordID / shardWidth
		}
	}

	return errors.Wrap(eg.Wait(), "importing value data")
}

// reset is called at the end of importing to ready the batch for the
// next round. Where possible it does not re-allocate memory.
func (b *Batch) reset() {
	b.ids = b.ids[:0]
	for fieldName, rowIDs := range b.rowIDs {
		b.rowIDs[fieldName] = rowIDs[:0]
		m := b.toTranslate[fieldName]
		for k := range m {
			delete(m, k)
		}
	}
	for k := range b.toTranslateID {
		delete(b.toTranslateID, k)
	}
	for k, _ := range b.values {
		delete(b.values, k)
	}
}

type fieldView struct { // TODO rename to fieldview
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
