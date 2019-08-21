package pilosa

import (
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Batch struct {
	client *Client
	index  *Index
	header []*Field

	// ids is a slice of length batchSize of record IDs
	ids []uint64

	// rowIDs is a slice of length len(Batch.header) which contains slices of length batchSize
	rowIDs [][]uint64
	// TODO, support int fields, set fields without translation, timestamps, set fields with more than one value per record.

	// for each field, keep a map of key to which record indexes that key mapped to
	toTranslate []map[string][]int

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
	rowIDs := make([][]uint64, len(fields))
	tt := make([]map[string][]int, len(fields))
	for i, _ := range fields {
		rowIDs[i] = make([]uint64, 0, size)
		tt[i] = make(map[string][]int)
	}
	return &Batch{
		client:        client,
		header:        fields,
		index:         index,
		ids:           make([]uint64, 0, size),
		rowIDs:        rowIDs,
		toTranslate:   tt,
		toTranslateID: make(map[string][]int),
	}
}

type Row struct {
	ID     interface{}
	Values []interface{}
}

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
		if val, ok := rec.Values[i].(string); ok {
			// translate val and append to b.rowIDs[i]
			if rowID, ok := b.client.translator.GetRow(b.index.Name(), field.Name(), val); ok {
				b.rowIDs[i] = append(b.rowIDs[i], rowID)
			} else {
				ints, ok := b.toTranslate[i][val]
				if !ok {
					ints = make([]int, 0)
				}
				ints = append(ints, len(b.rowIDs[i]))
				b.toTranslate[i][val] = ints
				b.rowIDs[i] = append(b.rowIDs[i], 0)
			}
		} else {
			return errors.New("TODO support types other than string")
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
		}
	} else {
		keys = make([]string, 0, len(b.toTranslate[0]))
	}

	// translate row keys
	for i, field := range b.header {
		tt := b.toTranslate[i]
		keys = keys[:0]

		// make a slice of keys
		for k, _ := range tt {
			keys = append(keys, k)
		}

		if len(keys) == 0 {
			continue
		}

		// translate keys from Pilosa
		ids, err := b.client.translateRowKeys(field, keys)
		if err != nil {
			return errors.Wrap(err, "translating row keys")
		}

		// fill out missing IDs in local batch records with translated IDs
		for j, key := range keys {
			id := ids[j]
			for _, recordIdx := range tt[key] {
				b.rowIDs[i][recordIdx] = id
			}
			b.client.translator.AddRow(b.index.Name(), field.Name(), key, id)
		}
	}
	return nil
}

func (b *Batch) doImport() error {
	eg := errgroup.Group{}

	frags := b.makeFragments()
	uri := b.client.cluster.hosts[0] // TODO get URI per-shard performantly.
	for shard, viewMap := range frags {
		for fieldView, bitmap := range viewMap {
			fieldView := fieldView
			bitmap := bitmap
			eg.Go(func() error {
				err := b.client.importRoaringBitmap(uri, b.index.Field(fieldView.field), shard, map[string]*roaring.Bitmap{"": bitmap}, &ImportOptions{})
				return errors.Wrap(err, "doing import")
			})
		}
	}
	return eg.Wait()
}

func (b *Batch) makeFragments() fragments {
	shardWidth := b.index.shardWidth
	if shardWidth == 0 {
		shardWidth = DefaultShardWidth
	}
	frags := make(fragments)
	for i, field := range b.header {
		curShard := ^uint64(0) // impossible sentinel value.
		var curBM *roaring.Bitmap
		rowIDs := b.rowIDs[i]
		for j, _ := range b.ids {
			col, row := b.ids[j], rowIDs[j]
			if col/shardWidth != curShard {
				curShard = col / shardWidth
				curBM = frags.GetOrCreate(curShard, field.Name(), "")
			}
			curBM.DirectAdd(row*shardWidth + (col % shardWidth))
		}
	}
	return frags
}

// reset is called at the end of importing to ready the batch for the
// next round. Where possible it does not re-allocate memory.
func (b *Batch) reset() {
	b.ids = b.ids[:0]
	for i, rowIDs := range b.rowIDs {
		b.rowIDs[i] = rowIDs[:0]
		m := b.toTranslate[i]
		for k := range m {
			delete(m, k)
		}
	}
	for k := range b.toTranslateID {
		delete(b.toTranslateID, k)
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
