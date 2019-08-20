package pilosa

import (
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Row struct {
	ID     interface{}
	Values []interface{}
}

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
	for i, field := range b.header {
		tt := b.toTranslate[i]
		keys := make([]string, 0, len(tt))

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
			b.client.translator.AddRow(b.index, field.Name(), key, id)
		}
	}
	return nil
}

func (b *Batch) doImport() error {
	eg := errgroup.Group{}
	index := b.header[0].index

	frags := b.makeFragments()
	uri := b.client.cluster.hosts[0] // TODO get URI per-shard performantly.
	for shard, viewMap := range frags {
		for viewSpec, bitmap := range viewMap {
			viewSpec := viewSpec
			bitmap := bitmap
			eg.Go(func() error {
				err := b.client.importRoaringBitmap(uri, index.Field(viewSpec.field), shard, map[string]*roaring.Bitmap{"": bitmap}, &ImportOptions{})
				return errors.Wrap(err, "doing import")
			})
		}
	}
	return eg.Wait()
}

func (b *Batch) makeFragments() fragments {
	shardWidth := b.header[0].index.shardWidth
	if shardWidth == 0 {
		shardWidth = DefaultShardWidth
	}
	frags := make(fragments)
	if len(b.ids) == 0 {
		return frags // exit early if no records
	}
	for i, field := range b.header {
		curShard := b.ids[0] / shardWidth
		curBM := frags.GetOrCreate(curShard, field.Name(), "")
		rowIDs := b.rowIDs[i]
		for j, _ := range b.ids {
			col, row := b.ids[j], rowIDs[j]
			if col%shardWidth != curShard {
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
}

type Batch struct {
	client *Client
	header []*Field
	index  string

	// ids is a slice of length batchSize of record IDs
	// TODO support string IDs
	ids []uint64
	// rowIDs is a slice of length len(Batch.header) which contains slices of length batchSize
	rowIDs [][]uint64
	// TODO, support int fields, set fields without translation, timestamps, set fields with more than one value per record.

	// for each field, keep a map of key to which record indexes that key mapped to
	toTranslate []map[string][]int
}

func NewBatch(client *Client, size int, fields []*Field) *Batch {
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
		client:      client,
		header:      fields,
		index:       fields[0].index.Name(),
		ids:         make([]uint64, 0, size),
		rowIDs:      rowIDs,
		toTranslate: tt,
	}
}

var ErrBatchNowFull = errors.New("batch is now full - you cannot add any more records (though the one you just added was accepted)")
var ErrBatchAlreadyFull = errors.New("batch was already full, record was rejected")

func (b *Batch) Add(rec Row) error {
	if len(b.ids) == cap(b.ids) {
		return ErrBatchAlreadyFull
	}
	if len(rec.Values) != len(b.header) {
		return errors.Errorf("record needs to match up with batch fields, got %d fields and %d record", len(b.header), len(rec.Values))
	}

	if _, ok := rec.ID.(uint64); !ok {
		return errors.New("TODO support non integer IDs")
	}
	b.ids = append(b.ids, rec.ID.(uint64))

	for i := 0; i < len(rec.Values); i++ {
		field := b.header[i]
		if val, ok := rec.Values[i].(string); ok {
			// translate val and append to b.rowIDs[i]
			if rowID, ok := b.client.translator.GetRow(b.index, field.Name(), val); ok {
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

type viewSpec struct {
	field string
	view  string
}

// type fragments map[viewSpec]*roaring.Bitmap

// map[shard][fieldview]fragmentData
type fragments map[uint64]map[viewSpec]*roaring.Bitmap

func (f fragments) GetOrCreate(shard uint64, field, view string) *roaring.Bitmap {
	viewMap, ok := f[shard]
	if !ok {
		viewMap = make(map[viewSpec]*roaring.Bitmap)
	}
	bm, ok := viewMap[viewSpec{field: field, view: view}]
	if !ok {
		bm = roaring.NewBTreeBitmap()
		viewMap[viewSpec{field: field, view: view}] = bm
	}
	f[shard] = viewMap
	return bm
}
