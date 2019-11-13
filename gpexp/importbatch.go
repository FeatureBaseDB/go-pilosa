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
// | float64| int               | scale     | TODO
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

	// rowIDs is a map of field index (in the header) to slices of
	// length batchSize which contain row IDs.
	rowIDs map[int][]uint64
	// clearRowIDs is a map[fieldIndex][idsIndex]rowID we don't expect
	// clears to happen very often, so we store the idIndex/value
	// mapping in a map rather than a slice as we do for rowIDs. This
	// is a potentially temporary workaround to allow packed boolean
	// fields to clear "false" values. Packed fields may be more
	// completely supported by Pilosa in future.
	clearRowIDs map[int]map[int]uint64

	// rowIDSets is a map from field name to a batchSize slice of
	// slices of row IDs. When a given record can have more than one
	// value for a field, rowIDSets stores that information.
	rowIDSets map[string][][]uint64

	// values holds the values for each record of an int field
	values map[string][]int64

	// times holds a time for each record. (if any of the fields are time fields)
	times []QuantizedTime

	// nullIndices holds a slice of indices into b.ids for each
	// integer field which has nil values.
	nullIndices map[string][]uint64

	// TODO support bool fields.

	// for each field, keep a map of key to which record indexes that key mapped to
	toTranslate      map[int]map[string][]int
	toTranslateClear map[int]map[string][]int

	// toTranslateSets is a map from field name to a map of string
	// keys that need to be translated to sets of record indexes which
	// those keys map to.
	toTranslateSets map[string]map[string][]int

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
	rowIDs := make(map[int][]uint64, len(fields))
	values := make(map[string][]int64)
	tt := make(map[int]map[string][]int, len(fields))
	ttSets := make(map[string]map[string][]int)
	hasTime := false
	for i, field := range fields {
		headerMap[field.Name()] = field
		opts := field.Opts()
		switch typ := opts.Type(); typ {
		case pilosa.FieldTypeDefault, pilosa.FieldTypeSet, pilosa.FieldTypeTime:
			if opts.Keys() {
				tt[i] = make(map[string][]int)
				ttSets[field.Name()] = make(map[string][]int)
			}
			rowIDs[i] = make([]uint64, 0, size) // TODO make this on-demand when it gets used. could be a string array field.
			hasTime = typ == pilosa.FieldTypeTime || hasTime
		case pilosa.FieldTypeInt, pilosa.FieldTypeDecimal:
			values[field.Name()] = make([]int64, 0, size)
		case pilosa.FieldTypeMutex:
			// similar to set/time fields, but no need to support sets
			// of values (hence no ttSets)
			if opts.Keys() {
				tt[i] = make(map[string][]int)
			}
			rowIDs[i] = make([]uint64, 0, size)
		default:
			return nil, errors.Errorf("field type '%s' is not currently supported through Batch", typ)
		}
	}
	b := &Batch{
		client:           client,
		header:           fields,
		headerMap:        headerMap,
		index:            index,
		ids:              make([]uint64, 0, size),
		rowIDs:           rowIDs,
		clearRowIDs:      make(map[int]map[int]uint64),
		rowIDSets:        make(map[string][][]uint64),
		values:           values,
		nullIndices:      make(map[string][]uint64),
		toTranslate:      tt,
		toTranslateClear: make(map[int]map[string][]int),
		toTranslateSets:  ttSets,
		toTranslateID:    make(map[string][]int),
		transCache:       NewMapTranslator(),
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
	Clears map[int]interface{}
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
// also in-order within a shard this will likely help as well. Add
// clears rec.Clears when it returns normally (either a nil error or
// BatchNowFull), it does not clear rec.Values although... TODO.
func (b *Batch) Add(rec Row) error {
	if len(b.ids) == cap(b.ids) {
		return ErrBatchAlreadyFull
	}
	if len(rec.Values) != len(b.header) {
		return errors.Errorf("record needs to match up with batch fields, got %d fields and %d record", len(b.header), len(rec.Values))
	}

	handleStringID := func(rid string) error {
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
		return nil
	}
	var err error

	switch rid := rec.ID.(type) {
	case uint64:
		b.ids = append(b.ids, rid)
	case string:
		err := handleStringID(rid)
		if err != nil {
			return err
		}
	case []byte:
		err = handleStringID(string(rid))
		if err != nil {
			return err
		}
	default: // TODO support nil ID as being auto-allocated.
		return errors.Errorf("unsupported id type %T value %v", rid, rid)
	}

	// curPos is the current position in b.ids, rowIDs[*], etc.
	curPos := len(b.ids) - 1

	if b.times != nil {
		b.times = append(b.times, rec.Time)
	}

	for i := 0; i < len(rec.Values); i++ {
		field := b.header[i]
		switch val := rec.Values[i].(type) {
		case string:
			rowIDs := b.rowIDs[i]
			// translate val and append to b.rowIDs[i]
			if rowID, ok, err := b.transCache.GetRow(b.index.Name(), field.Name(), val); err != nil {
				return errors.Wrap(err, "translating row")
			} else if ok {
				b.rowIDs[i] = append(rowIDs, rowID)
			} else {
				ints, ok := b.toTranslate[i][val]
				if !ok {
					ints = make([]int, 0)
				}
				ints = append(ints, curPos)
				b.toTranslate[i][val] = ints
				b.rowIDs[i] = append(rowIDs, 0)
			}
		case uint64:
			b.rowIDs[i] = append(b.rowIDs[i], val)
		case int64:
			b.values[field.Name()] = append(b.values[field.Name()], val)
		case []string:
			rowIDSets, ok := b.rowIDSets[field.Name()]
			if !ok {
				rowIDSets = make([][]uint64, len(b.ids)-1, cap(b.ids))
			} else {
				rowIDSets = rowIDSets[:len(b.ids)-1] // grow this field's rowIDSets if necessary
			}

			rowIDs := make([]uint64, 0, len(val))
			for _, k := range val {
				if rowID, ok, err := b.transCache.GetRow(b.index.Name(), field.Name(), k); err != nil {
					return errors.Wrap(err, "translating row from []string")
				} else if ok {
					rowIDs = append(rowIDs, rowID)
				} else {
					ints, ok := b.toTranslateSets[field.Name()][k]
					if !ok {
						ints = make([]int, 0, 1)
					}
					ints = append(ints, curPos)
					b.toTranslateSets[field.Name()][k] = ints
				}
			}
			b.rowIDSets[field.Name()] = append(rowIDSets, rowIDs)
		case nil:
			if field.Opts().Type() == pilosa.FieldTypeInt || field.Opts().Type() == pilosa.FieldTypeDecimal {
				b.values[field.Name()] = append(b.values[field.Name()], 0)
				nullIndices, ok := b.nullIndices[field.Name()]
				if !ok {
					nullIndices = make([]uint64, 0)
				}
				nullIndices = append(nullIndices, uint64(curPos))
				b.nullIndices[field.Name()] = nullIndices

			} else {
				b.rowIDs[i] = append(b.rowIDs[i], nilSentinel)
			}
		default:
			return errors.Errorf("Val %v Type %[1]T is not currently supported. Use string, uint64 (row id), or int64 (integer value)", val)
		}
	}

	for i, uval := range rec.Clears {
		field := b.header[i]
		if _, ok := b.clearRowIDs[i]; !ok {
			b.clearRowIDs[i] = make(map[int]uint64)
		}
		switch val := uval.(type) {
		case string:
			clearRows := b.clearRowIDs[i]
			// translate val and add to clearRows
			if rowID, ok, err := b.transCache.GetRow(b.index.Name(), field.Name(), val); err != nil {
				return errors.Wrap(err, "translating row")
			} else if ok {
				clearRows[curPos] = rowID
			} else {
				_, ok := b.toTranslateClear[i]
				if !ok {
					b.toTranslateClear[i] = make(map[string][]int)
				}
				ints, ok := b.toTranslateClear[i][val]
				if !ok {
					ints = make([]int, 0)
				}
				ints = append(ints, curPos)
				b.toTranslateClear[i][val] = ints
			}
		case uint64:
			b.clearRowIDs[i][curPos] = val
		default:
			return errors.Errorf("Clearing a value '%v' Type %[1]T is not currently supported (field '%s')", val, field.Name())
		}
	}

	if len(b.ids) == cap(b.ids) {
		return ErrBatchNowFull
	}
	for k := range rec.Clears {
		delete(rec.Clears, k)
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
	for i, tt := range b.toTranslate {
		fieldName := b.header[i].Name()
		keys = keys[:0]

		// make a slice of keys
		for k := range tt {
			keys = append(keys, k)
		}
		// append keys to clear so we can translate them all in one
		// request. ttEnd is the index where clearing starts which we
		// use later on.
		ttEnd := len(keys)
		ttc := b.toTranslateClear[i]
		for k := range ttc {
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
		rows := b.rowIDs[i]
		for j := 0; j < ttEnd; j++ {
			key := keys[j]
			id := ids[j]
			for _, recordIdx := range tt[key] {
				rows[recordIdx] = id
			}
		}
		// fill out missing IDs in clear lists.
		clearRows := b.clearRowIDs[i]
		for j := ttEnd; j < len(keys); j++ {
			key := keys[j]
			id := ids[j]
			for _, recordIdx := range ttc[key] {
				clearRows[recordIdx] = id
			}
		}
	}

	for fieldName, tt := range b.toTranslateSets {
		keys = keys[:0]

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
		rowIDSets := b.rowIDSets[fieldName]
		for j, key := range keys {
			rowID := ids[j]
			for _, recordIdx := range tt[key] {
				rowIDSets[recordIdx] = append(rowIDSets[recordIdx], rowID)
			}
		}
	}

	return nil
}

func (b *Batch) doImport() error {
	eg := errgroup.Group{}

	frags, clearFrags, err := b.makeFragments()
	if err != nil {
		return errors.Wrap(err, "making fragments")
	}

	for shard, fieldMap := range frags {
		for field, viewMap := range fieldMap {
			field := field
			viewMap := viewMap
			eg.Go(func() error {
				clearViewMap := clearFrags.GetViewMap(shard, field)
				if len(clearViewMap) > 0 {
					err := b.client.ImportRoaringBitmap(b.index.Field(field), shard, clearViewMap, true)
					if err != nil {
						return errors.Wrapf(err, "import clearing clearing data for %s", field)
					}
				}
				err := b.client.ImportRoaringBitmap(b.index.Field(field), shard, viewMap, false)
				return errors.Wrapf(err, "importing data for %s", field)
			})
		}
	}
	eg.Go(func() error {
		return b.importValueData()
	})
	eg.Go(func() error {
		return b.importMutexData()
	})
	return eg.Wait()
}

// this is kind of bad as it means we can never import column id
// ^uint64(0) which is a valid column ID. I think it's unlikely to
// matter much in practice (we could maybe special case it somewhere
// if needed though).
var nilSentinel = ^uint64(0)

func (b *Batch) makeFragments() (frags fragments, clearFrags fragments, err error) {
	shardWidth := b.index.ShardWidth()
	if shardWidth == 0 {
		shardWidth = pilosa.DefaultShardWidth
	}
	frags = make(fragments)
	clearFrags = make(fragments)
	emptyClearRows := make(map[int]uint64)
	for i, rowIDs := range b.rowIDs {
		if len(rowIDs) == 0 {
			continue // this can happen when the values that came in for this field were string slices
		}
		clearRows := b.clearRowIDs[i]
		if clearRows == nil {
			clearRows = emptyClearRows
		}
		field := b.header[i]
		opts := field.Opts()
		if opts.Type() == pilosa.FieldTypeMutex {
			continue // we handle mutex fields separately — they can't use importRoaring
		}
		curShard := ^uint64(0) // impossible sentinel value for shard.
		var curBM *roaring.Bitmap
		var clearBM *roaring.Bitmap
		for j := range b.ids {
			col, row := b.ids[j], rowIDs[j]
			if col/shardWidth != curShard {
				curShard = col / shardWidth
				curBM = frags.GetOrCreate(curShard, field.Name(), "")
				clearBM = clearFrags.GetOrCreate(curShard, field.Name(), "")
			}
			if row != nilSentinel {
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
						return nil, nil, errors.Wrap(err, "calculating views")
					}
					for _, view := range views {
						tbm := frags.GetOrCreate(curShard, field.Name(), view)
						tbm.DirectAdd(row*shardWidth + (col % shardWidth))
					}
				}
			}

			clearRow, ok := clearRows[j]
			if ok {
				clearBM.DirectAddN(clearRow*shardWidth + (col % shardWidth))
				// we're going to execute the clear before the set, so
				// we want to make sure that at this point, the "set"
				// fragments don't contian the bit that we're clearing
				curBM.DirectRemoveN(clearRow*shardWidth + (col % shardWidth))
			}
		}
	}

	for fname, rowIDSets := range b.rowIDSets {
		if len(rowIDSets) == 0 {
			continue
		}
		field := b.headerMap[fname]
		opts := field.Opts()
		curShard := ^uint64(0) // impossible sentinel value for shard.
		var curBM *roaring.Bitmap
		for j := range b.ids {
			col, rowIDs := b.ids[j], rowIDSets[j]
			if len(rowIDs) == 0 {
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
				for _, row := range rowIDs {
					curBM.DirectAdd(row*shardWidth + (col % shardWidth))
				}
			}
			if opts.Type() == pilosa.FieldTypeTime {
				views, err := b.times[j].views(opts.TimeQuantum())
				if err != nil {
					return nil, nil, errors.Wrap(err, "calculating views")
				}
				for _, view := range views {
					tbm := frags.GetOrCreate(curShard, fname, view)
					for _, row := range rowIDs {
						tbm.DirectAdd(row*shardWidth + (col % shardWidth))
					}
				}
			}
		}
	}
	return frags, clearFrags, nil
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
		// TODO(jaffee) I think this may be very inefficient. It looks
		// like we're copying the `ids` and `values` slices over
		// themselves (an O(n) operation) for each nullIndex so this
		// is effectively O(n^2). What we could do is iterate through
		// ids and values each once, while simultaneously iterating
		// through nullindices and keeping track of how many
		// nullIndices we've passed, and so how far back we need to
		// copy each item.
		//
		// It was a couple weeks ago that I wrote this code, and I
		// vaguely remember thinking about this, so I may just be
		// missing something now. We should benchmark on what should
		// be a bad case (an int field which is mostly null), and see
		// if the improved implementation helps a lot.
		for i, nullIndex := range nullIndices {
			nullIndex -= uint64(i) // offset the index by the number of items removed so far
			ids = append(ids[:nullIndex], ids[nullIndex+1:]...)
			values = append(values[:nullIndex], values[nullIndex+1:]...)
		}

		// now do imports by shard
		if len(ids) == 0 {
			continue // TODO test this "all nil" case
		}
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

// TODO this should work for bools as well - just need to support them
// at batch creation time and when calling Add, I think.
func (b *Batch) importMutexData() error {
	shardWidth := b.index.ShardWidth()
	if shardWidth == 0 {
		shardWidth = pilosa.DefaultShardWidth
	}
	eg := errgroup.Group{}
	ids := make([]uint64, 0, len(b.ids))

	for findex, rowIDs := range b.rowIDs {
		field := b.header[findex]
		if field.Opts().Type() != pilosa.FieldTypeMutex {
			continue
		}
		ids = ids[:0]

		// get slice of column ids for non-nil rowIDs and cut nil row
		// IDs out of rowIDs.
		idsIndex := 0
		for i, id := range b.ids {
			rowID := rowIDs[i]
			if rowID == nilSentinel {
				continue
			}
			rowIDs[idsIndex] = rowID
			ids = append(ids, id)
			idsIndex++
		}
		rowIDs = rowIDs[:idsIndex]

		if len(ids) == 0 {
			continue
		}
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
				path, data, err := b.client.EncodeImport(b.index.Name(), field.Name(), shard, rowIDs[startIdx:endIdx], ids[startIdx:endIdx], false)
				if err != nil {
					return errors.Wrap(err, "encoding mutex import")
				}
				eg.Go(func() error {
					err := b.client.DoImport(b.index.Name(), shard, path, data)
					return errors.Wrapf(err, "importing values for %s", field)
				})
				startIdx = i
				curShard = recordID / shardWidth
			}
		}
	}
	err := eg.Wait()
	return errors.Wrap(err, "importing mutex data")
}

// reset is called at the end of importing to ready the batch for the
// next round. Where possible it does not re-allocate memory.
func (b *Batch) reset() {
	b.ids = b.ids[:0]
	b.times = b.times[:0]
	for i, rowIDs := range b.rowIDs {
		fieldName := b.header[i].Name()
		b.rowIDs[i] = rowIDs[:0]
		rowIDSet := b.rowIDSets[fieldName]
		b.rowIDSets[fieldName] = rowIDSet[:0]
		m := b.toTranslate[i]
		for k := range m {
			delete(m, k) // TODO pool these slices
		}
		m = b.toTranslateSets[fieldName]
		for k := range m {
			delete(m, k)
		}
	}
	for _, rowIDs := range b.clearRowIDs {
		for k := range rowIDs {
			delete(rowIDs, k)
		}
	}
	for _, clearMap := range b.toTranslateClear {
		for k := range clearMap {
			delete(clearMap, k)
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

func (f fragments) GetViewMap(shard uint64, field string) map[string]*roaring.Bitmap {
	fieldMap, ok := f[shard]
	if !ok {
		return nil
	}
	viewMap, ok := fieldMap[field]
	if !ok {
		return nil
	}
	return viewMap
}
