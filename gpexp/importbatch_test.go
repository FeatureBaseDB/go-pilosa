package gpexp

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

// TODO test against cluster

func TestImportBatchInts(t *testing.T) {
	client := pilosa.DefaultClient()
	schema := pilosa.NewSchema()
	idx := schema.Index("gopilosatest-blah")
	field := idx.Field("anint", pilosa.OptFieldTypeInt())
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 3, idx, []*pilosa.Field{field})
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	r := Row{Values: make([]interface{}, 1)}

	for i := uint64(0); i < 3; i++ {
		r.ID = i
		r.Values[0] = int64(i)
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("adding to batch: %v", err)
		}
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	r.ID = uint64(0)
	r.Values[0] = nil
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding after import: %v", err)
	}
	r.ID = uint64(1)
	r.Values[0] = int64(7)
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding second after import: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("second import: %v", err)
	}

	resp, err := client.Query(idx.BatchQuery(field.Equals(0), field.Equals(7), field.Equals(2)))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	for i, result := range resp.Results() {
		if !reflect.DeepEqual(result.Row().Columns, []uint64{uint64(i)}) {
			t.Errorf("expected %v for %d, but got %v", []uint64{uint64(i)}, i, result.Row().Columns)
		}
	}
}

func TestStringSlice(t *testing.T) {
	client := pilosa.DefaultClient()
	schema := pilosa.NewSchema()
	idx := schema.Index("test-string-slice")
	fields := make([]*pilosa.Field, 1)
	fields[0] = idx.Field("strslice", pilosa.OptFieldKeys(true), pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 100))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	trans := NewMapTranslator()
	err = trans.AddRows("test-string-slice", "strslice", []string{"c", "d", "f"}, []uint64{9, 10, 13})
	if err != nil {
		t.Fatalf("adding to translator: %v", err)
	}

	b, err := NewBatch(client, 3, idx, fields, OptTranslator(trans))
	if err != nil {
		t.Fatalf("creating new batch: %v", err)
	}

	r := Row{Values: make([]interface{}, len(fields))}
	r.ID = uint64(0)
	r.Values[0] = []string{"a"}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding to batch: %v", err)
	}
	if got := b.toTranslateSets["strslice"]["a"]; !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("expected []int{0}, got: %v", got)
	}

	r.ID = uint64(1)
	r.Values[0] = []string{"a", "b", "c"}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding to batch: %v", err)
	}
	if got := b.toTranslateSets["strslice"]["a"]; !reflect.DeepEqual(got, []int{0, 1}) {
		t.Fatalf("expected []int{0,1}, got: %v", got)
	}
	if got := b.toTranslateSets["strslice"]["b"]; !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("expected []int{1}, got: %v", got)
	}
	if got, ok := b.toTranslateSets["strslice"]["c"]; ok {
		t.Fatalf("should be nothing at c, got: %v", got)
	}
	if got := b.rowIDSets["strslice"][1]; !reflect.DeepEqual(got, []uint64{9}) {
		t.Fatalf("expected c to map to rowID 9 but got %v", got)
	}

	r.ID = uint64(2)
	r.Values[0] = []string{"d", "e", "f"}
	err = b.Add(r)
	if err != ErrBatchNowFull {
		t.Fatalf("adding to batch: %v", err)
	}
	if got, ok := b.toTranslateSets["strslice"]["d"]; ok {
		t.Fatalf("should be nothing at d, got: %v", got)
	}
	if got, ok := b.toTranslateSets["strslice"]["f"]; ok {
		t.Fatalf("should be nothing at f, got: %v", got)
	}
	if got := b.toTranslateSets["strslice"]["e"]; !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("expected []int{2}, got: %v", got)
	}
	if got := b.rowIDSets["strslice"][2]; !reflect.DeepEqual(got, []uint64{10, 13}) {
		t.Fatalf("expected c to map to rowID 9 but got %v", got)
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("translating: %v", err)
	}

	if got0 := b.rowIDSets["strslice"][0]; len(got0) != 1 {
		t.Errorf("after translation, rec 0, wrong len: %v", got0)
	} else if got1 := b.rowIDSets["strslice"][1]; len(got1) != 3 || got1[0] != 9 || (got1[1] != got0[0] && got1[2] != got0[0]) {
		t.Errorf("after translation, rec 1: %v, rec 0: %v", got1, got0)
	} else if got2 := b.rowIDSets["strslice"][2]; len(got2) != 3 || got2[0] != 10 || got2[1] != 13 || got2[2] == got1[2] || got2[2] == got0[0] {
		t.Errorf("after translation, rec 2: %v", got2)
	}

	err = b.doImport()
	if err != nil {
		t.Fatalf("doing import: %v", err)
	}

	resp, err := client.Query(idx.BatchQuery(fields[0].Row("a")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	result := resp.Result()
	if !reflect.DeepEqual(result.Row().Columns, []uint64{0, 1}) {
		t.Fatalf("expected a to be [0,1], got %v", result.Row().Columns)
	}

}

func TestBatches(t *testing.T) {
	client := pilosa.DefaultClient()
	schema := pilosa.NewSchema()
	idx := schema.Index("gopilosatest-blah")
	numFields := 5
	fields := make([]*pilosa.Field, numFields)
	fields[0] = idx.Field("zero", pilosa.OptFieldKeys(true))
	fields[1] = idx.Field("one", pilosa.OptFieldKeys(true))
	fields[2] = idx.Field("two", pilosa.OptFieldKeys(true))
	fields[3] = idx.Field("three", pilosa.OptFieldTypeInt())
	fields[4] = idx.Field("four", pilosa.OptFieldTypeTime(pilosa.TimeQuantumYearMonthDay))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()
	b, err := NewBatch(client, 10, idx, fields)
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}
	r := Row{Values: make([]interface{}, numFields), Clears: make(map[int]interface{})}
	r.Time.Set(time.Date(2019, time.January, 2, 15, 45, 0, 0, time.UTC))

	for i := 0; i < 9; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "a"
			r.Values[1] = "b"
			r.Values[2] = "c"
			r.Values[3] = int64(99)
			r.Values[4] = uint64(1)
			r.Time.SetMonth("01")
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
			r.Values[3] = int64(-10)
			r.Values[4] = uint64(1)
			r.Time.SetMonth("02")
		}
		if i == 8 {
			r.Values[0] = nil
			r.Clears[1] = uint64(97)
			r.Clears[2] = "c"
			r.Values[3] = nil
			r.Values[4] = nil
		}
		err := b.Add(r)
		if err != nil {
			t.Fatalf("unexpected err adding record: %v", err)
		}

	}

	if len(b.toTranslate[0]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[0]")
	}
	for k, ints := range b.toTranslate[0] {
		if k == "a" {
			if !reflect.DeepEqual(ints, []int{0, 2, 4, 6}) {
				t.Fatalf("wrong ints for key a in field zero: %v", ints)
			}
		} else if k == "x" {
			if !reflect.DeepEqual(ints, []int{1, 3, 5, 7}) {
				t.Fatalf("wrong ints for key x in field zero: %v", ints)
			}

		} else {
			t.Fatalf("unexpected key %s", k)
		}
	}
	if !reflect.DeepEqual(b.toTranslateClear, map[int]map[string][]int{2: map[string][]int{"c": []int{8}}}) {
		t.Errorf("unexpected toTranslateClear: %+v", b.toTranslateClear)
	}
	if !reflect.DeepEqual(b.clearRowIDs, map[int]map[int]uint64{1: map[int]uint64{8: 97}, 2: map[int]uint64{}}) {
		t.Errorf("unexpected clearRowIDs: %+v", b.clearRowIDs)
	}

	if !reflect.DeepEqual(b.values["three"], []int64{99, -10, 99, -10, 99, -10, 99, -10, 0}) {
		t.Fatalf("unexpected values: %v", b.values["three"])
	}
	if !reflect.DeepEqual(b.nullIndices["three"], []uint64{8}) {
		t.Fatalf("unexpected nullIndices: %v", b.nullIndices["three"])
	}

	if len(b.toTranslate[1]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[1]")
	}
	for k, ints := range b.toTranslate[1] {
		if k == "b" {
			if !reflect.DeepEqual(ints, []int{0, 2, 4, 6, 8}) {
				t.Fatalf("wrong ints for key b in field one: %v", ints)
			}
		} else if k == "y" {
			if !reflect.DeepEqual(ints, []int{1, 3, 5, 7}) {
				t.Fatalf("wrong ints for key y in field one: %v", ints)
			}

		} else {
			t.Fatalf("unexpected key %s", k)
		}
	}

	if len(b.toTranslate[2]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[2]")
	}
	for k, ints := range b.toTranslate[2] {
		if k == "c" {
			if !reflect.DeepEqual(ints, []int{0, 2, 4, 6, 8}) {
				t.Fatalf("wrong ints for key c in field two: %v", ints)
			}
		} else if k == "z" {
			if !reflect.DeepEqual(ints, []int{1, 3, 5, 7}) {
				t.Fatalf("wrong ints for key z in field two: %v", ints)
			}

		} else {
			t.Fatalf("unexpected key %s", k)
		}
	}

	err = b.Add(r)
	if err != ErrBatchNowFull {
		t.Fatalf("should have gotten full batch error, but got %v", err)
	}

	err = b.Add(r)
	if err != ErrBatchAlreadyFull {
		t.Fatalf("should have gotten already full batch error, but got %v", err)
	}

	if !reflect.DeepEqual(b.values["three"], []int64{99, -10, 99, -10, 99, -10, 99, -10, 0, 0}) {
		t.Fatalf("unexpected values: %v", b.values["three"])
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("doing translation: %v", err)
	}

	for fidx, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if fidx == 0 {
			if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, nilSentinel, nilSentinel}) &&
				!reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, nilSentinel, nilSentinel}) {
				t.Fatalf("unexpected row ids for field %d: %v", fidx, rowIDs)
			}

		} else if fidx == 4 {
			if !reflect.DeepEqual(rowIDs, []uint64{1, 1, 1, 1, 1, 1, 1, 1, nilSentinel, nilSentinel}) {
				t.Fatalf("unexpected rowids for time field")
			}
		} else if fidx == 3 {
			if len(rowIDs) != 0 {
				t.Fatalf("expected no rowIDs for int field, but got: %v", rowIDs)
			}
		} else {
			if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 1}) && !reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, 2, 2}) {
				t.Fatalf("unexpected row ids for field %d: %v", fidx, rowIDs)
			}
		}
	}

	if !reflect.DeepEqual(b.clearRowIDs[1], map[int]uint64{8: 97}) {
		t.Errorf("unexpected clearRowIDs after translation: %+v", b.clearRowIDs[1])
	}
	if !reflect.DeepEqual(b.clearRowIDs[2], map[int]uint64{8: 2}) && !reflect.DeepEqual(b.clearRowIDs[2], map[int]uint64{8: 1}) {
		t.Errorf("unexpected clearRowIDs: after translation%+v", b.clearRowIDs[2])
	}

	err = b.doImport()
	if err != nil {
		t.Fatalf("doing import: %v", err)
	}

	b.reset()

	for i := 9; i < 19; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "a"
			r.Values[1] = "b"
			r.Values[2] = "c"
			r.Values[3] = int64(99)
			r.Values[4] = uint64(1)
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
			r.Values[3] = int64(-10)
			r.Values[4] = uint64(2)
		}
		err := b.Add(r)
		if i != 18 && err != nil {
			t.Fatalf("unexpected err adding record: %v", err)
		}
		if i == 18 && err != ErrBatchNowFull {
			t.Fatalf("unexpected err: %v", err)
		}
	}

	// should do nothing
	err = b.doTranslation()
	if err != nil {
		t.Fatalf("doing translation: %v", err)
	}

	err = b.doImport()
	if err != nil {
		t.Fatalf("doing import: %v", err)
	}

	for fidx, rowIDs := range b.rowIDs {
		if fidx == 3 {
			if len(rowIDs) != 0 {
				t.Fatalf("expected no rowIDs for int field, but got: %v", rowIDs)
			}
			continue
		}
		// we don't know which key will get translated first, but we do know the pattern
		if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}) && !reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, 2, 1}) {
			t.Fatalf("unexpected row ids for field %d: %v", fidx, rowIDs)
		}
	}

	b.reset()

	for i := 19; i < 29; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "d"
			r.Values[1] = "e"
			r.Values[2] = "f"
			r.Values[3] = int64(100)
			r.Values[4] = uint64(3)
		} else {
			r.Values[0] = "u"
			r.Values[1] = "v"
			r.Values[2] = "w"
			r.Values[3] = int64(0)
			r.Values[4] = uint64(4)
		}
		err := b.Add(r)
		if i != 28 && err != nil {
			t.Fatalf("unexpected err adding record: %v", err)
		}
		if i == 28 && err != ErrBatchNowFull {
			t.Fatalf("unexpected err: %v", err)
		}
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("doing translation: %v", err)
	}

	err = b.doImport()
	if err != nil {
		t.Fatalf("doing import: %v", err)
	}

	for fidx, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if fidx == 3 {
			if len(rowIDs) != 0 {
				t.Fatalf("expected no rowIDs for int field, but got: %v", rowIDs)
			}
			continue
		}
		if !reflect.DeepEqual(rowIDs, []uint64{3, 4, 3, 4, 3, 4, 3, 4, 3, 4}) && !reflect.DeepEqual(rowIDs, []uint64{4, 3, 4, 3, 4, 3, 4, 3, 4, 3}) {
			t.Fatalf("unexpected row ids for field %d: %v", fidx, rowIDs)
		}
	}

	frags, _, err := b.makeFragments()
	if err != nil {
		t.Fatalf("making fragments: %v", err)
	}

	if len(frags) != 1 {
		t.Fatalf("unexpected # of shards in fragments: %d", len(frags))
	}
	viewMap, ok := frags[0]
	if !ok {
		t.Fatalf("shard 0 should be in frags")
	}
	if len(viewMap) != 4 {
		t.Fatalf("there should be 4 views")
	}

	resp, err := client.Query(idx.BatchQuery(fields[0].Row("a"),
		fields[1].Row("b"),
		fields[2].Row("c"),
		fields[3].Equals(99)))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results := resp.Results()
	for _, j := range []int{0, 3} {
		cols := results[j].Row().Columns
		if !reflect.DeepEqual(cols, []uint64{0, 2, 4, 6, 10, 12, 14, 16, 18}) {
			t.Fatalf("unexpected columns for a: %v", cols)
		}
	}
	for i, res := range results[1:3] {
		cols := res.Row().Columns
		if !reflect.DeepEqual(cols, []uint64{0, 2, 4, 6, 8, 10, 12, 14, 16, 18}) {
			t.Fatalf("unexpected columns at %d: %v", i, cols)
		}
	}

	resp, err = client.Query(idx.BatchQuery(fields[0].Row("d"),
		fields[1].Row("e"),
		fields[2].Row("f")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results = resp.Results()
	for _, res := range results {
		cols := res.Row().Columns
		if !reflect.DeepEqual(cols, []uint64{20, 22, 24, 26, 28}) {
			t.Fatalf("unexpected columns: %v", cols)
		}
	}

	resp, err = client.Query(idx.BatchQuery(fields[3].GT(-11),
		fields[3].Equals(0),
		fields[3].Equals(100),
		fields[4].Range(1, time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, time.January, 29, 0, 0, 0, 0, time.UTC)),
		fields[4].Range(1, time.Date(2019, time.February, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, time.February, 29, 0, 0, 0, 0, time.UTC))))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	results = resp.Results()
	cols := results[0].Row().Columns
	if !reflect.DeepEqual(cols, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28}) {
		t.Fatalf("all columns (but 8) should be greater than -11, but got: %v", cols)
	}
	cols = results[1].Row().Columns
	if !reflect.DeepEqual(cols, []uint64{19, 21, 23, 25, 27}) {
		t.Fatalf("wrong cols for ==0: %v", cols)
	}
	cols = results[2].Row().Columns
	if !reflect.DeepEqual(cols, []uint64{20, 22, 24, 26, 28}) {
		t.Fatalf("wrong cols for ==100: %v", cols)
	}
	cols = results[3].Row().Columns
	exp := []uint64{0, 2, 4, 6, 10, 12, 14, 16, 18}
	if !reflect.DeepEqual(cols, exp) {
		t.Fatalf("wrong cols for January: got/want\n%v\n%v", cols, exp)
	}
	cols = results[4].Row().Columns
	exp = []uint64{1, 3, 5, 7}
	if !reflect.DeepEqual(cols, exp) {
		t.Fatalf("wrong cols for January: got/want\n%v\n%v", cols, exp)
	}

	b.reset()
	r.ID = uint64(0)
	r.Values[0] = "x"
	r.Values[1] = "b"
	r.Clears[0] = "a"
	r.Clears[1] = "b" // b should get cleared
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding with clears: %v", err)
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing w/clears: %v", err)
	}
	resp, err = client.Query(idx.BatchQuery(
		fields[0].Row("a"),
		fields[0].Row("x"),
		fields[1].Row("b"),
	))
	if err != nil {
		t.Fatalf("querying after clears: %v", err)
	}
	if arow := resp.Results()[0].Row().Columns; arow[0] == 0 {
		t.Errorf("shouldn't have id 0 in row a after clearing! %v", arow)
	}
	if xrow := resp.Results()[1].Row().Columns; xrow[0] != 0 {
		t.Errorf("should have id 0 in row x after setting %v", xrow)
	}
	if brow := resp.Results()[2].Row().Columns; brow[0] == 0 {
		t.Errorf("shouldn't have id 0 in row b after clearing! %v", brow)
	}

	// TODO test importing across multiple shards
}

func TestBatchesStringIDs(t *testing.T) {
	client := pilosa.DefaultClient()
	schema := pilosa.NewSchema()
	idx := schema.Index("gopilosatest-blah", pilosa.OptIndexKeys(true))
	fields := make([]*pilosa.Field, 1)
	fields[0] = idx.Field("zero", pilosa.OptFieldKeys(true))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	b, err := NewBatch(client, 3, idx, fields)
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}

	r := Row{Values: make([]interface{}, 1)}

	for i := 0; i < 3; i++ {
		r.ID = strconv.Itoa(i)
		if i%2 == 0 {
			r.Values[0] = "a"
		} else {
			r.Values[0] = "x"
		}
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("unexpected err adding record: %v", err)
		}
	}

	if len(b.toTranslateID) != 3 {
		t.Fatalf("id translation table unexpected size: %v", b.toTranslateID)
	}
	for k, indexes := range b.toTranslateID {
		if k == "0" {
			if !reflect.DeepEqual(indexes, []int{0}) {
				t.Fatalf("unexpected result k: %s, indexes: %v", k, indexes)
			}
		}
		if k == "1" {
			if !reflect.DeepEqual(indexes, []int{1}) {
				t.Fatalf("unexpected result k: %s, indexes: %v", k, indexes)
			}
		}
		if k == "2" {
			if !reflect.DeepEqual(indexes, []int{2}) {
				t.Fatalf("unexpected result k: %s, indexes: %v", k, indexes)
			}
		}
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("translating: %v", err)
	}

	if err := isPermutationOfInt(b.ids, []uint64{1, 2, 3}); err != nil {
		t.Fatalf("wrong ids: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp, err := client.Query(idx.BatchQuery(fields[0].Row("a"), fields[0].Row("x")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results := resp.Results()
	for i, res := range results {
		cols := res.Row().Keys
		if i == 0 && !reflect.DeepEqual(cols, []string{"0", "2"}) && !reflect.DeepEqual(cols, []string{"2", "0"}) {
			t.Fatalf("unexpected columns: %v", cols)
		}
		if i == 1 && !reflect.DeepEqual(cols, []string{"1"}) {
			t.Fatalf("unexpected columns: %v", cols)
		}
	}

	b.reset()

	r.ID = "1"
	r.Values[0] = "a"
	err = b.Add(r)
	if err != nil {
		t.Fatalf("unexpected err adding record: %v", err)
	}

	r.ID = "3"
	r.Values[0] = "z"
	err = b.Add(r)
	if err != nil {
		t.Fatalf("unexpected err adding record: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp, err = client.Query(idx.BatchQuery(fields[0].Row("a"), fields[0].Row("z")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results = resp.Results()
	for i, res := range results {
		cols := res.Row().Keys
		if err := isPermutationOf(cols, []string{"0", "1", "2"}); i == 0 && err != nil {
			t.Fatalf("unexpected columns: %v: %v", cols, err)
		}
		if i == 1 && !reflect.DeepEqual(cols, []string{"3"}) {
			t.Fatalf("unexpected columns: %v", cols)
		}
	}

}

func isPermutationOf(one, two []string) error {
	if len(one) != len(two) {
		return errors.Errorf("different lengths %d and %d", len(one), len(two))
	}
outer:
	for _, vOne := range one {
		for j, vTwo := range two {
			if vOne == vTwo {
				two = append(two[:j], two[j+1:]...)
				continue outer
			}
		}
		return errors.Errorf("%s in one but not two", vOne)
	}
	if len(two) != 0 {
		return errors.Errorf("vals in two but not one: %v", two)
	}
	return nil
}

func isPermutationOfInt(one, two []uint64) error {
	if len(one) != len(two) {
		return errors.Errorf("different lengths %d and %d", len(one), len(two))
	}
outer:
	for _, vOne := range one {
		for j, vTwo := range two {
			if vOne == vTwo {
				two = append(two[:j], two[j+1:]...)
				continue outer
			}
		}
		return errors.Errorf("%d in one but not two", vOne)
	}
	if len(two) != 0 {
		return errors.Errorf("vals in two but not one: %v", two)
	}
	return nil
}

func TestQuantizedTime(t *testing.T) {
	cases := []struct {
		name    string
		time    time.Time
		year    string
		month   string
		day     string
		hour    string
		quantum pilosa.TimeQuantum
		reset   bool
		exp     []string
		expErr  string
	}{
		{
			name:   "no time quantum",
			expErr: "",
		},
		{
			name:   "no time quantum with data",
			year:   "2017",
			exp:    []string{},
			expErr: "",
		},
		{
			name:    "no data",
			quantum: pilosa.TimeQuantumYear,
			exp:     nil,
			expErr:  "",
		},
		{
			name:    "timestamp",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			quantum: "YMDH",
			exp:     []string{"2013", "201310", "20131016", "2013101617"},
		},
		{
			name:    "timestamp-less-granular",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			quantum: "YM",
			exp:     []string{"2013", "201310"},
		},
		{
			name:    "timestamp-mid-granular",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			quantum: "MD",
			exp:     []string{"201310", "20131016"},
		},
		{
			name:    "justyear",
			year:    "2013",
			quantum: "Y",
			exp:     []string{"2013"},
		},
		{
			name:    "justyear-wantmonth",
			year:    "2013",
			quantum: "YM",
			expErr:  "no data set for month",
		},
		{
			name:    "timestamp-changeyear",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			year:    "2019",
			quantum: "YMDH",
			exp:     []string{"2019", "201910", "20191016", "2019101617"},
		},
		{
			name:    "yearmonthdayhour",
			year:    "2013",
			month:   "10",
			day:     "16",
			hour:    "17",
			quantum: "YMDH",
			exp:     []string{"2013", "201310", "20131016", "2013101617"},
		},
		{
			name:    "timestamp-changehour",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			hour:    "05",
			quantum: "MDH",
			exp:     []string{"201310", "20131016", "2013101605"},
		},
		{
			name:    "timestamp",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			quantum: "YMDH",
			reset:   true,
			exp:     nil,
		},
	}

	for i, test := range cases {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			tq := QuantizedTime{}
			var zt time.Time
			if zt != test.time {
				tq.Set(test.time)
			}
			if test.year != "" {
				tq.SetYear(test.year)
			}
			if test.month != "" {
				tq.SetMonth(test.month)
			}
			if test.day != "" {
				tq.SetDay(test.day)
			}
			if test.hour != "" {
				tq.SetHour(test.hour)
			}
			if test.reset {
				tq.Reset()
			}

			views, err := tq.views(test.quantum)
			if !reflect.DeepEqual(views, test.exp) {
				t.Errorf("unexpected views, got/want:\n%v\n%v\n", views, test.exp)
			}
			if (err != nil && err.Error() != test.expErr) || (err == nil && test.expErr != "") {
				t.Errorf("unexpected error, got/want:\n%v\n%s\n", err, test.expErr)
			}
		})
	}

}
