package pilosa

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/pkg/errors"
)

// TODO test against cluster

func TestBatches(t *testing.T) {
	client := DefaultClient()
	schema := NewSchema()
	idx := schema.Index("gopilosatest-blah")
	fields := make([]*Field, 4)
	fields[0] = idx.Field("zero", OptFieldKeys(true))
	fields[1] = idx.Field("one", OptFieldKeys(true))
	fields[2] = idx.Field("two", OptFieldKeys(true))
	fields[3] = idx.Field("three", OptFieldTypeInt())
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
	b := NewBatch(client, 10, idx, fields)
	r := Row{Values: make([]interface{}, 4)}

	for i := 0; i < 9; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "a"
			r.Values[1] = "b"
			r.Values[2] = "c"
			r.Values[3] = int64(99)
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
			r.Values[3] = int64(-10)
		}
		if i == 8 {
			r.Values[0] = nil
			r.Values[3] = nil
		}
		err := b.Add(r)
		if err != nil {
			t.Fatalf("unexpected err adding record: %v", err)
		}

	}

	if len(b.toTranslate["zero"]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[0]")
	}
	for k, ints := range b.toTranslate["zero"] {
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

	if !reflect.DeepEqual(b.values["three"], []int64{99, -10, 99, -10, 99, -10, 99, -10, 0}) {
		t.Fatalf("unexpected values: %v", b.values["three"])
	}
	if !reflect.DeepEqual(b.clearValues["three"], []uint64{8}) {
		t.Fatalf("unexpected clearValues: %v", b.clearValues["three"])
	}

	if len(b.toTranslate["one"]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[\"one\"]")
	}
	for k, ints := range b.toTranslate["one"] {
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

	if len(b.toTranslate["two"]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[2]")
	}
	for k, ints := range b.toTranslate["two"] {
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

	for fname, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if fname == "zero" {
			if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, nilSentinel, nilSentinel}) &&
				!reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, nilSentinel, nilSentinel}) {
				t.Fatalf("unexpected row ids for field %s: %v", fname, rowIDs)
			}

		} else {
			if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 1}) && !reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, 2, 2}) {
				t.Fatalf("unexpected row ids for field %s: %v", fname, rowIDs)
			}
		}
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
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
			r.Values[3] = int64(-10)
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

	for fname, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}) && !reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, 2, 1}) {
			t.Fatalf("unexpected row ids for field %s: %v", fname, rowIDs)
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
		} else {
			r.Values[0] = "u"
			r.Values[1] = "v"
			r.Values[2] = "w"
			r.Values[3] = int64(0)
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

	for fname, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if !reflect.DeepEqual(rowIDs, []uint64{3, 4, 3, 4, 3, 4, 3, 4, 3, 4}) && !reflect.DeepEqual(rowIDs, []uint64{4, 3, 4, 3, 4, 3, 4, 3, 4, 3}) {
			t.Fatalf("unexpected row ids for field %s: %v", fname, rowIDs)
		}
	}

	frags := b.makeFragments()

	if len(frags) != 1 {
		t.Fatalf("unexpected # of shards in fragments: %d", len(frags))
	}
	viewMap, ok := frags[0]
	if !ok {
		t.Fatalf("shard 0 should be in frags")
	}
	if len(viewMap) != 3 {
		t.Fatalf("there should be 3 views")
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
		fields[3].Equals(100)))
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

	// TODO test non-full batches, test behavior of doing import on empty batch
	// TODO test importing across multiple shards
}

func TestBatchesStringIDs(t *testing.T) {
	client := DefaultClient()
	schema := NewSchema()
	idx := schema.Index("gopilosatest-blah", OptIndexKeys(true))
	fields := make([]*Field, 1)
	fields[0] = idx.Field("zero", OptFieldKeys(true))
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

	b := NewBatch(client, 3, idx, fields)

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
