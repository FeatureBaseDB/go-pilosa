package pilosa

import (
	"reflect"
	"testing"
)

func TestBatches(t *testing.T) {
	client := DefaultClient()
	schema := NewSchema()
	idx := schema.Index("gopilosatest-blah")
	fields := make([]*Field, 3)
	fields[0] = idx.Field("zero", OptFieldKeys(true))
	fields[1] = idx.Field("one", OptFieldKeys(true))
	fields[2] = idx.Field("two", OptFieldKeys(true))
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
	b := NewBatch(client, 10, fields)

	r := Row{Values: make([]interface{}, 3)}

	for i := 0; i < 9; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "a"
			r.Values[1] = "b"
			r.Values[2] = "c"
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
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
			if !reflect.DeepEqual(ints, []int{0, 2, 4, 6, 8}) {
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

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("doing translation: %v", err)
	}

	for i, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 1}) && !reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, 2, 2}) {
			t.Fatalf("unexpected row ids for field %d: %v", i, rowIDs)
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
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
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

	for i, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}) && !reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, 2, 1}) {
			t.Fatalf("unexpected row ids for field %d: %v", i, rowIDs)
		}
	}

	b.reset()

	for i := 19; i < 29; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "d"
			r.Values[1] = "e"
			r.Values[2] = "f"
		} else {
			r.Values[0] = "u"
			r.Values[1] = "v"
			r.Values[2] = "w"
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

	for i, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if !reflect.DeepEqual(rowIDs, []uint64{3, 4, 3, 4, 3, 4, 3, 4, 3, 4}) && !reflect.DeepEqual(rowIDs, []uint64{4, 3, 4, 3, 4, 3, 4, 3, 4, 3}) {
			t.Fatalf("unexpected row ids for field %d: %v", i, rowIDs)
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

	// TODO query Pilosa to confirm data is in place
	resp, err := client.Query(idx.BatchQuery(fields[0].Row("a"),
		fields[1].Row("b"),
		fields[2].Row("c")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results := resp.Results()
	for _, res := range results {
		cols := res.Row().Columns
		if !reflect.DeepEqual(cols, []uint64{0, 2, 4, 6, 8, 10, 12, 14, 16, 18}) {
			t.Fatalf("unexpected columns: %v", cols)
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

	// TODO test non-full batches, test behavior of doing import on empty batch
}
