// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package pilosa

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
)

var schema = NewSchema()
var sampleIndex = schema.Index("sample-index")
var sampleField = sampleIndex.Field("sample-field")
var projectIndex = schema.Index("project-index")
var collabField = projectIndex.Field("collaboration")
var b1 = sampleField.Row(10)
var b2 = sampleField.Row(20)
var b3 = sampleField.Row(42)
var b4 = collabField.Row(2)

func TestSchemaDiff(t *testing.T) {
	schema1 := NewSchema()
	index11 := schema1.Index("diff-index1")
	index11.Field("field1-1")
	index11.Field("field1-2")
	index12 := schema1.Index("diff-index2", OptIndexKeys(true), OptIndexTrackExistence(false))
	index12.Field("field2-1")

	schema2 := NewSchema()
	index21 := schema2.Index("diff-index1")
	index21.Field("another-field")

	targetDiff12 := NewSchema()
	targetIndex1 := targetDiff12.Index("diff-index1")
	targetIndex1.Field("field1-1")
	targetIndex1.Field("field1-2")
	targetIndex2 := targetDiff12.Index("diff-index2", OptIndexKeys(true), OptIndexTrackExistence(false))
	targetIndex2.Field("field2-1")

	diff12 := schema1.diff(schema2)
	if !reflect.DeepEqual(targetDiff12, diff12) {
		t.Fatalf("The diff must be correctly calculated")
	}
}

func TestSchemaIndexes(t *testing.T) {
	schema1 := NewSchema()
	index11 := schema1.Index("diff-index1")
	index12 := schema1.Index("diff-index2")
	indexes := schema1.Indexes()
	target := map[string]*Index{
		"diff-index1": index11,
		"diff-index2": index12,
	}
	if !reflect.DeepEqual(target, indexes) {
		t.Fatalf("calling schema.Indexes should return indexes")
	}
}

func TestSchemaToString(t *testing.T) {
	schema1 := NewSchema()
	index := schema1.Index("test-index")
	target := fmt.Sprintf(`map[string]*pilosa.Index{"test-index":(*pilosa.Index)(%p)}`, index)
	if target != schema1.String() {
		t.Fatalf("%s != %s", target, schema1.String())
	}
}

func TestNewIndex(t *testing.T) {
	index1 := schema.Index("index-name")
	if index1.Name() != "index-name" {
		t.Fatalf("index name was not set")
	}
	// calling schema.Index again should return the same index
	index2 := schema.Index("index-name")
	if index1 != index2 {
		t.Fatalf("calling schema.Index again should return the same index")
	}
	if !schema.HasIndex("index-name") {
		t.Fatalf("HasIndex should return true")
	}
	if schema.HasIndex("index-x") {
		t.Fatalf("HasIndex should return false")
	}
}

func TestIndexCopy(t *testing.T) {
	index := schema.Index("my-index-4copy", OptIndexKeys(true))
	index.Field("my-field-4copy", OptFieldTypeTime(TimeQuantumDayHour))
	copiedIndex := index.copy()
	if !reflect.DeepEqual(index, copiedIndex) {
		t.Fatalf("copied index should be equivalent")
	}
}

func TestIndexOptions(t *testing.T) {
	schema := NewSchema()
	// test the defaults
	index := schema.Index("index-default-options")
	target := `{"options":{}}`
	if target != index.options.String() {
		t.Fatalf("%s != %s", target, index.options.String())
	}

	index = schema.Index("index-keys", OptIndexKeys(true))
	if true != index.Opts().Keys() {
		t.Fatalf("index keys %v != %v", true, index.Opts().Keys())
	}
	target = `{"options":{"keys":true}}`
	if target != index.options.String() {
		t.Fatalf("%s != %s", target, index.options.String())
	}

	index = schema.Index("index-trackexistence", OptIndexTrackExistence(false))
	if false != index.Opts().TrackExistence() {
		t.Fatalf("index trackExistene %v != %v", true, index.Opts().TrackExistence())
	}
	target = `{"options":{"trackExistence":false}}`
	if target != index.options.String() {
		t.Fatalf("%s != %s", target, index.options.String())
	}
}

func TestNilIndexOption(t *testing.T) {
	schema.Index("index-with-nil-option", nil)
}

func TestIndexFields(t *testing.T) {
	schema1 := NewSchema()
	index11 := schema1.Index("diff-index1")
	field11 := index11.Field("field1-1")
	field12 := index11.Field("field1-2")
	fields := index11.Fields()
	target := map[string]*Field{
		"field1-1": field11,
		"field1-2": field12,
	}
	if !reflect.DeepEqual(target, fields) {
		t.Fatalf("calling index.Fields should return fields")
	}
	if !index11.HasField("field1-1") {
		t.Fatalf("HasField should return true")
	}
	if index11.HasField("field-x") {
		t.Fatalf("HasField should return false")
	}
}

func TestIndexToString(t *testing.T) {
	schema1 := NewSchema()
	index := schema1.Index("test-index")
	target := fmt.Sprintf(`&pilosa.Index{name:"test-index", options:(*pilosa.IndexOptions)(%p), fields:map[string]*pilosa.Field{}, shardWidth:0x0}`, index.options)
	if target != index.String() {
		t.Fatalf("%s != %s", target, index.String())
	}
}

func TestField(t *testing.T) {
	field1 := sampleIndex.Field("nonexistent-field")
	field2 := sampleIndex.Field("nonexistent-field")
	if field1 != field2 {
		t.Fatalf("calling index.Field again should return the same field")
	}
	if field1.Name() != "nonexistent-field" {
		t.Fatalf("calling field.Name should return field's name")
	}
}

func TestFieldCopy(t *testing.T) {
	field := sampleIndex.Field("my-field-4copy", OptFieldTypeSet(CacheTypeRanked, 123456))
	copiedField := field.copy()
	if !reflect.DeepEqual(field, copiedField) {
		t.Fatalf("copied field should be equivalent")
	}
}

func TestFieldToString(t *testing.T) {
	schema1 := NewSchema()
	index := schema1.Index("test-index")
	field := index.Field("test-field")
	target := fmt.Sprintf(`&pilosa.Field{name:"test-field", index:(*pilosa.Index)(%p), options:(*pilosa.FieldOptions)(%p)}`,
		field.index, field.options)
	if target != field.String() {
		t.Fatalf("%s != %s", target, field.String())
	}
}

func TestNilFieldOption(t *testing.T) {
	schema1 := NewSchema()
	index := schema1.Index("test-index")
	index.Field("test-field-with-nil-option", nil)
}

func TestFieldSetType(t *testing.T) {
	schema1 := NewSchema()
	index := schema1.Index("test-index")
	field := index.Field("test-set-field", OptFieldTypeSet(CacheTypeLRU, 1000), OptFieldKeys(true))
	target := `{"options":{"type":"set","cacheType":"lru","cacheSize":1000,"keys":true}}`
	if sortedString(target) != sortedString(field.options.String()) {
		t.Fatalf("%s != %s", target, field.options.String())
	}

	field = index.Field("test-set-field2", OptFieldTypeSet(CacheTypeLRU, -10), OptFieldKeys(true))
	target = `{"options":{"type":"set","cacheType":"lru","keys":true}}`
	if sortedString(target) != sortedString(field.options.String()) {
		t.Fatalf("%s != %s", target, field.options.String())
	}
}

func TestRow(t *testing.T) {
	comparePQL(t,
		"Row(collaboration=5)",
		collabField.Row(5))

	comparePQL(t,
		"Row(collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
		collabField.Row("b7feb014-8ea7-49a8-9cd8-19709161ab63"))

	q := collabField.Row(nil)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestSet(t *testing.T) {
	comparePQL(t,
		"Set(10,collaboration=5)",
		collabField.Set(5, 10))

	comparePQL(t,
		`Set('some_id',collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')`,
		collabField.Set("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id"))

	q := collabField.Set(nil, 10)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
	q = collabField.Set(5, false)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestTimestamp(t *testing.T) {
	timestamp := time.Date(2017, time.April, 24, 12, 14, 0, 0, time.UTC)
	comparePQL(t,
		"Set(20,collaboration=10,2017-04-24T12:14)",
		collabField.SetTimestamp(10, 20, timestamp))

	comparePQL(t,
		"Set('mycol',collaboration='myrow',2017-04-24T12:14)",
		collabField.SetTimestamp("myrow", "mycol", timestamp))

	q := collabField.SetTimestamp(nil, 20, timestamp)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestClear(t *testing.T) {
	comparePQL(t,
		"Clear(10,collaboration=5)",
		collabField.Clear(5, 10))

	comparePQL(t,
		"Clear('some_id',collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
		collabField.Clear("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id"))

	q := collabField.Clear(nil, 10)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
	q = collabField.Clear(5, false)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestClearRow(t *testing.T) {
	comparePQL(t,
		"ClearRow(collaboration=5)",
		collabField.ClearRow(5))

	comparePQL(t,
		"ClearRow(collaboration='five')",
		collabField.ClearRow("five"))

	comparePQL(t,
		"ClearRow(collaboration=true)",
		collabField.ClearRow(true))

	q := collabField.ClearRow(nil)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestUnion(t *testing.T) {
	comparePQL(t,
		"Union(Row(sample-field=10),Row(sample-field=20))",
		sampleIndex.Union(b1, b2))
	comparePQL(t,
		"Union(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
		sampleIndex.Union(b1, b2, b3))
	comparePQL(t,
		"Union(Row(sample-field=10),Row(collaboration=2))",
		sampleIndex.Union(b1, b4))
	comparePQL(t,
		"Union(Row(sample-field=10))",
		sampleIndex.Union(b1))
	comparePQL(t,
		"Union()",
		sampleIndex.Union())
}

func TestIntersect(t *testing.T) {
	comparePQL(t,
		"Intersect(Row(sample-field=10),Row(sample-field=20))",
		sampleIndex.Intersect(b1, b2))
	comparePQL(t,
		"Intersect(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
		sampleIndex.Intersect(b1, b2, b3))
	comparePQL(t,
		"Intersect(Row(sample-field=10),Row(collaboration=2))",
		sampleIndex.Intersect(b1, b4))
	comparePQL(t,
		"Intersect(Row(sample-field=10))",
		sampleIndex.Intersect(b1))
}

func TestDifference(t *testing.T) {
	comparePQL(t,
		"Difference(Row(sample-field=10),Row(sample-field=20))",
		sampleIndex.Difference(b1, b2))
	comparePQL(t,
		"Difference(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
		sampleIndex.Difference(b1, b2, b3))
	comparePQL(t,
		"Difference(Row(sample-field=10),Row(collaboration=2))",
		sampleIndex.Difference(b1, b4))
	comparePQL(t,
		"Difference(Row(sample-field=10))",
		sampleIndex.Difference(b1))
}

func TestXor(t *testing.T) {
	comparePQL(t,
		"Xor(Row(sample-field=10),Row(sample-field=20))",
		sampleIndex.Xor(b1, b2))
	comparePQL(t,
		"Xor(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
		sampleIndex.Xor(b1, b2, b3))
	comparePQL(t,
		"Xor(Row(sample-field=10),Row(collaboration=2))",
		sampleIndex.Xor(b1, b4))
}

func TestNot(t *testing.T) {
	comparePQL(t,
		"Not(Row(sample-field=10))",
		sampleIndex.Not(sampleField.Row(10)))
}

func TestTopN(t *testing.T) {
	comparePQL(t,
		"TopN(collaboration,n=27)",
		collabField.TopN(27))
	comparePQL(t,
		"TopN(collaboration,Row(collaboration=3),n=10)",
		collabField.RowTopN(10, collabField.Row(3)))
	comparePQL(t,
		"TopN(sample-field,Row(collaboration=7),n=12,attrName='category',attrValues=[80,81])",
		sampleField.FilterAttrTopN(12, collabField.Row(7), "category", 80, 81))
	comparePQL(t,
		"TopN(sample-field,n=12,attrName='category',attrValues=[80,81])",
		sampleField.FilterAttrTopN(12, nil, "category", 80, 81))
}

func TestFieldLT(t *testing.T) {
	comparePQL(t,
		"Range(collaboration < 10)",
		collabField.LT(10))
}

func TestFieldLTE(t *testing.T) {
	comparePQL(t,
		"Range(collaboration <= 10)",
		collabField.LTE(10))
}

func TestFieldGT(t *testing.T) {
	comparePQL(t,
		"Range(collaboration > 10)",
		collabField.GT(10))
}

func TestFieldGTE(t *testing.T) {
	comparePQL(t,
		"Range(collaboration >= 10)",
		collabField.GTE(10))
}

func TestFieldEquals(t *testing.T) {
	comparePQL(t,
		"Range(collaboration == 10)",
		collabField.Equals(10))
}

func TestFieldNotEquals(t *testing.T) {
	comparePQL(t,
		"Range(collaboration != 10)",
		collabField.NotEquals(10))
}

func TestFieldNotNull(t *testing.T) {
	comparePQL(t,
		"Range(collaboration != null)",
		collabField.NotNull())
}

func TestFieldBetween(t *testing.T) {
	comparePQL(t,
		"Range(collaboration >< [10,20])",
		collabField.Between(10, 20))
}

func TestFieldSum(t *testing.T) {
	comparePQL(t,
		"Sum(Row(collaboration=10),field='collaboration')",
		collabField.Sum(collabField.Row(10)))
	comparePQL(t,
		"Sum(field='collaboration')",
		collabField.Sum(nil))
}

func TestSetValue(t *testing.T) {
	comparePQL(t,
		"Set(50, collaboration=15)",
		collabField.SetIntValue(50, 15))

	comparePQL(t,
		"Set('mycol', sample-field=22)",
		sampleField.SetIntValue("mycol", 22))

	q := sampleField.SetIntValue(false, 22)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestFilterFieldTopNInvalidField(t *testing.T) {
	q := sampleField.FilterAttrTopN(12, collabField.Row(7), "$invalid$", 80, 81)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestFilterFieldTopNInvalidValue(t *testing.T) {
	q := sampleField.FilterAttrTopN(12, collabField.Row(7), "category", 80, func() {})
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowOperationInvalidArg(t *testing.T) {
	invalid := sampleField.FilterAttrTopN(12, collabField.Row(7), "$invalid$", 80, 81)
	// invalid argument in pos 1
	q := sampleIndex.Union(invalid, b1)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// invalid argument in pos 2
	q = sampleIndex.Intersect(b1, invalid)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// invalid argument in pos 3
	q = sampleIndex.Intersect(b1, b2, invalid)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// not enough rows supplied
	q = sampleIndex.Difference()
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// not enough rows supplied
	q = sampleIndex.Intersect()
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}

	// not enough rows supplied
	q = sampleIndex.Xor(b1)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestSetColumnAttrsTest(t *testing.T) {
	attrs := map[string]interface{}{
		"quote": "\"Don't worry, be happy\"",
		"happy": true,
	}
	comparePQL(t,
		"SetColumnAttrs(5,happy=true,quote=\"\\\"Don't worry, be happy\\\"\")",
		projectIndex.SetColumnAttrs(5, attrs))

	q := projectIndex.SetColumnAttrs(false, attrs)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestSetColumnAttrsInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if projectIndex.SetColumnAttrs(5, attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSetRowAttrsTest(t *testing.T) {
	attrs := map[string]interface{}{
		"quote":  "\"Don't worry, be happy\"",
		"active": true,
	}
	comparePQL(t,
		`SetRowAttrs(collaboration,5,active=true,quote="\"Don't worry, be happy\"")`,
		collabField.SetRowAttrs(5, attrs))

	comparePQL(t,
		"SetRowAttrs(collaboration,'foo',active=true,quote=\"\\\"Don't worry, be happy\\\"\")",
		collabField.SetRowAttrs("foo", attrs))

	q := collabField.SetRowAttrs(nil, attrs)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestSetRowAttrsInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if collabField.SetRowAttrs(5, attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}

	if collabField.SetRowAttrs("foo", attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestStore(t *testing.T) {
	comparePQL(t,
		"Store(Row(collaboration=5),sample-field=10)",
		sampleField.Store(collabField.Row(5), 10))
	q := sampleField.Store(collabField.Row(5), 1.2)
	if q.Error() == nil {
		t.Fatalf("query error should be not nil")
	}
}

func TestOptions(t *testing.T) {
	comparePQL(t,
		"Options(Row(collaboration=5),columnAttrs=true,excludeColumns=true,excludeRowAttrs=true,shards=[1,3])",
		sampleIndex.Options(collabField.Row(5),
			OptOptionsColumnAttrs(true),
			OptOptionsExcludeColumns(true),
			OptOptionsExcludeRowAttrs(true),
			OptOptionsShards(1, 3),
		))
	comparePQL(t,
		"Options(Row(collaboration=5),columnAttrs=true,excludeColumns=false,excludeRowAttrs=false)",
		sampleIndex.Options(collabField.Row(5),
			OptOptionsColumnAttrs(true),
		))
}

func TestBatchQuery(t *testing.T) {
	q := sampleIndex.BatchQuery()
	if q.Index() != sampleIndex {
		t.Fatalf("The correct index should be assigned")
	}
	q.Add(sampleField.Row(44))
	q.Add(sampleField.Row(10101))
	if q.Error() != nil {
		t.Fatalf("Error should be nil")
	}
	comparePQL(t, "Row(sample-field=44)Row(sample-field=10101)", q)

	q2 := sampleField.Row(nil)
	if q2.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestBatchQueryWithError(t *testing.T) {
	q := sampleIndex.BatchQuery()
	q.Add(sampleField.FilterAttrTopN(12, collabField.Row(7), "$invalid$", 80, 81))
	if q.Error() == nil {
		t.Fatalf("The error must be set")
	}
}

func TestCount(t *testing.T) {
	q := projectIndex.Count(collabField.Row(42))
	comparePQL(t, "Count(Row(collaboration=42))", q)
}

func TestRange(t *testing.T) {
	start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
	comparePQL(t,
		"Range(collaboration=10,1970-01-01T00:00,2000-02-02T03:04)",
		collabField.Range(10, start, end))

	comparePQL(t,
		"Range(collaboration='foo',1970-01-01T00:00,2000-02-02T03:04)",
		collabField.Range("foo", start, end))

	q := collabField.Range(nil, start, end)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowRange(t *testing.T) {
	start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
	comparePQL(t,
		"Row(collaboration=10,from='1970-01-01T00:00',to='2000-02-02T03:04')",
		collabField.RowRange(10, start, end))

	comparePQL(t,
		"Row(collaboration='foo',from='1970-01-01T00:00',to='2000-02-02T03:04')",
		collabField.RowRange("foo", start, end))

	q := collabField.RowRange(nil, start, end)
	if q.err == nil {
		t.Fatalf("should have failed")
	}
}

func TestRows(t *testing.T) {
	comparePQL(t,
		"Rows(field='collaboration')",
		collabField.Rows())
}

func TestRowsPrevious(t *testing.T) {
	comparePQL(t,
		"Rows(field='collaboration',previous=42)",
		collabField.RowsPrevious(42))
	comparePQL(t,
		"Rows(field='collaboration',previous='forty-two')",
		collabField.RowsPrevious("forty-two"))
	q := collabField.RowsPrevious(1.2)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowLimit(t *testing.T) {
	comparePQL(t,
		"Rows(field='collaboration',limit=10)",
		collabField.RowsLimit(10))
	q := collabField.RowsLimit(-1)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowsColumn(t *testing.T) {
	comparePQL(t,
		"Rows(field='collaboration',column=1000)",
		collabField.RowsColumn(1000))
	comparePQL(t,
		"Rows(field='collaboration',column='one-thousand')",
		collabField.RowsColumn("one-thousand"))
	q := collabField.RowsColumn(1.2)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowsPreviousLimit(t *testing.T) {
	comparePQL(t,
		"Rows(field='collaboration',previous=42,limit=10)",
		collabField.RowsPreviousLimit(42, 10))
	comparePQL(t,
		"Rows(field='collaboration',previous='forty-two',limit=10)",
		collabField.RowsPreviousLimit("forty-two", 10))
	q := collabField.RowsPreviousLimit(1.2, 10)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	q = collabField.RowsPreviousLimit("forty-two", -1)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowsPreviousColumn(t *testing.T) {
	comparePQL(t,
		"Rows(field='collaboration',previous=42,column=1000)",
		collabField.RowsPreviousColumn(42, 1000))
	comparePQL(t,
		"Rows(field='collaboration',previous='forty-two',column='one-thousand')",
		collabField.RowsPreviousColumn("forty-two", "one-thousand"))
	q := collabField.RowsPreviousColumn(1.2, 1000)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	q = collabField.RowsPreviousColumn("forty-two", 1.2)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowLimitColumn(t *testing.T) {
	comparePQL(t,
		"Rows(field='collaboration',limit=10,column=1000)",
		collabField.RowsLimitColumn(10, 1000))
	comparePQL(t,
		"Rows(field='collaboration',limit=10,column='one-thousand')",
		collabField.RowsLimitColumn(10, "one-thousand"))
	q := collabField.RowsLimitColumn(10, 1.2)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	q = collabField.RowsLimitColumn(-1, 1000)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowsPreviousLimitColumn(t *testing.T) {
	comparePQL(t,
		"Rows(field='collaboration',previous=42,limit=10,column=1000)",
		collabField.RowsPreviousLimitColumn(42, 10, 1000))
	comparePQL(t,
		"Rows(field='collaboration',previous='forty-two',limit=10,column='one-thousand')",
		collabField.RowsPreviousLimitColumn("forty-two", 10, "one-thousand"))
	q := collabField.RowsPreviousLimitColumn(1.2, 10, 1000)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	q = collabField.RowsPreviousLimitColumn(42, -1, 1000)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	q = collabField.RowsPreviousLimitColumn(42, 10, 1.2)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestGroupBy(t *testing.T) {
	field := sampleIndex.Field("test")
	comparePQL(t,
		"GroupBy(Rows(field='collaboration'))",
		sampleIndex.GroupBy(collabField.Rows()))
	comparePQL(t,
		"GroupBy(Rows(field='collaboration'),Rows(field='test'))",
		sampleIndex.GroupBy(collabField.Rows(), field.Rows()))
	q := sampleIndex.GroupBy()
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestGroupByLimit(t *testing.T) {
	field := sampleIndex.Field("test")
	comparePQL(t,
		"GroupBy(Rows(field='collaboration'),limit=10)",
		sampleIndex.GroupByLimit(10, collabField.Rows()))
	comparePQL(t,
		"GroupBy(Rows(field='collaboration'),Rows(field='test'),limit=10)",
		sampleIndex.GroupByLimit(10, collabField.Rows(), field.Rows()))
	q := sampleIndex.GroupByLimit(10)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	q = sampleIndex.GroupByLimit(-1, collabField.Rows())
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestGroupByFilter(t *testing.T) {
	field := sampleIndex.Field("test")
	comparePQL(t,
		"GroupBy(Rows(field='collaboration'),filter=Row(test=5))",
		sampleIndex.GroupByFilter(field.Row(5), collabField.Rows()))
	comparePQL(t,
		"GroupBy(Rows(field='collaboration'),Rows(field='test'),filter=Row(test=5))",
		sampleIndex.GroupByFilter(field.Row(5), collabField.Rows(), field.Rows()))
	q := sampleIndex.GroupByFilter(field.Row(5))
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestGroupByLimitFilter(t *testing.T) {
	field := sampleIndex.Field("test")
	comparePQL(t,
		"GroupBy(Rows(field='collaboration'),limit=10,filter=Row(test=5))",
		sampleIndex.GroupByLimitFilter(10, field.Row(5), collabField.Rows()))
	comparePQL(t,
		"GroupBy(Rows(field='collaboration'),Rows(field='test'),limit=10,filter=Row(test=5))",
		sampleIndex.GroupByLimitFilter(10, field.Row(5), collabField.Rows(), field.Rows()))
	q := sampleIndex.GroupByLimitFilter(10, field.Row(5))
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	q = sampleIndex.GroupByLimitFilter(-1, field.Row(5), collabField.Rows())
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestFieldOptions(t *testing.T) {
	field := sampleIndex.Field("foo", OptFieldKeys(true))
	if true != field.Opts().Keys() {
		t.Fatalf("field keys: %v != %v", true, field.Opts().Keys())
	}
}

func TestSetFieldOptions(t *testing.T) {
	field := sampleIndex.Field("set-field", OptFieldTypeSet(CacheTypeRanked, 9999))
	jsonString := field.options.String()
	targetString := `{"options":{"type":"set","cacheType":"ranked","cacheSize":9999}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
	compareFieldOptions(t, field.Options(), FieldTypeSet, TimeQuantumNone, CacheTypeRanked, 9999, 0, 0)
}

func TestIntFieldOptions(t *testing.T) {
	field := sampleIndex.Field("int-field", OptFieldTypeInt(-10, 100))
	jsonString := field.options.String()
	targetString := `{"options":{"type":"int","min":-10,"max":100}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
	compareFieldOptions(t, field.Options(), FieldTypeInt, TimeQuantumNone, CacheTypeDefault, 0, -10, 100)

	field = sampleIndex.Field("int-field2", OptFieldTypeInt(-10))
	jsonString = field.options.String()
	targetString = fmt.Sprintf(`{"options":{"type":"int","min":-10,"max":%d}}`, math.MaxInt64)
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}

	compareFieldOptions(t, field.Options(), FieldTypeInt, TimeQuantumNone, CacheTypeDefault, 0, -10, math.MaxInt64)
	field = sampleIndex.Field("int-field3", OptFieldTypeInt())
	jsonString = field.options.String()
	targetString = fmt.Sprintf(`{"options":{"type":"int","min":%d,"max":%d}}`, math.MinInt64, math.MaxInt64)
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
	compareFieldOptions(t, field.Options(), FieldTypeInt, TimeQuantumNone, CacheTypeDefault, 0, math.MinInt64, math.MaxInt64)
}

func TestTimeFieldOptions(t *testing.T) {
	field := sampleIndex.Field("time-field", OptFieldTypeTime(TimeQuantumDayHour, true))
	if true != field.Opts().NoStandardView() {
		t.Fatalf("field noStandardView %v != %v", true, field.Opts().NoStandardView())
	}
	jsonString := field.options.String()
	targetString := `{"options":{"noStandardView":true,"type":"time","timeQuantum":"DH"}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
	compareFieldOptions(t, field.Options(), FieldTypeTime, TimeQuantumDayHour, CacheTypeDefault, 0, 0, 0)
}

func TestMutexFieldOptions(t *testing.T) {
	field := sampleIndex.Field("mutex-field", OptFieldTypeMutex(CacheTypeRanked, 9999))
	jsonString := field.options.String()
	targetString := `{"options":{"type":"mutex","cacheType":"ranked","cacheSize":9999}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
	compareFieldOptions(t, field.Options(), FieldTypeMutex, TimeQuantumNone, CacheTypeRanked, 9999, 0, 0)
}

func TestBoolFieldOptions(t *testing.T) {
	field := sampleIndex.Field("bool-field", OptFieldTypeBool())
	jsonString := field.options.String()
	targetString := `{"options":{"type":"bool"}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
	compareFieldOptions(t, field.Options(), FieldTypeBool, TimeQuantumNone, CacheTypeDefault, 0, 0, 0)
}

func TestEncodeMapPanicsOnMarshalFailure(t *testing.T) {
	defer func() {
		recover()
	}()
	m := map[string]interface{}{
		"foo": func() {},
	}
	encodeMap(m)
	t.Fatal("Should have panicked")
}

func TestFormatIDKey(t *testing.T) {
	testCase := [][]interface{}{
		{uint(42), "42", nil},
		{uint32(42), "42", nil},
		{uint64(42), "42", nil},
		{42, "42", nil},
		{int32(42), "42", nil},
		{int64(42), "42", nil},
		{"foo", `'foo'`, nil},
		{false, "", errors.New("error")},
	}
	for i, item := range testCase {
		s, err := formatIDKey(item[0])
		if item[2] != nil {
			if err == nil {
				t.Fatalf("Should have failed: %d", i)
			}
			continue
		}
		if item[1] != s {
			t.Fatalf("%s != %s", item[1], s)
		}
	}
}

func comparePQL(t *testing.T, target string, q PQLQuery) {
	t.Helper()
	pql := q.Serialize().String()
	if target != pql {
		t.Fatalf("%s != %s", target, pql)
	}
}

func compareFieldOptions(t *testing.T, opts *FieldOptions, fieldType FieldType, timeQuantum TimeQuantum, cacheType CacheType, cacheSize int, min int64, max int64) {
	if fieldType != opts.Type() {
		t.Fatalf("%s != %s", fieldType, opts.Type())
	}
	if timeQuantum != opts.TimeQuantum() {
		t.Fatalf("%s != %s", timeQuantum, opts.TimeQuantum())
	}
	if cacheType != opts.CacheType() {
		t.Fatalf("%s != %s", cacheType, opts.CacheType())
	}
	if cacheSize != opts.CacheSize() {
		t.Fatalf("%d != %d", cacheSize, opts.CacheSize())
	}
	if min != opts.Min() {
		t.Fatalf("%d != %d", min, opts.Min())
	}
	if max != opts.Max() {
		t.Fatalf("%d != %d", max, opts.Max())
	}
}

func sortedString(s string) string {
	arr := strings.Split(s, "")
	sort.Strings(arr)
	return strings.Join(arr, "")
}
