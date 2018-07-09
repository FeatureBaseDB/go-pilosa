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
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

var schema = NewSchema()
var sampleIndex = mustNewIndex(schema, "sample-index")
var sampleField = mustNewField(sampleIndex, "sample-field")
var projectIndex = mustNewIndex(schema, "project-index")
var collabField = mustNewField(projectIndex, "collaboration")
var b1 = sampleField.Row(10)
var b2 = sampleField.Row(20)
var b3 = sampleField.Row(42)
var b4 = collabField.Row(2)

func TestSchemaDiff(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1")
	index11.Field("field1-1")
	index11.Field("field1-2")
	index12, _ := schema1.Index("diff-index2")
	index12.Field("field2-1")

	schema2 := NewSchema()
	index21, _ := schema2.Index("diff-index1")
	index21.Field("another-field")

	targetDiff12 := NewSchema()
	targetIndex1, _ := targetDiff12.Index("diff-index1")
	targetIndex1.Field("field1-1")
	targetIndex1.Field("field1-2")
	targetIndex2, _ := targetDiff12.Index("diff-index2")
	targetIndex2.Field("field2-1")

	diff12 := schema1.diff(schema2)
	if !reflect.DeepEqual(targetDiff12, diff12) {
		t.Fatalf("The diff must be correctly calculated")
	}
}

func TestSchemaIndexes(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1")
	index12, _ := schema1.Index("diff-index2")
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
	index, _ := schema1.Index("test-index")
	target := fmt.Sprintf(`map[string]*pilosa.Index{"test-index":(*pilosa.Index)(%p)}`, index)
	if target != schema1.String() {
		t.Fatalf("%s != %s", target, schema1.String())
	}
}

func TestNewIndex(t *testing.T) {
	index1, err := schema.Index("index-name")
	if err != nil {
		t.Fatal(err)
	}
	if index1.Name() != "index-name" {
		t.Fatalf("index name was not set")
	}
	// calling schema.Index again should return the same index
	index2, err := schema.Index("index-name")
	if err != nil {
		t.Fatal(err)
	}
	if index1 != index2 {
		t.Fatalf("calling schema.Index again should return the same index")
	}
}

func TestNewIndexWithInvalidName(t *testing.T) {
	_, err := schema.Index("$FOO")
	if err == nil {
		t.Fatal(err)
	}
}

func TestIndexCopy(t *testing.T) {
	index, err := schema.Index("my-index-4copy")
	if err != nil {
		t.Fatal(err)
	}
	_, err = index.Field("my-field-4copy", TimeQuantumDayHour)
	if err != nil {
		t.Fatal(err)
	}
	copiedIndex := index.copy()
	if !reflect.DeepEqual(index, copiedIndex) {
		t.Fatalf("copied index should be equivalent")
	}
}

func TestIndexFields(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1")
	field11, _ := index11.Field("field1-1")
	field12, _ := index11.Field("field1-2")
	fields := index11.Fields()
	target := map[string]*Field{
		"field1-1": field11,
		"field1-2": field12,
	}
	if !reflect.DeepEqual(target, fields) {
		t.Fatalf("calling index.Fields should return fields")
	}
}

func TestIndexToString(t *testing.T) {
	schema1 := NewSchema()
	index, _ := schema1.Index("test-index")
	target := fmt.Sprintf(`&pilosa.Index{name:"test-index", fields:map[string]*pilosa.Field{}}`)
	if target != index.String() {
		t.Fatalf("%s != %s", target, index.String())
	}
}

func TestField(t *testing.T) {
	field1, err := sampleIndex.Field("nonexistent-field")
	if err != nil {
		t.Fatal(err)
	}
	field2, err := sampleIndex.Field("nonexistent-field")
	if err != nil {
		t.Fatal(err)
	}
	if field1 != field2 {
		t.Fatalf("calling index.Field again should return the same field")
	}
	if field1.Name() != "nonexistent-field" {
		t.Fatalf("calling field.Name should return field's name")
	}
}

func TestFieldCopy(t *testing.T) {
	options := &FieldOptions{
		timeQuantum: TimeQuantumMonthDayHour,
		cacheType:   CacheTypeRanked,
		cacheSize:   123456,
	}
	field, err := sampleIndex.Field("my-field-4copy", options)
	if err != nil {
		t.Fatal(err)
	}
	copiedField := field.copy()
	if !reflect.DeepEqual(field, copiedField) {
		t.Fatalf("copied field should be equivalent")
	}
}

func TestNewFieldWithInvalidName(t *testing.T) {
	index, err := NewIndex("foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = index.Field("$$INVALIDFIELD$$")
	if err == nil {
		t.Fatal("Creating fields with invalid row labels should fail")
	}
}

func TestFieldToString(t *testing.T) {
	schema1 := NewSchema()
	index, _ := schema1.Index("test-index")
	field, _ := index.Field("test-field")
	target := fmt.Sprintf(`&pilosa.Field{name:"test-field", index:(*pilosa.Index)(%p), options:(*pilosa.FieldOptions)(%p)}`,
		field.index, field.options)
	if target != field.String() {
		t.Fatalf("%s != %s", target, field.String())
	}
}

func TestFieldSetType(t *testing.T) {
	schema1 := NewSchema()
	index, _ := schema1.Index("test-index")
	field, _ := index.Field("test-field", OptFieldSet(CacheTypeLRU, 1000))
	target := `{"options":{"type":"set","cacheType":"lru","cacheSize":1000}}`
	if sortedString(target) != sortedString(field.options.String()) {
		t.Fatalf("%s != %s", target, field.options.String())
	}
}

func TestRow(t *testing.T) {
	comparePQL(t,
		"Row(collaboration=5)",
		collabField.Row(5))
}

func TestRowK(t *testing.T) {
	comparePQL(t,
		"Row(collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
		collabField.RowK("b7feb014-8ea7-49a8-9cd8-19709161ab63"))
}

func TestSet(t *testing.T) {
	comparePQL(t,
		"Set(10,collaboration=5)",
		collabField.Set(5, 10))
}

func TestSetK(t *testing.T) {
	comparePQL(t,
		"Set(some_id,collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
		collabField.SetK("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id"))
}

func TestTimestamp(t *testing.T) {
	timestamp := time.Date(2017, time.April, 24, 12, 14, 0, 0, time.UTC)
	comparePQL(t,
		"Set(20,collaboration=10,2017-04-24T12:14)",
		collabField.SetTimestamp(10, 20, timestamp))
}

func TestSetTimestampK(t *testing.T) {
	timestamp := time.Date(2017, time.April, 24, 12, 14, 0, 0, time.UTC)
	comparePQL(t,
		"Set('mycol',collaboration='myrow',2017-04-24T12:14)",
		collabField.SetTimestampK("myrow", "mycol", timestamp))
}

func TestClear(t *testing.T) {
	comparePQL(t,
		"Clear(10,collaboration=5)",
		collabField.Clear(5, 10))
}

func TestClearK(t *testing.T) {
	comparePQL(t,
		"Clear('some_id',collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
		collabField.ClearK("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id"))
}

func TestSetValue(t *testing.T) {
	comparePQL(t,
		"SetValue(col=50, collaboration=15)",
		collabField.SetIntValue(50, 15))
}

func TestSetValueK(t *testing.T) {
	comparePQL(t,
		"SetValue(col='mycol', sample-field=22)",
		sampleField.SetIntValueK("mycol", 22))
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

func TestTopN(t *testing.T) {
	comparePQL(t,
		"TopN(collaboration,n=27)",
		collabField.TopN(27))
	comparePQL(t,
		"TopN(collaboration,Row(collaboration=3),n=10)",
		collabField.RowTopN(10, collabField.Row(3)))
	comparePQL(t,
		"TopN(sample-field,Row(collaboration=7),n=12,field='category',filters=[80,81])",
		sampleField.FilterFieldTopN(12, collabField.Row(7), "category", 80, 81))
	comparePQL(t,
		"TopN(sample-field,n=12,field='category',filters=[80,81])",
		sampleField.FilterFieldTopN(12, nil, "category", 80, 81))
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

func TestFieldBSetIntValue(t *testing.T) {
	comparePQL(t,
		"SetValue(col=10, collaboration=20)",
		collabField.SetIntValue(10, 20))
}

func TestFilterFieldTopNInvalidField(t *testing.T) {
	q := sampleField.FilterFieldTopN(12, collabField.Row(7), "$invalid$", 80, 81)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestFilterFieldTopNInvalidValue(t *testing.T) {
	q := sampleField.FilterFieldTopN(12, collabField.Row(7), "category", 80, func() {})
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestRowOperationInvalidArg(t *testing.T) {
	invalid := sampleField.FilterFieldTopN(12, collabField.Row(7), "$invalid$", 80, 81)
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
}

func TestSetRowAttrsInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if collabField.SetRowAttrs(5, attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSetRowAttrsKTest(t *testing.T) {
	attrs := map[string]interface{}{
		"quote":  "\"Don't worry, be happy\"",
		"active": true,
	}

	comparePQL(t,
		"SetRowAttrs('collaboration','foo',active=true,quote=\"\\\"Don't worry, be happy\\\"\")",
		collabField.SetRowAttrsK("foo", attrs))
}

func TestSetRowAttrsKInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if collabField.SetRowAttrsK("foo", attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
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
}

func TestBatchQueryWithError(t *testing.T) {
	q := sampleIndex.BatchQuery()
	q.Add(sampleField.FilterFieldTopN(12, collabField.Row(7), "$invalid$", 80, 81))
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
}

func TestRangeK(t *testing.T) {
	start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
	comparePQL(t,
		"Range(collaboration='foo',1970-01-01T00:00,2000-02-02T03:04)",
		collabField.RangeK("foo", start, end))
}

func TestIntFieldOptionsToString(t *testing.T) {
	field, err := sampleIndex.Field("int-field", OptFieldInt(-10, 100))
	if err != nil {
		t.Fatal(err)
	}
	jsonString := field.options.String()
	targetString := `{"options":{"type":"int","min":-10,"max":100}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
}

func TestTimeFieldOptionsToString(t *testing.T) {
	field, err := sampleIndex.Field("time-field", OptFieldTime(TimeQuantumDayHour))
	if err != nil {
		t.Fatal(err)
	}
	jsonString := field.options.String()
	targetString := `{"options":{"type":"time","timeQuantum":"DH"}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
}

func TestInvalidFieldOption(t *testing.T) {
	_, err := sampleIndex.Field("invalid-field-opt", 1)
	if err == nil {
		t.Fatalf("should have failed")
	}
	_, err = sampleIndex.Field("invalid-field-opt", TimeQuantumDayHour, nil)
	if err == nil {
		t.Fatalf("should have failed")
	}
	_, err = sampleIndex.Field("invalid-field-opt", TimeQuantumDayHour, &FieldOptions{})
	if err == nil {
		t.Fatalf("should have failed")
	}
	_, err = sampleIndex.Field("invalid-field-opt", FieldOptionErr(0))
	if err == nil {
		t.Fatalf("should have failed")
	}
	_, err = sampleIndex.Field("invalid-field-opt", OptFieldInt(10, 9))
	if err == nil {
		t.Fatalf("should have failed")
	}
	_, err = sampleIndex.Field("invalid-field-opt", OptFieldSet(CacheTypeDefault, -1))
	if err == nil {
		t.Fatalf("should have failed")
	}
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

func comparePQL(t *testing.T, target string, q PQLQuery) {
	pql := q.serialize()
	if target != pql {
		t.Fatalf("%s != %s", target, pql)
	}
}

func mustNewIndex(schema *Schema, name string) (index *Index) {
	index, err := schema.Index(name)
	if err != nil {
		panic(err)
	}
	return
}

func mustNewField(index *Index, name string) *Field {
	var err error
	field, err := index.Field(name)
	if err != nil {
		panic(err)
	}
	return field
}

func sortedString(s string) string {
	arr := strings.Split(s, "")
	sort.Strings(arr)
	return strings.Join(arr, "")
}

func FieldOptionErr(int) FieldOption {
	return func(*FieldOptions) error {
		return errors.New("Some error")
	}
}
