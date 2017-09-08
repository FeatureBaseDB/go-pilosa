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
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

var schema = NewSchema()
var sampleIndex = mustNewIndex(schema, "sample-index", "")
var sampleFrame = mustNewFrame(sampleIndex, "sample-frame", "")
var projectIndex = mustNewIndex(schema, "project-index", "user")
var collabFrame = mustNewFrame(projectIndex, "collaboration", "project")
var b1 = sampleFrame.Bitmap(10)
var b2 = sampleFrame.Bitmap(20)
var b3 = sampleFrame.Bitmap(42)
var b4 = collabFrame.Bitmap(2)

func TestSchemaDiff(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1", nil)
	index11.Frame("frame1-1", nil)
	index11.Frame("frame1-2", nil)
	index12, _ := schema1.Index("diff-index2", nil)
	index12.Frame("frame2-1", nil)

	schema2 := NewSchema()
	index21, _ := schema2.Index("diff-index1", nil)
	index21.Frame("another-frame", nil)

	targetDiff12 := NewSchema()
	targetIndex1, _ := targetDiff12.Index("diff-index1", nil)
	targetIndex1.Frame("frame1-1", nil)
	targetIndex1.Frame("frame1-2", nil)
	targetIndex2, _ := targetDiff12.Index("diff-index2", nil)
	targetIndex2.Frame("frame2-1", nil)

	diff12 := schema1.diff(schema2)
	if !reflect.DeepEqual(targetDiff12, diff12) {
		t.Fatalf("The diff must be correctly calculated")
	}
}

func TestSchemaIndexes(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1", nil)
	index12, _ := schema1.Index("diff-index2", nil)
	indexes := schema1.Indexes()
	target := map[string]*Index{
		"diff-index1": index11,
		"diff-index2": index12,
	}
	if !reflect.DeepEqual(target, indexes) {
		t.Fatalf("calling schema.Indexes should return indexes")
	}
}

func TestNewIndex(t *testing.T) {
	index1, err := schema.Index("index-name", nil)
	if err != nil {
		t.Fatal(err)
	}
	if index1.Name() != "index-name" {
		t.Fatalf("index name was not set")
	}
	// calling schema.Index again should return the same index
	index2, err := schema.Index("index-name", nil)
	if err != nil {
		t.Fatal(err)
	}
	if index1 != index2 {
		t.Fatalf("calling schema.Index again should return the same index")
	}
}

func TestNewIndexWithInvalidName(t *testing.T) {
	_, err := schema.Index("$FOO", nil)
	if err == nil {
		t.Fatal(err)
	}
}

func TestIndexCopy(t *testing.T) {
	indexOptions := &IndexOptions{
		ColumnLabel: "columnlabel",
		TimeQuantum: TimeQuantumMonthDay,
	}
	index, err := schema.Index("my-index-4copy", indexOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := &FrameOptions{
		RowLabel: "rowlabel",
	}
	_, err = index.Frame("my-frame-4copy", options)
	if err != nil {
		t.Fatal(err)
	}
	copiedIndex := index.copy()
	if !reflect.DeepEqual(index, copiedIndex) {
		t.Fatalf("copied index should be equivalent")
	}
}

func TestIndexFrames(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1", nil)
	frame11, _ := index11.Frame("frame1-1", nil)
	frame12, _ := index11.Frame("frame1-2", nil)
	frames := index11.Frames()
	target := map[string]*Frame{
		"frame1-1": frame11,
		"frame1-2": frame12,
	}
	if !reflect.DeepEqual(target, frames) {
		t.Fatalf("calling index.Frames should return frames")
	}

}

func TestFrame(t *testing.T) {
	frame1, err := sampleIndex.Frame("nonexistent-frame", nil)
	if err != nil {
		t.Fatal(err)
	}
	frame2, err := sampleIndex.Frame("nonexistent-frame", nil)
	if err != nil {
		t.Fatal(err)
	}
	if frame1 != frame2 {
		t.Fatalf("calling index.Frame again should return the same frame")
	}
	if frame1.Name() != "nonexistent-frame" {
		t.Fatalf("calling frame.Name should return frame's name")
	}
}

func TestFrameCopy(t *testing.T) {
	options := &FrameOptions{
		RowLabel:       "rowlabel",
		TimeQuantum:    TimeQuantumMonthDayHour,
		CacheType:      CacheTypeRanked,
		CacheSize:      123456,
		InverseEnabled: true,
	}
	frame, err := sampleIndex.Frame("my-frame-4copy", options)
	if err != nil {
		t.Fatal(err)
	}
	copiedFrame := frame.copy()
	if !reflect.DeepEqual(frame, copiedFrame) {
		t.Fatalf("copied frame should be equivalent")
	}
}

func TestNewFrameWithInvalidName(t *testing.T) {
	index, err := NewIndex("foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = index.Frame("$$INVALIDFRAME$$", nil)
	if err == nil {
		t.Fatal("Creating frames with invalid row labels should fail")
	}
}

func TestBitmap(t *testing.T) {
	comparePQL(t,
		"Bitmap(rowID=5, frame='sample-frame')",
		sampleFrame.Bitmap(5))
	comparePQL(t,
		"Bitmap(project=10, frame='collaboration')",
		collabFrame.Bitmap(10))
}

func TestInverseBitmap(t *testing.T) {
	options := &FrameOptions{
		RowLabel:       "row_label",
		InverseEnabled: true,
	}
	f1, err := projectIndex.Frame("f1-inversable", options)
	if err != nil {
		t.Fatal(err)
	}
	comparePQL(t,
		"Bitmap(user=5, frame='f1-inversable')",
		f1.InverseBitmap(5))
}

func TestSetBit(t *testing.T) {
	comparePQL(t,
		"SetBit(rowID=5, frame='sample-frame', columnID=10)",
		sampleFrame.SetBit(5, 10))
	comparePQL(t,
		"SetBit(project=10, frame='collaboration', user=20)",
		collabFrame.SetBit(10, 20))
}

func TestSetBitTimestamp(t *testing.T) {
	timestamp := time.Date(2017, time.April, 24, 12, 14, 0, 0, time.UTC)
	comparePQL(t,
		"SetBit(project=10, frame='collaboration', user=20, timestamp='2017-04-24T12:14')",
		collabFrame.SetBitTimestamp(10, 20, timestamp))
}

func TestClearBit(t *testing.T) {
	comparePQL(t,
		"ClearBit(rowID=5, frame='sample-frame', columnID=10)",
		sampleFrame.ClearBit(5, 10))
	comparePQL(t,
		"ClearBit(project=10, frame='collaboration', user=20)",
		collabFrame.ClearBit(10, 20))
}

func TestSetFieldValue(t *testing.T) {
	comparePQL(t,
		"SetFieldValue(frame='collaboration', user=50, foo=15)",
		collabFrame.SetIntFieldValue(50, "foo", 15))
}

func TestUnion(t *testing.T) {
	comparePQL(t,
		"Union(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'))",
		sampleIndex.Union(b1, b2))
	comparePQL(t,
		"Union(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'), Bitmap(rowID=42, frame='sample-frame'))",
		sampleIndex.Union(b1, b2, b3))
	comparePQL(t,
		"Union(Bitmap(rowID=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
		sampleIndex.Union(b1, b4))
	comparePQL(t,
		"Union(Bitmap(rowID=10, frame='sample-frame'))",
		sampleIndex.Union(b1))
	comparePQL(t,
		"Union()",
		sampleIndex.Union())
}

func TestIntersect(t *testing.T) {
	comparePQL(t,
		"Intersect(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'))",
		sampleIndex.Intersect(b1, b2))
	comparePQL(t,
		"Intersect(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'), Bitmap(rowID=42, frame='sample-frame'))",
		sampleIndex.Intersect(b1, b2, b3))
	comparePQL(t,
		"Intersect(Bitmap(rowID=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
		sampleIndex.Intersect(b1, b4))
	comparePQL(t,
		"Intersect(Bitmap(rowID=10, frame='sample-frame'))",
		sampleIndex.Intersect(b1))
}

func TestDifference(t *testing.T) {
	comparePQL(t,
		"Difference(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'))",
		sampleIndex.Difference(b1, b2))
	comparePQL(t,
		"Difference(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'), Bitmap(rowID=42, frame='sample-frame'))",
		sampleIndex.Difference(b1, b2, b3))
	comparePQL(t,
		"Difference(Bitmap(rowID=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
		sampleIndex.Difference(b1, b4))
	comparePQL(t,
		"Difference(Bitmap(rowID=10, frame='sample-frame'))",
		sampleIndex.Difference(b1))
}

func TestXor(t *testing.T) {
	comparePQL(t,
		"Xor(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'))",
		sampleIndex.Xor(b1, b2))
	comparePQL(t,
		"Xor(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'), Bitmap(rowID=42, frame='sample-frame'))",
		sampleIndex.Xor(b1, b2, b3))
	comparePQL(t,
		"Xor(Bitmap(rowID=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
		sampleIndex.Xor(b1, b4))
}

func TestTopN(t *testing.T) {
	comparePQL(t,
		"TopN(frame='sample-frame', n=27, inverse=false)",
		sampleFrame.TopN(27))
	comparePQL(t,
		"TopN(frame='sample-frame', n=27, inverse=true)",
		sampleFrame.InverseTopN(27))
	comparePQL(t,
		"TopN(Bitmap(project=3, frame='collaboration'), frame='sample-frame', n=10, inverse=false)",
		sampleFrame.BitmapTopN(10, collabFrame.Bitmap(3)))
	comparePQL(t,
		"TopN(Bitmap(project=3, frame='collaboration'), frame='sample-frame', n=10, inverse=true)",
		sampleFrame.InverseBitmapTopN(10, collabFrame.Bitmap(3)))
	comparePQL(t,
		"TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, inverse=false, field='category', filters=[80,81])",
		sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, 81))
	comparePQL(t,
		"TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, inverse=true, field='category', filters=[80,81])",
		sampleFrame.InverseFilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, 81))
	comparePQL(t,
		"TopN(frame='sample-frame', n=12, inverse=true, field='category', filters=[80,81])",
		sampleFrame.InverseFilterFieldTopN(12, nil, "category", 80, 81))
}

func TestFilterFieldTopNInvalidField(t *testing.T) {
	q := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "$invalid$", 80, 81)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestFilterFieldTopNInvalidValue(t *testing.T) {
	q := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, func() {})
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestBitmapOperationInvalidArg(t *testing.T) {
	invalid := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "$invalid$", 80, 81)
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
	// not enough bitmaps supplied
	q = sampleIndex.Difference()
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// not enough bitmaps supplied
	q = sampleIndex.Intersect()
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}

	// not enough bitmaps supplied
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
		"SetColumnAttrs(user=5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
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
		"SetRowAttrs(project=5, frame='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
		collabFrame.SetRowAttrs(5, attrs))
}

func TestSetRowAttrsInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if collabFrame.SetRowAttrs(5, attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestAverage(t *testing.T) {
	b := collabFrame.Bitmap(42)
	comparePQL(t,
		"Average(frame='sample-frame', Bitmap(project=42, frame='collaboration'), field='foo')",
		sampleFrame.Average(b, "foo"))
}

func TestSum(t *testing.T) {
	b := collabFrame.Bitmap(42)
	comparePQL(t,
		"Sum(frame='sample-frame', Bitmap(project=42, frame='collaboration'), field='foo')",
		sampleFrame.Sum(b, "foo"))
}

func TestBatchQuery(t *testing.T) {
	q := sampleIndex.BatchQuery()
	if q.Index() != sampleIndex {
		t.Fatalf("The correct index should be assigned")
	}
	q.Add(sampleFrame.Bitmap(44))
	q.Add(sampleFrame.Bitmap(10101))
	if q.Error() != nil {
		t.Fatalf("Error should be nil")
	}
	comparePQL(t, "Bitmap(rowID=44, frame='sample-frame')Bitmap(rowID=10101, frame='sample-frame')", q)
}

func TestBatchQueryWithError(t *testing.T) {
	q := sampleIndex.BatchQuery()
	q.Add(sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "$invalid$", 80, 81))
	if q.Error() == nil {
		t.Fatalf("The error must be set")
	}
}

func TestCount(t *testing.T) {
	q := projectIndex.Count(collabFrame.Bitmap(42))
	comparePQL(t, "Count(Bitmap(project=42, frame='collaboration'))", q)
}

func TestRange(t *testing.T) {
	start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
	comparePQL(t,
		"Range(project=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
		collabFrame.Range(10, start, end))
	comparePQL(t,
		"Range(user=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
		collabFrame.InverseRange(10, start, end))
}

func TestInvalidColumnLabelFails(t *testing.T) {
	options := &IndexOptions{
		ColumnLabel: "$$INVALID$$",
	}
	_, err := NewIndex("foo", options)
	if err == nil {
		t.Fatalf("Setting invalid column label should fail")
	}

}

func TestInvalidRowLabelFails(t *testing.T) {
	options := &FrameOptions{RowLabel: "$INVALID$"}
	_, err := sampleIndex.Frame("foo", options)
	if err == nil {
		t.Fatalf("Creating frames with invalid row label should fail")
	}
}

func TestFrameOptionsToString(t *testing.T) {
	frameOptions := &FrameOptions{
		RowLabel:       "stargazer_id",
		TimeQuantum:    TimeQuantumDayHour,
		InverseEnabled: true,
		CacheType:      CacheTypeRanked,
		CacheSize:      1000,
	}
	err := frameOptions.AddIntField("foo", 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	err = frameOptions.AddIntField("bar", -1, 1)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := sampleIndex.Frame("stargazer", frameOptions)
	if err != nil {
		t.Fatal(err)
	}
	jsonString := frame.options.String()
	targetString := `{"options": {"cacheSize":1000,"cacheType":"ranked","fields":[{"max":100,"min":10,"name":"foo","type":"int"},{"max":1,"min":-1,"name":"bar","type":"int"}],"inverseEnabled":true,"rangeEnabled":true,"rowLabel":"stargazer_id","timeQuantum":"DH"}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
}

func TestAddInvalidField(t *testing.T) {
	frameOptions := &FrameOptions{}
	err := frameOptions.AddIntField("?invalid field!", 0, 100)
	if err == nil {
		t.Fatalf("Adding a field with an invalid name should have failed")
	}
	err = frameOptions.AddIntField("valid", 10, 10)
	if err == nil {
		t.Fatalf("Adding a field with max <= min should have failed")
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
	if pql != target {
		t.Fatalf("%s != %s", pql, target)
	}
}

func mustNewIndex(schema *Schema, name string, columnLabel string) (index *Index) {
	var options *IndexOptions
	if columnLabel != "" {
		options = &IndexOptions{ColumnLabel: columnLabel}
	} else {
		options = &IndexOptions{}
	}
	index, err := schema.Index(name, options)
	if err != nil {
		panic(err)
	}
	return
}

func mustNewFrame(index *Index, name string, rowLabel string) (frame *Frame) {
	var err error
	var options *FrameOptions
	if rowLabel != "" {
		options = &FrameOptions{RowLabel: rowLabel}
		if err != nil {
			panic(err)
		}
		frame, err = index.Frame(name, options)
	} else {
		frame, err = index.Frame(name, nil)
	}
	if err != nil {
		panic(err)
	}
	return
}

func sortedString(s string) string {
	arr := strings.Split(s, "")
	sort.Strings(arr)
	return strings.Join(arr, "")
}
