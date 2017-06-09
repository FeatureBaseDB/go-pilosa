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
	"testing"
	"time"
)

var sampleIndex = mustNewIndex("sample-index", "")
var sampleFrame = mustNewFrame(sampleIndex, "sample-frame", "")
var projectIndex = mustNewIndex("project-index", "user")
var collabFrame = mustNewFrame(projectIndex, "collaboration", "project")
var b1 = sampleFrame.Bitmap(10)
var b2 = sampleFrame.Bitmap(20)
var b3 = sampleFrame.Bitmap(42)
var b4 = collabFrame.Bitmap(2)

func TestNewIndex(t *testing.T) {
	index, err := NewIndex("index-name", nil)
	if err != nil {
		t.Fatal(err)
	}
	if index.Name() != "index-name" {
		t.Fatalf("index name was not set")
	}
}

func TestNewIndexWithInvalidName(t *testing.T) {
	_, err := NewIndex("$FOO", nil)
	if err == nil {
		t.Fatal()
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
		"TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, inverse=false, field='category', [80,81])",
		sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, 81))
	comparePQL(t,
		"TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, inverse=true, field='category', [80,81])",
		sampleFrame.InverseFilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, 81))
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
		"Range(project=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04', inverse=false)",
		collabFrame.Range(10, start, end))
	comparePQL(t,
		"Range(project=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04', inverse=true)",
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
	frame, err := sampleIndex.Frame("stargazer", frameOptions)
	if err != nil {
		t.Fatal(err)
	}
	jsonString := frame.options.String()
	targetString := `{"options": {"cacheSize":1000,"cacheType":"ranked","inverseEnabled":true,"rowLabel":"stargazer_id","timeQuantum":"DH"}}`
	if targetString != jsonString {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
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

func mustNewIndex(name string, columnLabel string) (index *Index) {
	var err error
	var options *IndexOptions
	if columnLabel != "" {
		options = &IndexOptions{ColumnLabel: columnLabel}
		if err != nil {
			panic(err)
		}
		index, err = NewIndex(name, options)
	} else {
		index, err = NewIndex(name, nil)
	}
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
