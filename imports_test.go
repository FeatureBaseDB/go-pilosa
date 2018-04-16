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

package pilosa_test

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	pilosa "github.com/pilosa/go-pilosa"
)

func TestCSVBitIterator(t *testing.T) {
	reader := strings.NewReader(`1,10,683793200
		5,20,683793300
		3,41,683793385`)
	iterator := pilosa.NewCSVBitIterator(reader)
	bits := []pilosa.Record{}
	for {
		bit, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		bits = append(bits, bit)
	}
	if len(bits) != 3 {
		t.Fatalf("There should be 3 bits")
	}
	target := []pilosa.Bit{
		{RowID: 1, ColumnID: 10, Timestamp: 683793200},
		{RowID: 5, ColumnID: 20, Timestamp: 683793300},
		{RowID: 3, ColumnID: 41, Timestamp: 683793385},
	}
	for i := range target {
		if !reflect.DeepEqual(target[i], bits[i]) {
			t.Fatalf("%v != %v", target[i], bits[i])
		}
	}
}

func TestCSVBitIteratorWithTimestampFormat(t *testing.T) {
	format := "2006-01-02T04:05"
	reader := strings.NewReader(`1,10,1991-09-02T09:33
		5,20,1991-09-02T09:35
		3,41,1991-09-02T09:36`)
	iterator := pilosa.NewCSVBitIteratorWithTimestampFormat(reader, format)
	bits := []pilosa.Record{}
	for {
		bit, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		bits = append(bits, bit)
	}
	target := []pilosa.Bit{
		{RowID: 1, ColumnID: 10, Timestamp: 683770173},
		{RowID: 5, ColumnID: 20, Timestamp: 683770175},
		{RowID: 3, ColumnID: 41, Timestamp: 683770176},
	}
	if len(bits) != len(target) {
		t.Fatalf("There should be %d bits", len(target))
	}
	for i := range target {
		if !reflect.DeepEqual(target[i], bits[i]) {
			t.Fatalf("%v != %v", target[i], bits[i])
		}
	}
}

func TestCSVBitIteratorWithTimestampFormatFail(t *testing.T) {
	format := "2014-07-16"
	reader := strings.NewReader(`1,10,X`)
	iterator := pilosa.NewCSVBitIteratorWithTimestampFormat(reader, format)
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestCSVValueIterator(t *testing.T) {
	reader := strings.NewReader(`1,10
		5,-20
		3,41
	`)
	iterator := pilosa.NewCSVValueIterator(reader)
	values := []pilosa.Record{}
	for {
		value, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	target := []pilosa.FieldValue{
		{ColumnID: 1, Value: 10},
		{ColumnID: 5, Value: -20},
		{ColumnID: 3, Value: 41},
	}
	if len(values) != len(target) {
		t.Fatalf("There should be %d values, got %d", len(target), len(values))
	}
	for i := range target {
		if !reflect.DeepEqual(values[i], target[i]) {
			t.Fatalf("%v != %v", target[i], values[i])
		}
	}
}

func TestCSVBitIteratorInvalidInput(t *testing.T) {
	invalidInputs := []string{
		// less than 2 columns
		"155",
		// invalid row ID
		"a5,155",
		// invalid column ID
		"155,a5",
		// invalid timestamp
		"155,255,a5",
	}
	for _, text := range invalidInputs {
		iterator := pilosa.NewCSVBitIterator(strings.NewReader(text))
		_, err := iterator.NextRecord()
		if err == nil {
			t.Fatalf("CSVBitIterator input: %s should fail", text)
		}
	}
}

func TestCSVValueIteratorInvalidInput(t *testing.T) {
	invalidInputs := []string{
		// less than 2 columns
		"155",
		// invalid column ID
		"a5,155",
		// invalid value
		"155,a5",
	}
	for _, text := range invalidInputs {
		iterator := pilosa.NewCSVValueIterator(strings.NewReader(text))
		_, err := iterator.NextRecord()
		if err == nil {
			t.Fatalf("CSVValueIterator input: %s should fail", text)
		}
	}
}

func TestCSVBitIteratorError(t *testing.T) {
	iterator := pilosa.NewCSVBitIterator(&BrokenReader{})
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatal("CSVBitIterator should fail with error")
	}
}

func TestCSVValueIteratorError(t *testing.T) {
	iterator := pilosa.NewCSVValueIterator(&BrokenReader{})
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatal("CSVValueIterator should fail with error")
	}
}

type BrokenReader struct{}

func (r BrokenReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("broken reader")
}

func TestBitInt64Field(t *testing.T) {
	b := pilosa.Bit{RowID: 15, ColumnID: 55, Timestamp: 100101}
	target := []int64{15, 55, 100101, 0}
	checkInt64Record(t, target, b)
}

func TestBitUint64Field(t *testing.T) {
	b := pilosa.Bit{RowID: 15, ColumnID: 55, Timestamp: 100101}
	target := []uint64{15, 55, 100101, 0}
	checkUint64Record(t, target, b)
}

func TestBitStringField(t *testing.T) {
	b := pilosa.Bit{}
	target := []string{""}
	checkStringRecord(t, target, b)
}

func TestBitLess(t *testing.T) {
	a := pilosa.Bit{RowID: 10, ColumnID: 200}
	a2 := pilosa.Bit{RowID: 10, ColumnID: 1000}
	b := pilosa.Bit{RowID: 200, ColumnID: 10}
	c := pilosa.FieldValue{ColumnID: 1}
	if !a.Less(a2) {
		t.Fatalf("%v should be less than %v", a, a2)
	}
	if !a.Less(b) {
		t.Fatalf("%v should be less than %v", a, b)
	}
	if b.Less(a) {
		t.Fatalf("%v should not be less than %v", b, a)
	}
	if c.Less(a) {
		t.Fatalf("%v should not be less than %v", c, a)
	}
}

func TestFieldValueInt64Field(t *testing.T) {
	b := pilosa.FieldValue{ColumnID: 55, Value: 125}
	target := []int64{55, 125, 0}
	checkInt64Record(t, target, b)
}

func TestFieldValueUint64Field(t *testing.T) {
	b := pilosa.FieldValue{ColumnID: 55, Value: 125}
	target := []uint64{55, 125, 0}
	checkUint64Record(t, target, b)
}

func TestFieldValueStringField(t *testing.T) {
	b := pilosa.FieldValue{ColumnKey: "abc", Value: 125}
	target := []string{"abc", ""}
	checkStringRecord(t, target, b)
}

func TestFieldValueLess(t *testing.T) {
	a := pilosa.FieldValue{ColumnID: 55, Value: 125}
	b := pilosa.FieldValue{ColumnID: 100, Value: 125}
	c := pilosa.Bit{ColumnID: 1, RowID: 2}
	if !a.Less(b) {
		t.Fatalf("%v should be less than %v", a, b)
	}
	if b.Less(a) {
		t.Fatalf("%v should not be less than %v", b, a)
	}
	if c.Less(a) {
		t.Fatalf("%v should not be less than %v", c, a)
	}
}

func checkInt64Record(t *testing.T, target []int64, rc pilosa.Record) {
	for i := range target {
		value := rc.Int64Field(i)
		if target[i] != value {
			t.Fatalf("%d != %d", target[i], value)
		}
	}
}

func checkUint64Record(t *testing.T, target []uint64, rc pilosa.Record) {
	for i := range target {
		value := rc.Uint64Field(i)
		if target[i] != value {
			t.Fatalf("%d != %d", target[i], value)
		}
	}
}

func checkStringRecord(t *testing.T, target []string, rc pilosa.Record) {
	for i := range target {
		value := rc.StringField(i)
		if target[i] != value {
			t.Fatalf("%s != %s", target[i], value)
		}
	}
}
