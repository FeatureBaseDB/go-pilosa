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
	"testing"

	"github.com/pilosa/go-pilosa/internal"
)

func TestNewBitmapResultFromInternal(t *testing.T) {
	targetAttrs := map[string]interface{}{
		"name":       "some string",
		"age":        int64(95),
		"registered": true,
		"height":     1.83,
	}
	targetBits := []uint64{5, 10}
	attrs := []*internal.Attr{
		&internal.Attr{Key: "name", StringValue: "some string", Type: 1},
		&internal.Attr{Key: "age", IntValue: 95, Type: 2},
		&internal.Attr{Key: "registered", BoolValue: true, Type: 3},
		&internal.Attr{Key: "height", FloatValue: 1.83, Type: 4},
	}
	bitmap := &internal.Bitmap{
		Attrs: attrs,
		Bits:  []uint64{5, 10},
	}
	result, err := newBitmapResultFromInternal(bitmap)
	if err != nil {
		t.Fatalf("Failed with error: %s", err)
	}
	// assertMapEquals(t, targetAttrs, result.Attributes)
	if !reflect.DeepEqual(targetAttrs, result.Attributes) {
		t.Fatal()
	}
	if !reflect.DeepEqual(targetBits, result.Bits) {
		t.Fatal()
	}
}

func TestNewQueryResponseFromInternal(t *testing.T) {
	targetAttrs := map[string]interface{}{
		"name":       "some string",
		"age":        int64(95),
		"registered": true,
		"height":     1.83,
	}
	targetBits := []uint64{5, 10}
	targetCountItems := []*CountResultItem{
		&CountResultItem{ID: 10, Count: 100},
	}
	attrs := []*internal.Attr{
		&internal.Attr{Key: "name", StringValue: "some string", Type: 1},
		&internal.Attr{Key: "age", IntValue: 95, Type: 2},
		&internal.Attr{Key: "registered", BoolValue: true, Type: 3},
		&internal.Attr{Key: "height", FloatValue: 1.83, Type: 4},
	}
	bitmap := &internal.Bitmap{
		Attrs: attrs,
		Bits:  []uint64{5, 10},
	}
	pairs := []*internal.Pair{
		&internal.Pair{Key: 10, Count: 100},
	}
	response := &internal.QueryResponse{
		Results: []*internal.QueryResult{
			&internal.QueryResult{Bitmap: bitmap},
			&internal.QueryResult{Pairs: pairs},
		},
		Err: "",
	}
	qr, err := newQueryResponseFromInternal(response)
	if err != nil {
		t.Fatalf("Failed with error: %s", err)
	}
	if qr.ErrorMessage != "" {
		t.Fatalf("ErrorMessage should be empty")
	}
	if !qr.Success {
		t.Fatalf("IsSuccess should be true")
	}

	results := qr.Results()
	if len(results) != 2 {
		t.Fatalf("Number of results should be 2")
	}
	if results[0] != qr.Result() {
		t.Fatalf("Result() should return the first result")
	}
	if !reflect.DeepEqual(targetAttrs, results[0].Bitmap.Attributes) {
		t.Fatalf("The bitmap result should contain the attributes")
	}
	if !reflect.DeepEqual(targetBits, results[0].Bitmap.Bits) {
		t.Fatalf("The bitmap result should contain the bits")
	}
	if !reflect.DeepEqual(targetCountItems, results[1].CountItems) {
		t.Fatalf("The response should include count items")
	}
}

func TestNewQueryResponseWithErrorFromInternal(t *testing.T) {
	response := &internal.QueryResponse{
		Err: "some error",
	}
	qr, err := newQueryResponseFromInternal(response)
	if err != nil {
		t.Fatalf("Failed with error: %s", err)
	}
	if qr.ErrorMessage != "some error" {
		t.Fatalf("The response should include the error message")
	}
	if qr.Success {
		t.Fatalf("IsSuccess should be false")
	}
	if qr.Result() != nil {
		t.Fatalf("If there are no results, Result should return nil")
	}
}

func TestNewQueryResponseFromInternalFailure(t *testing.T) {
	attrs := []*internal.Attr{
		&internal.Attr{Key: "name", StringValue: "some string", Type: 99},
	}
	bitmap := &internal.Bitmap{
		Attrs: attrs,
	}
	response := &internal.QueryResponse{
		Results: []*internal.QueryResult{&internal.QueryResult{Bitmap: bitmap}},
	}
	qr, err := newQueryResponseFromInternal(response)
	if qr != nil && err == nil {
		t.Fatalf("Should have failed")
	}
	response = &internal.QueryResponse{
		ColumnAttrSets: []*internal.ColumnAttrSet{&internal.ColumnAttrSet{ID: 1, Attrs: attrs}},
	}
	qr, err = newQueryResponseFromInternal(response)
	if qr != nil && err == nil {
		t.Fatalf("Should have failed")
	}

}
