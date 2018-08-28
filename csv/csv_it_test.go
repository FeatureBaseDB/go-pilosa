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

// build integration

package csv_test

import (
	"io"
	"reflect"
	"strings"
	"testing"

	pilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/go-pilosa/csv"
)

func TestCSVIterate(t *testing.T) {
	text := `10,7
		10,5
		2,3
		7,1`
	iterator := csv.NewColumnIterator(csv.RowIDColumnID, strings.NewReader(text))
	recs := consumeIterator(t, iterator)
	target := []pilosa.Record{
		pilosa.Column{RowID: 10, ColumnID: 7},
		pilosa.Column{RowID: 10, ColumnID: 5},
		pilosa.Column{RowID: 2, ColumnID: 3},
		pilosa.Column{RowID: 7, ColumnID: 1},
	}
	if !reflect.DeepEqual(target, recs) {
		t.Fatalf("%v != %v", target, recs)
	}
}

func consumeIterator(t *testing.T, it *csv.Iterator) []pilosa.Record {
	recs := []pilosa.Record{}
	for {
		r, err := it.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r)
	}
	return recs
}
