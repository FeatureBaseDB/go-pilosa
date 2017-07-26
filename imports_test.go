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
	"reflect"
	"strings"
	"testing"

	pilosa "github.com/pilosa/go-pilosa"
)

func TestCSVBitIterator(t *testing.T) {
	iterator := pilosa.NewCSVBitIterator(strings.NewReader(`1,10,683793200
		5,20,683793300
		3,41,683793385
	`))
	bits := []pilosa.Bit{}
	for {
		bit, err := iterator.NextBit()
		if err != nil {
			break
		}
		bits = append(bits, bit)
	}
	if len(bits) != 3 {
		t.Fatalf("There should be 3 bits")
	}
	target := []pilosa.Bit{
		pilosa.Bit{RowID: 1, ColumnID: 10, Timestamp: 683793200},
		pilosa.Bit{RowID: 5, ColumnID: 20, Timestamp: 683793300},
		pilosa.Bit{RowID: 3, ColumnID: 41, Timestamp: 683793385},
	}
	if !reflect.DeepEqual(target, bits) {
		t.Fatalf("%v != %v", target, bits)
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
		_, err := iterator.NextBit()
		if err == nil {
			t.Fatalf("CSVBitIterator input: %s should fail", text)
		}
	}
}

func TestCSVBitIteratorError(t *testing.T) {
	iterator := pilosa.NewCSVBitIterator(&BrokenReader{})
	_, err := iterator.NextBit()
	if err == nil {
		t.Fatal("CSVBitIterator should fail with error")
	}
}

type BrokenReader struct{}

func (r BrokenReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("broken reader")
}
