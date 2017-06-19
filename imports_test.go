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
	"strings"
	"testing"

	"errors"

	pilosa "github.com/pilosa/go-pilosa"
)

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
		err := iterator.Iterate(func(b pilosa.Bit) bool { return true })
		if err == nil {
			t.Fatalf("CSVBitIterator input: %s should fail", text)
		}
	}
}

func TestCSVBitIteratorError(t *testing.T) {
	iterator := pilosa.NewCSVBitIterator(&BrokenReader{})
	err := iterator.Iterate(func(b pilosa.Bit) bool { return true })
	if err == nil {
		t.Fatal("CSVBitIterator should fail with error")
	}
}

type BrokenReader struct{}

func (r BrokenReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("broken reader")
}
