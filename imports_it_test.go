// +build integration

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
	"os"
	"path"
	"reflect"
	"testing"
)

func TestCSVBitIterator(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	csvFilename := path.Join(cwd, "fixture", "sample1.csv")
	file, err := os.Open(csvFilename)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	iterator := NewCSVBitIterator(file)
	if err != nil {
		t.Fatal(err)
	}
	bits := []Bit{}
	// simulates partial iteration
	iterator.Iterate(func(bit Bit) bool {
		bits = append(bits, bit)
		return len(bits) != 2
	})
	if len(bits) != 2 {
		t.Fatalf("There should be 2 bits")
	}
	iterator.Iterate(func(bit Bit) bool {
		bits = append(bits, bit)
		return true
	})
	if len(bits) != 3 {
		t.Fatalf("There should be 3 bits")
	}
	target := []Bit{
		Bit{1, 10, 683793200},
		Bit{5, 20, 683793300},
		Bit{3, 41, 683793385},
	}
	if !reflect.DeepEqual(target, bits) {
		t.Fatalf("%v != %v", target, bits)
	}
}
