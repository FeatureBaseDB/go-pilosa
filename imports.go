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
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Bit defines a single Pilosa bit.
type Bit struct {
	RowID     uint64
	ColumnID  uint64
	Timestamp int64
}

// BitIterator structs return bits one by one.
type BitIterator interface {
	NextBit() (Bit, error)
}

// CSVBitIterator reads bits from a Reader.
// Each line should contain a single bit in the following form:
// rowID,columnID[,timestamp]
type CSVBitIterator struct {
	reader  io.Reader
	line    int
	scanner *bufio.Scanner
}

// NewCSVBitIterator creates a CSVBitIterator from a Reader.
func NewCSVBitIterator(reader io.Reader) *CSVBitIterator {
	return &CSVBitIterator{
		reader:  reader,
		line:    0,
		scanner: bufio.NewScanner(reader),
	}
}

// NextBit iterates on lines of a Reader.
// Returns io.EOF on end of iteration
func (c *CSVBitIterator) NextBit() (Bit, error) {
	if ok := c.scanner.Scan(); ok {
		c.line++
		text := strings.TrimSpace(c.scanner.Text())
		parts := strings.Split(text, ",")
		if len(parts) < 2 {
			return Bit{}, fmt.Errorf("Invalid CSV line: %d", c.line)
		}
		rowID, err := strconv.Atoi(parts[0])
		if err != nil {
			return Bit{}, fmt.Errorf("Invalid row ID at line: %d", c.line)
		}
		columnID, err := strconv.Atoi(parts[1])
		if err != nil {
			return Bit{}, fmt.Errorf("Invalid column ID at line: %d", c.line)
		}
		timestamp := 0
		if len(parts) == 3 {
			timestamp, err = strconv.Atoi(parts[2])
			if err != nil {
				return Bit{}, fmt.Errorf("Invalid timestamp at line: %d", c.line)
			}
		}
		bit := Bit{
			RowID:     uint64(rowID),
			ColumnID:  uint64(columnID),
			Timestamp: int64(timestamp),
		}
		return bit, nil
	}
	err := c.scanner.Err()
	if err != nil {
		return Bit{}, err
	}
	return Bit{}, io.EOF

}

type bitsForSort []Bit

func (b bitsForSort) Len() int {
	return len(b)
}

func (b bitsForSort) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b bitsForSort) Less(i, j int) bool {
	bitCmp := b[i].RowID - b[j].RowID
	if bitCmp == 0 {
		return b[i].ColumnID < b[j].ColumnID
	}
	return bitCmp < 0
}
