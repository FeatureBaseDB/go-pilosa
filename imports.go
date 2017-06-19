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

// BitIteratorCallback gets called for each bit in a stream.
// return false to pause iteration.
type BitIteratorCallback func(bit Bit) bool

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
		reader: reader,
		line:   0,
	}
}

// Iterate iterates on all lines of a Reader.
// If the callback returns false, the iteration is paused; call it again to resume.
// Returns io.EOF on end of iteration
func (c *CSVBitIterator) Iterate(callback BitIteratorCallback) error {
	if c.scanner == nil {
		c.scanner = bufio.NewScanner(c.reader)
	}

	for c.scanner.Scan() {
		c.line++
		text := strings.TrimSpace(c.scanner.Text())
		if text != "" {
			parts := strings.Split(text, ",")
			if len(parts) < 2 {
				return fmt.Errorf("Invalid CSV line: %d", c.line)
			}
			rowID, err := strconv.Atoi(parts[0])
			if err != nil {
				return fmt.Errorf("Invalid row ID at line: %d", c.line)
			}
			columnID, err := strconv.Atoi(parts[1])
			if err != nil {
				return fmt.Errorf("Invalid column ID at line: %d", c.line)
			}
			timestamp := 0
			if len(parts) == 3 {
				timestamp, err = strconv.Atoi(parts[2])
				if err != nil {
					return fmt.Errorf("Invalid timestamp at line: %d", c.line)
				}
			}
			bit := Bit{
				RowID:     uint64(rowID),
				ColumnID:  uint64(columnID),
				Timestamp: int64(timestamp),
			}
			if ok := callback(bit); !ok {
				return nil
			}
		}
	}
	err := c.scanner.Err()
	if err != nil {
		return err
	}
	return io.EOF
}
