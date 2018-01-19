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
	"time"
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
	reader          io.Reader
	line            int
	scanner         *bufio.Scanner
	timestampFormat string
}

// NewCSVBitIterator creates a CSVBitIterator from a Reader.
func NewCSVBitIterator(reader io.Reader) *CSVBitIterator {
	return &CSVBitIterator{
		reader:  reader,
		line:    0,
		scanner: bufio.NewScanner(reader),
	}
}

// NewCSVBitIteratorWithTimestampFormat creates a CSVBitIterator from a Reader with a custom timestamp format.
func NewCSVBitIteratorWithTimestampFormat(reader io.Reader, timestampFormat string) *CSVBitIterator {
	return &CSVBitIterator{
		reader:          reader,
		line:            0,
		scanner:         bufio.NewScanner(reader),
		timestampFormat: timeFormat,
	}
}

// NextBit iterates on lines of a Reader.
// Returns io.EOF on end of iteration.
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
			if c.timestampFormat == "" {
				timestamp, err = strconv.Atoi(parts[2])
				if err != nil {
					return Bit{}, fmt.Errorf("Invalid timestamp at line: %d", c.line)
				}
			} else {
				t, err := time.Parse(c.timestampFormat, parts[2])
				if err != nil {
					return Bit{}, fmt.Errorf("Invalid timestamp at line: %d", c.line)
				}
				timestamp = int(t.Unix())
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

// BitK defines a single Pilosa bitK.
type BitK struct {
	RowKey    string
	ColumnKey string
	Timestamp int64
}

// BitKIterator structs return bitKs one by one.
type BitKIterator interface {
	NextBitK() (BitK, error)
}

// CSVBitKIterator reads bits from a Reader.
// Each line should contain a single bitK in the following form:
// rowKey,columnKey[,timestamp]
type CSVBitKIterator struct {
	reader          io.Reader
	line            int
	scanner         *bufio.Scanner
	timestampFormat string
}

// NewCSVBitKIterator creates a CSVBitKIterator from a Reader.
func NewCSVBitKIterator(reader io.Reader) *CSVBitKIterator {
	return &CSVBitKIterator{
		reader:  reader,
		line:    0,
		scanner: bufio.NewScanner(reader),
	}
}

// NewCSVBitKIteratorWithTimestampFormat creates a CSVBitKIterator from a Reader with a custom timestamp format.
func NewCSVBitKIteratorWithTimestampFormat(reader io.Reader, timestampFormat string) *CSVBitKIterator {
	return &CSVBitKIterator{
		reader:          reader,
		line:            0,
		scanner:         bufio.NewScanner(reader),
		timestampFormat: timeFormat,
	}
}

// NextBitK iterates on lines of a Reader.
// Returns io.EOF on end of iteration.
func (c *CSVBitKIterator) NextBitK() (BitK, error) {
	if ok := c.scanner.Scan(); ok {
		c.line++
		text := strings.TrimSpace(c.scanner.Text())
		parts := strings.Split(text, ",")
		if len(parts) < 2 {
			return BitK{}, fmt.Errorf("Invalid CSV line: %d", c.line)
		}
		if parts[0] == "" {
			return BitK{}, fmt.Errorf("Invalid row Key at line: %d", c.line)
		}
		rowKey := parts[0]
		if parts[1] == "" {
			return BitK{}, fmt.Errorf("Invalid column Key at line: %d", c.line)
		}
		columnKey := parts[1]
		timestamp := 0
		var err error
		if len(parts) == 3 {
			if c.timestampFormat == "" {
				timestamp, err = strconv.Atoi(parts[2])
				if err != nil {
					return BitK{}, fmt.Errorf("Invalid timestamp at line: %d", c.line)
				}
			} else {
				t, err := time.Parse(c.timestampFormat, parts[2])
				if err != nil {
					return BitK{}, fmt.Errorf("Invalid timestamp at line: %d", c.line)
				}
				timestamp = int(t.Unix())
			}
		}
		bitK := BitK{
			RowKey:    rowKey,
			ColumnKey: columnKey,
			Timestamp: int64(timestamp),
		}
		return bitK, nil
	}
	err := c.scanner.Err()
	if err != nil {
		return BitK{}, err
	}
	return BitK{}, io.EOF
}

// FieldValue represents the value for a column within a
// range-encoded frame.
type FieldValue struct {
	ColumnID uint64
	Value    int64
}

// ValueIterator structs return field values one by one.
type ValueIterator interface {
	NextValue() (FieldValue, error)
}

// CSVValueIterator reads field values from a Reader.
// Each line should contain a single field value in the following form:
// columnID,value
type CSVValueIterator struct {
	reader  io.Reader
	line    int
	scanner *bufio.Scanner
}

// NewCSVValueIterator creates a CSVValueIterator from a Reader.
func NewCSVValueIterator(reader io.Reader) *CSVValueIterator {
	return &CSVValueIterator{
		reader:  reader,
		line:    0,
		scanner: bufio.NewScanner(reader),
	}
}

// NextValue iterates on lines of a Reader.
// Returns io.EOF on end of iteration.
func (c *CSVValueIterator) NextValue() (FieldValue, error) {
	if ok := c.scanner.Scan(); ok {
		c.line++
		text := strings.TrimSpace(c.scanner.Text())
		parts := strings.Split(text, ",")
		if len(parts) < 2 {
			return FieldValue{}, fmt.Errorf("Invalid CSV line: %d", c.line)
		}
		columnID, err := strconv.Atoi(parts[0])
		if err != nil {
			return FieldValue{}, fmt.Errorf("Invalid column ID at line: %d", c.line)
		}
		value, err := strconv.Atoi(parts[1])
		if err != nil {
			return FieldValue{}, fmt.Errorf("Invalid value at line: %d", c.line)
		}
		fieldValue := FieldValue{
			ColumnID: uint64(columnID),
			Value:    int64(value),
		}
		return fieldValue, nil
	}
	err := c.scanner.Err()
	if err != nil {
		return FieldValue{}, err
	}
	return FieldValue{}, io.EOF
}

type valsForSort []FieldValue

func (v valsForSort) Len() int {
	return len(v)
}

func (v valsForSort) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func (v valsForSort) Less(i, j int) bool {
	return v[i].ColumnID < v[j].ColumnID
}

// FieldValueK represents the valueK for a column within a
// range-encoded frame with a string column key.
type FieldValueK struct {
	ColumnKey string
	Value     int64
}

// ValueKIterator structs return fieldK values one by one.
type ValueKIterator interface {
	NextValueK() (FieldValueK, error)
}

// CSVValueKIterator reads field valueKs from a Reader.
// Each line should contain a single field valueK in the following form:
// columnKey,value
type CSVValueKIterator struct {
	reader  io.Reader
	line    int
	scanner *bufio.Scanner
}

// NewCSVValueKIterator creates a CSVValueKIterator from a Reader.
func NewCSVValueKIterator(reader io.Reader) *CSVValueKIterator {
	return &CSVValueKIterator{
		reader:  reader,
		line:    0,
		scanner: bufio.NewScanner(reader),
	}
}

// NextValueK iterates on lines of a Reader.
// Returns io.EOF on end of iteration.
func (c *CSVValueKIterator) NextValueK() (FieldValueK, error) {
	if ok := c.scanner.Scan(); ok {
		c.line++
		text := strings.TrimSpace(c.scanner.Text())
		parts := strings.Split(text, ",")
		if len(parts) < 2 {
			return FieldValueK{}, fmt.Errorf("Invalid CSV line: %d", c.line)
		}
		if parts[0] == "" {
			return FieldValueK{}, fmt.Errorf("Invalid column Key at line: %d", c.line)
		}
		columnKey := parts[0]
		value, err := strconv.Atoi(parts[1])
		if err != nil {
			return FieldValueK{}, fmt.Errorf("Invalid value at line: %d", c.line)
		}
		fieldValueK := FieldValueK{
			ColumnKey: columnKey,
			Value:     int64(value),
		}
		return fieldValueK, nil
	}
	err := c.scanner.Err()
	if err != nil {
		return FieldValueK{}, err
	}
	return FieldValueK{}, io.EOF
}
