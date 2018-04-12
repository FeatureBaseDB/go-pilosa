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

	"github.com/pkg/errors"
)

type RowContainer interface {
	Int64Field(index int) int64
	Uint64Field(index int) uint64
	StringField(index int) string
	Less(other RowContainer) bool
}

type rowContainerSort []RowContainer

func (rc rowContainerSort) Len() int {
	return len(rc)
}

func (rc rowContainerSort) Swap(i, j int) {
	rc[i], rc[j] = rc[j], rc[i]
}

func (rc rowContainerSort) Less(i, j int) bool {
	return rc[i].Less(rc[j])
}

type RowIterator interface {
	NextRow() (RowContainer, error)
}

// Bit defines a single Pilosa bit.
type Bit struct {
	RowID     uint64
	ColumnID  uint64
	Timestamp int64
}

func (b Bit) Int64Field(index int) int64 {
	switch index {
	case 0:
		return int64(b.RowID)
	case 1:
		return int64(b.ColumnID)
	case 2:
		return b.Timestamp
	default:
		return 0
	}
}

func (b Bit) Uint64Field(index int) uint64 {
	switch index {
	case 0:
		return b.RowID
	case 1:
		return b.ColumnID
	case 2:
		return uint64(b.Timestamp)
	default:
		return 0
	}
}

func (b Bit) StringField(index int) string {
	return ""
}

func (b Bit) Less(other RowContainer) bool {
	if ob, ok := other.(Bit); ok {
		if b.RowID < ob.RowID {
			return true
		} else if b.RowID > ob.RowID {
			return false
		}
		return b.ColumnID < ob.ColumnID
	}
	return false
}

func BitCSVUnmarshaller() CSVRowUnmarshaller {
	return BitCSVUnmarshallerWithTimestamp("")
}

func BitCSVUnmarshallerWithTimestamp(timestampFormat string) CSVRowUnmarshaller {
	return func(text string) (RowContainer, error) {
		parts := strings.Split(text, ",")
		if len(parts) < 2 {
			return nil, errors.New("Invalid CSV line")
		}
		rowID, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, errors.New("Invalid row ID")
		}
		columnID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, errors.New("Invalid column ID")
		}
		timestamp := 0
		if len(parts) == 3 {
			if timestampFormat == "" {
				timestamp, err = strconv.Atoi(parts[2])
				if err != nil {
					return nil, err
				}
			} else {
				t, err := time.Parse(timestampFormat, parts[2])
				if err != nil {
					return nil, err
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
}

/*
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
*/

/*
// BitIterator structs return bits one by one.
type BitIterator interface {
	NextBit() (Bit, error)
}
*/

type CSVRowUnmarshaller func(text string) (RowContainer, error)

// CSVBitIterator reads rows from a Reader.
// Each line should contain a single row in the following form:
// field1,field2,...
type CSVIterator struct {
	reader       io.Reader
	line         int
	scanner      *bufio.Scanner
	unmarshaller CSVRowUnmarshaller
}

// NewCSVIterator creates a CSVIterator from a Reader.
func NewCSVIterator(reader io.Reader, unmarshaller CSVRowUnmarshaller) *CSVIterator {
	return &CSVIterator{
		reader:       reader,
		line:         0,
		scanner:      bufio.NewScanner(reader),
		unmarshaller: unmarshaller,
	}
}

func NewCSVBitIterator(reader io.Reader) *CSVIterator {
	return NewCSVIterator(reader, BitCSVUnmarshaller())
}

func NewCSVBitIteratorWithTimestampFormat(reader io.Reader, timestampFormat string) *CSVIterator {
	return NewCSVIterator(reader, BitCSVUnmarshallerWithTimestamp(timestampFormat))
}

func NewCSVValueIterator(reader io.Reader) *CSVIterator {
	return NewCSVIterator(reader, FieldValueCSVUnmarshaller)
}

// NextRow iterates on lines of a Reader.
// Returns io.EOF on end of iteration.
func (c *CSVIterator) NextRow() (RowContainer, error) {
	if ok := c.scanner.Scan(); ok {
		c.line++
		text := strings.TrimSpace(c.scanner.Text())
		if text != "" {
			rc, err := c.unmarshaller(text)
			if err != nil {
				return nil, fmt.Errorf("%s at line: %d", err.Error(), c.line)
			}
			return rc, nil
		}
	}
	err := c.scanner.Err()
	if err != nil {
		return nil, err
	}
	return nil, io.EOF
}

/*
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
*/

// FieldValue represents the value for a column within a
// range-encoded frame.
type FieldValue struct {
	ColumnID  uint64
	ColumnKey string
	Value     int64
}

func (f FieldValue) Int64Field(index int) int64 {
	switch index {
	case 0:
		return int64(f.ColumnID)
	case 1:
		return f.Value
	default:
		return 0
	}
}

func (f FieldValue) Uint64Field(index int) uint64 {
	switch index {
	case 0:
		return f.ColumnID
	case 1:
		return uint64(f.Value)
	default:
		return 0
	}
}

func (f FieldValue) StringField(index int) string {
	switch index {
	case 0:
		return f.ColumnKey
	default:
		return ""
	}
}

func (v FieldValue) Less(other RowContainer) bool {
	if ov, ok := other.(FieldValue); ok {
		return v.ColumnID < ov.ColumnID
	}
	return false
}

func FieldValueCSVUnmarshaller(text string) (RowContainer, error) {
	parts := strings.Split(text, ",")
	if len(parts) < 2 {
		return nil, errors.New("Invalid CSV")
	}
	columnID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, errors.New("Invalid column ID at line: %d")
	}
	value, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, errors.New("Invalid value")
	}
	fieldValue := FieldValue{
		ColumnID: uint64(columnID),
		Value:    value,
	}
	return fieldValue, nil
}

/*

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
*/
