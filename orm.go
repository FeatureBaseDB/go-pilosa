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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const timeFormat = "2006-01-02T15:04"

// PQLQuery is a interface for PQL queries
type PQLQuery interface {
	Index() *Index
	serialize() string
	Error() error
}

// PQLBaseQuery is the base implementation for IPqlQuery
type PQLBaseQuery struct {
	index *Index
	pql   string
	err   error
}

// NewPQLBaseQuery creates a new PqlQuery with the given PQL and index
func NewPQLBaseQuery(pql string, index *Index, err error) *PQLBaseQuery {
	return &PQLBaseQuery{
		index: index,
		pql:   pql,
		err:   err,
	}
}

// Index returns the index for this query
func (q *PQLBaseQuery) Index() *Index {
	return q.index
}

// serialize converts this query to string
func (q *PQLBaseQuery) serialize() string {
	return q.pql
}

// Error returns the error or nil for this query
func (q PQLBaseQuery) Error() error {
	return q.err
}

// PQLBitmapQuery is the return type for bitmap queries
type PQLBitmapQuery struct {
	index *Index
	pql   string
	err   error
}

// Index returns the index for this query
func (q *PQLBitmapQuery) Index() *Index {
	return q.index
}

// serialize converts this query to string
func (q *PQLBitmapQuery) serialize() string {
	return q.pql
}

// Error returns the error or nil for this query
func (q PQLBitmapQuery) Error() error {
	return q.err
}

// PQLBatchQuery contains several queries to increase throughput
type PQLBatchQuery struct {
	index   *Index
	queries []string
	err     error
}

// Index returns the index for this query
func (q *PQLBatchQuery) Index() *Index {
	return q.index
}

func (q *PQLBatchQuery) serialize() string {
	return strings.Join(q.queries, "")
}

func (q *PQLBatchQuery) Error() error {
	return q.err
}

// Add adds a query to the batch
func (q *PQLBatchQuery) Add(query PQLQuery) {
	err := query.Error()
	if err != nil {
		q.err = err
	}
	q.queries = append(q.queries, query.serialize())
}

// IndexOptions contains the options for a Pilosa index
type IndexOptions struct {
	ColumnLabel string
	TimeQuantum TimeQuantum
}

func (options *IndexOptions) withDefaults() (updated *IndexOptions) {
	// copy options so the original is not updated
	updated = &IndexOptions{}
	*updated = *options
	// impose defaults
	if updated.ColumnLabel == "" {
		updated.ColumnLabel = "columnID"
	}
	return
}

func (options IndexOptions) String() string {
	return fmt.Sprintf(`{"options": {"columnLabel": "%s"}}`, options.ColumnLabel)
}

// NewPQLBitmapQuery creates a new PqlBitmapQuery
func NewPQLBitmapQuery(pql string, index *Index, err error) *PQLBitmapQuery {
	return &PQLBitmapQuery{
		index: index,
		pql:   pql,
		err:   err,
	}
}

// Index is a Pilosa index
type Index struct {
	name    string
	options *IndexOptions
}

// NewIndex creates the info for a Pilosa index with the given options
func NewIndex(name string, options *IndexOptions) (*Index, error) {
	if err := validateIndexName(name); err != nil {
		return nil, err
	}
	if options == nil {
		options = &IndexOptions{}
	}
	options = options.withDefaults()
	if err := validateLabel(options.ColumnLabel); err != nil {
		return nil, err
	}
	return &Index{
		name:    name,
		options: options,
	}, nil
}

// Name returns the name of this index
func (d *Index) Name() string {
	return d.name
}

// Frame creates the info for a Pilosa frame with default options
func (d *Index) Frame(name string, options *FrameOptions) (*Frame, error) {
	if options == nil {
		options = &FrameOptions{}
	}
	if err := validateFrameName(name); err != nil {
		return nil, err
	}
	options = options.withDefaults()
	if err := validateLabel(options.RowLabel); err != nil {
		return nil, err
	}
	return &Frame{
		name:    name,
		index:   d,
		options: options,
	}, nil
}

// BatchQuery creates a batch query
func (d *Index) BatchQuery(queries ...PQLQuery) *PQLBatchQuery {
	stringQueries := make([]string, 0, len(queries))
	for _, query := range queries {
		stringQueries = append(stringQueries, query.serialize())
	}
	return &PQLBatchQuery{
		index:   d,
		queries: stringQueries,
	}
}

// RawQuery creates a query with the given string
func (d *Index) RawQuery(query string) *PQLBaseQuery {
	return NewPQLBaseQuery(query, d, nil)
}

// Union creates a Union query
func (d *Index) Union(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	return d.bitmapOperation("Union", bitmap1, bitmap2, bitmaps...)
}

// Intersect creates an Intersect query
func (d *Index) Intersect(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	return d.bitmapOperation("Intersect", bitmap1, bitmap2, bitmaps...)
}

// Difference creates an Intersect query
func (d *Index) Difference(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	return d.bitmapOperation("Difference", bitmap1, bitmap2, bitmaps...)
}

// Count creates a Count query
func (d *Index) Count(bitmap *PQLBitmapQuery) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Count(%s)", bitmap.serialize()), d, nil)
}

// SetColumnAttrs creates a SetColumnAttrs query
func (d *Index) SetColumnAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", d, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetColumnAttrs(%s=%d, %s)",
		d.options.ColumnLabel, columnID, attrsString), d, nil)
}

func (d *Index) bitmapOperation(name string, bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	var err error
	if err = bitmap1.Error(); err != nil {
		return NewPQLBitmapQuery("", d, err)
	}
	if err = bitmap2.Error(); err != nil {
		return NewPQLBitmapQuery("", d, err)
	}
	args := make([]string, 0, 2+len(bitmaps))
	args = append(args, bitmap1.serialize(), bitmap2.serialize())
	for _, bitmap := range bitmaps {
		if err = bitmap.Error(); err != nil {
			return NewPQLBitmapQuery("", d, err)
		}
		args = append(args, bitmap.serialize())
	}
	return NewPQLBitmapQuery(fmt.Sprintf("%s(%s)", name, strings.Join(args, ", ")), d, nil)
}

// FrameInfo represents schema information for a frame.
type FrameInfo struct {
	Name string `json:"name"`
}

// FrameOptions contains frame options
type FrameOptions struct {
	RowLabel       string
	TimeQuantum    TimeQuantum
	InverseEnabled bool
}

func (options *FrameOptions) withDefaults() (updated *FrameOptions) {
	// copy options so the original is not updated
	updated = &FrameOptions{}
	*updated = *options
	// impose defaults
	if updated.RowLabel == "" {
		updated.RowLabel = "rowID"
	}
	return
}

func (options FrameOptions) String() string {
	return fmt.Sprintf(`{"options": {"rowLabel": "%s", "inverseEnabled": %v}}`,
		options.RowLabel, options.InverseEnabled)
}

// Frame is a Pilosa frame
type Frame struct {
	name    string
	index   *Index
	options *FrameOptions
}

// Bitmap creates a bitmap query
func (f *Frame) Bitmap(rowID uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Bitmap(%s=%d, frame='%s')",
		f.options.RowLabel, rowID, f.name), f.index, nil)
}

// InverseBitmap creates an inverse bitmap query
func (f *Frame) InverseBitmap(columnID uint64) *PQLBaseQuery {
	if !f.options.InverseEnabled {
		return NewPQLBaseQuery("", f.index, ErrorInverseBitmapsNotEnabled)
	}
	return NewPQLBaseQuery(fmt.Sprintf("Bitmap(%s=%d, frame='%s')",
		f.index.options.ColumnLabel, columnID, f.name), f.index, nil)
}

// SetBit creates a SetBit query
func (f *Frame) SetBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(%s=%d, frame='%s', %s=%d)",
		f.options.RowLabel, rowID, f.name, f.index.options.ColumnLabel, columnID), f.index, nil)
}

// SetBitTimestamp creates a SetBit query with timestamp
func (f *Frame) SetBitTimestamp(rowID uint64, columnID uint64, timestamp time.Time) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(%s=%d, frame='%s', %s=%d, timestamp='%s')",
		f.options.RowLabel, rowID, f.name, f.index.options.ColumnLabel, columnID, timestamp.Format(timeFormat)),
		f.index, nil)
}

// ClearBit creates a ClearBit query
func (f *Frame) ClearBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("ClearBit(%s=%d, frame='%s', %s=%d)",
		f.options.RowLabel, rowID, f.name, f.index.options.ColumnLabel, columnID), f.index, nil)
}

// TopN creates a TopN query with the given item count
func (f *Frame) TopN(n uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d)", f.name, n), f.index, nil)
}

// BitmapTopN creates a TopN query with the given item count and bitmap
func (f *Frame) BitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d)",
		bitmap.serialize(), f.name, n), f.index, nil)
}

// FilterFieldTopN creates a TopN query with the given item count, bitmap, field and the filter for that field
func (f *Frame) FilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery {
	if err := validateLabel(field); err != nil {
		return NewPQLBitmapQuery("", f.index, err)
	}
	b, err := json.Marshal(values)
	if err != nil {
		return NewPQLBitmapQuery("", f.index, err)
	}
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d, field='%s', %s)",
		bitmap.serialize(), f.name, n, field, string(b)), f.index, nil)
}

// Range creates a Range query
func (f *Frame) Range(rowID uint64, start time.Time, end time.Time) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Range(%s=%d, frame='%s', start='%s', end='%s')",
		f.options.RowLabel, rowID, f.name, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// SetRowAttrs creates a SetRowAttrs query
func (f *Frame) SetRowAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetRowAttrs(%s=%d, frame='%s', %s)",
		f.options.RowLabel, rowID, f.name, attrsString), f.index, nil)
}

func createAttributesString(attrs map[string]interface{}) (string, error) {
	attrsList := make([]string, 0, len(attrs))
	for k, v := range attrs {
		// TODO: validate the type of v is one of string, int64, float64, bool
		if err := validateLabel(k); err != nil {
			return "", err
		}
		if vs, ok := v.(string); ok {
			attrsList = append(attrsList, fmt.Sprintf("%s=\"%s\"", k, strings.Replace(vs, "\"", "\\\"", -1)))
		} else {
			attrsList = append(attrsList, fmt.Sprintf("%s=%v", k, v))
		}
	}
	sort.Strings(attrsList)
	return strings.Join(attrsList, ", "), nil
}

// TimeQuantum is the time resolution
type TimeQuantum string

// TimeQuantum resolution constants
const (
	TimeQuantumNone             TimeQuantum = ""
	TimeQuantumYear             TimeQuantum = "Y"
	TimeQuantumMonth            TimeQuantum = "M"
	TimeQuantumDay              TimeQuantum = "D"
	TimeQuantumHour             TimeQuantum = "H"
	TimeQuantumYearMonth        TimeQuantum = "YM"
	TimeQuantumMonthDay         TimeQuantum = "MD"
	TimeQuantumDayHour          TimeQuantum = "DH"
	TimeQuantumYearMonthDay     TimeQuantum = "YMD"
	TimeQuantumMonthDayHour     TimeQuantum = "MDH"
	TimeQuantumYearMonthDayHour TimeQuantum = "YMDH"
)
