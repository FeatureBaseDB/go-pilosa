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

// Schema contains the index properties
type Schema struct {
	indexes map[string]*Index
}

// NewSchema creates a new Schema
func NewSchema() *Schema {
	return &Schema{
		indexes: make(map[string]*Index),
	}
}

// Index returns an index with a name and options.
// Pass nil for default options.
func (s *Schema) Index(name string, options *IndexOptions) (*Index, error) {
	if index, ok := s.indexes[name]; ok {
		return index, nil
	}
	index, err := NewIndex(name, options)
	if err != nil {
		return nil, err
	}
	s.indexes[name] = index
	return index, nil
}

// PQLQuery is an interface for PQL queries.
type PQLQuery interface {
	Index() *Index
	serialize() string
	Error() error
}

// PQLBaseQuery is the base implementation for PQLQuery.
type PQLBaseQuery struct {
	index *Index
	pql   string
	err   error
}

// NewPQLBaseQuery creates a new PQLQuery with the given PQL and index.
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

func (q *PQLBaseQuery) serialize() string {
	return q.pql
}

// Error returns the error or nil for this query.
func (q PQLBaseQuery) Error() error {
	return q.err
}

// PQLBitmapQuery is the return type for bitmap queries.
type PQLBitmapQuery struct {
	index *Index
	pql   string
	err   error
}

// Index returns the index for this query/
func (q *PQLBitmapQuery) Index() *Index {
	return q.index
}

func (q *PQLBitmapQuery) serialize() string {
	return q.pql
}

// Error returns the error or nil for this query.
func (q PQLBitmapQuery) Error() error {
	return q.err
}

// PQLBatchQuery contains a batch of PQL queries.
// Use Index.BatchQuery function to create an instance.
//
// Usage:
//
// 	index, err := NewIndex("repository", nil)
// 	stargazer, err := index.Frame("stargazer", nil)
// 	query := repo.BatchQuery(
// 		stargazer.Bitmap(5),
//		stargazer.Bitmap(15),
//		repo.Union(stargazer.Bitmap(20), stargazer.Bitmap(25)))
type PQLBatchQuery struct {
	index   *Index
	queries []string
	err     error
}

// Index returns the index for this query.
func (q *PQLBatchQuery) Index() *Index {
	return q.index
}

func (q *PQLBatchQuery) serialize() string {
	return strings.Join(q.queries, "")
}

func (q *PQLBatchQuery) Error() error {
	return q.err
}

// Add adds a query to the batch.
func (q *PQLBatchQuery) Add(query PQLQuery) {
	err := query.Error()
	if err != nil {
		q.err = err
	}
	q.queries = append(q.queries, query.serialize())
}

// IndexOptions contains options to customize Index structs and column queries.
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

// NewPQLBitmapQuery creates a new PqlBitmapQuery.
func NewPQLBitmapQuery(pql string, index *Index, err error) *PQLBitmapQuery {
	return &PQLBitmapQuery{
		index: index,
		pql:   pql,
		err:   err,
	}
}

// Index is a Pilosa index. The purpose of the Index is to represent a data namespace.
// You cannot perform cross-index queries. Column-level attributes are global to the Index.
type Index struct {
	name    string
	options *IndexOptions
	frames  map[string]*Frame
}

// NewIndex creates an index with a name and options.
// Pass nil for default options.
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
		frames:  map[string]*Frame{},
	}, nil
}

// Name returns the name of this index.
func (d *Index) Name() string {
	return d.name
}

// Frame creates a frame struct with the specified name and defaults.
func (d *Index) Frame(name string, options *FrameOptions) (*Frame, error) {
	if frame, ok := d.frames[name]; ok {
		return frame, nil
	}
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
	frame := &Frame{
		name:    name,
		index:   d,
		options: options,
	}
	d.frames[name] = frame
	return frame, nil
}

// BatchQuery creates a batch query with the given queries.
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

// RawQuery creates a query with the given string.
// Note that the query is not validated before sending to the server.
func (d *Index) RawQuery(query string) *PQLBaseQuery {
	return NewPQLBaseQuery(query, d, nil)
}

// Union creates a Union query.
// Union performs a logical OR on the results of each BITMAP_CALL query passed to it.
func (d *Index) Union(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	return d.bitmapOperation("Union", bitmaps...)
}

// Intersect creates an Intersect query.
// Intersect performs a logical AND on the results of each BITMAP_CALL query passed to it.
func (d *Index) Intersect(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	if len(bitmaps) < 1 {
		return NewPQLBitmapQuery("", d, NewError("Intersect operation requires at least 1 bitmap"))
	}
	return d.bitmapOperation("Intersect", bitmaps...)
}

// Difference creates an Intersect query.
// Difference returns all of the bits from the first BITMAP_CALL argument passed to it, without the bits from each subsequent BITMAP_CALL.
func (d *Index) Difference(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	if len(bitmaps) < 1 {
		return NewPQLBitmapQuery("", d, NewError("Difference operation requires at least 1 bitmap"))
	}
	return d.bitmapOperation("Difference", bitmaps...)
}

// Count creates a Count query.
// Returns the number of set bits in the BITMAP_CALL passed in.
func (d *Index) Count(bitmap *PQLBitmapQuery) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Count(%s)", bitmap.serialize()), d, nil)
}

// SetColumnAttrs creates a SetColumnAttrs query.
// SetColumnAttrs associates arbitrary key/value pairs with a column in an index.
// Following types are accepted: integer, float, string and boolean types.
func (d *Index) SetColumnAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", d, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetColumnAttrs(%s=%d, %s)",
		d.options.ColumnLabel, columnID, attrsString), d, nil)
}

func (d *Index) bitmapOperation(name string, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	var err error
	args := make([]string, 0, len(bitmaps))
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

// FrameOptions contains options to customize Frame objects and frame queries.
type FrameOptions struct {
	RowLabel string
	// If a Frame has a time quantum, then Views are generated for each of the defined time segments.
	TimeQuantum TimeQuantum
	// Enables inverted frames
	InverseEnabled bool
	CacheType      CacheType
	CacheSize      uint
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
	mopt := map[string]interface{}{
		"rowLabel": options.RowLabel,
	}
	if options.InverseEnabled {
		mopt["inverseEnabled"] = true
	}
	if options.TimeQuantum != TimeQuantumNone {
		mopt["timeQuantum"] = string(options.TimeQuantum)
	}
	if options.CacheType != CacheTypeDefault {
		mopt["cacheType"] = string(options.CacheType)
	}
	if options.CacheSize != 0 {
		mopt["cacheSize"] = options.CacheSize
	}
	return fmt.Sprintf(`{"options": %s}`, encodeMap(mopt))
}

// Frame structs are used to segment and define different functional characteristics within your entire index.
// You can think of a Frame as a table-like data partition within your Index.
// Row-level attributes are namespaced at the Frame level.
type Frame struct {
	name    string
	index   *Index
	options *FrameOptions
}

// Bitmap creates a bitmap query using the row label.
// Bitmap retrieves the indices of all the set bits in a row or column based on whether the row label or column label is given in the query.
// It also retrieves any attributes set on that row or column.
func (f *Frame) Bitmap(rowID uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Bitmap(%s=%d, frame='%s')",
		f.options.RowLabel, rowID, f.name), f.index, nil)
}

// InverseBitmap creates a bitmap query using the column label.
// Bitmap retrieves the indices of all the set bits in a row or column based on whether the row label or column label is given in the query.
// It also retrieves any attributes set on that row or column.
func (f *Frame) InverseBitmap(columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Bitmap(%s=%d, frame='%s')",
		f.index.options.ColumnLabel, columnID, f.name), f.index, nil)
}

// SetBit creates a SetBit query.
// SetBit, assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given frame with the given column.
func (f *Frame) SetBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(%s=%d, frame='%s', %s=%d)",
		f.options.RowLabel, rowID, f.name, f.index.options.ColumnLabel, columnID), f.index, nil)
}

// SetBitTimestamp creates a SetBit query with timestamp.
// SetBit, assigns a value of 1 to a bit in the binary matrix,
// thus associating the given row in the given frame with the given column.
func (f *Frame) SetBitTimestamp(rowID uint64, columnID uint64, timestamp time.Time) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(%s=%d, frame='%s', %s=%d, timestamp='%s')",
		f.options.RowLabel, rowID, f.name, f.index.options.ColumnLabel, columnID, timestamp.Format(timeFormat)),
		f.index, nil)
}

// ClearBit creates a ClearBit query.
// ClearBit, assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given frame from the given column.
func (f *Frame) ClearBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("ClearBit(%s=%d, frame='%s', %s=%d)",
		f.options.RowLabel, rowID, f.name, f.index.options.ColumnLabel, columnID), f.index, nil)
}

// TopN creates a TopN query with the given item count.
// Returns the id and count of the top n bitmaps (by count of bits) in the frame.
func (f *Frame) TopN(n uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d, inverse=false)", f.name, n), f.index, nil)
}

// InverseTopN creates a TopN query with the given item count.
// Returns the id and count of the top n bitmaps (by count of bits) in the frame.
// This variant sets inverse=true
func (f *Frame) InverseTopN(n uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d, inverse=true)", f.name, n), f.index, nil)
}

// BitmapTopN creates a TopN query with the given item count and bitmap.
// This variant supports customizing the bitmap query.
func (f *Frame) BitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d, inverse=false)",
		bitmap.serialize(), f.name, n), f.index, nil)
}

// InverseBitmapTopN creates a TopN query with the given item count and bitmap.
// This variant supports customizing the bitmap query and sets inverse=true.
func (f *Frame) InverseBitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d, inverse=true)",
		bitmap.serialize(), f.name, n), f.index, nil)
}

// FilterFieldTopN creates a TopN query with the given item count, bitmap, field and the filter for that field
// The field and filters arguments work together to only return Bitmaps which have the attribute specified by field with one of the values specified in filters.
func (f *Frame) FilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery {
	return f.filterFieldTopN(n, bitmap, false, field, values...)
}

// InverseFilterFieldTopN creates a TopN query with the given item count, bitmap, field and the filter for that field
// The field and filters arguments work together to only return Bitmaps which have the attribute specified by field with one of the values specified in filters.
// This variant sets inverse=true.
func (f *Frame) InverseFilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery {
	return f.filterFieldTopN(n, bitmap, true, field, values...)
}

func (f *Frame) filterFieldTopN(n uint64, bitmap *PQLBitmapQuery, inverse bool, field string, values ...interface{}) *PQLBitmapQuery {
	if err := validateLabel(field); err != nil {
		return NewPQLBitmapQuery("", f.index, err)
	}
	b, err := json.Marshal(values)
	if err != nil {
		return NewPQLBitmapQuery("", f.index, err)
	}
	inverseStr := "true"
	if !inverse {
		inverseStr = "false"
	}
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d, inverse=%s, field='%s', %s)",
		bitmap.serialize(), f.name, n, inverseStr, field, string(b)), f.index, nil)
}

// Range creates a Range query.
// Similar to Bitmap, but only returns bits which were set with timestamps between the given start and end timestamps.
func (f *Frame) Range(rowID uint64, start time.Time, end time.Time) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Range(%s=%d, frame='%s', start='%s', end='%s')",
		f.options.RowLabel, rowID, f.name, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// InverseRange creates a Range query.
// Similar to Bitmap, but only returns bits which were set with timestamps between the given start and end timestamps.
func (f *Frame) InverseRange(columnID uint64, start time.Time, end time.Time) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Range(%s=%d, frame='%s', start='%s', end='%s')",
		f.index.options.ColumnLabel, columnID, f.name, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// SetRowAttrs creates a SetRowAttrs query.
// SetRowAttrs associates arbitrary key/value pairs with a row in a frame.
// Following types are accepted: integer, float, string and boolean types.
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

// TimeQuantum type represents valid time quantum values for frames having support for that.
type TimeQuantum string

// TimeQuantum constants
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

type CacheType string

// CacheType constants
const (
	CacheTypeDefault CacheType = ""
	CacheTypeLRU     CacheType = "lru"
	CacheTypeRanked  CacheType = "ranked"
)

func encodeMap(m map[string]interface{}) string {
	result, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(result)
}
