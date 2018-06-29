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

func (s *Schema) String() string {
	return fmt.Sprintf("%#v", s.indexes)
}

// NewSchema creates a new Schema
func NewSchema() *Schema {
	return &Schema{
		indexes: make(map[string]*Index),
	}
}

// Index returns an index with a name.
func (s *Schema) Index(name string) (*Index, error) {
	if index, ok := s.indexes[name]; ok {
		return index, nil
	}
	index, err := NewIndex(name)
	if err != nil {
		return nil, err
	}
	s.indexes[name] = index
	return index, nil
}

// Indexes return a copy of the indexes in this schema
func (s *Schema) Indexes() map[string]*Index {
	result := make(map[string]*Index)
	for k, v := range s.indexes {
		result[k] = v.copy()
	}
	return result
}

func (s *Schema) diff(other *Schema) *Schema {
	result := NewSchema()
	for indexName, index := range s.indexes {
		if otherIndex, ok := other.indexes[indexName]; !ok {
			// if the index doesn't exist in the other schema, simply copy it
			result.indexes[indexName] = index.copy()
		} else {
			// the index exists in the other schema; check the fields
			resultIndex, _ := NewIndex(indexName)
			for fieldName, field := range index.fields {
				if _, ok := otherIndex.fields[fieldName]; !ok {
					// the field doesn't exist in the other schema, copy it
					resultIndex.fields[fieldName] = field.copy()
				}
			}
			// check whether we modified result index
			if len(resultIndex.fields) > 0 {
				// if so, move it to the result
				result.indexes[indexName] = resultIndex
			}
		}
	}
	return result
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

// PQLRowQuery is the return type for row queries.
type PQLRowQuery struct {
	index *Index
	pql   string
	err   error
}

// Index returns the index for this query/
func (q *PQLRowQuery) Index() *Index {
	return q.index
}

func (q *PQLRowQuery) serialize() string {
	return q.pql
}

// Error returns the error or nil for this query.
func (q PQLRowQuery) Error() error {
	return q.err
}

// PQLBatchQuery contains a batch of PQL queries.
// Use Index.BatchQuery function to create an instance.
//
// Usage:
//
// 	index, err := NewIndex("repository")
// 	stargazer, err := index.Field("stargazer")
// 	query := repo.BatchQuery(
// 		stargazer.Row(5),
//		stargazer.Row(15),
//		repo.Union(stargazer.Row(20), stargazer.Row(25)))
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

// NewPQLRowQuery creates a new PqlRowQuery.
func NewPQLRowQuery(pql string, index *Index, err error) *PQLRowQuery {
	return &PQLRowQuery{
		index: index,
		pql:   pql,
		err:   err,
	}
}

// Index is a Pilosa index. The purpose of the Index is to represent a data namespace.
// You cannot perform cross-index queries. Column-level attributes are global to the Index.
type Index struct {
	name   string
	fields map[string]*Field
}

func (idx *Index) String() string {
	return fmt.Sprintf("%#v", idx)
}

// NewIndex creates an index with a name.
func NewIndex(name string) (*Index, error) {
	if err := validateIndexName(name); err != nil {
		return nil, err
	}
	return &Index{
		name:   name,
		fields: map[string]*Field{},
	}, nil
}

// Fields return a copy of the fields in this index
func (idx *Index) Fields() map[string]*Field {
	result := make(map[string]*Field)
	for k, v := range idx.fields {
		result[k] = v.copy()
	}
	return result
}

func (idx *Index) copy() *Index {
	fields := make(map[string]*Field)
	for name, f := range idx.fields {
		fields[name] = f.copy()
	}
	index := &Index{
		name:   idx.name,
		fields: fields,
	}
	return index
}

// Name returns the name of this index.
func (idx *Index) Name() string {
	return idx.name
}

// Field creates a Field struct with the specified name and defaults.
func (idx *Index) Field(name string, options ...interface{}) (*Field, error) {
	if field, ok := idx.fields[name]; ok {
		return field, nil
	}
	if err := validateFieldName(name); err != nil {
		return nil, err
	}
	fieldOptions := &FieldOptions{}
	err := fieldOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}
	fieldOptions = fieldOptions.withDefaults()
	field := newField(name, idx)
	field.options = fieldOptions
	idx.fields[name] = field
	return field, nil
}

// BatchQuery creates a batch query with the given queries.
func (idx *Index) BatchQuery(queries ...PQLQuery) *PQLBatchQuery {
	stringQueries := make([]string, 0, len(queries))
	for _, query := range queries {
		stringQueries = append(stringQueries, query.serialize())
	}
	return &PQLBatchQuery{
		index:   idx,
		queries: stringQueries,
	}
}

// RawQuery creates a query with the given string.
// Note that the query is not validated before sending to the server.
func (idx *Index) RawQuery(query string) *PQLBaseQuery {
	return NewPQLBaseQuery(query, idx, nil)
}

// Union creates a Union query.
// Union performs a logical OR on the results of each ROW_CALL query passed to it.
func (idx *Index) Union(rows ...*PQLRowQuery) *PQLRowQuery {
	return idx.rowOperation("Union", rows...)
}

// Intersect creates an Intersect query.
// Intersect performs a logical AND on the results of each ROW_CALL query passed to it.
func (idx *Index) Intersect(rows ...*PQLRowQuery) *PQLRowQuery {
	if len(rows) < 1 {
		return NewPQLRowQuery("", idx, NewError("Intersect operation requires at least 1 row"))
	}
	return idx.rowOperation("Intersect", rows...)
}

// Difference creates an Intersect query.
// Difference returns all of the columns from the first ROW_CALL argument passed to it, without the columns from each subsequent ROW_CALL.
func (idx *Index) Difference(rows ...*PQLRowQuery) *PQLRowQuery {
	if len(rows) < 1 {
		return NewPQLRowQuery("", idx, NewError("Difference operation requires at least 1 row"))
	}
	return idx.rowOperation("Difference", rows...)
}

// Xor creates an Xor query.
func (idx *Index) Xor(rows ...*PQLRowQuery) *PQLRowQuery {
	if len(rows) < 2 {
		return NewPQLRowQuery("", idx, NewError("Xor operation requires at least 2 rows"))
	}
	return idx.rowOperation("Xor", rows...)
}

// Count creates a Count query.
// Returns the number of set columns in the ROW_CALL passed in.
func (idx *Index) Count(row *PQLRowQuery) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Count(%s)", row.serialize()), idx, nil)
}

// SetColumnAttrs creates a SetColumnAttrs query.
// SetColumnAttrs associates arbitrary key/value pairs with a column in an index.
// Following types are accepted: integer, float, string and boolean types.
func (idx *Index) SetColumnAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", idx, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetColumnAttrs(%d,%s)",
		columnID, attrsString), idx, nil)
}

func (idx *Index) rowOperation(name string, rows ...*PQLRowQuery) *PQLRowQuery {
	var err error
	args := make([]string, 0, len(rows))
	for _, row := range rows {
		if err = row.Error(); err != nil {
			return NewPQLRowQuery("", idx, err)
		}
		args = append(args, row.serialize())
	}
	return NewPQLRowQuery(fmt.Sprintf("%s(%s)", name, strings.Join(args, ",")), idx, nil)
}

// FieldInfo represents schema information for a field.
type FieldInfo struct {
	Name string `json:"name"`
}

// FieldOptions contains options to customize Field objects and field queries.
type FieldOptions struct {
	fieldType   FieldType
	timeQuantum TimeQuantum
	cacheType   CacheType
	cacheSize   int
	min         int64
	max         int64
}

func (fo *FieldOptions) withDefaults() (updated *FieldOptions) {
	// copy options so the original is not updated
	updated = &FieldOptions{}
	*updated = *fo
	return
}

func (fo FieldOptions) String() string {
	mopt := map[string]interface{}{}

	switch fo.fieldType {
	case FieldTypeSet:
		if fo.cacheType != CacheTypeDefault {
			mopt["cacheType"] = string(fo.cacheType)
		}
		if fo.cacheSize > 0 {
			mopt["cacheSize"] = fo.cacheSize
		}
	case FieldTypeInt:
		mopt["min"] = fo.min
		mopt["max"] = fo.max
	case FieldTypeTime:
		mopt["timeQuantum"] = string(fo.timeQuantum)
	}

	if fo.fieldType != FieldTypeDefault {
		mopt["type"] = string(fo.fieldType)
	}
	return fmt.Sprintf(`{"options":%s}`, encodeMap(mopt))
}

func (fo *FieldOptions) addOptions(options ...interface{}) error {
	for i, option := range options {
		switch o := option.(type) {
		case nil:
			if i != 0 {
				return ErrInvalidFieldOption
			}
			continue
		case *FieldOptions:
			if i != 0 {
				return ErrInvalidFieldOption
			}
			*fo = *o
		case FieldOption:
			err := o(fo)
			if err != nil {
				return err
			}
		case TimeQuantum:
			fo.timeQuantum = o
		default:
			return ErrInvalidFieldOption
		}
	}
	return nil
}

// FieldOption is used to pass an option to index.Field function.
type FieldOption func(options *FieldOptions) error

// OptFieldSet adds a set field.
// Specify CacheTypeDefault for the default cache type.
// Specify CacheSizeDefault for the default cache size.
func OptFieldSet(cacheType CacheType, cacheSize int) FieldOption {
	return func(options *FieldOptions) error {
		if cacheSize < 0 {
			return ErrInvalidFieldOption
		}
		options.fieldType = FieldTypeSet
		options.cacheType = cacheType
		options.cacheSize = cacheSize
		return nil
	}
}

// OptFieldInt adds an integer field.
func OptFieldInt(min int64, max int64) FieldOption {
	return func(options *FieldOptions) error {
		options.fieldType = FieldTypeInt
		if max < min {
			return ErrInvalidFieldOption
		}
		options.min = min
		options.max = max
		return nil
	}
}

func OptFieldTime(quantum TimeQuantum) FieldOption {
	return func(options *FieldOptions) error {
		options.fieldType = FieldTypeTime
		options.timeQuantum = quantum
		return nil
	}
}

// Field structs are used to segment and define different functional characteristics within your entire index.
// You can think of a Field as a table-like data partition within your Index.
// Row-level attributes are namespaced at the Field level.
type Field struct {
	name    string
	index   *Index
	options *FieldOptions
}

func (f *Field) String() string {
	return fmt.Sprintf("%#v", f)
}

func newField(name string, index *Index) *Field {
	return &Field{
		name:    name,
		index:   index,
		options: &FieldOptions{},
	}
}

// Name returns the name of the field
func (f *Field) Name() string {
	return f.name
}

func (f *Field) copy() *Field {
	field := newField(f.name, f.index)
	*field.options = *f.options
	return field
}

// Row creates a Row query.
// Row retrieves the indices of all the set columns in a row.
// It also retrieves any attributes set on that row or column.
func (f *Field) Row(rowID uint64) *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("Row(%s=%d)",
		f.name, rowID), f.index, nil)
}

// RowK creates a Row query using a string key instead of an integer
// rowID. This will only work against a Pilosa Enterprise server.
func (f *Field) RowK(rowKey string) *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("Row(%s='%s')",
		f.name, rowKey), f.index, nil)
}

// Set creates a Set query.
// Set, assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given field with the given column.
func (f *Field) Set(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Set(%d,%s=%d)",
		columnID, f.name, rowID), f.index, nil)
}

// SetK creates a Set query using string row and column keys. This will
// only work against a Pilosa Enterprise server.
func (f *Field) SetK(rowKey string, columnKey string) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Set(%s,%s='%s')",
		columnKey, f.name, rowKey), f.index, nil)
}

// SetTimestamp creates a Set query with timestamp.
// Set, assigns a value of 1 to a column in the binary matrix,
// thus associating the given row in the given field with the given column.
func (f *Field) SetTimestamp(rowID uint64, columnID uint64, timestamp time.Time) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Set(%d,%s=%d,%s)",
		columnID, f.name, rowID, timestamp.Format(timeFormat)),
		f.index, nil)
}

// SetTimestampK creates a SetK query with timestamp.
func (f *Field) SetTimestampK(rowKey string, columnKey string, timestamp time.Time) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Set('%s',%s='%s',%s)",
		columnKey, f.name, rowKey, timestamp.Format(timeFormat)),
		f.index, nil)
}

// Clear creates a Clear query.
// Clear, assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given field from the given column.
func (f *Field) Clear(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Clear(%d,%s=%d)",
		columnID, f.name, rowID), f.index, nil)
}

// ClearK creates a Clear query using string row and column keys. This
// will only work against a Pilosa Enterprise server.
func (f *Field) ClearK(rowKey string, columnKey string) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Clear('%s',%s='%s')",
		columnKey, f.name, rowKey), f.index, nil)
}

// TopN creates a TopN query with the given item count.
// Returns the id and count of the top n rows (by count of columns) in the field.
func (f *Field) TopN(n uint64) *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("TopN(%s,n=%d)", f.name, n), f.index, nil)
}

// RowTopN creates a TopN query with the given item count and row.
// This variant supports customizing the row query.
func (f *Field) RowTopN(n uint64, row *PQLRowQuery) *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("TopN(%s,%s,n=%d)",
		f.name, row.serialize(), n), f.index, nil)
}

// FilterFieldTopN creates a TopN query with the given item count, row, field and the filter for that field
// The field and filters arguments work together to only return Rows which have the attribute specified by field with one of the values specified in filters.
func (f *Field) FilterFieldTopN(n uint64, row *PQLRowQuery, field string, values ...interface{}) *PQLRowQuery {
	return f.filterFieldTopN(n, row, field, values...)
}

func (f *Field) filterFieldTopN(n uint64, row *PQLRowQuery, field string, values ...interface{}) *PQLRowQuery {
	if err := validateLabel(field); err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	b, err := json.Marshal(values)
	if err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	if row == nil {
		return NewPQLRowQuery(fmt.Sprintf("TopN(%s,n=%d,field='%s',filters=%s)",
			f.name, n, field, string(b)), f.index, nil)
	}
	return NewPQLRowQuery(fmt.Sprintf("TopN(%s,%s,n=%d,field='%s',filters=%s)",
		f.name, row.serialize(), n, field, string(b)), f.index, nil)
}

// Range creates a Range query.
// Similar to Row, but only returns columns which were set with timestamps between the given start and end timestamps.
func (f *Field) Range(rowID uint64, start time.Time, end time.Time) *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("Range(%s=%d,%s,%s)",
		f.name, rowID, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// RangeK creates a Range query using a string row key. This will only work
// against a Pilosa Enterprise server.
func (f *Field) RangeK(rowKey string, start time.Time, end time.Time) *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("Range(%s='%s',%s,%s)",
		f.name, rowKey, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// SetRowAttrs creates a SetRowAttrs query.
// SetRowAttrs associates arbitrary key/value pairs with a row in a field.
// Following types are accepted: integer, float, string and boolean types.
func (f *Field) SetRowAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetRowAttrs(%s,%d,%s)",
		f.name, rowID, attrsString), f.index, nil)
}

// SetRowAttrsK creates a SetRowAttrs query using a string row key. This will
// only work against a Pilosa Enterprise server.
func (f *Field) SetRowAttrsK(rowKey string, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetRowAttrs('%s','%s',%s)",
		f.name, rowKey, attrsString), f.index, nil)
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
	return strings.Join(attrsList, ","), nil
}

// FieldType
type FieldType string

const (
	FieldTypeDefault FieldType = ""
	FieldTypeSet     FieldType = "set"
	FieldTypeInt     FieldType = "int"
	FieldTypeTime    FieldType = "time"
)

// TimeQuantum type represents valid time quantum values time fields.
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

// CacheType represents cache type for a field
type CacheType string

// CacheType constants
const (
	CacheTypeDefault CacheType = ""
	CacheTypeLRU     CacheType = "lru"
	CacheTypeRanked  CacheType = "ranked"
	CacheTypeNone    CacheType = "none"
)

// CacheSizeDefault is the default cache size
const CacheSizeDefault = 0

// LT creates a less than query.
func (field *Field) LT(n int) *PQLRowQuery {
	return field.binaryOperation("<", n)
}

// LTE creates a less than or equal query.
func (field *Field) LTE(n int) *PQLRowQuery {
	return field.binaryOperation("<=", n)
}

// GT creates a greater than query.
func (field *Field) GT(n int) *PQLRowQuery {
	return field.binaryOperation(">", n)
}

// GTE creates a greater than or equal query.
func (field *Field) GTE(n int) *PQLRowQuery {
	return field.binaryOperation(">=", n)
}

// Equals creates an equals query.
func (field *Field) Equals(n int) *PQLRowQuery {
	return field.binaryOperation("==", n)
}

// NotEquals creates a not equals query.
func (field *Field) NotEquals(n int) *PQLRowQuery {
	return field.binaryOperation("!=", n)
}

// NotNull creates a not equal to null query.
func (field *Field) NotNull() *PQLRowQuery {
	qry := fmt.Sprintf("Range(%s != null)", field.name)
	return NewPQLRowQuery(qry, field.index, nil)
}

// Between creates a between query.
func (field *Field) Between(a int, b int) *PQLRowQuery {
	qry := fmt.Sprintf("Range(%s >< [%d,%d])", field.name, a, b)
	return NewPQLRowQuery(qry, field.index, nil)
}

// Sum creates a sum query.
func (field *Field) Sum(row *PQLRowQuery) *PQLBaseQuery {
	return field.valQuery("Sum", row)
}

// Min creates a min query.
func (field *Field) Min(row *PQLRowQuery) *PQLBaseQuery {
	return field.valQuery("Min", row)
}

// Max creates a min query.
func (field *Field) Max(row *PQLRowQuery) *PQLBaseQuery {
	return field.valQuery("Max", row)
}

// SetIntValue creates a SetValue query.
func (field *Field) SetIntValue(columnID uint64, value int) *PQLBaseQuery {
	qry := fmt.Sprintf("SetValue(col=%d, %s=%d)", columnID, field.name, value)
	return NewPQLBaseQuery(qry, field.index, nil)
}

// SetIntValueK creates a SetValue query using a string column key. This will
// only work against a Pilosa Enterprise server.
func (field *Field) SetIntValueK(columnKey string, value int) *PQLBaseQuery {
	qry := fmt.Sprintf("SetValue(col='%s', %s=%d)", columnKey, field.name, value)
	return NewPQLBaseQuery(qry, field.index, nil)
}

func (field *Field) binaryOperation(op string, n int) *PQLRowQuery {
	qry := fmt.Sprintf("Range(%s %s %d)", field.name, op, n)
	return NewPQLRowQuery(qry, field.index, nil)
}

func (field *Field) valQuery(op string, row *PQLRowQuery) *PQLBaseQuery {
	rowStr := ""
	if row != nil {
		rowStr = fmt.Sprintf("%s,", row.serialize())
	}
	qry := fmt.Sprintf("%s(%sfield='%s')", op, rowStr, field.name)
	return NewPQLBaseQuery(qry, field.index, nil)
}

func encodeMap(m map[string]interface{}) string {
	result, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(result)
}
