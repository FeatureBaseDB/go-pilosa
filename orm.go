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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
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
func (s *Schema) Index(name string, options ...IndexOption) *Index {
	if index, ok := s.indexes[name]; ok {
		return index
	}
	indexOptions := &IndexOptions{}
	indexOptions.addOptions(options...)
	return s.indexWithOptions(name, 0, indexOptions)
}

func (s *Schema) indexWithOptions(name string, shardWidth uint64, options *IndexOptions) *Index {
	index := NewIndex(name)
	index.options = options.withDefaults()
	index.shardWidth = shardWidth
	s.indexes[name] = index
	return index
}

// Indexes return a copy of the indexes in this schema
func (s *Schema) Indexes() map[string]*Index {
	result := make(map[string]*Index)
	for k, v := range s.indexes {
		result[k] = v.copy()
	}
	return result
}

// HasIndex returns true if the given index is in the schema.
func (s *Schema) HasIndex(indexName string) bool {
	_, ok := s.indexes[indexName]
	return ok
}

func (s *Schema) diff(other *Schema) *Schema {
	result := NewSchema()
	for indexName, index := range s.indexes {
		if otherIndex, ok := other.indexes[indexName]; !ok {
			// if the index doesn't exist in the other schema, simply copy it
			result.indexes[indexName] = index.copy()
		} else {
			// the index exists in the other schema; check the fields
			resultIndex := NewIndex(indexName)
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

type SerializedQuery interface {
	String() string
	HasWriteKeys() bool
}

type serializedQuery struct {
	query        string
	hasWriteKeys bool
}

func newSerializedQuery(query string, hasWriteKeys bool) serializedQuery {
	return serializedQuery{
		query:        query,
		hasWriteKeys: hasWriteKeys,
	}
}

func (s serializedQuery) String() string {
	return s.query
}

func (s serializedQuery) HasWriteKeys() bool {
	return s.hasWriteKeys
}

// PQLQuery is an interface for PQL queries.
type PQLQuery interface {
	Index() *Index
	Serialize() SerializedQuery
	Error() error
}

// PQLBaseQuery is the base implementation for PQLQuery.
type PQLBaseQuery struct {
	index   *Index
	pql     string
	err     error
	hasKeys bool
}

// NewPQLBaseQuery creates a new PQLQuery with the given PQL and index.
func NewPQLBaseQuery(pql string, index *Index, err error) *PQLBaseQuery {
	return &PQLBaseQuery{
		index:   index,
		pql:     pql,
		err:     err,
		hasKeys: index.options.keys,
	}
}

// Index returns the index for this query
func (q *PQLBaseQuery) Index() *Index {
	return q.index
}

func (q *PQLBaseQuery) Serialize() SerializedQuery {
	return newSerializedQuery(q.pql, q.hasKeys)
}

// Error returns the error or nil for this query.
func (q PQLBaseQuery) Error() error {
	return q.err
}

// PQLRowQuery is the return type for row queries.
type PQLRowQuery struct {
	index   *Index
	pql     string
	err     error
	hasKeys bool
}

// Index returns the index for this query/
func (q *PQLRowQuery) Index() *Index {
	return q.index
}

func (q *PQLRowQuery) Serialize() SerializedQuery {
	return q.serialize()
}

func (q *PQLRowQuery) serialize() SerializedQuery {
	return newSerializedQuery(q.pql, q.hasKeys)
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
	hasKeys bool
}

// Index returns the index for this query.
func (q *PQLBatchQuery) Index() *Index {
	return q.index
}

func (q *PQLBatchQuery) Serialize() SerializedQuery {
	query := strings.Join(q.queries, "")
	return newSerializedQuery(query, q.hasKeys)
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
	serializedQuery := query.Serialize()
	q.hasKeys = q.hasKeys || serializedQuery.HasWriteKeys()
	q.queries = append(q.queries, serializedQuery.String())
}

// NewPQLRowQuery creates a new PqlRowQuery.
func NewPQLRowQuery(pql string, index *Index, err error) *PQLRowQuery {
	return &PQLRowQuery{
		index:   index,
		pql:     pql,
		err:     err,
		hasKeys: index.options.keys,
	}
}

// IndexOptions contains options to customize Index objects.
type IndexOptions struct {
	keys              bool
	keysSet           bool
	trackExistence    bool
	trackExistenceSet bool
}

func (io *IndexOptions) withDefaults() (updated *IndexOptions) {
	// copy options so the original is not updated
	updated = &IndexOptions{}
	*updated = *io
	if !updated.keysSet {
		updated.keys = false
	}
	if !updated.trackExistenceSet {
		updated.trackExistence = true
	}
	return
}

// Keys return true if this index has keys.
func (io IndexOptions) Keys() bool {
	return io.keys
}

// TrackExistence returns true if existence is tracked for this index.
func (io IndexOptions) TrackExistence() bool {
	return io.trackExistence
}

// String serializes this index to a JSON string.
func (io IndexOptions) String() string {
	mopt := map[string]interface{}{}
	if io.keysSet {
		mopt["keys"] = io.keys
	}
	if io.trackExistenceSet {
		mopt["trackExistence"] = io.trackExistence
	}
	return fmt.Sprintf(`{"options":%s}`, encodeMap(mopt))
}

func (io *IndexOptions) addOptions(options ...IndexOption) {
	for _, option := range options {
		if option == nil {
			continue
		}
		option(io)
	}
}

// IndexOption is used to pass an option to Index function.
type IndexOption func(options *IndexOptions)

// OptIndexKeys sets whether index uses string keys.
func OptIndexKeys(keys bool) IndexOption {
	return func(options *IndexOptions) {
		options.keys = keys
		options.keysSet = true
	}
}

// OptIndexTrackExistence enables keeping track of existence of columns.
func OptIndexTrackExistence(trackExistence bool) IndexOption {
	return func(options *IndexOptions) {
		options.trackExistence = trackExistence
		options.trackExistenceSet = true
	}
}

// OptionsOptions is used to pass an option to Option call.
type OptionsOptions struct {
	columnAttrs     bool
	excludeColumns  bool
	excludeRowAttrs bool
	shards          []uint64
}

func (oo OptionsOptions) marshal() string {
	part1 := fmt.Sprintf("columnAttrs=%s,excludeColumns=%s,excludeRowAttrs=%s",
		strconv.FormatBool(oo.columnAttrs),
		strconv.FormatBool(oo.excludeColumns),
		strconv.FormatBool(oo.excludeRowAttrs))
	if oo.shards != nil {
		shardsStr := make([]string, len(oo.shards))
		for i, shard := range oo.shards {
			shardsStr[i] = strconv.FormatUint(shard, 10)
		}
		return fmt.Sprintf("%s,shards=[%s]", part1, strings.Join(shardsStr, ","))
	}
	return part1
}

// OptionsOption is an option for Index.Options call.
type OptionsOption func(options *OptionsOptions)

// OptOptionsColumnAttrs enables returning column attributes.
func OptOptionsColumnAttrs(enable bool) OptionsOption {
	return func(options *OptionsOptions) {
		options.columnAttrs = enable
	}
}

// OptOptionsExcludeColumns enables preventing returning columns.
func OptOptionsExcludeColumns(enable bool) OptionsOption {
	return func(options *OptionsOptions) {
		options.excludeColumns = enable
	}
}

// OptOptionsExcludeRowAttrs enables preventing returning row attributes.
func OptOptionsExcludeRowAttrs(enable bool) OptionsOption {
	return func(options *OptionsOptions) {
		options.excludeRowAttrs = enable
	}
}

// OptOptionsShards run the query using only the data from the given shards.
// By default, the entire data set (i.e. data from all shards) is used.
func OptOptionsShards(shards ...uint64) OptionsOption {
	return func(options *OptionsOptions) {
		options.shards = shards
	}
}

// Index is a Pilosa index. The purpose of the Index is to represent a data namespace.
// You cannot perform cross-index queries. Column-level attributes are global to the Index.
type Index struct {
	name       string
	options    *IndexOptions
	fields     map[string]*Field
	shardWidth uint64
}

func (idx *Index) String() string {
	return fmt.Sprintf("%#v", idx)
}

// NewIndex creates an index with a name.
func NewIndex(name string) *Index {
	options := &IndexOptions{}
	return &Index{
		name:    name,
		options: options.withDefaults(),
		fields:  map[string]*Field{},
	}
}

// Fields return a copy of the fields in this index
func (idx *Index) Fields() map[string]*Field {
	result := make(map[string]*Field)
	for k, v := range idx.fields {
		result[k] = v.copy()
	}
	return result
}

// HasFields returns true if the given field exists in the index.
func (idx *Index) HasField(fieldName string) bool {
	_, ok := idx.fields[fieldName]
	return ok
}

func (idx *Index) copy() *Index {
	fields := make(map[string]*Field)
	for name, f := range idx.fields {
		fields[name] = f.copy()
	}
	index := &Index{
		name:       idx.name,
		options:    &IndexOptions{},
		fields:     fields,
		shardWidth: idx.shardWidth,
	}
	*index.options = *idx.options
	return index
}

// Name returns the name of this index.
func (idx *Index) Name() string {
	return idx.name
}

// Opts returns the options of this index.
func (idx *Index) Opts() IndexOptions {
	return *idx.options
}

// Field creates a Field struct with the specified name and defaults.
func (idx *Index) Field(name string, options ...FieldOption) *Field {
	if field, ok := idx.fields[name]; ok {
		return field
	}
	fieldOptions := &FieldOptions{}
	fieldOptions = fieldOptions.withDefaults()
	fieldOptions.addOptions(options...)
	return idx.fieldWithOptions(name, fieldOptions)
}

func (idx *Index) fieldWithOptions(name string, fieldOptions *FieldOptions) *Field {
	field := newField(name, idx)
	fieldOptions = fieldOptions.withDefaults()
	field.options = fieldOptions
	idx.fields[name] = field
	return field
}

// BatchQuery creates a batch query with the given queries.
func (idx *Index) BatchQuery(queries ...PQLQuery) *PQLBatchQuery {
	stringQueries := make([]string, 0, len(queries))
	hasKeys := false
	for _, query := range queries {
		serializedQuery := query.Serialize()
		hasKeys = hasKeys || serializedQuery.HasWriteKeys()
		stringQueries = append(stringQueries, serializedQuery.String())
	}
	return &PQLBatchQuery{
		index:   idx,
		queries: stringQueries,
		hasKeys: hasKeys,
	}
}

// RawQuery creates a query with the given string.
// Note that the query is not validated before sending to the server.
func (idx *Index) RawQuery(query string) *PQLBaseQuery {
	q := NewPQLBaseQuery(query, idx, nil)
	// NOTE: raw queries always assumed to have keys set
	q.hasKeys = true
	return q
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

// Not creates a Not query.
func (idx *Index) Not(row *PQLRowQuery) *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("Not(%s)", row.serialize()), idx, row.Error())
}

// Count creates a Count query.
// Returns the number of set columns in the ROW_CALL passed in.
func (idx *Index) Count(row *PQLRowQuery) *PQLBaseQuery {
	serializedQuery := row.serialize()
	q := NewPQLBaseQuery(fmt.Sprintf("Count(%s)", serializedQuery.String()), idx, nil)
	q.hasKeys = q.hasKeys || serializedQuery.HasWriteKeys()
	return q
}

// SetColumnAttrs creates a SetColumnAttrs query.
// SetColumnAttrs associates arbitrary key/value pairs with a column in an index.
// Following types are accepted: integer, float, string and boolean types.
func (idx *Index) SetColumnAttrs(colIDOrKey interface{}, attrs map[string]interface{}) *PQLBaseQuery {
	colStr, err := formatIDKey(colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", idx, err)
	}
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", idx, err)
	}
	q := fmt.Sprintf("SetColumnAttrs(%s,%s)", colStr, attrsString)
	return NewPQLBaseQuery(q, idx, nil)
}

// Options creates an Options query.
func (idx *Index) Options(row *PQLRowQuery, opts ...OptionsOption) *PQLBaseQuery {
	oo := &OptionsOptions{}
	for _, opt := range opts {
		opt(oo)
	}
	text := fmt.Sprintf("Options(%s,%s)", row.serialize(), oo.marshal())
	return NewPQLBaseQuery(text, idx, nil)
}

// GroupBy creates a GroupBy query with the given Rows queries
func (idx *Index) GroupBy(rowsQueries ...*PQLRowsQuery) *PQLBaseQuery {
	if len(rowsQueries) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	text := fmt.Sprintf("GroupBy(%s)", strings.Join(serializeGroupBy(rowsQueries...), ","))
	return NewPQLBaseQuery(text, idx, nil)
}

// GroupByLimit creates a GroupBy query with the given limit and Rows queries
func (idx *Index) GroupByLimit(limit int64, rowsQueries ...*PQLRowsQuery) *PQLBaseQuery {
	if len(rowsQueries) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	if limit < 0 {
		return NewPQLBaseQuery("", idx, errors.New("limit must be non-negative"))
	}
	text := fmt.Sprintf("GroupBy(%s,limit=%d)", strings.Join(serializeGroupBy(rowsQueries...), ","), limit)
	return NewPQLBaseQuery(text, idx, nil)
}

// GroupByFilter creates a GroupBy query with the given filter and Rows queries
func (idx *Index) GroupByFilter(filterQuery *PQLRowQuery, rowsQueries ...*PQLRowsQuery) *PQLBaseQuery {
	if len(rowsQueries) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	filterText := filterQuery.serialize().String()
	text := fmt.Sprintf("GroupBy(%s,filter=%s)", strings.Join(serializeGroupBy(rowsQueries...), ","), filterText)
	return NewPQLBaseQuery(text, idx, nil)
}

// GroupByLimitFilter creates a GroupBy query with the given filter and Rows queries
func (idx *Index) GroupByLimitFilter(limit int64, filterQuery *PQLRowQuery, rowsQueries ...*PQLRowsQuery) *PQLBaseQuery {
	if len(rowsQueries) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	if limit < 0 {
		return NewPQLBaseQuery("", idx, errors.New("limit must be non-negative"))
	}
	filterText := filterQuery.serialize().String()
	text := fmt.Sprintf("GroupBy(%s,limit=%d,filter=%s)", strings.Join(serializeGroupBy(rowsQueries...), ","), limit, filterText)
	return NewPQLBaseQuery(text, idx, nil)
}

func (idx *Index) rowOperation(name string, rows ...*PQLRowQuery) *PQLRowQuery {
	var err error
	args := make([]string, 0, len(rows))
	for _, row := range rows {
		if err = row.Error(); err != nil {
			return NewPQLRowQuery("", idx, err)
		}
		args = append(args, row.serialize().String())
	}
	query := NewPQLRowQuery(fmt.Sprintf("%s(%s)", name, strings.Join(args, ",")), idx, nil)
	return query
}

func serializeGroupBy(rowsQueries ...*PQLRowsQuery) []string {
	qs := make([]string, 0, len(rowsQueries))
	for _, qry := range rowsQueries {
		qs = append(qs, qry.serialize().String())
	}
	return qs
}

// FieldInfo represents schema information for a field.
type FieldInfo struct {
	Name string `json:"name"`
}

// FieldOptions contains options to customize Field objects and field queries.
type FieldOptions struct {
	fieldType      FieldType
	timeQuantum    TimeQuantum
	cacheType      CacheType
	cacheSize      int
	min            int64
	max            int64
	keys           bool
	noStandardView bool
}

// Type returns the type of the field. Currently "set", "int", or "time".
func (fo FieldOptions) Type() FieldType {
	return fo.fieldType
}

// TimeQuantum returns the configured time quantum for a time field. Empty
// string otherwise.
func (fo FieldOptions) TimeQuantum() TimeQuantum {
	return fo.timeQuantum
}

// CacheType returns the configured cache type for a "set" field. Empty string
// otherwise.
func (fo FieldOptions) CacheType() CacheType {
	return fo.cacheType
}

// CacheSize returns the cache size for a set field. Zero otherwise.
func (fo FieldOptions) CacheSize() int {
	return fo.cacheSize
}

// Min returns the minimum accepted value for an integer field. Zero otherwise.
func (fo FieldOptions) Min() int64 {
	return fo.min
}

// Max returns the maximum accepted value for an integer field. Zero otherwise.
func (fo FieldOptions) Max() int64 {
	return fo.max
}

// Keys returns whether this field uses keys instead of IDs
func (fo FieldOptions) Keys() bool {
	return fo.keys
}

// NoStandardView suppresses creating the standard view for supported field types (currently, time)
func (fo FieldOptions) NoStandardView() bool {
	return fo.noStandardView
}

func (fo *FieldOptions) withDefaults() (updated *FieldOptions) {
	// copy options so the original is not updated
	updated = &FieldOptions{}
	*updated = *fo
	if updated.fieldType == "" {
		updated.fieldType = FieldTypeSet
	}
	return
}

func (fo FieldOptions) String() string {
	mopt := map[string]interface{}{}

	switch fo.fieldType {
	case FieldTypeSet, FieldTypeMutex:
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
		mopt["noStandardView"] = fo.noStandardView
	}

	if fo.fieldType != FieldTypeDefault {
		mopt["type"] = string(fo.fieldType)
	}
	if fo.keys {
		mopt["keys"] = fo.keys
	}
	return fmt.Sprintf(`{"options":%s}`, encodeMap(mopt))
}

func (fo *FieldOptions) addOptions(options ...FieldOption) {
	for _, option := range options {
		if option == nil {
			continue
		}
		option(fo)
	}
}

// FieldOption is used to pass an option to index.Field function.
type FieldOption func(options *FieldOptions)

// OptFieldTypeSet adds a set field.
// Specify CacheTypeDefault for the default cache type.
// Specify CacheSizeDefault for the default cache size.
func OptFieldTypeSet(cacheType CacheType, cacheSize int) FieldOption {
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeSet
		options.cacheType = cacheType
		options.cacheSize = cacheSize
	}
}

// OptFieldTypeInt adds an integer field.
// No arguments: min = min_int, max = max_int
// 1 argument: min = limit[0], max = max_int
// 2 or more arguments: min = limit[0], max = limit[1]
func OptFieldTypeInt(limits ...int64) FieldOption {
	min := int64(math.MinInt64)
	max := int64(math.MaxInt64)
	// note that we are not able to raise an error if len(limits) > 2
	if len(limits) > 0 {
		min = limits[0]
	}
	if len(limits) > 1 {
		max = limits[1]
	}
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeInt
		options.min = min
		options.max = max
	}
}

// OptFieldTypeTime adds a time field.
func OptFieldTypeTime(quantum TimeQuantum, opts ...bool) FieldOption {
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeTime
		options.timeQuantum = quantum
		if len(opts) > 0 && opts[0] {
			options.noStandardView = true
		}
	}
}

// OptFieldTypeMutex adds a mutex field.
func OptFieldTypeMutex(cacheType CacheType, cacheSize int) FieldOption {
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeMutex
		options.cacheType = cacheType
		options.cacheSize = cacheSize
	}
}

// OptFieldTypeBool adds a bool field.
func OptFieldTypeBool() FieldOption {
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeBool
	}
}

// OptFieldKeys sets whether field uses string keys.
func OptFieldKeys(keys bool) FieldOption {
	return func(options *FieldOptions) {
		options.keys = keys
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

// Opts returns the options of the field
func (f *Field) Opts() FieldOptions {
	return *f.options
}

func (f *Field) copy() *Field {
	field := newField(f.name, f.index)
	*field.options = *f.options
	return field
}

// Row creates a Row query.
// Row retrieves the indices of all the set columns in a row.
// It also retrieves any attributes set on that row or column.
func (f *Field) Row(rowIDOrKey interface{}) *PQLRowQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	text := fmt.Sprintf("Row(%s=%s)", f.name, rowStr)
	q := NewPQLRowQuery(text, f.index, nil)
	return q
}

// Set creates a Set query.
// Set, assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given field with the given column.
func (f *Field) Set(rowIDOrKey, colIDOrKey interface{}) *PQLBaseQuery {
	rowStr, colStr, err := formatRowColIDKey(rowIDOrKey, colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("Set(%s,%s=%s)", colStr, f.name, rowStr)
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// SetTimestamp creates a Set query with timestamp.
// Set, assigns a value of 1 to a column in the binary matrix,
// thus associating the given row in the given field with the given column.
func (f *Field) SetTimestamp(rowIDOrKey, colIDOrKey interface{}, timestamp time.Time) *PQLBaseQuery {
	rowStr, colStr, err := formatRowColIDKey(rowIDOrKey, colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("Set(%s,%s=%s,%s)", colStr, f.name, rowStr, timestamp.Format(timeFormat))
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// Clear creates a Clear query.
// Clear, assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given field from the given column.
func (f *Field) Clear(rowIDOrKey, colIDOrKey interface{}) *PQLBaseQuery {
	rowStr, colStr, err := formatRowColIDKey(rowIDOrKey, colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("Clear(%s,%s=%s)", colStr, f.name, rowStr)
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// ClearRow creates a ClearRow query.
// ClearRow sets all bits to 0 in a given row of the binary matrix, thus disassociating the given row in the given field from all columns.
func (f *Field) ClearRow(rowIDOrKey interface{}) *PQLBaseQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("ClearRow(%s=%s)", f.name, rowStr)
	q := NewPQLBaseQuery(text, f.index, nil)
	return q
}

// TopN creates a TopN query with the given item count.
// Returns the id and count of the top n rows (by count of columns) in the field.
func (f *Field) TopN(n uint64) *PQLRowQuery {
	q := NewPQLRowQuery(fmt.Sprintf("TopN(%s,n=%d)", f.name, n), f.index, nil)
	return q
}

// RowTopN creates a TopN query with the given item count and row.
// This variant supports customizing the row query.
func (f *Field) RowTopN(n uint64, row *PQLRowQuery) *PQLRowQuery {
	q := NewPQLRowQuery(fmt.Sprintf("TopN(%s,%s,n=%d)",
		f.name, row.serialize(), n), f.index, nil)
	return q
}

// FilterAttrTopN creates a TopN query with the given item count, row, attribute name and filter values for that field
// The attrName and attrValues arguments work together to only return Rows which have the attribute specified by attrName with one of the values specified in attrValues.
func (f *Field) FilterAttrTopN(n uint64, row *PQLRowQuery, attrName string, attrValues ...interface{}) *PQLRowQuery {
	return f.filterAttrTopN(n, row, attrName, attrValues...)
}

func (f *Field) filterAttrTopN(n uint64, row *PQLRowQuery, field string, values ...interface{}) *PQLRowQuery {
	if err := validateLabel(field); err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	b, err := json.Marshal(values)
	if err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	var q *PQLRowQuery
	if row == nil {
		q = NewPQLRowQuery(fmt.Sprintf("TopN(%s,n=%d,attrName='%s',attrValues=%s)",
			f.name, n, field, string(b)), f.index, nil)
	} else {
		serializedRow := row.serialize()
		q = NewPQLRowQuery(fmt.Sprintf("TopN(%s,%s,n=%d,attrName='%s',attrValues=%s)",
			f.name, serializedRow.String(), n, field, string(b)), f.index, nil)
	}
	return q
}

// Range creates a Range query.
// Similar to Row, but only returns columns which were set with timestamps between the given start and end timestamps.
// *Deprecated at Pilosa 1.3*
func (f *Field) Range(rowIDOrKey interface{}, start time.Time, end time.Time) *PQLRowQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	text := fmt.Sprintf("Range(%s=%s,%s,%s)", f.name, rowStr, start.Format(timeFormat), end.Format(timeFormat))
	q := NewPQLRowQuery(text, f.index, nil)
	return q
}

// RowRange creates a Row query with timestamps.
// Similar to Row, but only returns columns which were set with timestamps between the given start and end timestamps.
// *Introduced at Pilosa 1.3*
func (f *Field) RowRange(rowIDOrKey interface{}, start time.Time, end time.Time) *PQLRowQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	text := fmt.Sprintf("Row(%s=%s,from='%s',to='%s')", f.name, rowStr, start.Format(timeFormat), end.Format(timeFormat))
	q := NewPQLRowQuery(text, f.index, nil)
	return q
}

// SetRowAttrs creates a SetRowAttrs query.
// SetRowAttrs associates arbitrary key/value pairs with a row in a field.
// Following types are accepted: integer, float, string and boolean types.
func (f *Field) SetRowAttrs(rowIDOrKey interface{}, attrs map[string]interface{}) *PQLBaseQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("SetRowAttrs(%s,%s,%s)", f.name, rowStr, attrsString)
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// Store creates a Store call.
// Store writes the result of the row query to the specified row. If the row already exists, it will be replaced. The destination field must be of field type set.
func (f *Field) Store(row *PQLRowQuery, rowIDOrKey interface{}) *PQLBaseQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("Store(%s,%s=%s)", row.serialize().String(), f.name, rowStr), f.index, nil)
}

func createAttributesString(attrs map[string]interface{}) (string, error) {
	attrsList := make([]string, 0, len(attrs))
	for k, v := range attrs {
		// TODO: validate the type of v is one of string, int64, float64, bool
		if err := validateLabel(k); err != nil {
			return "", err
		}
		if vs, ok := v.(string); ok {
			attrsList = append(attrsList, fmt.Sprintf("%s=%s", k, strconv.Quote(vs)))
		} else {
			attrsList = append(attrsList, fmt.Sprintf("%s=%v", k, v))
		}
	}
	sort.Strings(attrsList)
	return strings.Join(attrsList, ","), nil
}

func formatIDKey(idKey interface{}) (string, error) {
	switch v := idKey.(type) {
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case string:
		return fmt.Sprintf(`'%s'`, v), nil
	default:
		return "", errors.Errorf("id/key is not a string or integer type: %#v", idKey)
	}
}

func formatIDKeyBool(idKeyBool interface{}) (string, error) {
	if b, ok := idKeyBool.(bool); ok {
		return strconv.FormatBool(b), nil
	}
	return formatIDKey(idKeyBool)
}

func formatRowColIDKey(rowIDOrKey, colIDOrKey interface{}) (string, string, error) {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return "", "", errors.Wrap(err, "formatting row")
	}
	colStr, err := formatIDKey(colIDOrKey)
	if err != nil {
		return "", "", errors.Wrap(err, "formatting column")
	}
	return rowStr, colStr, err
}

// FieldType is the type of a field.
// See: https://www.pilosa.com/docs/latest/data-model/#field-type
type FieldType string

const (
	// FieldTypeDefault is the default field type.
	FieldTypeDefault FieldType = ""
	// FieldTypeSet is the set field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#set
	FieldTypeSet FieldType = "set"
	// FieldTypeInt is the int field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#int
	FieldTypeInt FieldType = "int"
	// FieldTypeTime is the time field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#time
	FieldTypeTime FieldType = "time"
	// FieldTypeMutex is the mutex field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#mutex
	FieldTypeMutex FieldType = "mutex"
	// FieldTypeBool is the boolean field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#boolean
	FieldTypeBool FieldType = "bool"
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

// Options returns the options set for the field. Which fields of the
// FieldOptions struct are actually being used depends on the field's type.
// *DEPRECATED*
func (f *Field) Options() *FieldOptions {
	return f.options
}

// LT creates a less than query.
func (f *Field) LT(n int) *PQLRowQuery {
	return f.binaryOperation("<", n)
}

// LTE creates a less than or equal query.
func (f *Field) LTE(n int) *PQLRowQuery {
	return f.binaryOperation("<=", n)
}

// GT creates a greater than query.
func (f *Field) GT(n int) *PQLRowQuery {
	return f.binaryOperation(">", n)
}

// GTE creates a greater than or equal query.
func (f *Field) GTE(n int) *PQLRowQuery {
	return f.binaryOperation(">=", n)
}

// Equals creates an equals query.
func (f *Field) Equals(n int) *PQLRowQuery {
	return f.binaryOperation("==", n)
}

// NotEquals creates a not equals query.
func (f *Field) NotEquals(n int) *PQLRowQuery {
	return f.binaryOperation("!=", n)
}

// NotNull creates a not equal to null query.
func (f *Field) NotNull() *PQLRowQuery {
	text := fmt.Sprintf("Range(%s != null)", f.name)
	q := NewPQLRowQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// Between creates a between query.
func (f *Field) Between(a int, b int) *PQLRowQuery {
	text := fmt.Sprintf("Range(%s >< [%d,%d])", f.name, a, b)
	q := NewPQLRowQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// Sum creates a sum query.
func (f *Field) Sum(row *PQLRowQuery) *PQLBaseQuery {
	return f.valQuery("Sum", row)
}

// Min creates a min query.
func (f *Field) Min(row *PQLRowQuery) *PQLBaseQuery {
	return f.valQuery("Min", row)
}

// Max creates a min query.
func (f *Field) Max(row *PQLRowQuery) *PQLBaseQuery {
	return f.valQuery("Max", row)
}

// SetIntValue creates a Set query.
func (f *Field) SetIntValue(colIDOrKey interface{}, value int) *PQLBaseQuery {
	colStr, err := formatIDKey(colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	q := fmt.Sprintf("Set(%s, %s=%d)", colStr, f.name, value)
	return NewPQLBaseQuery(q, f.index, nil)
}

// PQLRowsQuery is the return type for Rows calls.
type PQLRowsQuery struct {
	index *Index
	pql   string
	err   error
}

// NewPQLRowsQuery creates a new PQLRowsQuery.
func NewPQLRowsQuery(pql string, index *Index, err error) *PQLRowsQuery {
	return &PQLRowsQuery{
		index: index,
		pql:   pql,
		err:   err,
	}
}

// Index returns the index for this query/
func (q *PQLRowsQuery) Index() *Index {
	return q.index
}

func (q *PQLRowsQuery) Serialize() SerializedQuery {
	return q.serialize()
}

func (q *PQLRowsQuery) serialize() SerializedQuery {
	return newSerializedQuery(q.pql, false)
}

// Error returns the error or nil for this query.
func (q PQLRowsQuery) Error() error {
	return q.err
}

// Rows creates a Rows query with defaults
func (f *Field) Rows() *PQLRowsQuery {
	text := fmt.Sprintf("Rows(field='%s')", f.name)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsPrevious creates a Rows query with the given previous row ID/key
func (f *Field) RowsPrevious(rowIDOrKey interface{}) *PQLRowsQuery {
	idKey, err := formatIDKey(rowIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',previous=%s)", f.name, idKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsLimit creates a Rows query with the given limit
func (f *Field) RowsLimit(limit int64) *PQLRowsQuery {
	if limit < 0 {
		return NewPQLRowsQuery("", f.index, errors.New("rows limit must be non-negative"))
	}
	text := fmt.Sprintf("Rows(field='%s',limit=%d)", f.name, limit)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsColumn creates a Rows query with the given column ID/key
func (f *Field) RowsColumn(columnIDOrKey interface{}) *PQLRowsQuery {
	idKey, err := formatIDKey(columnIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',column=%s)", f.name, idKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsPreviousLimit creates a Rows query with the given previous row ID/key and limit
func (f *Field) RowsPreviousLimit(rowIDOrKey interface{}, limit int64) *PQLRowsQuery {
	idKey, err := formatIDKey(rowIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	if limit < 0 {
		return NewPQLRowsQuery("", f.index, errors.New("rows limit must be non-negative"))
	}
	text := fmt.Sprintf("Rows(field='%s',previous=%s,limit=%d)", f.name, idKey, limit)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsPreviousColumn creates a Rows query with the given previous row ID/key and column ID/key
func (f *Field) RowsPreviousColumn(rowIDOrKey interface{}, columnIDOrKey interface{}) *PQLRowsQuery {
	rowIDKey, err := formatIDKey(rowIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	columnIDKey, err := formatIDKey(columnIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',previous=%s,column=%s)", f.name, rowIDKey, columnIDKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsLimitColumn creates a Row query with the given limit and column ID/key
func (f *Field) RowsLimitColumn(limit int64, columnIDOrKey interface{}) *PQLRowsQuery {
	if limit < 0 {
		return NewPQLRowsQuery("", f.index, errors.New("rows limit must be non-negative"))
	}
	columnIDKey, err := formatIDKey(columnIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',limit=%d,column=%s)", f.name, limit, columnIDKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsPreviousLimitColumn creates a Row query with the given previous row ID/key, limit and column ID/key
func (f *Field) RowsPreviousLimitColumn(rowIDOrKey interface{}, limit int64, columnIDOrKey interface{}) *PQLRowsQuery {
	rowIDKey, err := formatIDKey(rowIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	if limit < 0 {
		return NewPQLRowsQuery("", f.index, errors.New("rows limit must be non-negative"))
	}
	columnIDKey, err := formatIDKey(columnIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',previous=%s,limit=%d,column=%s)", f.name, rowIDKey, limit, columnIDKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

func (f *Field) binaryOperation(op string, n int) *PQLRowQuery {
	text := fmt.Sprintf("Range(%s %s %d)", f.name, op, n)
	q := NewPQLRowQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

func (f *Field) valQuery(op string, row *PQLRowQuery) *PQLBaseQuery {
	rowStr := ""
	hasKeys := f.options.keys || f.index.options.keys
	if row != nil {
		serializedRow := row.serialize()
		hasKeys = hasKeys || serializedRow.HasWriteKeys()
		rowStr = fmt.Sprintf("%s,", serializedRow.String())
	}
	text := fmt.Sprintf("%s(%sfield='%s')", op, rowStr, f.name)
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = hasKeys
	return q
}

func encodeMap(m map[string]interface{}) string {
	result, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(result)
}
