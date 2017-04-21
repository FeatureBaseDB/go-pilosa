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
	Database() *Database
	serialize() string
	Error() error
}

// PQLBaseQuery is the base implementation for IPqlQuery
type PQLBaseQuery struct {
	database *Database
	pql      string
	err      error
}

// NewPQLBaseQuery creates a new PqlQuery with the given PQL and database
func NewPQLBaseQuery(pql string, database *Database, err error) *PQLBaseQuery {
	return &PQLBaseQuery{
		database: database,
		pql:      pql,
		err:      err,
	}
}

// Database returns the database for this query
func (q *PQLBaseQuery) Database() *Database {
	return q.database
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
	database *Database
	pql      string
	err      error
}

// Database returns the database for this query
func (q *PQLBitmapQuery) Database() *Database {
	return q.database
}

// serialize converts this query to string
func (q *PQLBitmapQuery) serialize() string {
	return q.pql
}

// Error returns the error or nil for this query
func (q PQLBitmapQuery) Error() error {
	return q.err
}

type PQLBatchQuery struct {
	database *Database
	queries  []string
	err      error
}

// Database returns the database for this query
func (q *PQLBatchQuery) Database() *Database {
	return q.database
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

// DatabaseOptions contains the options for a Pilosa database
type DatabaseOptions struct {
	ColumnLabel string
	TimeQuantum TimeQuantum
}

func (options *DatabaseOptions) withDefaults() (updated *DatabaseOptions) {
	// copy options so the original is not updated
	updated = &DatabaseOptions{}
	*updated = *options
	// impose defaults
	if updated.ColumnLabel == "" {
		updated.ColumnLabel = "col_id"
	}
	return
}

func (options DatabaseOptions) String() string {
	return fmt.Sprintf(`{"options": {"columnLabel": "%s"}}`, options.ColumnLabel)
}

// NewPQLBitmapQuery creates a new PqlBitmapQuery
func NewPQLBitmapQuery(pql string, database *Database, err error) *PQLBitmapQuery {
	return &PQLBitmapQuery{
		database: database,
		pql:      pql,
		err:      err,
	}
}

// Database is a Pilosa database
type Database struct {
	name    string
	options *DatabaseOptions
}

// NewDatabase creates the info for a Pilosa database with the given options
func NewDatabase(name string, options *DatabaseOptions) (*Database, error) {
	if err := validateDatabaseName(name); err != nil {
		return nil, err
	}
	if options == nil {
		options = &DatabaseOptions{}
	}
	options = options.withDefaults()
	if err := validateLabel(options.ColumnLabel); err != nil {
		return nil, err
	}
	return &Database{
		name:    name,
		options: options,
	}, nil
}

// Name returns the name of this database
func (d *Database) Name() string {
	return d.name
}

// Frame creates the info for a Pilosa frame with default options
func (d *Database) Frame(name string, options *FrameOptions) (*Frame, error) {
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
		name:     name,
		database: d,
		options:  options,
	}, nil
}

// BatchQuery creates a batch query
func (d *Database) BatchQuery(queries ...PQLQuery) *PQLBatchQuery {
	stringQueries := make([]string, 0, len(queries))
	for _, query := range queries {
		stringQueries = append(stringQueries, query.serialize())
	}
	return &PQLBatchQuery{
		database: d,
		queries:  stringQueries,
	}
}

// RawQuery creates a query with the given string
func (d *Database) RawQuery(query string) *PQLBaseQuery {
	return NewPQLBaseQuery(query, d, nil)
}

// Union creates a Union query
func (d *Database) Union(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	return d.bitmapOperation("Union", bitmap1, bitmap2, bitmaps...)
}

// Intersect creates an Intersect query
func (d *Database) Intersect(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	return d.bitmapOperation("Intersect", bitmap1, bitmap2, bitmaps...)
}

// Difference creates an Intersect query
func (d *Database) Difference(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	return d.bitmapOperation("Difference", bitmap1, bitmap2, bitmaps...)
}

// Count creates a Count query
func (d *Database) Count(bitmap *PQLBitmapQuery) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Count(%s)", bitmap.serialize()), d, nil)
}

// SetProfileAttrs creates a SetProfileAttrs query
func (d *Database) SetProfileAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", d, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetProfileAttrs(%s=%d, %s)",
		d.options.ColumnLabel, columnID, attrsString), d, nil)
}

func (d *Database) bitmapOperation(name string, bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
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
		updated.RowLabel = "id"
	}
	return
}

func (options FrameOptions) String() string {
	return fmt.Sprintf(`{"options": {"rowLabel": "%s", "inverseEnabled": %v}}`,
		options.RowLabel, options.InverseEnabled)
}

// Frame is a Pilosa frame
type Frame struct {
	name     string
	database *Database
	options  *FrameOptions
}

// Bitmap creates a bitmap query
func (f *Frame) Bitmap(rowID uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Bitmap(%s=%d, frame='%s')",
		f.options.RowLabel, rowID, f.name), f.database, nil)
}

// InverseBitmap creates an inverse bitmap query
func (f *Frame) InverseBitmap(columnID uint64) *PQLBaseQuery {
	if !f.options.InverseEnabled {
		return NewPQLBaseQuery("", f.database, ErrorInverseBitmapsNotEnabled)
	}
	return NewPQLBaseQuery(fmt.Sprintf("Bitmap(%s=%d, frame='%s')",
		f.database.options.ColumnLabel, columnID, f.name), f.database, nil)
}

// SetBit creates a SetBit query
func (f *Frame) SetBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(%s=%d, frame='%s', %s=%d)",
		f.options.RowLabel, rowID, f.name, f.database.options.ColumnLabel, columnID), f.database, nil)
}

// ClearBit creates a ClearBit query
func (f *Frame) ClearBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("ClearBit(%s=%d, frame='%s', %s=%d)",
		f.options.RowLabel, rowID, f.name, f.database.options.ColumnLabel, columnID), f.database, nil)
}

// TopN creates a TopN query with the given item count
func (f *Frame) TopN(n uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d)", f.name, n), f.database, nil)
}

// BitmapTopN creates a TopN query with the given item count and bitmap
func (f *Frame) BitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d)",
		bitmap.serialize(), f.name, n), f.database, nil)
}

// FilterFieldTopN creates a TopN query with the given item count, bitmap, field and the filter for that field
func (f *Frame) FilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery {
	if err := validateLabel(field); err != nil {
		return NewPQLBitmapQuery("", f.database, err)
	}
	b, err := json.Marshal(values)
	if err != nil {
		return NewPQLBitmapQuery("", f.database, err)
	}
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d, field='%s', %s)",
		bitmap.serialize(), f.name, n, field, string(b)), f.database, nil)
}

// Range creates a Range query
func (f *Frame) Range(rowID uint64, start time.Time, end time.Time) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Range(%s=%d, frame='%s', start='%s', end='%s')",
		f.options.RowLabel, rowID, f.name, start.Format(timeFormat), end.Format(timeFormat)), f.database, nil)
}

// SetBitmapAttrs creates a SetBitmapAttrs query
func (f *Frame) SetBitmapAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", f.database, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetBitmapAttrs(%s=%d, frame='%s', %s)",
		f.options.RowLabel, rowID, f.name, attrsString), f.database, nil)
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
