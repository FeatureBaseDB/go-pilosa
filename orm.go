package pilosa

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// PQLQuery is a interface for PQL queries
type PQLQuery interface {
	Database() *Database
	String() string
	Error() error
}

// PQLBaseQuery is the base implementation for IPqlQuery
type PQLBaseQuery struct {
	database *Database
	pql      string
	err      error
}

// NewPQLQuery creates a new PqlQuery with the given PQL and database
func NewPQLQuery(pql string, database *Database, err error) *PQLBaseQuery {
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

// String converts this query to string
func (q *PQLBaseQuery) String() string {
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

// String converts this query to string
func (q *PQLBitmapQuery) String() string {
	return q.pql
}

// Error returns the error or nil for this query
func (q PQLBitmapQuery) Error() error {
	return q.err
}

// DatabaseOptions contains the options for a Pilosa database
type DatabaseOptions struct {
	columnLabel string
}

// NewPQLBitmapQuery creates a new PqlBitmapQuery
func NewPQLBitmapQuery(pql string, database *Database, err error) *PQLBitmapQuery {
	return &PQLBitmapQuery{
		database: database,
		pql:      pql,
		err:      err,
	}
}

// NewDatabaseOptionsWithColumnLabel creates a DatabaseOptions struct with the given label
func NewDatabaseOptionsWithColumnLabel(columnLabel string) (*DatabaseOptions, error) {
	if err := validateLabel(columnLabel); err != nil {
		return nil, err
	}
	return &DatabaseOptions{
		columnLabel: columnLabel,
	}, nil
}

// Database is a Pilosa database
type Database struct {
	name    string
	options DatabaseOptions
}

// NewDatabase creates the info for a Pilosa database with default options
func NewDatabase(name string) (*Database, error) {
	return NewDatabaseWithColumnLabel(name, "profileID")
}

// NewDatabaseWithColumnLabel creates the info for a Pilosa database with the given column label
func NewDatabaseWithColumnLabel(name string, label string) (*Database, error) {
	options, err := NewDatabaseOptionsWithColumnLabel(label)
	if err != nil {
		return nil, err
	}
	return NewDatabaseWithOptions(name, options)
}

// NewDatabaseWithOptions creates the info for a Pilosa database with the given options
func NewDatabaseWithOptions(name string, options *DatabaseOptions) (*Database, error) {
	if err := validateDatabaseName(name); err != nil {
		return nil, err
	}
	return &Database{
		name:    name,
		options: *options,
	}, nil
}

// Name returns the name of this database
func (d *Database) Name() string {
	return d.name
}

// Frame creates the info for a Pilosa frame with default options
func (d *Database) Frame(name string) (*Frame, error) {
	return d.FrameWithRowLabel(name, "id")
}

// FrameWithRowLabel creates the info for a Pilosa frame with the given label
func (d *Database) FrameWithRowLabel(name string, label string) (*Frame, error) {
	if err := validateFrameName(name); err != nil {
		return nil, err
	}
	return &Frame{
		name:     name,
		database: d,
		options:  FrameOptions{rowLabel: label},
	}, nil
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
	return NewPQLQuery(fmt.Sprintf("Count(%s)", bitmap.String()), d, nil)
}

// SetProfileAttrs creates a SetProfileAttrs query
func (d *Database) SetProfileAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLQuery("", d, err)
	}
	return NewPQLQuery(fmt.Sprintf("SetProfileAttrs(%s=%d, %s)",
		d.options.columnLabel, columnID, attrsString), d, nil)
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
	args = append(args, bitmap1.String(), bitmap2.String())
	for _, bitmap := range bitmaps {
		if err = bitmap.Error(); err != nil {
			return NewPQLBitmapQuery("", d, err)
		}
		args = append(args, bitmap.String())
	}
	return NewPQLBitmapQuery(fmt.Sprintf("%s(%s)", name, strings.Join(args, ", ")), d, nil)
}

// FrameInfo represents schema information for a frame.
type FrameInfo struct {
	Name string `json:"name"`
}

// FrameOptions contains frame options
type FrameOptions struct {
	rowLabel string
}

// Frame is a Pilosa frame
type Frame struct {
	name     string
	database *Database
	options  FrameOptions
}

// Bitmap creates a bitmap query
func (f *Frame) Bitmap(rowID uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Bitmap(%s=%d, frame='%s')",
		f.options.rowLabel, rowID, f.name), f.database, nil)
}

// SetBit creates a SetBit query
func (f *Frame) SetBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLQuery(fmt.Sprintf("SetBit(%s=%d, frame='%s', %s=%d)",
		f.options.rowLabel, rowID, f.name, f.database.options.columnLabel, columnID), f.database, nil)
}

// ClearBit creates a ClearBit query
func (f *Frame) ClearBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLQuery(fmt.Sprintf("ClearBit(%s=%d, frame='%s', %s=%d)",
		f.options.rowLabel, rowID, f.name, f.database.options.columnLabel, columnID), f.database, nil)
}

// TopN creates a TopN query with the given item count
func (f *Frame) TopN(n uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d)", f.name, n), f.database, nil)
}

// BitmapTopN creates a TopN query with the given item count and bitmap
func (f *Frame) BitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d)",
		bitmap, f.name, n), f.database, nil)
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
		bitmap, f.name, n, field, string(b)), f.database, nil)
}

// SetBitmapAttrs creates a SetBitmapAttrs query
func (f *Frame) SetBitmapAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLQuery("", f.database, err)
	}
	return NewPQLQuery(fmt.Sprintf("SetBitmapAttrs(%s=%d, frame='%s', %s)",
		f.options.rowLabel, rowID, f.name, attrsString), f.database, nil)
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
