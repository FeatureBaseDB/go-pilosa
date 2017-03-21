package pilosa

import (
	"fmt"
	"strings"
)

// IPqlQuery is a interface for PQL queries
type IPqlQuery interface {
	GetDatabase() *Database
	ToString() string
}

// PqlQuery is the base implementation for IPqlQuery
type PqlQuery struct {
	database *Database
	pql      string
}

// NewPqlQuery creates a new PqlQuery with the given PQL and database
func NewPqlQuery(pql string, database *Database) *PqlQuery {
	return &PqlQuery{
		database: database,
		pql:      pql,
	}
}

// GetDatabase returns the database for this query
func (q *PqlQuery) GetDatabase() *Database {
	return q.database
}

// ToString converts this query to string
func (q *PqlQuery) ToString() string {
	return q.pql
}

// PqlBitmapQuery is the return type for bitmap queries
type PqlBitmapQuery struct {
	database *Database
	pql      string
}

// GetDatabase returns the database for this query
func (q *PqlBitmapQuery) GetDatabase() *Database {
	return q.database
}

// ToString converts this query to string
func (q *PqlBitmapQuery) ToString() string {
	return q.pql
}

// DatabaseOptions contains the options for a Pilosa database
type DatabaseOptions struct {
	columnLabel string
}

// NewPqlBitmapQuery creates a new PqlBitmapQuery
func NewPqlBitmapQuery(pql string, database *Database) *PqlBitmapQuery {
	return &PqlBitmapQuery{
		database: database,
		pql:      pql,
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

// GetName returns the name of this database
func (d *Database) GetName() string {
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
func (d *Database) Union(bitmap1 *PqlBitmapQuery, bitmap2 *PqlBitmapQuery, bitmaps ...*PqlBitmapQuery) *PqlBitmapQuery {
	return d.bitmapOperation("Union", bitmap1, bitmap2, bitmaps...)
}

// Intersect creates an Intersect query
func (d *Database) Intersect(bitmap1 *PqlBitmapQuery, bitmap2 *PqlBitmapQuery, bitmaps ...*PqlBitmapQuery) *PqlBitmapQuery {
	return d.bitmapOperation("Intersect", bitmap1, bitmap2, bitmaps...)
}

// Difference creates an Intersect query
func (d *Database) Difference(bitmap1 *PqlBitmapQuery, bitmap2 *PqlBitmapQuery, bitmaps ...*PqlBitmapQuery) *PqlBitmapQuery {
	return d.bitmapOperation("Difference", bitmap1, bitmap2, bitmaps...)
}

// Count creates a Count query
func (d *Database) Count(bitmap *PqlBitmapQuery) IPqlQuery {
	return NewPqlQuery(fmt.Sprintf("Count(%s)", bitmap.ToString()), d)
}

func (d *Database) bitmapOperation(name string, bitmap1 *PqlBitmapQuery, bitmap2 *PqlBitmapQuery, bitmaps ...*PqlBitmapQuery) *PqlBitmapQuery {
	args := make([]string, 0, 2+len(bitmaps))
	args = append(args, bitmap1.ToString(), bitmap2.ToString())
	for _, bitmap := range bitmaps {
		args = append(args, bitmap.ToString())
	}
	return NewPqlBitmapQuery(fmt.Sprintf("%s(%s)", name, strings.Join(args, ", ")), d)
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
func (f *Frame) Bitmap(rowID uint64) *PqlBitmapQuery {
	return NewPqlBitmapQuery(fmt.Sprintf("Bitmap(%s=%d, frame='%s')",
		f.options.rowLabel, rowID, f.name), f.database)
}

// SetBit creates a SetBit query
func (f *Frame) SetBit(rowID uint64, columnID uint64) IPqlQuery {
	return NewPqlQuery(fmt.Sprintf("SetBit(%s=%d, frame='%s', %s=%d)",
		f.options.rowLabel, rowID, f.name, f.database.options.columnLabel, columnID), f.database)
}

// ClearBit creates a ClearBit query
func (f *Frame) ClearBit(rowID uint64, columnID uint64) IPqlQuery {
	return NewPqlQuery(fmt.Sprintf("ClearBit(%s=%d, frame='%s', %s=%d)",
		f.options.rowLabel, rowID, f.name, f.database.options.columnLabel, columnID), f.database)
}

// TopN creates a TopN query with the given item count
func (f *Frame) TopN(n uint64) *PqlBitmapQuery {
	return NewPqlBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d)", f.name, n), f.database)
}

// BitmapTopN creates a TopN query with the given item count and bitmap
func (f *Frame) BitmapTopN(n uint64, bitmap *PqlBitmapQuery) *PqlBitmapQuery {
	return NewPqlBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d)",
		bitmap.ToString(), f.name, n), f.database)
}

// TODO: FilterTopN variant.
