package csv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	pilosa "github.com/pilosa/go-pilosa"
)

type Format uint

const (
	RowIDColumnID Format = iota
	RowIDColumnKey
	RowKeyColumnID
	RowKeyColumnKey
	ColumnID
	ColumnKey
)

func ColumnUnmarshaller(format Format) RecordUnmarshaller {
	return ColumnUnmarshallerWithTimestamp(format, "")
}

func ColumnUnmarshallerWithTimestamp(format Format, timestampFormat string) RecordUnmarshaller {
	return func(text string) (pilosa.Record, error) {
		var err error
		column := pilosa.Column{}
		parts := strings.Split(text, ",")
		if len(parts) < 2 {
			return nil, errors.New("Invalid CSV line")
		}

		hasRowKey := format == RowKeyColumnID || format == RowKeyColumnKey
		hasColumnKey := format == RowIDColumnKey || format == RowKeyColumnKey

		if hasRowKey {
			column.RowKey = parts[0]
		} else {
			column.RowID, err = strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				return nil, errors.New("Invalid row ID")
			}
		}

		if hasColumnKey {
			column.ColumnKey = parts[1]
		} else {
			column.ColumnID, err = strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return nil, errors.New("Invalid column ID")
			}
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
		column.Timestamp = int64(timestamp)

		return column, nil
	}
}

type RecordUnmarshaller func(text string) (pilosa.Record, error)

// Iterator reads records from a Reader.
// Each line should contain a single record in the following form:
// field1,field2,...
type Iterator struct {
	reader       io.Reader
	line         int
	scanner      *bufio.Scanner
	unmarshaller RecordUnmarshaller
}

// NewIterator creates a CSVIterator from a Reader.
func NewIterator(reader io.Reader, unmarshaller RecordUnmarshaller) *Iterator {
	return &Iterator{
		reader:       reader,
		line:         0,
		scanner:      bufio.NewScanner(reader),
		unmarshaller: unmarshaller,
	}
}

func NewColumnIterator(format Format, reader io.Reader) *Iterator {
	return NewIterator(reader, ColumnUnmarshaller(format))
}

func NewColumnIteratorWithTimestampFormat(format Format, reader io.Reader, timestampFormat string) *Iterator {
	return NewIterator(reader, ColumnUnmarshallerWithTimestamp(format, timestampFormat))
}

func NewValueIterator(format Format, reader io.Reader) *Iterator {
	return NewIterator(reader, FieldValueUnmarshaller(format))
}

// NextRecord iterates on lines of a Reader.
// Returns io.EOF on end of iteration.
func (c *Iterator) NextRecord() (pilosa.Record, error) {
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

func FieldValueUnmarshaller(format Format) RecordUnmarshaller {
	return func(text string) (pilosa.Record, error) {
		parts := strings.Split(text, ",")
		if len(parts) < 2 {
			return nil, errors.New("Invalid CSV")
		}
		value, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, errors.New("Invalid value")
		}
		switch format {
		case ColumnID:
			columnID, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				return nil, errors.New("Invalid column ID at line: %d")
			}
			return pilosa.FieldValue{
				ColumnID: uint64(columnID),
				Value:    value,
			}, nil
		case ColumnKey:
			return pilosa.FieldValue{
				ColumnKey: parts[0],
				Value:     value,
			}, nil
		default:
			return nil, fmt.Errorf("Invalid format: %d", format)
		}
	}
}
