# Importing and Exporting Data

## Importing Data

### Overview

If you have large amounts of data, it is more efficient to import it into Pilosa instead of using multiple Set queries. This library supports importing anything which implements the `Record` interface, such as `Column`s or `FieldValue`s into an existing field.

Once you create the field in which to import data, you need to create an instance of a struct which implements the `RecordIterator` interface. This library ships with [csv.Iterator](#csv-iterator).

Finally you should call `client.ImportField` with the necessary parameters to start the import process.

### A Simple Example

Let's use the `csv.Iterator` which reads `Column`s or `FieldValue`s from a CSV reader. We have to choose one of the CSV format hints, depending on the data: `RowIDColumnID`, `RowIDColumnKey`, `RowKeyColumnID`, `RowKeyColumnKey` for `Column` data or `ColumnID`, `ColumnKey`. Here is an example with sample data, which is in the `ROW_ID,COLUMN_ID` format, hence uses `RowIDColumnID`:
```go
import "github.com/pilosa/go-pilosa/csv"
// Ensure field exists.
// ...
// Sample CSV data.
text := `10,7
         10,5
         2,3
         7,1`
// Create the iterator
iterator := csv.NewColumnIterator(csv.RowIDColumnID, strings.NewReader(text))
// Start the import
err := client.ImportField(field, iterator)
if err != nil {
	log.Fatal(err)
}
```

It is possible to set import options, e.g., the number of goroutines and also track the status of the import. We are going to see how to accomplish those in the [Advanced Usage](#advanced-usage) section.

### <a name="csv-iterator"></a>csv.Iterator

The `csv.Iterator` reads lines from an `io.Reader` and converts them to `Record`s using the appropriate `csv.RecordUnmarshaller`.

The expected CSV format is:
```
VALUE1,VALUE2,...
```

Note that each line corresponds to a single column and ends with a new line (`\n` or `\r\n`), except the last line (which is optional).

`csv.RecordUnmarshaller` is any function which takes some string and returns a `Record` or `error`. It is defined as follows:
```go
type RecordUnmarshaller func(text string) (pilosa.Record, error)
```

We have three predefined `csv.CSVRecordUnmarshaller`s in this library: `csv.ColumnUnmarshaller`, `csv.ColumnUnmarshallerWithTimestamp` and `csv.FieldValueUnmarshaller` which are explained in the subsections below.

#### Column Iterator

Depending on the data, the following format is expected:

* `csv.RowIDColumnID`: `ROW_ID,COLUMN_ID`
* `csv.RowIDColumnKey`: `ROW_ID,COLUMN_KEY`
* `csv.RowKeyColumnID`: `ROW_KEY,COLUMN_ID`
* `csv.RowKeyColumnKey`: `ROW_KEY,COLUMN_KEY`

Example:
```go
iterator := csv.NewColumnIterator(csv.RowIDColumnID, strings.NewReader(text))
```

Optionally, a timestamp can be added. Note that Pilosa is not time zone aware:
```
ROW_ID,COLUMN_ID,TIMESTAMP
```

```go
format := "2006-01-02T03:04"
iterator := csv.NewColumnIteratorWithTimestampFormat(strings.NewReader(text), format)
```

#### Field Value Iterator

Depending on the data, the following format is expected:

* `CSVColumnID`: `COLUMN_ID,INTEGER_VALUE`
* `CSVColumnKey`: `COLUMN_KEY,INTEGER_VALUE`

Example:
```go
iterator := pilosa.csv.NewValueIterator(csv.ColumnID, strings.NewReader(text))
```

### RecordIterator

In case your data is not coming from a CSV data source (*highly likely!*) you need to write your own struct which implements the `RecordIterator` interface. The `RecordIterator` is defined as follows:
```go
type RecordIterator interface {
	NextRecord() (Record, error)
}
```
So, a `RecordIterator` returns either the next `Record` when its `NextRecord` function is called, or an `error`. If there is no other `Record` to return, `NextRecord` returns `io.EOF.

A record is a struct instance which implements the `Record` interface. Currently that means `Column` and `FieldValue` structs.

Let's define a simple `RecordIterator` which returns a predefined number of random `Column`s:
```go
type RandomColumnGenerator struct {
	maxRowID uint64
	maxColumnID uint64
	maxColumns int
}

func (gen *RandomColumnGenerator) NextRecord() (pilosa.Record, error) {
	if gen.maxColumns <= 0 {
		return nil, io.EOF
	}
	gen.maxColumns -= 1
    return pilosa.Column{
        RowID: uint64(rand.Int63n(int64(gen.maxRowID))),
        ColumnID: uint64(rand.Int63n(int64(gen.maxColumnID))),
    }, nil
}
```

If you intend to import values for a range field, return `FieldValue`s instead of `Column`s:
```go
func (gen *RandomColumnGenerator) NextRecord() (pilosa.Record, error) {
	// ...
    return pilosa.FieldValue{
        ColumnID: uint64(rand.Int63n(int64(gen.maxRowID))),
        Value: 42,
    }, nil
}
```

### <a name="advanced-usage"></a>Advanced Usage

#### Import Options

You can change the thread count and other options by passing them to `client.ImportField` or `client.ImportFieldWithStatus` functions. Here are the import options:
* `OptImportThreadCount`: Number of import goroutines. By default only a single importer is used.
* `OptImportBatchSize`: Sets the `BatchSize`.
* `OptImportStatusChannel`: Sets the status channel to track the import progress.

Here's how you would set import options:
```go
err := client.ImportField(field, iterator,
	pilosa.OptImportThreadCount(4),
	pilosa.OptImportBatchSize(1000000))
```

### Tracking Import Status

You can pass a channel of type `ImportStatusUpdate` to `client.ImportField` using `OptImportStatusChannel` function to get notified when an importer imports a shard of columns. You are responsible to close the status channel when the import ends.

Note that if you use this feature, you have to consume from the status channel, otherwise import goroutines may get blocked.

`ImportStatusUpdate` is defined as follows:
```go
type ImportStatusUpdate struct {
	ThreadID      int  // goroutine index
	Shard         uint64 // shard that was imported
	ImportedCount int // imported number of columns
	Time          time.Duration // the time it took to import
}
```

Run the import process in a goroutine in order to be able to read from the status channel and act on it. Here's an example:
```go
statusChan := make(chan pilosa.ImportStatusUpdate, 1000)
go func() {
	err := client.ImportField(f1, columnIterator, pilosa.OptImportStatusChannel(statusChan), pilosa.OptImportThreadCount(2))
	if err != nil {
		log.Fatal(err)
	}
}()

var status pilosa.ImportStatusUpdate
ok := true
for ok {
	select {
	case status, ok = <-statusChan:
		if !ok {
			break
		}
		// act on the status update
	default:
		// do something while waiting for the next status update to arrive.
		time.Sleep(1000 * time.Millisecond)
	}
}

// Don't forget to close the status channel
close(statusChan)
```

## Exporting Data

You can export a field from Pilosa using `client.ExportField` function which returns a `ColumnIterator`. Use the `NextRecord` function of this iterator to receive all columns for the specified field. When there are no more columns, `io.EOF` is returned.

Here's sample code for exporting a field:

```go
iterator, err := client.ExportField(field)
if err != nil {
    log.Fatal(err)
}

columns := []pilosa.Column{}
for {
    record, err := iterator.NextRecord()
    if err == io.EOF {
        break
    }
    if err != nil {
        log.Fatal(err)
    }
    if column, ok := record.(pilosa.Column); ok {
        columns = append(columns, column)
    }
}
```
