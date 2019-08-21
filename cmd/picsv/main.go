package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

type Main struct {
	Pilosa    []string
	File      string
	Index     string
	BatchSize int
	IDField   string
}

func NewMain() *Main {
	return &Main{
		Pilosa:    []string{"localhost:10101"},
		File:      "data.csv",
		Index:     "picsvtest",
		BatchSize: 1000,
		IDField:   "id",
	}
}

func (m *Main) Run() error {
	start := time.Now()
	defer func() {
		fmt.Println("Duration: ", time.Since(start))
	}()
	f, err := os.Open(m.File)
	if err != nil {
		return errors.Wrap(err, "opening file")
	}
	defer f.Close()
	reader := csv.NewReader(f)

	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		return errors.Wrap(err, "getting pilosa client")
	}
	schema, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}
	opts := []pilosa.IndexOption{}
	if m.IDField != "" {
		opts = append(opts, pilosa.OptIndexKeys(true))
	}
	index := schema.Index(m.Index, opts...)

	headerRow, err := reader.Read()
	if err != nil {
		return errors.Wrap(err, "reading CSV header")
	}
	log.Println("Got Header: ", headerRow)
	fields, header, getIDFn := processHeader(index, m.IDField, headerRow)

	// this has a non-obvious dependence on the previous line... the fields are set up in the index which comes from the schema
	client.SyncSchema(schema)
	batch := pilosa.NewBatch(client, m.BatchSize, index, fields)
	record := pilosa.Row{
		Values: make([]interface{}, len(header)),
	}

	numRecords := uint64(0)
	for row, err := reader.Read(); err == nil; row, err = reader.Read() {
		record.ID = getIDFn(row, numRecords)
		for _, meta := range header {
			if meta.srcIndex < len(row) {
				record.Values[meta.recordIndex] = row[meta.srcIndex]
			} else {
				record.Values[meta.recordIndex] = nil
				log.Printf("row is shorter than header: %v", row)
			}
		}
		err := batch.Add(record)
		if err == pilosa.ErrBatchNowFull {
			err := batch.Import()
			if err != nil {
				return errors.Wrap(err, "importing")
			}
		} else if err != nil {
			return errors.Wrap(err, "adding to batch")
		}

		numRecords++
	}

	if err != io.EOF && err != nil {
		return errors.Wrap(err, "reading csv")
	}
	err = batch.Import()
	if err != nil {
		return errors.Wrap(err, "final import")
	}

	log.Printf("processed %d ids\n", numRecords)

	return nil
}

type valueMeta struct {
	srcIndex    int
	recordIndex int
}

type idGetter func(row []string, numRecords uint64) interface{}

func processHeader(index *pilosa.Index, idField string, headerRow []string) ([]*pilosa.Field, map[string]valueMeta, idGetter) {
	fields := make([]*pilosa.Field, 0, len(headerRow))
	header := make(map[string]valueMeta)
	getIDFn := func(row []string, numRecords uint64) interface{} {
		return numRecords
	}
	for i, fieldName := range headerRow {
		if fieldName == idField {
			idIndex := i
			getIDFn = func(row []string, numRecords uint64) interface{} {
				return row[idIndex]
			}
			continue
		}
		header[fieldName] = valueMeta{srcIndex: i, recordIndex: len(fields)}
		fields = append(fields, index.Field(fieldName, pilosa.OptFieldKeys(true), pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 100000)))
	}

	return fields, header, getIDFn
}

func main() {
	if err := commandeer.Run(NewMain()); err != nil {
		log.Fatal(err)
	}
}
