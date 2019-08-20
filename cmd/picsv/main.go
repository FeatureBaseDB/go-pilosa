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
}

func NewMain() *Main {
	return &Main{
		Pilosa:    []string{"localhost:10101"},
		File:      "data.csv",
		Index:     "picsvtest",
		BatchSize: 1000,
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
	index := schema.Index(m.Index)

	header, err := reader.Read()
	if err != nil {
		return errors.Wrap(err, "reading CSV header")
	}
	log.Println("Got Header: ", header)
	fields := processHeader(index, header)
	// this has a non-obvious dependence on the previous line... the fields are set up in the index which comes from the schema
	client.SyncSchema(schema)
	batch := pilosa.NewBatch(client, m.BatchSize, fields)
	record := pilosa.Row{
		Values: make([]interface{}, len(header)),
	}
	id := uint64(0)
	row, err := reader.Read()
	for ; err == nil; row, err = reader.Read() {
		record.ID = id
		for i, _ := range record.Values {
			if i < len(row) {
				record.Values[i] = row[i]
			} else {
				record.Values[i] = nil
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

		id++
	}
	log.Printf("processed %d ids\n", id)
	if err != io.EOF && err != nil {
		return errors.Wrap(err, "reading csv")
	}
	err = batch.Import()
	if err != nil {
		return errors.Wrap(err, "final import")
	}

	return nil
}

func processHeader(index *pilosa.Index, header []string) []*pilosa.Field {
	ret := make([]*pilosa.Field, 0, len(header))
	for _, fieldName := range header {
		ret = append(ret, index.Field(fieldName, pilosa.OptFieldKeys(true)))
	}
	return ret
}

func main() {
	if err := commandeer.Run(NewMain()); err != nil {
		log.Fatal(err)
	}
}
