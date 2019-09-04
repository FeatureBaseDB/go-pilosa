package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/go-pilosa"
)

// Adapt these to match the CSV file
const HasHeader = true
const IndexName = "ai"
const IndexKeys = true
const FieldDef = `[
	{"name": "reporting_country", "opts": {"keys": true}},
	{"name": "reporting_region", "opts": {"keys": true}},
	{"name": "eligible_for_30d_trial", "opts": {"category": "bool"}},
	{"name": "product", "opts": {"keys": true}},
	{"name": "product_category", "opts": {"keys": true}},
	{"name": "product_subcategory", "opts": {"keys": true}},
	{"name": "registration_funnel", "opts": {"keys": true}},
	{"name": "locale", "opts": {"keys": true}},
	{"name": "gender", "opts": {"keys": true}},
	{"name": "age", "opts": {
		"int_min": 0,
		"int_max": 150
	}},
	{"name": "is_dau", "opts": {"category": "bool"}},
	{"name": "is_wau", "opts": {"category": "bool"}},
	{"name": "is_mau", "opts": {"category": "bool"}},
	{"name": "days_active", "opts": {
		"int_min": 0,
		"int_max": 50
	}},
	{"name": "total_streams", "opts": {
		"int_min": 0,
		"int_max": 1000000
	}},
	{"name": "total_min_played", "opts": {
		"int_min": 0,
		"int_max": 10000000
	}},
	{"name": "is_fraud", "opts": {"category": "bool"}},
	{"name": "days_since_account_creation", "opts": {
		"int_min": 0,
		"int_max": 5000
	}},
	{"name": "top_metagenre", "opts": {"keys": true}},
	{"name": "unique_platform_types", "opts": {
		"int_min": 0,
		"int_max": 100
	}},
	{"name": "unique_platform_type_and_os", "opts": {"keys": true}}
]
`

// const FieldDef = `[
// 	{"name": "reporting_country", "opts": {"keys": true}},
// 	{"name": "reporting_region", "opts": {"keys": true}},
// 	{"name": "eligible_for_30d_trial", "opts": {"category": "bool"}},
// 	{"name": "product", "opts": {"keys": true}},
// 	{"name": "locale", "opts": {"keys": true}},
// 	{"name": "gender", "opts": {"keys": true}},
// 	{"name": "age", "opts": {
// 		"int_min": 0,
// 		"int_max": 150
// 	}},
// 	{"name": "is_dau", "opts": {"category": "bool"}},
// 	{"name": "days_active", "opts": {
// 		"int_min": 0,
// 		"int_max": 5000
// 	}}
// ]
// `

const ThreadCount = 0 // 0 == Available CPU count
const BatchSize = 100000
const ImportThreadCount = 2

// Not queries cannot be used when roaring imports is enabled
const EnableRoaringImport = true

func defineFields(index *pilosa.Index, confPath string) ([]*FieldInfo, error) {
	fieldInfos := []*FieldInfo{}
	confData := []byte(FieldDef)
	var err error
	if len(confPath) > 1{
		confData, err = ioutil.ReadFile(confPath)
		if err != nil {
			return nil, err
		}
	}
	err = json.Unmarshal(confData, &fieldInfos)
	if err != nil {
		return nil, err
	}

	for i, fi := range fieldInfos {
		if fi.Opts.Category == "bool" {
			// this is a bool field
			fi.field = index.Field(fi.Name, pilosa.OptFieldTypeBool())
			fi.csvFieldType = BoolField
		} else if fi.Opts.FloatFrac > 0 {
			// this is a float field
			pow := math.Pow10(fi.Opts.FloatFrac)
			intMin := int64(fi.Opts.FloatMin * pow)
			intMax := int64(fi.Opts.FloatMax * pow)
			fi.field = index.Field(fi.Name,
				pilosa.OptFieldTypeInt(intMin, intMax),
				pilosa.OptFieldKeys(fi.Opts.Keys),
			)
			fi.csvFieldType = FloatField
		} else if fi.Opts.IntMin != 0 || fi.Opts.IntMax != 0 {
			// this is an int field
			fi.field = index.Field(fi.Name,
				pilosa.OptFieldTypeInt(int64(fi.Opts.IntMin), int64(fi.Opts.IntMax)),
				pilosa.OptFieldKeys(fi.Opts.Keys),
			)
			fi.csvFieldType = IntField
		} else {
			// this is a set field
			fi.field = index.Field(fi.Name, pilosa.OptFieldKeys(fi.Opts.Keys))
			fi.csvFieldType = SetField
		}
		fi.index = i + 1 // since the first field is the column
	}
	return fieldInfos, nil
}

func importCSV(addr string, confPath string, dataPath string, threadCount int) error {
	client, err := pilosa.NewClient(addr)
	if err != nil {
		return err
	}

	// create the schema
	schema, err := client.Schema()
	if err != nil {
		return err
	}
	index := schema.Index(IndexName, pilosa.OptIndexKeys(IndexKeys))
	fieldInfos, err := defineFields(index, confPath)
	if err != nil {
		return err
	}
	// update the schema
	err = client.SyncSchema(schema)
	if err != nil {
		return err
	}

	statusChan := make(chan pilosa.ImportStatusUpdate, 1000)
	fieldChan := make(chan *FieldInfo, threadCount)
	wg := &sync.WaitGroup{}
	wg.Add(len(fieldInfos))
	for i := 0; i < threadCount; i++ {
		go func(ch <-chan *FieldInfo) {
			for fi := range ch {
				iter, err := NewIterator(dataPath, fi, HasHeader, IndexKeys)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("Importing field: %s\n", fi.field.Name())
				var enabled = EnableRoaringImport
				if fi.Opts.Category == "bool" {
					enabled = false
				}
				err = client.ImportField(fi.field, iter,
					pilosa.OptImportBatchSize(BatchSize),
					pilosa.OptImportThreadCount(ThreadCount),
					pilosa.OptImportRoaring(enabled),
					pilosa.OptImportStatusChannel(statusChan),
				)
				wg.Done()
				if err != nil {
					log.Fatal(err)
				}
			}
		}(fieldChan)
	}

	for _, fi := range fieldInfos {
		fieldChan <- fi
	}

	statusWg := &sync.WaitGroup{}
	go func() {
		var status pilosa.ImportStatusUpdate
		totalImported := 0
		ok := true
		for ok {
			select {
			case status, ok = <-statusChan:
				if !ok {
					break
				}
				totalImported += status.ImportedCount
				log.Printf("Imported %d bits in %v (total: %d)", status.ImportedCount, status.Time, totalImported)
			default:
				// do something while waiting for the next status update to arrive.
				time.Sleep(1000 * time.Millisecond)
			}
		}
		statusWg.Done()
	}()
	statusWg.Add(1)

	wg.Wait()
	close(fieldChan)
	close(statusChan)
	statusWg.Wait()

	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s pilosa_address conf csv_file\n", os.Args[0])
		os.Exit(1)
	}
	addr := os.Args[1]
	confPath := os.Args[2]
	dataPath := os.Args[3]

	threadCount := ThreadCount
	if threadCount == 0 {
		threadCount = runtime.NumCPU()
	}

	fmt.Printf("Pilosa Address: %s\n", addr)
	fmt.Printf("Thread Count: %d\n", threadCount)
	fmt.Printf("conf Path: %s\n", confPath)
	fmt.Printf("CSV Path: %s\n", dataPath)
	fmt.Println()

	err := importCSV(addr, confPath, dataPath, threadCount)
	if err != nil {
		log.Fatal(err)
	}
}

type CSVFieldType int

const (
	SetField CSVFieldType = iota
	IntField
	FloatField
	BoolField
)

type FieldOptsInfo struct {
	Category  string  `json:"category"`
	Keys      bool    `json:"keys"`
	IntMin    int     `json:"int_min"`
	IntMax    int     `json:"int_max"`
	FloatMin  float64 `json:"float_min"`
	FloatMax  float64 `json:"float_max"`
	FloatFrac int     `json:"float_frac"`
}

type FieldInfo struct {
	Name         string        `json:"name"`
	Opts         FieldOptsInfo `json:"opts"`
	field        *pilosa.Field
	index        int
	csvFieldType CSVFieldType
}

type MultiColCSVRecordIterator struct {
	reader    io.Reader
	line      int
	scanner   *bufio.Scanner
	fieldInfo *FieldInfo
	indexKeys bool
}

func NewIterator(path string, fieldInfo *FieldInfo, hasHeader bool, indexKeys bool) (*MultiColCSVRecordIterator, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(f)
	scanner := bufio.NewScanner(reader)
	if hasHeader {
		if ok := scanner.Scan(); !ok {
			err := scanner.Err()
			if err == nil {
				return nil, io.EOF
			}
			return nil, err
		}
	}
	return &MultiColCSVRecordIterator{
		reader:    reader,
		line:      0,
		scanner:   scanner,
		fieldInfo: fieldInfo,
		indexKeys: indexKeys,
	}, nil
}

func (c *MultiColCSVRecordIterator) NextRecord() (pilosa.Record, error) {
	var err error
	for {
		if ok := c.scanner.Scan(); ok {
			c.line++
			text := strings.TrimSpace(c.scanner.Text())
			if text != "" {
				cols := strings.Split(text, ",")
				valueCol := cols[c.fieldInfo.index]
				colID := uint64(0)
				if !c.indexKeys {
					colID, err = strconv.ParseUint(cols[0], 10, 64)
					if err != nil {
						return nil, err
					}
				}
				switch c.fieldInfo.csvFieldType {
				case BoolField:
					col := pilosa.Column{}
					if c.indexKeys {
						col.ColumnKey = cols[0]
					} else {
						col.ColumnID = colID
					}
					if c.fieldInfo.Opts.Keys {
						value, err := strconv.ParseBool(valueCol)
						if err != nil {
							continue
						}
						var bitSetVar = "0"
						if value {
							bitSetVar = "1"
						}
						col.RowKey = bitSetVar
					} else {
						value, err := strconv.ParseBool(valueCol)
						if err != nil {
							return nil, err
						}
						var bitSetVar uint64
						if value {
							bitSetVar = 1
						}
						col.RowID = bitSetVar
					}
					return col, nil
				case SetField:
					col := pilosa.Column{}
					if c.indexKeys {
						col.ColumnKey = cols[0]
					} else {
						col.ColumnID = colID
					}
					if c.fieldInfo.Opts.Keys {
						col.RowKey = valueCol
					} else {
						rowID, err := strconv.ParseUint(valueCol, 10, 64)
						if err != nil {
							return nil, err
						}
						col.RowID = rowID
					}
					return col, nil
				case IntField:
					value, err := strconv.Atoi(valueCol)
					if err != nil {
						// return nil, err
						continue
					}
					if c.indexKeys {
						return pilosa.FieldValue{ColumnKey: cols[0], Value: int64(value)}, nil
					}
					return pilosa.FieldValue{ColumnID: uint64(colID), Value: int64(value)}, nil
				case FloatField:
					value, err := strconv.ParseFloat(valueCol, 64)
					if err != nil {
						continue
						// return nil, fmt.Errorf("err: parsing float field %s at index %d: %s", c.fieldInfo.Name, c.fieldInfo.index, err.Error())
					}
					value *= math.Pow10(c.fieldInfo.Opts.FloatFrac)
					if c.indexKeys {
						return pilosa.FieldValue{ColumnKey: cols[0], Value: int64(value)}, nil
					}
					return pilosa.FieldValue{ColumnID: uint64(colID), Value: int64(value)}, nil
				default:
					return nil, fmt.Errorf("unknown field type: %d", c.fieldInfo.csvFieldType)
				}
			}
		}
		break
	}
	err = c.scanner.Err()
	if err != nil {
		return nil, err
	}
	return nil, io.EOF
}
