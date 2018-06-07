package pilosa

import (
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"
)

type recordImportManager struct {
	client *Client
}

func newRecordImportManager(client *Client) *recordImportManager {
	return &recordImportManager{
		client: client,
	}
}

func (rim recordImportManager) Run(field *Field, iterator RecordIterator, options ImportOptions) error {
	sliceWidth := options.sliceWidth
	threadCount := uint64(options.threadCount)
	recordChans := make([]chan Record, threadCount)
	errChans := make([]chan error, threadCount)
	statusChan := options.statusChan

	if options.importRecordsFunction == nil {
		return errors.New("importRecords function is required")
	}

	for i := range recordChans {
		recordChans[i] = make(chan Record, options.batchSize)
		errChans[i] = make(chan error)
		go recordImportWorker(i, rim.client, field, recordChans[i], errChans[i], statusChan, options)
	}

	var record Record
	var recordIteratorError error

	for {
		record, recordIteratorError = iterator.NextRecord()
		if recordIteratorError != nil {
			if recordIteratorError == io.EOF {
				recordIteratorError = nil
			}
			break
		}
		slice := record.Slice(sliceWidth)
		recordChans[slice%threadCount] <- record
	}

	for _, q := range recordChans {
		close(q)
	}

	// wait for workers to stop
	var workerErr error
	for _, q := range errChans {
		workerErr = <-q
		if workerErr != nil {
			break
		}
	}

	// TODO: Closing this channel will panic if the field has multiple fields.
	if statusChan != nil {
		close(statusChan)
	}

	if recordIteratorError != nil {
		return recordIteratorError
	}

	if workerErr != nil {
		return workerErr
	}

	return nil
}

func recordImportWorker(id int, client *Client, field *Field, recordChan <-chan Record, errChan chan<- error, statusChan chan<- ImportStatusUpdate, options ImportOptions) {
	batchForSlice := map[uint64][]Record{}
	fieldName := field.Name()
	indexName := field.index.Name()
	importFun := options.importRecordsFunction

	importRecords := func(slice uint64, records []Record) error {
		tic := time.Now()
		sort.Sort(recordSort(records))
		err := importFun(indexName, fieldName, slice, records)
		if err != nil {
			return err
		}
		took := time.Since(tic)
		if statusChan != nil {
			statusChan <- ImportStatusUpdate{
				ThreadID:      id,
				Slice:         slice,
				ImportedCount: len(records),
				Time:          took,
			}
		}
		return nil
	}

	largestSlice := func() uint64 {
		largest := 0
		resultSlice := uint64(0)
		for slice, records := range batchForSlice {
			if len(records) > largest {
				largest = len(records)
				resultSlice = slice
			}
		}
		return resultSlice
	}

	var err error
	tic := time.Now()
	strategy := options.strategy
	recordCount := 0
	timeout := options.timeout
	batchSize := options.batchSize
	sliceWidth := options.sliceWidth

	for record := range recordChan {
		recordCount += 1
		slice := record.Slice(sliceWidth)
		batchForSlice[slice] = append(batchForSlice[slice], record)

		if strategy == BatchImport && recordCount >= batchSize {
			for slice, records := range batchForSlice {
				if len(records) == 0 {
					continue
				}
				err = importRecords(slice, records)
				if err != nil {
					break
				}
				batchForSlice[slice] = nil
			}
			recordCount = 0
			tic = time.Now()
		} else if strategy == TimeoutImport && time.Since(tic) >= timeout {
			slice := largestSlice()
			err = importRecords(slice, batchForSlice[slice])
			if err != nil {
				break
			}
			batchForSlice[slice] = nil
			recordCount = 0
			tic = time.Now()
		}
	}

	if err != nil {
		errChan <- err
		return
	}

	// import remaining records
	for slice, records := range batchForSlice {
		if len(records) == 0 {
			continue
		}
		err = importRecords(slice, records)
		if err != nil {
			break
		}
	}

	errChan <- err
}

type ImportStatusUpdate struct {
	ThreadID      int
	Slice         uint64
	ImportedCount int
	Time          time.Duration
}
