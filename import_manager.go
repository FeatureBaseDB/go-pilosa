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
	shardWidth := options.shardWidth
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
		shard := record.Shard(shardWidth)
		recordChans[shard%threadCount] <- record
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
	batchForShard := map[uint64][]Record{}
	fieldName := field.Name()
	indexName := field.index.Name()
	importFun := options.importRecordsFunction

	importRecords := func(shard uint64, records []Record) error {
		tic := time.Now()
		sort.Sort(recordSort(records))
		err := importFun(indexName, fieldName, shard, records)
		if err != nil {
			return err
		}
		took := time.Since(tic)
		if statusChan != nil {
			statusChan <- ImportStatusUpdate{
				ThreadID:      id,
				Shard:         shard,
				ImportedCount: len(records),
				Time:          took,
			}
		}
		return nil
	}

	largestShard := func() uint64 {
		largest := 0
		resultShard := uint64(0)
		for shard, records := range batchForShard {
			if len(records) > largest {
				largest = len(records)
				resultShard = shard
			}
		}
		return resultShard
	}

	var err error
	tic := time.Now()
	strategy := options.strategy
	recordCount := 0
	timeout := options.timeout
	batchSize := options.batchSize
	shardWidth := options.shardWidth

	for record := range recordChan {
		recordCount += 1
		shard := record.Shard(shardWidth)
		batchForShard[shard] = append(batchForShard[shard], record)

		if strategy == BatchImport && recordCount >= batchSize {
			for shard, records := range batchForShard {
				if len(records) == 0 {
					continue
				}
				err = importRecords(shard, records)
				if err != nil {
					break
				}
				batchForShard[shard] = nil
			}
			recordCount = 0
			tic = time.Now()
		} else if strategy == TimeoutImport && time.Since(tic) >= timeout {
			shard := largestShard()
			err = importRecords(shard, batchForShard[shard])
			if err != nil {
				break
			}
			batchForShard[shard] = nil
			recordCount = 0
			tic = time.Now()
		}
	}

	if err != nil {
		errChan <- err
		return
	}

	// import remaining records
	for shard, records := range batchForShard {
		if len(records) == 0 {
			continue
		}
		err = importRecords(shard, records)
		if err != nil {
			break
		}
	}

	errChan <- err
}

type ImportStatusUpdate struct {
	ThreadID      int
	Shard         uint64
	ImportedCount int
	Time          time.Duration
}
