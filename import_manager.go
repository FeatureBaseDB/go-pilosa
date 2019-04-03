package pilosa

import (
	"fmt"
	"io"
	"sort"
	"time"

	lru "github.com/hashicorp/golang-lru/simplelru"
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

type importWorkerChannels struct {
	records <-chan Record
	errs    chan<- error
	status  chan<- ImportStatusUpdate
}

func (rim recordImportManager) run(field *Field, iterator RecordIterator, options ImportOptions) error {
	shardWidth := field.index.shardWidth
	threadCount := uint64(options.threadCount)
	recordChans := make([]chan Record, threadCount)
	errChan := make(chan error)
	recordErrChan := make(chan error, 1)
	statusChan := options.statusChan

	if options.importRecordsFunction == nil {
		return errors.New("importRecords function is required")
	}

	for i := range recordChans {
		recordChans[i] = make(chan Record, options.batchSize)
		chans := importWorkerChannels{
			records: recordChans[i],
			errs:    errChan,
			status:  statusChan,
		}
		go recordImportWorker(i, rim.client, field, chans, &options)
	}

	var importErr error
	done := uint64(0)

	go func(it RecordIterator) {
		var record Record
		var err error
		for {
			record, err = it.NextRecord()
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				break
			}
			shard := record.Shard(shardWidth)
			recordChans[shard%threadCount] <- record
		}
		recordErrChan <- err
	}(iterator)

sendRecords:
	for done < threadCount {
		select {
		case workerErr := <-errChan:
			done++
			if workerErr != nil {
				importErr = workerErr
				break sendRecords
			}
		case recordErr := <-recordErrChan:
			for _, q := range recordChans {
				close(q)
			}
			if recordErr != nil {
				importErr = recordErr
				break sendRecords
			}
		}
	}

	return importErr
}

func recordImportWorker(id int, client *Client, field *Field, chans importWorkerChannels, options *ImportOptions) {
	var err error
	batchForShard := map[uint64][]Record{}
	statusChan := chans.status
	recordChan := chans.records
	errChan := chans.errs
	shardNodes := map[uint64][]fragmentNode{}

	defer func() {
		if r := recover(); r != nil {
			if err == nil {
				err = fmt.Errorf("worker %d panic: %v", id, r)
			}
		}
		errChan <- err
	}()

	rowKeyIDMap, err := lru.NewLRU(options.rowKeyCacheSize, nil)
	if err != nil {
		panic(errors.Wrap(err, "while creating rowKey to ID map"))
	}
	columnKeyIDMap, err := lru.NewLRU(options.columnKeyCacheSize, nil)
	if err != nil {
		panic(errors.Wrap(err, "while creating columnKey to ID map"))
	}
	state := &importState{
		rowKeyIDMap:    rowKeyIDMap,
		columnKeyIDMap: columnKeyIDMap,
	}

	recordCount := 0
	batchSize := options.batchSize
	shardWidth := field.index.shardWidth

readRecords:
	for record := range recordChan {
		recordCount++
		shard := record.Shard(shardWidth)
		if batchForShard[shard] == nil {
			batchForShard[shard] = make([]Record, 0, batchSize)
		}
		batchForShard[shard] = append(batchForShard[shard], record)

		if recordCount >= batchSize {
			for shard, records := range batchForShard {
				if len(records) == 0 {
					continue
				}
				err = importRecords(id, client, field, shardNodes, shard, records, options, statusChan, state)
				if err != nil {
					break readRecords
				}
				batchForShard[shard] = batchForShard[shard][:0]
			}
			recordCount = 0
		}
	}

	if err != nil {
		return
	}

	// import remaining records
	for shard, records := range batchForShard {
		if len(records) == 0 {
			continue
		}
		err = importRecords(id, client, field, shardNodes, shard, records, options, statusChan, state)
		if err != nil {
			break
		}
	}
}

func importRecords(id int, client *Client, field *Field,
	shardNodes map[uint64][]fragmentNode,
	shard uint64,
	records []Record,
	options *ImportOptions,
	statusChan chan<- ImportStatusUpdate,
	state *importState) error {

	var nodes []fragmentNode
	var ok bool
	var err error
	importFun := options.importRecordsFunction
	if nodes, ok = shardNodes[shard]; !ok {
		// if the data has row or column keys, send the data only to the coordinator
		if field.index.options.keys || field.options.keys {
			node, err := client.fetchCoordinatorNode()
			if err != nil {
				return err
			}
			nodes = []fragmentNode{node}
		} else {
			nodes, err = client.fetchFragmentNodes(field.index.name, shard)
			if err != nil {
				return err
			}
		}
	}
	tic := time.Now()
	if !options.skipSort {
		sort.Sort(recordSort(records))
	}
	err = importFun(field, shard, records, nodes, options, state)
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

// ImportStatusUpdate contains the import progress information.
type ImportStatusUpdate struct {
	ThreadID      int
	Shard         uint64
	ImportedCount int
	Time          time.Duration
}

type recordSort []Record

func (rc recordSort) Len() int {
	return len(rc)
}

func (rc recordSort) Swap(i, j int) {
	rc[i], rc[j] = rc[j], rc[i]
}

func (rc recordSort) Less(i, j int) bool {
	return rc[i].Less(rc[j])
}
