package pilosa

import (
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"
)

type bitImportManager struct {
	client *Client
}

func newBitImportManager(client *Client) *bitImportManager {
	return &bitImportManager{
		client: client,
	}
}

func (bim bitImportManager) Run(frame *Frame, iterator RowIterator, options ImportOptions, statusChan chan<- ImportStatusUpdate) error {
	sliceWidth := options.sliceWidth
	threadCount := uint64(options.ThreadCount)
	bitChans := make([]chan RowContainer, threadCount)
	errChans := make([]chan error, threadCount)

	if options.importBitsFunction == nil {
		return errors.New("importBits function is required")
	}

	for i := range bitChans {
		bitChans[i] = make(chan RowContainer, options.BatchSize)
		errChans[i] = make(chan error)
		go bitImportWorker(i, bim.client, frame, bitChans[i], errChans[i], statusChan, options)
	}

	var bit RowContainer
	var bitIteratorError error

	for {
		bit, bitIteratorError = iterator.NextRow()
		if bitIteratorError != nil {
			if bitIteratorError == io.EOF {
				bitIteratorError = nil
			}
			break
		}
		slice := bit.Uint64Field(1) / sliceWidth
		bitChans[slice%threadCount] <- bit
	}

	for _, q := range bitChans {
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

	if statusChan != nil {
		close(statusChan)
	}

	if bitIteratorError != nil {
		return bitIteratorError
	}

	if workerErr != nil {
		return workerErr
	}

	return nil
}

func bitImportWorker(id int, client *Client, frame *Frame, bitChan <-chan RowContainer, errChan chan<- error, statusChan chan<- ImportStatusUpdate, options ImportOptions) {
	batchForSlice := map[uint64][]RowContainer{}
	frameName := frame.Name()
	indexName := frame.index.Name()
	importFun := options.importBitsFunction

	importBits := func(slice uint64, bits []RowContainer) error {
		tic := time.Now()
		sort.Sort(rowContainerSort(bits))
		err := importFun(indexName, frameName, slice, bits)
		if err != nil {
			return err
		}
		took := time.Since(tic)
		if statusChan != nil {
			statusChan <- ImportStatusUpdate{
				ThreadID:      id,
				Slice:         slice,
				ImportedCount: len(bits),
				Time:          took,
			}
		}
		return nil
	}

	largestSlice := func() uint64 {
		largest := 0
		resultSlice := uint64(0)
		for slice, bits := range batchForSlice {
			if len(bits) > largest {
				largest = len(bits)
				resultSlice = slice
			}
		}
		return resultSlice
	}

	var err error
	tic := time.Now()
	strategy := options.ImportStrategy
	bitCount := 0
	timeout := options.Timeout
	batchSize := options.BatchSize

	for bit := range bitChan {
		bitCount += 1
		slice := bit.Uint64Field(1) / sliceWidth
		batchForSlice[slice] = append(batchForSlice[slice], bit)
		if strategy == BatchImport && bitCount >= batchSize {
			for slice, bits := range batchForSlice {
				err = importBits(slice, bits)
				if err != nil {
					break
				}
				batchForSlice[slice] = nil
			}
			bitCount = 0
			tic = time.Now()
		} else if strategy == TimeoutImport && time.Since(tic) >= timeout {
			slice := largestSlice()
			err = importBits(slice, batchForSlice[slice])
			if err != nil {
				break
			}
			batchForSlice[slice] = nil
			bitCount = 0
			tic = time.Now()
		}
	}

	if err != nil {
		errChan <- err
		return
	}

	// import remaining bits
	for slice, bits := range batchForSlice {
		if len(bits) > 0 {
			err = importBits(slice, bits)
			if err != nil {
				break
			}
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
