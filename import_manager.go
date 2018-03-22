package pilosa

import (
	"io"
	"time"
)

type bitImportManager struct {
	client *Client
}

func newBitImportManager(client *Client) *bitImportManager {
	return &bitImportManager{
		client: client,
	}
}

func (bim bitImportManager) Run(frame *Frame, iterator BitIterator, batchSize int, statusChan chan<- ImportStatusUpdate) error {
	sliceWidth := bim.client.sliceWidth
	threadCount := uint64(bim.client.importThreadCount)
	bitChans := make([]chan Bit, threadCount)
	errChans := make([]chan error, threadCount)

	for i := range bitChans {
		bitChans[i] = make(chan Bit, batchSize)
		errChans[i] = make(chan error)
		go bitImportWorker(i, bim.client, frame, bitChans[i], errChans[i], statusChan, batchSize, sliceWidth)
	}

	var bit Bit
	var err error

	for {
		bit, err = iterator.NextBit()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		slice := bit.ColumnID / sliceWidth
		bitChans[slice%threadCount] <- bit
	}

	for _, q := range bitChans {
		close(q)
	}

	// wait for workers to stop
	for _, q := range errChans {
		err = <-q
		if err != nil {
			break
		}
	}

	close(statusChan)

	return err
}

func bitImportWorker(id int, client *Client, frame *Frame, bitChan <-chan Bit, errChan chan<- error, statusChan chan<- ImportStatusUpdate, batchSize int, sliceWidth uint64) {
	batchForSlice := map[uint64][]Bit{}
	frameName := frame.Name()
	indexName := frame.index.Name()

	importBits := func(slice uint64, bits []Bit) error {
		tic := time.Now()
		err := client.importBits(indexName, frameName, slice, bits)
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

	var err error

	for bit := range bitChan {
		slice := bit.ColumnID / sliceWidth
		batchForSlice[slice] = append(batchForSlice[slice], bit)
		if len(batchForSlice[slice]) >= batchSize {
			err = importBits(slice, batchForSlice[slice])
			if err != nil {
				break
			}
			batchForSlice[slice] = nil
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
