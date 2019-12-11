package egpool

import (
	"sync"

	"golang.org/x/sync/errgroup"
)

type Group struct {
	PoolSize int

	jobs chan func() error

	errMu    sync.Mutex
	firstErr error
	poolEG   errgroup.Group
}

func (eg *Group) Go(f func() error) {
	if eg.PoolSize == 0 {
		eg.PoolSize = 1
	}

	if eg.jobs == nil {
		eg.jobs = make(chan func() error, eg.PoolSize*2)
		eg.startProcessJobsPool()
	}

	eg.jobs <- f
}

func (eg *Group) startProcessJobsPool() {
	eg.poolEG = errgroup.Group{}
	for i := 0; i < eg.PoolSize; i++ {
		eg.poolEG.Go(eg.processJobs)
	}
}

func (eg *Group) processJobs() error {
	for jobFn := range eg.jobs {
		err := jobFn()
		if err != nil {
			eg.errMu.Lock()
			if eg.firstErr == nil {
				eg.firstErr = err
			}
			eg.errMu.Unlock()
		}
	}
	return nil
}

func (eg *Group) Wait() error {
	if eg.jobs == nil {
		return nil
	}
	close(eg.jobs)
	_ = eg.poolEG.Wait() // never returns err
	return eg.firstErr
}
