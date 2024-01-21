package microbatch

import (
	"errors"
	"sync"
	"time"
)

type Batcher struct {
	processor   BatchProcessor
	size        int
	frequency   time.Duration
	shutdownMux sync.Mutex
	jobsMux     sync.Mutex
	jobs        []BatchedJob
	ready       chan bool
	ticker      *time.Ticker
	closed      bool
}

type Job struct {
	content interface{}
	// TODO: maybe we need id, key ... etc.
}

type BatchedJob struct {
	job      Job
	resultCh chan interface{}
	errCh    chan error
}

type JobResult struct {
	result   interface{}
	error    error
	once     sync.Once
	resultCh chan interface{}
	errCh    chan error
}

func (jr *JobResult) Get() (interface{}, error) {
	jr.once.Do(func() {
		select {
		case err := <-jr.errCh:
			jr.error = err
		case result := <-jr.resultCh:
			jr.result = result
		}
	})

        if jr.error != nil {
                return nil, jr.error
        } else {
	        return jr.result, nil
        }
}

type BatchProcessor interface {
	Process(Job) (interface{}, error)
}

func NewBatcher(bp BatchProcessor, options ...optfunc) *Batcher {
	b := &Batcher{
		processor: bp,
		ready:     make(chan bool),
	}

	for _, o := range options {
		o(b)
	}

	b.ticker = time.NewTicker(b.frequency)
	b.jobs = make([]BatchedJob, 0, b.size)

	go func() {
		for {
			select {
			case <-b.ticker.C:
			case <-b.ready:
			}
			b.processBatch()
		}
	}()

	return b
}

func (b *Batcher) processBatch() {
	var length int

	if len(b.jobs) < b.size {
		length = len(b.jobs)
	} else {
		length = b.size
	}

	b.jobsMux.Lock()
	defer b.jobsMux.Unlock()

	if length > 0 {
		go b.processJobs(b.jobs[:length])
	}

	b.jobs = b.jobs[length:]
}

func (b *Batcher) processJobs(jobs []BatchedJob) {
	for _, bj := range jobs {
                result, err := b.processor.Process(bj.job)
                if err != nil {
                        bj.errCh <- err
                } else {
		        bj.resultCh <- result
                }
	}

}

func (b *Batcher) Submit(j Job) (JobResult, error) {
	resultCh := make(chan interface{}, 1)
	errCh := make(chan error, 1)
	bj := BatchedJob{j, resultCh, errCh}

	b.shutdownMux.Lock()
	defer b.shutdownMux.Unlock()
	if b.closed == true {
		return JobResult{}, errors.New("batcher was shutdown")
	}

	b.jobsMux.Lock()
	defer b.jobsMux.Unlock()

	b.jobs = append(b.jobs, bj)

	if len(b.jobs) == b.size {
		b.ready <- true
	}

	return JobResult{once: sync.Once{}, resultCh: resultCh, errCh: errCh}, nil
}

func (b *Batcher) Shutdown() {
	b.shutdownMux.Lock()
	defer b.shutdownMux.Unlock()

	b.closed = true

	if len(b.jobs) > 0 {
		b.ready <- true
	}
}
