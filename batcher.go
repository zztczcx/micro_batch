package microbatch

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Batcher struct {
	processor BatchProcessor
	size      int
	frequency time.Duration
	mu        sync.Mutex
	jobs      []BatchedJob
	ready     chan bool
	ticker    *time.Ticker
	closed    bool
}

type batch struct {
}

type Job struct {
	content interface{}
	// TODO: maybe we need id, key ... etc.
}

type BatchedJob struct {
	job      Job
	resultCh chan interface{}
}

type JobResult struct {
	result interface{}
	error  error
	once   sync.Once
	ch     chan interface{}
}

func (jr *JobResult) Get() interface{} {
	jr.once.Do(func() {
		jr.result = <-jr.ch
	})

	return jr.result
}

type BatchProcessor interface {
	Process(Job) interface{}
}

func BatchSize(size int) func(*Batcher) {
	return func(b *Batcher) {
		b.size = size
	}
}

func ProcessFrequency(f time.Duration) func(*Batcher) {
	return func(b *Batcher) {
		b.frequency = f
	}
}

func NewBatcher(bp BatchProcessor, options ...func(*Batcher)) *Batcher {
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
	b.mu.Lock()
	defer b.mu.Unlock()

	var length int

	if len(b.jobs) < b.size {
		length = len(b.jobs)
	} else {
		length = b.size
	}

	if length > 0 {
		go b.processJobs(b.jobs[:(length - 1)])
	}

	fmt.Println("length: ", length)
	b.jobs = b.jobs[length:]
}

func (b *Batcher) processJobs(jobs []BatchedJob) {
	for _, bj := range jobs {
		bj.resultCh <- b.processor.Process(bj.job)
	}

}

func (b *Batcher) Submit(j Job) (JobResult, error) {
	resultCh := make(chan interface{}, 1)
	bj := BatchedJob{j, resultCh}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed == true {
		return JobResult{}, errors.New("batcher was shutdown")
	}

	b.jobs = append(b.jobs, bj)

	if len(b.jobs) == b.size {
		b.ready <- true
	}

	return JobResult{once: sync.Once{}, ch: resultCh}, nil
}

func (b *Batcher) Shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true

	if len(b.jobs) > 0 {
		b.ready <- true
	}
}
