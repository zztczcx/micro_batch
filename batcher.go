package microbatch

import (
	"sync"
	"time"
)

type Batcher struct {
	processor BatchProcessor
	size      int
	frequency time.Duration
	mu        sync.Mutex
	batch     *batch
	done      chan bool
	ticker    *time.Ticker
}

type batch struct {
	jobs []BatchedJob
}

type Job struct {
	content interface{}
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
		done:      make(chan bool),
	}

	for _, o := range options {
		o(b)
	}

	b.ticker = time.NewTicker(b.frequency)

	go func() {
		for {
			select {
			case <-b.ticker.C:
				b.processBatch()
			case <-b.done:
				b.processBatch()
			}
		}
	}()

	return b
}

func (b *Batcher) processBatch() {
	for _, bj := range b.batch.jobs {
		bj.resultCh <- b.processor.Process(bj.job)
	}

	b.batch = nil
}

func (b *Batcher) Submit(j Job) JobResult {
	resultCh := make(chan interface{}, 1)
	bj := BatchedJob{j, resultCh}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.batch == nil {
		b.batch = b.newBatch()
	}

	b.batch.jobs = append(b.batch.jobs, bj)

        return JobResult{once: sync.Once{}, ch: resultCh}
}

func (b *Batcher) newBatch() *batch {

	bh := &batch{
		jobs: make([]BatchedJob, 0, b.size),
	}

	return bh
}

func (b *Batcher) Shutdown() {
	b.done <- true
}
