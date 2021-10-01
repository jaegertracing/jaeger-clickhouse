package clickhousespanstore

import (
	"container/heap"
	"fmt"
	"sync"

	"github.com/jaegertracing/jaeger/model"
)

const maxSpanCount int = 10000000

//WriteWorkerPool is a worker pool for writing batches of spans.
// Given a new batch, WriteWorkerPool creates a new WriteWorker.
// If the number of currently processed spans if more than maxSpanCount, then the oldest worker is removed.
type WriteWorkerPool struct {
	params *WriteParams

	finish  chan bool
	done    sync.WaitGroup
	batches chan []*model.Span

	totalSpanCount int
	mutex          sync.Mutex
	workers    workerHeap
	workerDone chan *WriteWorker
}

func NewWorkerPool(params *WriteParams) WriteWorkerPool {
	return WriteWorkerPool{
		params:  params,
		finish:  make(chan bool),
		done:    sync.WaitGroup{},
		batches: make(chan []*model.Span),

		mutex: sync.Mutex{},
		workers:    newWorkerHeap(100),
		workerDone: make(chan *WriteWorker),
	}
}

func (pool *WriteWorkerPool) Work() {
	finish := false

	for {
		pool.done.Add(1)
		select {
		case batch := <-pool.batches:
			pool.CleanWorkers(len(batch))
			worker := WriteWorker{
				params: pool.params,

				counter:    &pool.totalSpanCount,
				mutex:      &pool.mutex,
				finish:     make(chan bool),
				workerDone: pool.workerDone,
				done:       sync.WaitGroup{},
			}
			pool.workers.AddWorker(&worker)
			go worker.Work(batch)
		case worker := <-pool.workerDone:
			if err := pool.workers.RemoveWorker(worker); err != nil {
				pool.params.logger.Error("could not remove worker", "worker", worker, "error", err)
			}
		case <-pool.finish:
			pool.workers.CLoseWorkers()
			finish = true
		}
		pool.done.Done()

		if finish {
			break
		}
	}
}

func (pool *WriteWorkerPool) WriteBatch(batch []*model.Span) {
	pool.batches <- batch
}

func (pool *WriteWorkerPool) CLose() {
	pool.finish <- true
	pool.done.Wait()
}

func (pool *WriteWorkerPool) CleanWorkers(batchSize int) {
	pool.mutex.Lock()
	if pool.totalSpanCount+batchSize > maxSpanCount {
		earliest := heap.Pop(pool.workers)
		switch worker := earliest.(type) {
		case WriteWorker:
			worker.CLose()
		default:
			panic(fmt.Sprintf("undefined type %T return from workerHeap", worker))
		}
	}
	pool.mutex.Unlock()
}
