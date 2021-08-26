package clickhousespanstore

import (
	"sync"

	"github.com/jaegertracing/jaeger/model"
)

const maxSpanCount int = 10000000

type WriteWorkerPool struct {
	params *WriteParams

	finish  chan bool
	done    sync.WaitGroup
	batches chan []*model.Span

	totalSpanCount int
	mutex          sync.Mutex
	// TODO: rewrite on using heap
	workers    []*WriteWorker
	indexes    map[*WriteWorker]int
	workerDone chan *WriteWorker
}

func NewWorkerPool(params *WriteParams) WriteWorkerPool {
	return WriteWorkerPool{
		params:  params,
		finish:  make(chan bool),
		done:    sync.WaitGroup{},
		batches: make(chan []*model.Span),

		mutex: sync.Mutex{},
		// TODO: decide on size
		workers:    make([]*WriteWorker, 0, 8),
		indexes:    make(map[*WriteWorker]int),
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
			pool.workers = append(pool.workers, &worker)
			go worker.Work(batch)
		case worker := <-pool.workerDone:
			idx := pool.indexes[worker]
			for i := idx; i < len(pool.workers)-1; i++ {
				pool.workers[i] = pool.workers[i+1]
				pool.indexes[pool.workers[i]] = i
			}
			pool.workers = pool.workers[:len(pool.workers)-1]
		case <-pool.finish:
			for _, worker := range pool.workers {
				worker.finish <- true
				worker.done.Wait()
			}
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
	if pool.totalSpanCount+batchSize > maxSpanCount || len(pool.workers) == cap(pool.workers) {
		pool.workers[0].finish <- true
		delete(pool.indexes, pool.workers[0])
		for i := 0; i < len(pool.workers)-1; i++ {
			pool.workers[i] = pool.workers[i+1]
			pool.indexes[pool.workers[i]] = i
		}
		pool.workers = pool.workers[:len(pool.workers)-1]
	}
	pool.mutex.Unlock()
}
