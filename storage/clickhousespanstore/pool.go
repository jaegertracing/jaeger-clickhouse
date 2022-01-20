package clickhousespanstore

import (
	"container/heap"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jaegertracing/jaeger/model"
)

var (
	numWaitsForMaxSpanCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "jaeger_clickhouse_wait_for_max_span_count_total",
		Help: "Number of waits for clickhouse writes to complete due to max_span_count",
	})
)

// WriteWorkerPool is a worker pool for writing batches of spans.
// Given a new batch, WriteWorkerPool creates a new WriteWorker.
// If the number of currently processed spans if more than maxSpanCount, then the oldest worker is removed.
type WriteWorkerPool struct {
	params *WriteParams

	finish  chan bool
	done    sync.WaitGroup
	batches chan []*model.Span

	totalSpanCount int
	maxSpanCount   int
	mutex          sync.Mutex
	workers        workerHeap
	workerDone     chan *WriteWorker
}

var registerPoolMetrics sync.Once

func NewWorkerPool(params *WriteParams, maxSpanCount int) WriteWorkerPool {
	registerPoolMetrics.Do(func() {
		prometheus.MustRegister(numWaitsForMaxSpanCount)
	})

	return WriteWorkerPool{
		params:  params,
		finish:  make(chan bool),
		done:    sync.WaitGroup{},
		batches: make(chan []*model.Span),

		mutex:      sync.Mutex{},
		workers:    newWorkerHeap(100),
		workerDone: make(chan *WriteWorker),

		maxSpanCount: maxSpanCount,
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
	cleanWorker := (*WriteWorker)(nil)
	pool.mutex.Lock()
	if pool.totalSpanCount+batchSize > pool.maxSpanCount {
		earliest := heap.Pop(pool.workers)
		switch worker := earliest.(type) {
		case *WriteWorker:
			cleanWorker = worker
		default:
			errmsg := fmt.Sprintf("undefined type %T return from workerHeap", worker)
			// Attempt to send error message to jaeger log collection before panicing
			pool.params.logger.Error(errmsg)
			panic(errmsg)
		}
	}
	pool.mutex.Unlock()

	// Avoid deadlock: don't close when mutex is already locked
	if cleanWorker != nil {
		numWaitsForMaxSpanCount.Inc()
		pool.params.logger.Debug("Waiting for existing batch to finish before starting new batch", "batch_size", batchSize, "max_span_count", pool.maxSpanCount)
		cleanWorker.CLose()
		pool.params.logger.Debug("Existing batch finished, continuing with new batch", "batch_size", batchSize, "max_span_count", pool.maxSpanCount)
	}
}
