package clickhousespanstore

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/jaegertracing/jaeger/model"
)

var delays = []int{2, 3, 5, 8}

//WriteWorker writes spans to CLickHouse.
// Given a batch of spans, WriteWorker attempts to write them to database.
// Interval in seconds between attempts changes due to delays slice, then it remains the same as the last value in delays.
type WriteWorker struct {
	params *WriteParams

	counter    *int
	mutex      *sync.Mutex
	finish     chan bool
	workerDone chan *WriteWorker
	done       sync.WaitGroup
}

func (worker *WriteWorker) Work(
	batch []*model.Span,
) {
	worker.done.Add(1)
	worker.mutex.Lock()
	*worker.counter += len(batch)
	worker.mutex.Unlock()

	defer worker.done.Done()

	// TODO: look for specific error(connection refused | database error)
	if err := worker.writeBatch(batch); err != nil {
		worker.params.logger.Error("Could not write a batch of spans", "error", err)
	} else {
		worker.close(len(batch))
		return
	}
	attempt := 0
	for {
		currentDelay := worker.getCurrentDelay(&attempt, worker.params.delay)
		timer := time.After(currentDelay)
		select {
		case <-worker.finish:
			worker.close(len(batch))
			return
		case <-timer:
			if err := worker.writeBatch(batch); err != nil {
				worker.params.logger.Error("Could not write a batch of spans", "error", err)
			} else {
				worker.close(len(batch))
				return
			}
		}
	}
}

func (worker *WriteWorker) CLose() {
	worker.finish <- true
	worker.done.Wait()
}

func (worker *WriteWorker) getCurrentDelay(attempt *int, delay time.Duration) time.Duration {
	if *attempt < len(delays) {
		*attempt++
	}
	return time.Duration(int64(delays[*attempt-1]) * delay.Nanoseconds())
}

func (worker *WriteWorker) close(batchSize int) {
	worker.mutex.Lock()
	*worker.counter -= batchSize
	worker.mutex.Unlock()
	worker.workerDone <- worker
}

func (worker *WriteWorker) writeBatch(batch []*model.Span) error {
	worker.params.logger.Debug("Writing spans", "size", len(batch))
	if err := worker.writeModelBatch(batch); err != nil {
		return err
	}

	if worker.params.indexTable != "" {
		if err := worker.writeIndexBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (worker *WriteWorker) writeModelBatch(batch []*model.Span) error {
	tx, err := worker.params.db.Begin()
	if err != nil {
		return err
	}

	committed := false

	defer func() {
		if !committed {
			// Clickhouse does not support real rollback
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", worker.params.spansTable))
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		var serialized []byte

		if worker.params.encoding == EncodingJSON {
			serialized, err = json.Marshal(span)
		} else {
			serialized, err = proto.Marshal(span)
		}

		if err != nil {
			return err
		}

		_, err = statement.Exec(span.StartTime, span.TraceID.String(), serialized)
		if err != nil {
			return err
		}
	}

	committed = true

	return tx.Commit()
}

func (worker *WriteWorker) writeIndexBatch(batch []*model.Span) error {
	tx, err := worker.params.db.Begin()
	if err != nil {
		return err
	}

	committed := false

	defer func() {
		if !committed {
			// Clickhouse does not support real rollback
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.Prepare(
		fmt.Sprintf(
			"INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags.key, tags.value) VALUES (?, ?, ?, ?, ?, ?, ?)",
			worker.params.indexTable,
		))
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		keys, values := uniqueTagsForSpan(span)
		_, err = statement.Exec(
			span.StartTime,
			span.TraceID.String(),
			span.Process.ServiceName,
			span.OperationName,
			span.Duration.Microseconds(),
			keys,
			values,
		)
		if err != nil {
			return err
		}
	}

	committed = true

	return tx.Commit()
}

type kvArray []*model.KeyValue

func (arr kvArray) Len() int {
	return len(arr)
}

func (arr kvArray) Swap(i, j int) {
	if i < 0 || i >= arr.Len() || j < 0 || j > arr.Len() {
		panic(fmt.Errorf("indices are incorrect"))
	}
	arr[i], arr[j] = arr[j], arr[i]
}

func (arr kvArray) Less(i, j int) bool {
	if i < 0 || i >= arr.Len() || j < 0 || j > arr.Len() {
		panic(fmt.Errorf("indices are incorrect"))
	}
	return arr[i].Key < arr[j].Key || (arr[i].Key == arr[j].Key && arr[i].AsString() < arr[j].AsString())
}

func uniqueTagsForSpan(span *model.Span) (keys, values []string) {
	uniqueTags := make(map[string]*model.KeyValue, len(span.Tags)+len(span.Process.Tags))

	for i := range span.Tags {
		uniqueTags[tagString(&span.GetTags()[i])] = &span.GetTags()[i]
	}

	for i := range span.Process.Tags {
		uniqueTags[tagString(&span.GetProcess().GetTags()[i])] = &span.GetProcess().GetTags()[i]
	}

	for _, event := range span.Logs {
		for i := range event.Fields {
			uniqueTags[tagString(&event.GetFields()[i])] = &event.GetFields()[i]
		}
	}

	uniqueTagsSlice := make(kvArray, 0, len(uniqueTags))
	for _, kv := range uniqueTags {
		uniqueTagsSlice = append(uniqueTagsSlice, kv)
	}
	sort.Sort(uniqueTagsSlice)

	keys = make([]string, 0, len(uniqueTags))
	values = make([]string, 0, len(uniqueTags))
	for _, tws := range uniqueTagsSlice {
		keys = append(keys, tws.Key)
		values = append(values, tws.AsString())
	}

	return keys, values
}

func tagString(kv *model.KeyValue) string {
	return kv.Key + "=" + kv.AsString()
}
