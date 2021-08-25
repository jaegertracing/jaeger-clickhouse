package clickhousespanstore

import (
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"sort"
	"sync"
	"time"

	"github.com/jaegertracing/jaeger/model"
)

var delays = []int{2, 3, 5, 8}

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

func (worker *WriteWorker) getCurrentDelay(attempt *int, delay time.Duration) time.Duration {
	if *attempt < len(delays) {
		*attempt++
	}
	return time.Duration(int64(delays[*attempt-1]) * delay.Nanoseconds())
}

func (worker *WriteWorker) close(batchSize int) {
	worker.mutex.Lock()
	*worker.counter -= batchSize
	worker.done.Done()
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

	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags) VALUES (?, ?, ?, ?, ?, ?)", worker.params.indexTable))
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		_, err = statement.Exec(
			span.StartTime,
			span.TraceID.String(),
			span.Process.ServiceName,
			span.OperationName,
			span.Duration.Microseconds(),
			uniqueTagsForSpan(span),
		)
		if err != nil {
			return err
		}
	}

	committed = true

	return tx.Commit()
}

func uniqueTagsForSpan(span *model.Span) []string {
	uniqueTags := make(map[string]struct{}, len(span.Tags)+len(span.Process.Tags))

	for i := range span.Tags {
		uniqueTags[tagString(&span.GetTags()[i])] = struct{}{}
	}

	for i := range span.Process.Tags {
		uniqueTags[tagString(&span.GetProcess().GetTags()[i])] = struct{}{}
	}

	for _, event := range span.Logs {
		for i := range event.Fields {
			uniqueTags[tagString(&event.GetFields()[i])] = struct{}{}
		}
	}

	tags := make([]string, 0, len(uniqueTags))

	for kv := range uniqueTags {
		tags = append(tags, kv)
	}

	sort.Strings(tags)

	return tags
}

func tagString(kv *model.KeyValue) string {
	return kv.Key + "=" + kv.AsString()
}
