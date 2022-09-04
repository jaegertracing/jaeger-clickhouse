package clickhousespanstore

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"
)

var delays = []int{2, 3, 5, 8}

// WriteWorker writes spans to CLickHouse.
// Given a batch of spans, WriteWorker attempts to write them to database.
// Interval in seconds between attempts changes due to delays slice, then it remains the same as the last value in delays.
type WriteWorker struct {
	// workerID is an arbitrary identifier for keeping track of this worker in logs
	workerID   int32
	params     *WorkerParams
	batch      []*model.Span
	finish     chan bool
	workerDone chan *WriteWorker
	done       sync.WaitGroup
}

func (worker *WriteWorker) Work() {
	worker.done.Add(1)

	defer worker.done.Done()

	// TODO: look for specific error(connection refused | database error)
	if err := worker.writeBatch(worker.batch); err != nil {
		worker.params.logger.Error("Could not write a batch of spans", "error", err, "worker_id", worker.workerID)
	} else {
		worker.close()
		return
	}
	attempt := 0
	for {
		currentDelay := worker.getCurrentDelay(&attempt, worker.params.delay)
		timer := time.After(currentDelay)
		select {
		case <-worker.finish:
			worker.close()
			return
		case <-timer:
			if err := worker.writeBatch(worker.batch); err != nil {
				worker.params.logger.Error("Could not write a batch of spans", "error", err, "worker_id", worker.workerID)
			} else {
				worker.close()
				return
			}
		}
	}
}

func (worker *WriteWorker) Close() {
	worker.finish <- true
	worker.done.Wait()
}

func (worker *WriteWorker) getCurrentDelay(attempt *int, delay time.Duration) time.Duration {
	if *attempt < len(delays) {
		*attempt++
	}
	return time.Duration(int64(delays[*attempt-1]) * delay.Nanoseconds())
}

func (worker *WriteWorker) close() {
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

	var query string
	if worker.params.tenant == "" {
		query = fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", worker.params.spansTable)
	} else {
		query = fmt.Sprintf("INSERT INTO %s (tenant, timestamp, traceID, model) VALUES (?, ?, ?, ?)", worker.params.spansTable)
	}

	statement, err := tx.Prepare(query)
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

		if worker.params.tenant == "" {
			_, err = statement.Exec(span.StartTime, span.TraceID.String(), serialized)
		} else {
			_, err = statement.Exec(worker.params.tenant, span.StartTime, span.TraceID.String(), serialized)
		}
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

	var query string
	if worker.params.tenant == "" {
		query = fmt.Sprintf(
			"INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags.key, tags.value) VALUES (?, ?, ?, ?, ?, ?, ?)",
			worker.params.indexTable,
		)
	} else {
		query = fmt.Sprintf(
			"INSERT INTO %s (tenant, timestamp, traceID, service, operation, durationUs, tags.key, tags.value) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			worker.params.indexTable,
		)
	}

	statement, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		keys, values := uniqueTagsForSpan(span)
		if worker.params.tenant == "" {
			_, err = statement.Exec(
				span.StartTime,
				span.TraceID.String(),
				span.Process.ServiceName,
				span.OperationName,
				Microseconds(span.Duration),
				keys,
				values,
			)
		} else {
			_, err = statement.Exec(
				worker.params.tenant,
				span.StartTime,
				span.TraceID.String(),
				span.Process.ServiceName,
				span.OperationName,
				Microseconds(span.Duration),
				keys,
				values,
			)
		}
		if err != nil {
			return err
		}
	}

	committed = true

	return tx.Commit()
}

func uniqueTagsForSpan(span *model.Span) (keys, values []string) {
	uniqueTags := make(map[string][]string, len(span.Tags)+len(span.Process.Tags))

	for i := range span.Tags {
		key := tagKey(&span.GetTags()[i])
		uniqueTags[key] = append(uniqueTags[key], tagValue(&span.GetTags()[i]))
	}

	for i := range span.Process.Tags {
		key := tagKey(&span.GetProcess().GetTags()[i])
		uniqueTags[key] = append(uniqueTags[key], tagValue(&span.GetProcess().GetTags()[i]))
	}

	for _, event := range span.Logs {
		for i := range event.Fields {
			key := tagKey(&event.GetFields()[i])
			uniqueTags[key] = append(uniqueTags[key], tagValue(&event.GetFields()[i]))
		}
	}

	keys = make([]string, 0, len(uniqueTags))
	for k := range uniqueTags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values = make([]string, 0, len(uniqueTags))
	for _, key := range keys {
		values = append(values, strings.Join(unique(uniqueTags[key]), ","))
	}

	return keys, values
}

func tagKey(kv *model.KeyValue) string {
	return kv.Key
}

func tagValue(kv *model.KeyValue) string {
	return kv.AsString()
}

func unique(slice []string) []string {
	if len(slice) == 1 {
		return slice
	}

	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range slice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

// Microseconds converts time.Duration (int64) to uint64
//
// Jaeger duration is of Time.Duration (Int64) type but ClickHouse's durationUs column is of UInt64 type
// clickhouse-go client tries to convert this duration to the clickhouse column data-type before writing the data and fails with
//
// Error: clickhouse [AppendRow]: durationUs clickhouse [AppendRow]: converting Int64 to UInt64 is unsupported
func Microseconds(d time.Duration) uint64 {
	return uint64(d) / 1e3
}
