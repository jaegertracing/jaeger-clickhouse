package mocks

import (
	"fmt"
	"io"
	"log"
	"reflect"
	"testing"

	"github.com/hashicorp/go-hclog"
)

const levelCount = 5

var _ hclog.Logger = SpyLogger{}

type SpyLogger struct {
	logs [][]string
}

func NewSpyLogger() SpyLogger {
	return SpyLogger{logs: make([][]string, levelCount)}
}

func (logger *SpyLogger) AssertLogsOfLevelEqual(t *testing.T, level hclog.Level, want []string) {
	got := logger.getLogs(level)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("Incorrect logs of the %s level, want %s, got %s", level, fmt.Sprint(want), fmt.Sprint(got))
	}
}

func (logger *SpyLogger) getLogs(level hclog.Level) []string {
	return logger.logs[level-1]
}

func (logger *SpyLogger) AssertLogsEmpty(t *testing.T) {
	for level, logs := range logger.logs {
		if len(logs) != 0 {
			t.Fatalf("Want logs to be empty, got %s on level %s", fmt.Sprint(logs), hclog.Level(level+1))
		}
	}
}

func (logger SpyLogger) Log(level hclog.Level, msg string, args ...interface{}) {
	logger.logs[level-1] = append(logger.getLogs(level), fmt.Sprintf(msg, args...))
}

func (logger SpyLogger) Trace(msg string, args ...interface{}) {
	logger.Log(hclog.Trace, msg, args...)
}

func (logger SpyLogger) Debug(msg string, args ...interface{}) {
	logger.Log(hclog.Debug, msg, args...)
}

func (logger SpyLogger) Info(msg string, args ...interface{}) {
	logger.Log(hclog.Info, msg, args...)
}

func (logger SpyLogger) Warn(msg string, args ...interface{}) {
	logger.Log(hclog.Warn, msg, args...)
}

func (logger SpyLogger) Error(msg string, args ...interface{}) {
	logger.Log(hclog.Error, msg, args...)
}

func (logger SpyLogger) IsTrace() bool {
	panic("implement me")
}

func (logger SpyLogger) IsDebug() bool {
	panic("implement me")
}

func (logger SpyLogger) IsInfo() bool {
	panic("implement me")
}

func (logger SpyLogger) IsWarn() bool {
	panic("implement me")
}

func (logger SpyLogger) IsError() bool {
	panic("implement me")
}

func (logger SpyLogger) ImpliedArgs() []interface{} {
	panic("implement me")
}

func (logger SpyLogger) With(args ...interface{}) hclog.Logger {
	panic("implement me")
}

func (logger SpyLogger) Name() string {
	panic("implement me")
}

func (logger SpyLogger) Named(name string) hclog.Logger {
	panic("implement me")
}

func (logger SpyLogger) ResetNamed(name string) hclog.Logger {
	panic("implement me")
}

func (logger SpyLogger) SetLevel(level hclog.Level) {
	panic("implement me")
}

func (logger SpyLogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	panic("implement me")
}

func (logger SpyLogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	panic("implement me")
}
