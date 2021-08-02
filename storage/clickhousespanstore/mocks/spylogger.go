package mocks

import (
	"io"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hashicorp/go-hclog"
)

const levelCount = 5

var _ hclog.Logger = SpyLogger{}

type LogMock struct {
	Msg  string
	Args []interface{}
}

type SpyLogger struct {
	logs [][]LogMock
}

func NewSpyLogger() SpyLogger {
	return SpyLogger{logs: make([][]LogMock, levelCount)}
}

func (logger *SpyLogger) AssertLogsOfLevelEqual(t *testing.T, level hclog.Level, want []LogMock) {
	assert.Equal(t, want, logger.getLogs(level))
}

func (logger *SpyLogger) getLogs(level hclog.Level) []LogMock {
	return logger.logs[level-1]
}

func (logger *SpyLogger) AssertLogsEmpty(t *testing.T) {
	assert.Equal(t, logger.logs, make([][]LogMock, levelCount))
}

func (logger SpyLogger) Log(level hclog.Level, msg string, args ...interface{}) {
	logger.logs[level-1] = append(logger.getLogs(level), LogMock{msg, args})
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
