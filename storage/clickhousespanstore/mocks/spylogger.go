package mocks

import (
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/go-hclog"
)

var _ hclog.Logger = SpyLogger{}

type SpyLogger struct {
	logs [][]string
}

func newSpyLogger() SpyLogger {
	const levelCount = 5
	return SpyLogger{logs: make([][]string, levelCount)}
}

func (s SpyLogger) Log(level hclog.Level, msg string, args ...interface{}) {
	s.logs[level-1] = append(s.logs[level-1], fmt.Sprintf(msg, args))
}

func (s SpyLogger) Trace(msg string, args ...interface{}) {
	s.Log(hclog.Trace, msg, args)
}

func (s SpyLogger) Debug(msg string, args ...interface{}) {
	s.Log(hclog.Debug, msg, args)
}

func (s SpyLogger) Info(msg string, args ...interface{}) {
	s.Log(hclog.Info, msg, args)
}

func (s SpyLogger) Warn(msg string, args ...interface{}) {
	s.Log(hclog.Warn, msg, args)
}

func (s SpyLogger) Error(msg string, args ...interface{}) {
	s.Log(hclog.Error, msg, args)
}

func (s SpyLogger) IsTrace() bool {
	panic("implement me")
}

func (s SpyLogger) IsDebug() bool {
	panic("implement me")
}

func (s SpyLogger) IsInfo() bool {
	panic("implement me")
}

func (s SpyLogger) IsWarn() bool {
	panic("implement me")
}

func (s SpyLogger) IsError() bool {
	panic("implement me")
}

func (s SpyLogger) ImpliedArgs() []interface{} {
	panic("implement me")
}

func (s SpyLogger) With(args ...interface{}) hclog.Logger {
	panic("implement me")
}

func (s SpyLogger) Name() string {
	panic("implement me")
}

func (s SpyLogger) Named(name string) hclog.Logger {
	panic("implement me")
}

func (s SpyLogger) ResetNamed(name string) hclog.Logger {
	panic("implement me")
}

func (s SpyLogger) SetLevel(level hclog.Level) {
	panic("implement me")
}

func (s SpyLogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	panic("implement me")
}

func (s SpyLogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	panic("implement me")
}
