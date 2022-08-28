package mocks

import (
	"math/rand"
	"strconv"
	"testing"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

const (
	maxLogCount = 80
	maxArgCount = 10
)

func TestSpyLogger_AssertLogsEmpty(t *testing.T) {
	logger := NewSpyLogger()
	logger.AssertLogsEmpty(t)
}

func TestSpyLogger_AssertLogsOfLevelEqualNoArgs(t *testing.T) {
	logger := NewSpyLogger()
	var logs = make([][]LogMock, levelCount)
	for level, levelLogs := range logs {
		logsCount := rand.Intn(maxLogCount)
		for i := 0; i < logsCount; i++ {
			msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
			levelLogs = append(levelLogs, LogMock{Msg: msg})
			logger.Log(hclog.Level(level+1), msg)
		}
		logs[level] = levelLogs
	}

	for level, levelLogs := range logs {
		logger.AssertLogsOfLevelEqual(t, hclog.Level(level+1), levelLogs)
	}
}

func TestSpyLogger_AssertLogsOfLevelEqualArgs(t *testing.T) {
	logger := NewSpyLogger()
	var logs = make([][]LogMock, levelCount)
	for level, levelLogs := range logs {
		logsCount := rand.Intn(maxLogCount)
		for i := 0; i < logsCount; i++ {
			msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
			args := generateArgs(rand.Intn(maxArgCount))
			levelLogs = append(levelLogs, LogMock{Msg: msg, Args: args})
			logger.Log(hclog.Level(level+1), msg, args...)
		}
		logs[level] = levelLogs
	}

	for level, levelLogs := range logs {
		logger.AssertLogsOfLevelEqual(t, hclog.Level(level+1), levelLogs)
	}
}

func TestSpyLogger_Trace(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(maxLogCount)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(maxArgCount))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Trace(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Trace, logs)
}

func TestSpyLogger_Debug(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(maxLogCount)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(maxArgCount))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Debug(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Debug, logs)
}

func TestSpyLogger_Info(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(maxLogCount)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(maxArgCount))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Info(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Info, logs)
}

func TestSpyLogger_Warn(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(maxLogCount)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(maxArgCount))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Warn(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Warn, logs)
}

func TestSpyLogger_Error(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(maxLogCount)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(maxArgCount))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Error(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Error, logs)
}

func TestSpyLogger_Name(t *testing.T) {
	assert.Equal(t, "spy logger", NewSpyLogger().Name())
}

func TestNotImplemented(t *testing.T) {
	logger := NewSpyLogger()

	tests := map[string]struct {
		function assert.PanicTestFunc
	}{
		"is_trace":        {function: func() { _ = logger.IsTrace() }},
		"is_debug":        {function: func() { _ = logger.IsDebug() }},
		"is_info":         {function: func() { _ = logger.IsInfo() }},
		"is_warn":         {function: func() { _ = logger.IsWarn() }},
		"is_error":        {function: func() { _ = logger.IsError() }},
		"implied_args":    {function: func() { _ = logger.ImpliedArgs() }},
		"with":            {function: func() { _ = logger.With() }},
		"named":           {function: func() { _ = logger.Named("") }},
		"reset_named":     {function: func() { _ = logger.ResetNamed("") }},
		"set_level":       {function: func() { logger.SetLevel(hclog.NoLevel) }},
		"standard_logger": {function: func() { _ = logger.StandardLogger(nil) }},
		"standard_writer": {function: func() { _ = logger.StandardWriter(nil) }},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Panics(t, test.function, "implement me")
		})
	}
}

func generateArgs(count int) []interface{} {
	args := make([]interface{}, 0, 2*count)
	for j := 0; j < count; j++ {
		args = append(
			args,
			"key"+strconv.FormatUint(rand.Uint64(), 10),
			"value"+strconv.FormatUint(rand.Uint64(), 10),
		)
	}
	return args
}
