package mocks

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/hashicorp/go-hclog"
)

func TestSpyLogger_AssertLogsEmpty(t *testing.T) {
	logger := NewSpyLogger()
	logger.AssertLogsEmpty(t)
}

func TestSpyLogger_AssertLogsOfLevelEqualNoArgs(t *testing.T) {
	logger := NewSpyLogger()
	var logs = make([][]LogMock, levelCount)
	for level, levelLogs := range logs {
		logsCount := rand.Intn(80)
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
		logsCount := rand.Intn(80)
		for i := 0; i < logsCount; i++ {
			msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
			args := generateArgs(rand.Intn(10))
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
	logsCount := rand.Intn(80)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(10))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Trace(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Trace, logs)
}

func TestSpyLogger_Debug(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(80)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(10))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Debug(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Debug, logs)
}

func TestSpyLogger_Info(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(80)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(10))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Info(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Info, logs)
}

func TestSpyLogger_Warn(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(80)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(10))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Warn(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Warn, logs)
}

func TestSpyLogger_Error(t *testing.T) {
	logger := NewSpyLogger()
	logsCount := rand.Intn(80)
	logs := make([]LogMock, 0, logsCount)
	for i := 0; i < logsCount; i++ {
		msg := "msg" + strconv.FormatUint(rand.Uint64(), 10)
		args := generateArgs(rand.Intn(10))
		logs = append(logs, LogMock{Msg: msg, Args: args})
		logger.Error(msg, args...)
	}

	logger.AssertLogsOfLevelEqual(t, hclog.Error, logs)
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
