package mocks

import (
	"github.com/hashicorp/go-hclog"
	"math/rand"
	"strconv"
	"testing"
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
			argsCount := rand.Intn(10)
			args := make([]interface{}, 0, 2*argsCount)
			for j := 0; j < argsCount; j++ {
				args = append(
					args,
					"key"+strconv.FormatUint(rand.Uint64(), 10),
					"value"+strconv.FormatUint(rand.Uint64(), 10),
				)
			}
			levelLogs = append(levelLogs, LogMock{Msg: msg, Args: args})
			logger.Log(hclog.Level(level+1), msg, args...)
		}
		logs[level] = levelLogs
	}

	for level, levelLogs := range logs {
		logger.AssertLogsOfLevelEqual(t, hclog.Level(level+1), levelLogs)
	}
}
