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

func TestSpyLogger_AssertLogsOfLevelEqual(t *testing.T) {
	logger := NewSpyLogger()
	var logs = make([][]string, levelCount)
	for level, levelLogs := range logs {
		logsCount := rand.Intn(80)
		for i := 0; i < logsCount; i++ {
			log := "log" + strconv.FormatUint(rand.Uint64(), 10)
			levelLogs = append(levelLogs, log)
			logger.Log(hclog.Level(level+1), log)
		}
	}

	for level, levelLogs := range logs {
		logger.AssertLogsOfLevelEqual(t, hclog.Level(level+1), levelLogs)
	}
}
