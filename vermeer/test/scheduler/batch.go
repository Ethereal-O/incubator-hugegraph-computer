package scheduler

import (
	"testing"
	"vermeer/client"
	"vermeer/test/functional"
)

func TestBatch(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, factor string, waitSecond int) {
	// TEST GROUP: BATCH
	// 1. send batch tasks to single graph
	// expect: the tasks should be executed in order of time
	// have been tested in priority.go
}
