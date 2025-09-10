package scheduler

import (
	"testing"
	"vermeer/client"
	"vermeer/test/functional"
)

func TestRoutine(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, factor string, waitSecond int) {
	// TEST GROUP: ROUTINE
	// 1. send tasks to single graph
	// expect: the tasks should be executed timely
}
