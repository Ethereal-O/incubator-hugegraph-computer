package scheduler

import (
	"fmt"
	"testing"
	"time"
	"vermeer/client"
	"vermeer/test/functional"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func SubTestRoutine(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Routine start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()

	taskComputeBody["cron_expr"] = "*/1 * * * * *" // every second

	logrus.Infof("params for routine test: %+v", taskComputeBody)

	computeTest.SendComputeReqAsync(taskComputeBody)
	computeTest.CheckRes()

	// wait for a while and check again
	time.Sleep(10 * time.Second)
	fmt.Printf("Test Routine: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

func TestRoutine(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, factor string, waitSecond int) {
	// TEST GROUP: ROUTINE
	// 1. send tasks to single graph
	// expect: the tasks should be executed timely

	SubTestRoutine(t, expectRes, healthCheck, masterHttp, graphName, factor, waitSecond)
}
