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

func SubTestPriority(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Priority start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()

	// send two tasks with different priority
	params := make([]map[string]string, 0)

	for i := 0; i < 2; i++ {
		param := make(map[string]string)
		param["priority"] = fmt.Sprintf("%d", i)
		for k, v := range taskComputeBody {
			param[k] = v
		}
		params = append(params, param)
	}

	logrus.Infof("params for priority test: %+v", params)

	taskids, sequence := computeTest.SendComputeReqAsyncBatchPriority(params) // send multiple requests asynchronously with priority

	require.Equal(t, 2, len(sequence))
	for i := 0; i < 2; i++ {
		require.Equal(t, taskids[1-i], sequence[i]) // expect task with priority 1 executed before priority 0
	}

	computeTest.CheckRes()
	fmt.Printf("Test Priority: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

func SubTestSmall(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Small start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	computeTaskSmall, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()
	computeTaskSmall.Init(graphName[1], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBodySmall := computeTaskSmall.TaskComputeBody()

	// send two tasks with different size
	params := make([]map[string]string, 0)
	taskComputeBody["graph_name"] = graphName[0]
	taskComputeBodySmall["graph_name"] = graphName[1]
	params = append(params, taskComputeBody)
	params = append(params, taskComputeBodySmall)

	logrus.Infof("params for small test: %+v", params)

	taskids, sequence := computeTest.SendComputeReqAsyncBatchPriority(params) // send multiple requests asynchronously with priority

	require.Equal(t, 2, len(sequence))
	for i := 0; i < 2; i++ {
		require.Equal(t, taskids[1-i], sequence[i]) // expect task smaller executed before larger
	}

	computeTest.CheckRes()
	fmt.Printf("Test Small: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

func TestPriority(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, factor string, waitSecond int) {
	fmt.Print("start test priority\n")

	// for scheduler, just test a simple task
	var computeTask = "pagerank"

	// TEST GROUP: PRIORITY
	// 1. send priority tasks to single graph
	// expect: the tasks should be executed in order of priority

	SubTestPriority(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// 2. send small tasks and large tasks to single graph
	// expect: the small tasks should be executed first

	SubTestSmall(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// 3. send support concurrent tasks to single graph
	// expect: the tasks should be executed concurrently

	// 4. send dependency-tasks to single graph
	// expect: the tasks should be executed in order of dependency

	// 5. send same priority tasks to single graph
	// expect: the tasks should be executed in order of time

	// 6. send tasks to different graphs
	// expect: the tasks should be executed concurrently
	// have been tested in SubTestSmall
}
