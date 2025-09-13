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

func SubTestConcurrent(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Concurrent start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[1], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()

	// send two tasks with different size
	params := make([]map[string]string, 0)
	// default is false, actually do not need to set
	taskComputeBody["exclusive"] = "false"
	params = append(params, taskComputeBody)
	params = append(params, taskComputeBody)

	logrus.Infof("params for concurrent test: %+v", params)

	_, sequence := computeTest.SendComputeReqAsyncBatchPriority(params) // send multiple requests asynchronously with priority

	require.Equal(t, 2, len(sequence))

	fmt.Printf("Test Concurrent: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
	// cost should be less than 2 * single task time
}

func SubTestDepends(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, computeTask string, waitSecond int) {
	fmt.Printf("Test Depends start with task: %s\n", computeTask)
	bTime := time.Now()
	computeTest, err := functional.MakeComputeTask(computeTask)
	require.NoError(t, err)
	computeTest.Init(graphName[0], computeTask, expectRes, waitSecond, masterHttp, t, healthCheck)
	taskComputeBody := computeTest.TaskComputeBody()

	// first alloc worker 4 for graph 3
	masterHttp.AllocGroupGraph(graphName[0]+"_3", "test")

	loadTest3 := functional.LoadTaskLocal{}
	loadTest3.Init(graphName[0]+"_3", expectRes, masterHttp, waitSecond, t, healthCheck)
	loadTest3.SendLoadRequest(loadTest3.TaskLoadBodyWithNum(10))

	// send a large task to $ worker group
	taskid := computeTest.SendComputeReqAsyncNotWait(taskComputeBody)

	// send two tasks with different dependency to the same graph
	taskComputeBody["graph_name"] = graphName[0] + "_3"
	params := make([]map[string]string, 0)
	new_body := make(map[string]string)
	for k, v := range taskComputeBody {
		new_body[k] = v
	}
	new_body["preorders"] = fmt.Sprintf("%d", taskid)
	params = append(params, new_body)
	params = append(params, taskComputeBody)

	logrus.Infof("params for depends test: %+v", params)

	taskids, sequence := computeTest.SendComputeReqAsyncBatchPriority(params) // send multiple requests asynchronously with priority

	require.Equal(t, 2, len(sequence))
	for i := 0; i < 2; i++ {
		require.Equal(t, taskids[1-i], sequence[i]) // expect task not depend executed first
	}

	// computeTest.CheckRes()
	fmt.Printf("Test Depends: %-30s [OK], cost: %v\n", computeTask, time.Since(bTime))
}

func TestPriority(t *testing.T, expectRes *functional.ExpectRes, healthCheck *functional.HealthCheck, masterHttp *client.VermeerClient, graphName []string, factor string, waitSecond int) {
	fmt.Print("start test priority\n")

	// // for scheduler, just test a simple task
	// var computeTask = "pagerank"

	// // TEST GROUP: PRIORITY
	// // 1. send priority tasks to single graph
	// // expect: the tasks should be executed in order of priority

	// SubTestPriority(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// // 2. send small tasks and large tasks to single graph
	// // expect: the small tasks should be executed first

	// SubTestSmall(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// // 3. send support concurrent tasks to single graph
	// // expect: the tasks should be executed concurrently
	// SubTestConcurrent(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// // 4. send dependency-tasks to single graph
	// // expect: the tasks should be executed in order of dependency

	// SubTestDepends(t, expectRes, healthCheck, masterHttp, graphName, computeTask, waitSecond)

	// // 5. send same priority tasks to single graph
	// // expect: the tasks should be executed in order of time
	// // skipped, too fragile

	// // 6. send tasks to different graphs
	// // expect: the tasks should be executed concurrently
	// // have been tested in SubTestSmall and SubTestDepends
}
