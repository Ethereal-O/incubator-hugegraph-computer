package schedules

import (
	"errors"
	"vermeer/apps/structure"
)

type WorkerOngoingStatus string

const (
	WorkerOngoingStatusIdle    WorkerOngoingStatus = "idle"
	WorkerOngoingStatusRunning WorkerOngoingStatus = "running"
	WorkerOngoingStatusPaused  WorkerOngoingStatus = "paused"
	WorkerOngoingStatusDeleted WorkerOngoingStatus = "deleted"
)

type ResourceManager struct {
	structure.MutexLocker
	workerStatus          map[string]WorkerOngoingStatus
	runningWorkerTasks    map[string][]int32 // worker ID to list of running task IDs
	availableWorkerGroups map[string]bool    // worker group name to availability status
	// broker just responsible for communication with workers
	// it can not apply tasks to workers directly
	broker *Broker
}

func (rm *ResourceManager) Init() {
	rm.workerStatus = make(map[string]WorkerOngoingStatus)
	rm.runningWorkerTasks = make(map[string][]int32)
	rm.availableWorkerGroups = make(map[string]bool)
	rm.broker = new(Broker).Init()
}

func (rm *ResourceManager) ReleaseByTaskID(taskID int32) {
	defer rm.Unlock(rm.Lock())

	for worker, status := range rm.workerStatus {
		if status == WorkerOngoingStatusRunning && rm.isTaskRunningOnWorker(worker, taskID) {
			delete(rm.workerStatus, worker)
			if tasks, exists := rm.runningWorkerTasks[worker]; exists {
				for i, id := range tasks {
					if id == taskID {
						rm.runningWorkerTasks[worker] = append(tasks[:i], tasks[i+1:]...)
						if len(rm.runningWorkerTasks[worker]) == 0 {
							delete(rm.runningWorkerTasks, worker)
						}
						break
					}
				}
			}
			rm.changeWorkerStatus(worker, WorkerOngoingStatusIdle)
		}
	}
}

func (rm *ResourceManager) isTaskRunningOnWorker(worker string, taskID int32) bool {
	if tasks, exists := rm.runningWorkerTasks[worker]; exists {
		for _, id := range tasks {
			if id == taskID {
				return true
			}
		}
	}
	return false
}

func (rm *ResourceManager) GetAgentAndAssignTask(taskInfo *structure.TaskInfo) (*Agent, AgentStatus, error) {
	if taskInfo == nil {
		return nil, AgentStatusError, errors.New("taskInfo is nil")
	}

	defer rm.Unlock(rm.Lock())

	agent, status, workers, err := rm.broker.ApplyAgent(taskInfo)
	if err != nil {
		return nil, AgentStatusError, err
	}
	if agent == nil {
		return nil, status, nil
	}

	// Assign the task to the agent
	agent.AssignTask(taskInfo)

	for _, worker := range workers {
		if worker == nil {
			continue
		}
		rm.workerStatus[worker.Name] = WorkerOngoingStatusRunning
		if _, exists := rm.runningWorkerTasks[worker.Name]; !exists {
			rm.runningWorkerTasks[worker.Name] = []int32{}
		}
		rm.runningWorkerTasks[worker.Name] = append(rm.runningWorkerTasks[worker.Name], taskInfo.ID)
	}

	return agent, status, nil
}

func (rm *ResourceManager) GetIdleWorkers() []string {
	defer rm.Unlock(rm.Lock())

	idleWorkers := make([]string, 0)
	for worker, status := range rm.workerStatus {
		if status == WorkerOngoingStatusIdle {
			idleWorkers = append(idleWorkers, worker)
		}
	}
	return idleWorkers
}

func (rm *ResourceManager) changeWorkerStatus(workerName string, status WorkerOngoingStatus) {
	rm.workerStatus[workerName] = status

	if status == WorkerOngoingStatusIdle {
		workerInfo := workerMgr.GetWorkerInfo(workerName)

		// get worker group name
		groupName := workerInfo.Group
		if groupName != "" {
			// check all workers in this group are idle
			allIdle := true
			for _, w := range workerMgr.GetGroupWorkers(groupName) {
				if rm.workerStatus[w.Name] != WorkerOngoingStatusIdle {
					allIdle = false
					break
				}
			}
			if allIdle {
				rm.availableWorkerGroups[groupName] = true
			} else {
				rm.availableWorkerGroups[groupName] = false
			}
		}
	} else if status == WorkerOngoingStatusDeleted {
		delete(rm.workerStatus, workerName)
		delete(rm.runningWorkerTasks, workerName)
		delete(rm.availableWorkerGroups, workerName)
	}

	// TODO: Other status changes can be handled here if needed
}

// TODO: when sync task created, need to alloc worker?
func (rm *ResourceManager) ChangeWorkerStatus(workerName string, status WorkerOngoingStatus) {
	defer rm.Unlock(rm.Lock())

	rm.changeWorkerStatus(workerName, status)
}
