package schedules

import (
	"errors"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type WorkerOngoingStatus string

const (
	WorkerOngoingStatusIdle              WorkerOngoingStatus = "idle"
	WorkerOngoingStatusRunning           WorkerOngoingStatus = "running"
	WorkerOngoingStatusConcurrentRunning WorkerOngoingStatus = "concurrent_running"
	WorkerOngoingStatusPaused            WorkerOngoingStatus = "paused"
	WorkerOngoingStatusDeleted           WorkerOngoingStatus = "deleted"
)

type SchedulerResourceManager struct {
	structure.MutexLocker
	workerStatus            map[string]WorkerOngoingStatus
	workerGroupStatus       map[string]WorkerOngoingStatus
	runningWorkerGroupTasks map[string][]int32 // worker group name to list of running task IDs
	// broker just responsible for communication with workers
	// it can not apply tasks to workers directly
	broker *Broker
}

func (rm *SchedulerResourceManager) Init() {
	rm.workerStatus = make(map[string]WorkerOngoingStatus)
	rm.workerGroupStatus = make(map[string]WorkerOngoingStatus)
	rm.runningWorkerGroupTasks = make(map[string][]int32)
	rm.broker = new(Broker).Init()
}

func (rm *SchedulerResourceManager) ReleaseByTaskID(taskID int32) {
	defer rm.Unlock(rm.Lock())

	for workerGroup, status := range rm.workerGroupStatus {
		if (status == WorkerOngoingStatusRunning || status == WorkerOngoingStatusConcurrentRunning) && rm.isTaskRunningOnWorkerGroup(workerGroup, taskID) {
			delete(rm.workerGroupStatus, workerGroup)
			if tasks, exists := rm.runningWorkerGroupTasks[workerGroup]; exists {
				for i, id := range tasks {
					if id == taskID {
						rm.runningWorkerGroupTasks[workerGroup] = append(tasks[:i], tasks[i+1:]...)
						if len(rm.runningWorkerGroupTasks[workerGroup]) == 0 {
							delete(rm.runningWorkerGroupTasks, workerGroup)
						}
						break
					}
				}
			}
			if tasks, exists := rm.runningWorkerGroupTasks[workerGroup]; !exists || len(tasks) == 0 {
				for _, worker := range workerMgr.GetGroupWorkers(workerGroup) {
					rm.changeWorkerStatus(worker.Name, WorkerOngoingStatusIdle)
				}
			} else {
				for _, worker := range workerMgr.GetGroupWorkers(workerGroup) {
					rm.changeWorkerStatus(worker.Name, WorkerOngoingStatusConcurrentRunning)
				}
			}
		}
	}
}

func (rm *SchedulerResourceManager) isTaskRunningOnWorkerGroup(workerGroup string, taskID int32) bool {
	if tasks, exists := rm.runningWorkerGroupTasks[workerGroup]; exists {
		for _, id := range tasks {
			if id == taskID {
				return true
			}
		}
	}
	return false
}

func (rm *SchedulerResourceManager) GetAgentAndAssignTask(taskInfo *structure.TaskInfo) (*Agent, AgentStatus, error) {
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

	runningStatus := WorkerOngoingStatusRunning
	if _, exists := rm.runningWorkerGroupTasks[agent.GroupName()]; !exists {
		rm.runningWorkerGroupTasks[agent.GroupName()] = []int32{}
		runningStatus = WorkerOngoingStatusRunning
		rm.workerGroupStatus[agent.GroupName()] = runningStatus
	} else {
		runningStatus = WorkerOngoingStatusConcurrentRunning
		rm.workerGroupStatus[agent.GroupName()] = runningStatus
	}
	rm.runningWorkerGroupTasks[agent.GroupName()] = append(rm.runningWorkerGroupTasks[agent.GroupName()], taskInfo.ID)

	for _, worker := range workers {
		if worker == nil {
			continue
		}
		rm.workerStatus[worker.Name] = runningStatus
	}

	return agent, status, nil
}

func (rm *SchedulerResourceManager) GetIdleWorkerGroups() []string {
	defer rm.Unlock(rm.Lock())

	idleWorkerGroups := make([]string, 0)
	for workerGroup, status := range rm.workerGroupStatus {
		if status == WorkerOngoingStatusIdle {
			idleWorkerGroups = append(idleWorkerGroups, workerGroup)
		}
	}
	return idleWorkerGroups
}

func (rm *SchedulerResourceManager) GetConcurrentWorkerGroups() []string {
	defer rm.Unlock(rm.Lock())

	concurrentWorkerGroups := make([]string, 0)
	for workerGroup, status := range rm.workerGroupStatus {
		if status == WorkerOngoingStatusConcurrentRunning {
			concurrentWorkerGroups = append(concurrentWorkerGroups, workerGroup)
		}
	}
	return concurrentWorkerGroups
}

func (rm *SchedulerResourceManager) changeWorkerStatus(workerName string, status WorkerOngoingStatus) {
	rm.workerStatus[workerName] = status

	if status == WorkerOngoingStatusIdle || status == WorkerOngoingStatusConcurrentRunning {
		workerInfo := workerMgr.GetWorkerInfo(workerName)

		// get worker group name
		groupName := workerInfo.Group
		if groupName != "" {
			// check all workers in this group are idle
			allIdleOrConcurrent := true
			for _, w := range workerMgr.GetGroupWorkers(groupName) {
				if rm.workerStatus[w.Name] != WorkerOngoingStatusIdle && rm.workerStatus[w.Name] != WorkerOngoingStatusConcurrentRunning {
					allIdleOrConcurrent = false
					break
				}
			}
			if allIdleOrConcurrent {
				logrus.Debugf("Change worker group '%s' status to '%s' because all %d workers are idle or concurrent running", groupName, status, len(workerMgr.GetGroupWorkers(groupName)))
				rm.changeWorkerGroupStatus(groupName, status)
			}
		}
	} else if status == WorkerOngoingStatusDeleted {
		delete(rm.workerStatus, workerName)
	}

	// TODO: Other status changes can be handled here if needed
}

func (rm *SchedulerResourceManager) changeWorkerGroupStatus(workerGroup string, status WorkerOngoingStatus) {
	logrus.Infof("Change worker group '%s' status to '%s'", workerGroup, status)
	rm.workerGroupStatus[workerGroup] = status
}

// TODO: when sync task created, need to alloc worker?
func (rm *SchedulerResourceManager) ChangeWorkerStatus(workerName string, status WorkerOngoingStatus) {
	defer rm.Unlock(rm.Lock())

	rm.changeWorkerStatus(workerName, status)
}
