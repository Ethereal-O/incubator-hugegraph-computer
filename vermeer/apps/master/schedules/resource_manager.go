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
)

type ResourceManager struct {
	workerStatus map[string]WorkerOngoingStatus
	// broker just responsible for communication with workers
	// it can not apply tasks to workers directly
	broker *Broker
}

func (rm *ResourceManager) Init() {
	rm.workerStatus = make(map[string]WorkerOngoingStatus)
}

func (rm *ResourceManager) Lock() {
	// Implement locking logic if necessary
}

func (rm *ResourceManager) Unlock() {
	// Implement unlocking logic if necessary
}

func (rm *ResourceManager) ReleaseByTaskID(taskID int32) {
	rm.Lock()
	defer rm.Unlock()

	for worker, status := range rm.workerStatus {
		if status == WorkerOngoingStatusRunning && rm.isTaskRunningOnWorker(worker, taskID) {
			delete(rm.workerStatus, worker)
			break
		}
	}
}

func (rm *ResourceManager) isTaskRunningOnWorker(worker string, taskID int32) bool {
	// Implement logic to check if a task is running on a specific worker
	// This is a placeholder implementation
	return false // Replace with actual logic
}

func (rm *ResourceManager) GetAgent(taskInfo *structure.TaskInfo) (*Agent, AgentStatus, error) {
	if taskInfo == nil {
		return nil, AgentStatusError, errors.New("taskInfo is nil")
	}

	rm.Lock()
	defer rm.Unlock()

	agent, status, err := rm.broker.ApplyAgent(taskInfo)
	if err != nil {
		return nil, AgentStatusError, err
	}
	if agent == nil {
		return nil, status, nil
	}

	// Assign the task to the agent
	agent.AssignTask(taskInfo)

	return agent, status, nil
}

func (rm *ResourceManager) IsDispatchPaused() bool {
	rm.Lock()
	defer rm.Unlock()

	for _, status := range rm.workerStatus {
		if status == WorkerOngoingStatusPaused {
			return true
		}
	}
	return false
}

func (rm *ResourceManager) PauseDispatch() {
	rm.Lock()
	defer rm.Unlock()

	for worker := range rm.workerStatus {
		rm.workerStatus[worker] = WorkerOngoingStatusPaused
	}
}

func (rm *ResourceManager) ResumeDispatch() {
	rm.Lock()
	defer rm.Unlock()

	for worker := range rm.workerStatus {
		if rm.workerStatus[worker] == WorkerOngoingStatusPaused {
			rm.workerStatus[worker] = WorkerOngoingStatusIdle
		}
	}
}
