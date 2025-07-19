package schedules

type WorkerOngoingStatus string

const (
	WorkerOngoingStatusIdle    WorkerOngoingStatus = "idle"
	WorkerOngoingStatusRunning WorkerOngoingStatus = "running"
	WorkerOngoingStatusPaused  WorkerOngoingStatus = "paused"
)

type ResourceManager struct {
	workerStatus map[string]WorkerOngoingStatus
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

func (rm *ResourceManager) GetWorkerGroupStatus() {}
