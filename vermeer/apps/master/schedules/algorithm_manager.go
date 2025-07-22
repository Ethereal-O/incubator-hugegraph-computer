package schedules

import "vermeer/apps/structure"

type Algorithm interface {
	// Name returns the name of the algorithm
	Name() string
	// Execute runs the algorithm with the provided parameters
	ScheduleNextTasks(waitingTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error)
}

type AlgorithmManager struct {
	supportedAlgorithms map[string]Algorithm
	nowAlgorithm        string
	dispatchPaused      bool
}

func (am *AlgorithmManager) Init() {
	am.supportedAlgorithms = make(map[string]Algorithm)
	am.dispatchPaused = false
	// Register default algorithms
	am.RegisterAlgorithm(&FIFOAlgorithm{})
	am.nowAlgorithm = "FIFO" // Default algorithm
}

func (am *AlgorithmManager) RegisterAlgorithm(algorithm Algorithm) {
	if algorithm == nil {
		return
	}
	name := algorithm.Name()
	if _, exists := am.supportedAlgorithms[name]; exists {
		return // Algorithm already registered
	}
	am.supportedAlgorithms[name] = algorithm
}

func (am *AlgorithmManager) IsDispatchPaused() bool {
	return am.dispatchPaused
}

func (am *AlgorithmManager) PauseDispatch() {
	am.dispatchPaused = true
}

func (am *AlgorithmManager) ResumeDispatch() {
	am.dispatchPaused = false
}

func (am *AlgorithmManager) ScheduleNextTasks(waitingTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if am.dispatchPaused {
		return nil, nil // No tasks to schedule if dispatch is paused
	}

	tasks, err := am.supportedAlgorithms[am.nowAlgorithm].ScheduleNextTasks(waitingTasks, taskToWorkerGroupMap, idleWorkers, softSchedule)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

type FIFOAlgorithm struct{}

func (f *FIFOAlgorithm) Name() string {
	return "FIFO"
}

func (f *FIFOAlgorithm) ScheduleNextTasks(waitingTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(waitingTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	// For FIFO, we simply return the available tasks in the order they are provided
	for _, task := range waitingTasks {
		if task.State != structure.TaskStateWaiting {
			continue // Only consider tasks that are in the waiting state
		}
		if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
			return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
		}
	}

	return nil, nil
}
