package schedules

import (
	"sort"
	"vermeer/apps/structure"
)

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
	am.RegisterAlgorithm(&PriorityAlgorithm{})
	am.RegisterAlgorithm(&DependsAlgorithm{})
	am.nowAlgorithm = "Priority" // Default algorithm
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

type PriorityAlgorithm struct{}

func (p *PriorityAlgorithm) Name() string {
	return "Priority"
}

func (p *PriorityAlgorithm) ScheduleNextTasks(waitingTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(waitingTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	// Sort tasks by priority (higher priority first)
	sort.Slice(waitingTasks, func(i, j int) bool {
		return waitingTasks[i].Priority > waitingTasks[j].Priority
	})

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

type DependsAlgorithm struct{}

func (d *DependsAlgorithm) Name() string {
	return "Depends"
}

func (d *DependsAlgorithm) ScheduleNextTasks(waitingTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(waitingTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	sort.Slice(waitingTasks, func(i, j int) bool {
		return waitingTasks[i].ID < waitingTasks[j].ID
	})

	waitingTaskIDs := make(map[int32]*structure.TaskInfo)
	for _, task := range waitingTasks {
		waitingTaskIDs[task.ID] = task
	}

	for _, task := range waitingTasks {
		depends := task.Preorders
		// Check if all dependencies are satisfied
		allDepsSatisfied := true
		for _, dep := range depends {
			if depTask, exists := waitingTaskIDs[dep]; !exists || depTask.State != structure.TaskStateWaiting {
				allDepsSatisfied = false
				break
			}
		}
		if allDepsSatisfied {
			if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
				return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
			}
		}
	}

	return nil, nil
}
