package schedules

import (
	"sort"
	"time"
	"vermeer/apps/structure"
)

type SchedulerAlgorithm interface {
	// Name returns the name of the SchedulerAlgorithm
	Name() string
	// FilterNextTasks filters the next tasks to be scheduled based on the provided parameters
	FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error)
	// ScheduleNextTasks schedules the next tasks based on the filtered tasks
	ScheduleNextTasks(filteredTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error)
}

type SchedulerAlgorithmManager struct {
	filteredSchedulerAlgorithms  map[string]SchedulerAlgorithm
	schuduledSchedulerAlgorithms map[string]SchedulerAlgorithm
	dispatchPaused               bool
}

func (am *SchedulerAlgorithmManager) Init() {
	am.filteredSchedulerAlgorithms = make(map[string]SchedulerAlgorithm)
	am.schuduledSchedulerAlgorithms = make(map[string]SchedulerAlgorithm)
	am.dispatchPaused = false
	// Register filter and schedule algorithms
	am.RegisterFilterAlgorithm(&WaitingSchedulerAlgorithm{})
	am.RegisterFilterAlgorithm(&DependsSchedulerAlgorithm{})
	// Register default SchedulerAlgorithms
	am.RegisterSchedulerAlgorithm(&PriorityElderSchedulerAlgorithm{})
}

func (am *SchedulerAlgorithmManager) RegisterSchedulerAlgorithm(SchedulerAlgorithm SchedulerAlgorithm) {
	if SchedulerAlgorithm == nil {
		return
	}
	name := SchedulerAlgorithm.Name()
	if _, exists := am.schuduledSchedulerAlgorithms[name]; exists {
		return // SchedulerAlgorithm already registered
	}

	// only support one scheduling algorithm for now
	if len(am.schuduledSchedulerAlgorithms) > 0 {
		return // Only one scheduling algorithm can be registered
	}
	am.schuduledSchedulerAlgorithms[name] = SchedulerAlgorithm
}

func (am *SchedulerAlgorithmManager) RegisterFilterAlgorithm(SchedulerAlgorithm SchedulerAlgorithm) {
	if SchedulerAlgorithm == nil {
		return
	}
	name := SchedulerAlgorithm.Name()
	if _, exists := am.filteredSchedulerAlgorithms[name]; exists {
		return // SchedulerAlgorithm already registered
	}
	am.filteredSchedulerAlgorithms[name] = SchedulerAlgorithm
}

func (am *SchedulerAlgorithmManager) IsDispatchPaused() bool {
	return am.dispatchPaused
}

func (am *SchedulerAlgorithmManager) PauseDispatch() {
	am.dispatchPaused = true
}

func (am *SchedulerAlgorithmManager) ResumeDispatch() {
	am.dispatchPaused = false
}

func (am *SchedulerAlgorithmManager) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if am.dispatchPaused {
		return nil, nil // No tasks to schedule if dispatch is paused
	}

	filteredTasks := allTasks
	for _, algorithm := range am.filteredSchedulerAlgorithms {
		var err error
		filteredTasks, err = algorithm.FilterNextTasks(filteredTasks, taskToWorkerGroupMap, idleWorkers, softSchedule)
		if err != nil {
			return nil, err
		}
	}
	if len(filteredTasks) == 0 {
		return nil, nil // No tasks to schedule after filtering
	}

	// only support one scheduling algorithm for now
	// get first algorithm
	for _, algorithm := range am.schuduledSchedulerAlgorithms {
		tasks, err := algorithm.ScheduleNextTasks(filteredTasks, taskToWorkerGroupMap, idleWorkers, softSchedule)
		if err != nil {
			return nil, err
		}
		return tasks, nil // Return the scheduled tasks
	}

	return nil, nil // No tasks scheduled
}

type FIFOSchedulerAlgorithm struct{}

func (f *FIFOSchedulerAlgorithm) Name() string {
	return "FIFO"
}

func (f *FIFOSchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	// just return the waiting tasks as is for FIFO
	return allTasks, nil
}

func (f *FIFOSchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	// For FIFO, we simply return the available tasks in the order they are provided
	for _, task := range allTasks {
		if task.State != structure.TaskStateWaiting {
			continue // Only consider tasks that are in the waiting state
		}
		if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
			return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
		}
	}

	return nil, nil
}

type PrioritySchedulerAlgorithm struct{}

func (p *PrioritySchedulerAlgorithm) Name() string {
	return "Priority"
}

func (p *PrioritySchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	// just return the waiting tasks as is for Priority
	return allTasks, nil
}

func (p *PrioritySchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	// Sort tasks by priority (higher priority first)
	sort.Slice(allTasks, func(i, j int) bool {
		return allTasks[i].Priority > allTasks[j].Priority
	})

	for _, task := range allTasks {
		if task.State != structure.TaskStateWaiting {
			continue // Only consider tasks that are in the waiting state
		}
		if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
			return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
		}
	}

	return nil, nil
}

type PriorityElderSchedulerAlgorithm struct{}

func (p *PriorityElderSchedulerAlgorithm) Name() string {
	return "PriorityElder"
}

func (p *PriorityElderSchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	// just return the waiting tasks as is for PriorityElder
	return allTasks, nil
}

func (p *PriorityElderSchedulerAlgorithm) CalculateTaskEmergency(task *structure.TaskInfo, taskToWorkerGroupMap map[int32]string) int64 {
	// step 1: age
	ageCost := time.Since(task.CreateTime).Milliseconds() / 1000 // in seconds
	// step 2: priority
	priorityCost := int64(task.Priority)
	// step 3: resource cost
	gm := structure.GraphManager
	resourceCost := 1 / gm.GetGraphByName(task.SpaceName, task.GraphName).VertexCount
	// step 4: some random value
	randomValue := int64(1) // Placeholder for any random value logic
	return ageCost + priorityCost + resourceCost + randomValue
}

func (p *PriorityElderSchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	// Sort tasks by priority (higher priority first)
	sort.Slice(allTasks, func(i, j int) bool {
		return p.CalculateTaskEmergency(allTasks[i], taskToWorkerGroupMap) > p.CalculateTaskEmergency(allTasks[j], taskToWorkerGroupMap)
	})

	for _, task := range allTasks {
		if task.State != structure.TaskStateWaiting {
			continue // Only consider tasks that are in the waiting state
		}
		if group, exists := taskToWorkerGroupMap[task.ID]; exists && group != "" {
			return []*structure.TaskInfo{task}, nil // Return the first task that can be scheduled
		}
	}

	return nil, nil
}

type WaitingSchedulerAlgorithm struct{}

func (w *WaitingSchedulerAlgorithm) Name() string {
	return "Waiting"
}

func (w *WaitingSchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	waitingTasks := make([]*structure.TaskInfo, 0)
	for _, task := range allTasks {
		if task.State == structure.TaskStateWaiting {
			waitingTasks = append(waitingTasks, task)
		}
	}
	return waitingTasks, nil
}

func (w *WaitingSchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	waitingTasks, err := w.FilterNextTasks(allTasks, taskToWorkerGroupMap, idleWorkers, softSchedule)
	if err != nil {
		return nil, err
	}
	if len(waitingTasks) == 0 {
		return nil, nil
	}
	// For waiting tasks, we simply return them as is
	return waitingTasks, nil
}

type DependsSchedulerAlgorithm struct{}

func (d *DependsSchedulerAlgorithm) Name() string {
	return "Depends"
}

func (d *DependsSchedulerAlgorithm) FilterNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	sort.Slice(allTasks, func(i, j int) bool {
		return allTasks[i].ID < allTasks[j].ID
	})

	waitingTaskIDs := make(map[int32]*structure.TaskInfo)
	for _, task := range allTasks {
		waitingTaskIDs[task.ID] = task
	}

	filteredTasks := make([]*structure.TaskInfo, 0)
	for _, task := range allTasks {
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
				filteredTasks = append(filteredTasks, task) // Add to filtered tasks if dependencies are satisfied
			}
		}
	}
	return filteredTasks, nil
}

func (d *DependsSchedulerAlgorithm) ScheduleNextTasks(allTasks []*structure.TaskInfo, taskToWorkerGroupMap map[int32]string, idleWorkers []string, softSchedule bool) ([]*structure.TaskInfo, error) {
	if len(allTasks) == 0 {
		return nil, nil // No tasks to schedule
	}

	sort.Slice(allTasks, func(i, j int) bool {
		return allTasks[i].ID < allTasks[j].ID
	})

	allTaskIDs := make(map[int32]*structure.TaskInfo)
	for _, task := range allTasks {
		allTaskIDs[task.ID] = task
	}

	for _, task := range allTasks {
		depends := task.Preorders
		// Check if all dependencies are satisfied
		allDepsSatisfied := true
		for _, dep := range depends {
			if depTask, exists := allTaskIDs[dep]; !exists || depTask.State != structure.TaskStateWaiting {
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
