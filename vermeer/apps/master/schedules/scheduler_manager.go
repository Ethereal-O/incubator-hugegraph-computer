package schedules

import "vermeer/apps/structure"

type SchedulerManager struct {
	// resource management
	// algorithm management
	// task management
}

func (s *SchedulerManager) Init() *SchedulerManager {
	return &SchedulerManager{}
}

func (s *SchedulerManager) ReleaseByTaskID(taskID int32) {
	// Implement logic to release resources by task ID

	// trace tasks need these workers, check if these tasks are available
}

func (s *SchedulerManager) GetNextTask(spaceName string) []*structure.TaskInfo {
	// Implement logic to get the next task in the queue for the given space

	// step 1: make sure all tasks have alloc to a worker group

	// step 2: sort available tasks by priority (by calling algorithm's GetNextTask method)

	// step 3: return the task with the highest priority or small tasks which can be executed immediately
	return nil
}
