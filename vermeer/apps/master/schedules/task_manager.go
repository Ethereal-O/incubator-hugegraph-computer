package schedules

import (
	"errors"
	"vermeer/apps/structure"
)

type TaskManager struct {
	// This struct is responsible for managing tasks in the scheduling system.
	// A map from task ID to TaskInfo can be used to track tasks.
	allTaskMap       map[int32]*structure.TaskInfo
	availableTaskMap map[int32]*structure.TaskInfo
	// A map from task ID to worker group can be used to track which worker group is handling which task.
	workerGroupMap map[int32]string
}

func (t *TaskManager) Init() *TaskManager {
	t.allTaskMap = make(map[int32]*structure.TaskInfo)
	t.availableTaskMap = make(map[int32]*structure.TaskInfo)
	t.workerGroupMap = make(map[int32]string)
	return t
}

func (t *TaskManager) QueueTask(taskInfo *structure.TaskInfo) (bool, error) {
	if taskInfo == nil {
		return false, errors.New("the argument `taskInfo` is nil")
	}

	if taskInfo.SpaceName == "" {
		return false, errors.New("the property `SpaceName` of taskInfo is empty")
	}

	// Add the task to the task map
	t.allTaskMap[taskInfo.ID] = taskInfo
	t.AssignGroup(taskInfo)
	return true, nil
}

func (t *TaskManager) RemoveTask(taskID int32) error {
	if _, exists := t.allTaskMap[taskID]; !exists {
		return errors.New("task not found")
	}
	delete(t.allTaskMap, taskID)
	delete(t.workerGroupMap, taskID)
	return nil
}

// update or create a task in the task map
func (t *TaskManager) AssignGroup(taskInfo *structure.TaskInfo) error {
	group := workerMgr.ApplyGroup(taskInfo.SpaceName, taskInfo.GraphName)
	if group == "" {
		return errors.New("failed to assign group for task")
	}
	t.workerGroupMap[taskInfo.ID] = group
	return nil
}

func (t *TaskManager) GetTaskByID(taskID int32) (*structure.TaskInfo, error) {
	task, exists := t.allTaskMap[taskID]
	if !exists {
		return nil, errors.New("task not found")
	}
	return task, nil
}

func (t *TaskManager) GetLastTask(spaceName string) *structure.TaskInfo {
	// Implement logic to get the last task in the queue for the given space
	for _, task := range t.allTaskMap {
		if task.SpaceName == spaceName {
			return task
		}
	}
	return nil
}

func (t *TaskManager) GetAllTasks() []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0, len(t.allTaskMap))
	for _, task := range t.allTaskMap {
		tasks = append(tasks, task)
	}
	return tasks
}

func (t *TaskManager) GetTasksInQueue(space string) []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0)
	for _, task := range t.allTaskMap {
		if task.SpaceName == space {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (t *TaskManager) IsTaskOngoing(taskID int32) bool {
	// Check if the task is currently ongoing
	task, exists := t.allTaskMap[taskID]
	if !exists {
		return false
	}
	return task.State == structure.TaskStateCreated
}
