package schedules

import (
	"errors"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type SchedulerTaskManager struct {
	// This struct is responsible for managing tasks in the scheduling system.
	// A map from task ID to TaskInfo can be used to track tasks.
	allTaskMap     map[int32]*structure.TaskInfo
	allTaskQueue   []*structure.TaskInfo
	startTaskQueue []*structure.TaskInfo
	// A map from task ID to worker group can be used to track which worker group is handling which task.
	taskToworkerGroupMap map[int32]string
}

func (t *SchedulerTaskManager) Init() *SchedulerTaskManager {
	t.allTaskMap = make(map[int32]*structure.TaskInfo)
	t.taskToworkerGroupMap = make(map[int32]string)
	return t
}

func (t *SchedulerTaskManager) QueueTask(taskInfo *structure.TaskInfo) (bool, error) {
	if taskInfo == nil {
		return false, errors.New("the argument `taskInfo` is nil")
	}

	if taskInfo.SpaceName == "" {
		return false, errors.New("the property `SpaceName` of taskInfo is empty")
	}

	// Add the task to the task map
	t.allTaskMap[taskInfo.ID] = taskInfo
	t.allTaskQueue = append(t.allTaskQueue, taskInfo)
	t.AssignGroup(taskInfo)
	return true, nil
}

// Only for debug or test, get task start sequence
func (t *SchedulerTaskManager) AddTaskStartSequence(taskID int32) error {
	if _, exists := t.allTaskMap[taskID]; !exists {
		return errors.New("task not found")
	}
	t.startTaskQueue = append(t.startTaskQueue, t.allTaskMap[taskID])
	return nil
}

func (t *SchedulerTaskManager) RemoveTask(taskID int32) error {
	if _, exists := t.allTaskMap[taskID]; !exists {
		return errors.New("task not found")
	}
	delete(t.allTaskMap, taskID)
	delete(t.taskToworkerGroupMap, taskID)
	return nil
}

// update or create a task in the task map
func (t *SchedulerTaskManager) AssignGroup(taskInfo *structure.TaskInfo) error {
	group := workerMgr.ApplyGroup(taskInfo.SpaceName, taskInfo.GraphName)
	if group == "" {
		return errors.New("failed to assign group for task")
	}
	t.taskToworkerGroupMap[taskInfo.ID] = group
	return nil
}

func (t *SchedulerTaskManager) GetTaskByID(taskID int32) (*structure.TaskInfo, error) {
	task, exists := t.allTaskMap[taskID]
	if !exists {
		return nil, errors.New("task not found")
	}
	return task, nil
}

func (t *SchedulerTaskManager) GetLastTask(spaceName string) *structure.TaskInfo {
	// Implement logic to get the last task in the queue for the given space
	if len(t.allTaskQueue) == 0 {
		return nil
	}
	for i := len(t.allTaskQueue) - 1; i >= 0; i-- {
		if t.allTaskQueue[i].SpaceName == spaceName {
			return t.allTaskQueue[i]
		}
	}
	return nil
}

func (t *SchedulerTaskManager) GetAllTasks() []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0, len(t.allTaskMap))
	for _, task := range t.allTaskMap {
		tasks = append(tasks, task)
	}
	return tasks
}

func (t *SchedulerTaskManager) GetAllTasksWaitng() []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0, len(t.allTaskMap))
	for _, task := range t.allTaskMap {
		if task.State == structure.TaskStateWaiting {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (t *SchedulerTaskManager) GetTasksInQueue(space string) []*structure.TaskInfo {
	tasks := make([]*structure.TaskInfo, 0)
	for _, task := range t.allTaskQueue {
		if task.SpaceName == space {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// Only for debug or test, get task start sequence
func (t *SchedulerTaskManager) GetTaskStartSequence(queryTasks []int32) []*structure.TaskInfo {
	if len(t.startTaskQueue) == 0 {
		return nil
	}
	if len(queryTasks) == 0 {
		return t.startTaskQueue
	}
	tasks := make([]*structure.TaskInfo, 0, len(queryTasks))
	taskSet := make(map[int32]struct{})
	for _, id := range queryTasks {
		taskSet[id] = struct{}{}
	}
	for _, task := range t.startTaskQueue {
		if _, exists := taskSet[task.ID]; exists {
			tasks = append(tasks, task)
		}
	}
	logrus.Infof("GetTaskStartSequence: return %d tasks", len(tasks))
	for _, task := range tasks {
		logrus.Debugf("TaskID: %d", task.ID)
	}
	return tasks
}

func (t *SchedulerTaskManager) GetTaskToWorkerGroupMap() map[int32]string {
	// Return a copy of the worker group map to avoid external modifications
	groupMap := make(map[int32]string, len(t.taskToworkerGroupMap))
	for k, v := range t.taskToworkerGroupMap {
		groupMap[k] = v
	}
	return groupMap
}

func (t *SchedulerTaskManager) IsTaskOngoing(taskID int32) bool {
	// Check if the task is currently ongoing
	task, exists := t.allTaskMap[taskID]
	if !exists {
		return false
	}
	return task.State == structure.TaskStateCreated
}
