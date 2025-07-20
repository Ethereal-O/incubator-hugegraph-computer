package schedules

import (
	"strconv"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type SchedulerManager struct {
	// resource management
	resourceManager *ResourceManager
	// algorithm management
	algorithmManager *AlgorithmManager
	// task management
	taskManager *TaskManager
	// start channel for tasks to be started
	startChan chan *structure.TaskInfo
	// register callbacks
	StartTaskCallback func(taskInfo *structure.TaskInfo) error
}

func (s *SchedulerManager) Init(SetTaskStatusCallback func(taskInfo *structure.TaskInfo, status structure.TaskState) error,
	SetTaskErrorCallback func(taskInfo *structure.TaskInfo, errMsg string) bool) *SchedulerManager {
	const defaultChanSizeConfig = "10"
	chanSize := common.GetConfigDefault("start_chan_size", defaultChanSizeConfig).(string)
	// Convert string to int
	chanSizeInt, err := strconv.Atoi(chanSize)
	if err != nil {
		logrus.Errorf("failed to convert start_chan_size to int: %v", err)
		logrus.Infof("using default start_chan_size: %s", defaultChanSizeConfig)
		chanSizeInt, _ = strconv.Atoi(defaultChanSizeConfig)
	}
	startChan := make(chan *structure.TaskInfo, chanSizeInt)
	s.startChan = startChan

	s.resourceManager = &ResourceManager{}
	s.resourceManager.Init()
	s.taskManager = &TaskManager{}
	s.taskManager.Init()
	s.algorithmManager = &AlgorithmManager{}
	s.algorithmManager.Init()
	go s.startTicker()
	return s
}

func (s *SchedulerManager) startTicker() {
	// Create a ticker that triggers every 3 seconds
	// TODO: make it configurable
	ticker := time.Tick(3 * time.Second)

	for range ticker {
		//logrus.Debug("Ticker ticked")
		s.TryScheduleNextTasks()
	}
}

func (s *SchedulerManager) waitingStartedTask() {
	for taskInfo := range s.startChan {
		if taskInfo == nil {
			logrus.Warnf("recieved a nil task from startChan")
			return
		}

		logrus.Infof("chan received task '%d' to start", taskInfo.ID)
		s.StartTaskCallback(taskInfo)
	}
}

// this make scheduler manager try to schedule next tasks
func (s *SchedulerManager) TryScheduleNextTasks() {
	defer func() {
		if err := recover(); err != nil {
			logrus.Errorln("TryScheduleNextTasks() has been recovered:", err)
		}
	}()

	// TODO: make it configurable
	if err := s.tryScheduleInner(true); err != nil {
		logrus.Errorf("do scheduling error:%v", err)
	}
}

// Main routine to schedule tasks
func (s *SchedulerManager) tryScheduleInner(softSchedule bool) error {
	// Implement logic to get the next task in the queue for the given space

	// step 1: make sure all tasks have alloc to a worker group

	// step 2: sort available tasks by priority (by calling algorithm's GetNextTask method)

	// step 3: return the task with the highest priority or small tasks which can be executed immediately

	// step 4: send to start channel

	return nil
}

func (s *SchedulerManager) ReleaseByTaskID(taskID int32) {
	// trace tasks need these workers, check if these tasks are available
	s.taskManager.RemoveTask(taskID)
	// release the worker group
	s.resourceManager.ReleaseByTaskID(taskID)
}

func (s *SchedulerManager) QueueTask(taskInfo *structure.TaskInfo) (bool, error) {
	// make sure all tasks have alloc to a worker group
	s.taskManager.QueueTask(taskInfo)

	return true, nil
}

func (s *SchedulerManager) GetLastTask(spaceName string) *structure.TaskInfo {
	// Implement logic to get the last task in the queue for the given space
	return s.taskManager.GetLastTask(spaceName)
}

func (s *SchedulerManager) IsDispatchPaused() bool {
	// Implement logic to check if dispatching is paused
	return s.resourceManager.IsDispatchPaused()
}

func (s *SchedulerManager) PauseDispatch() {
	// Implement logic to pause dispatching
	s.resourceManager.PauseDispatch()
}

func (s *SchedulerManager) ResumeDispatch() {
	// Implement logic to resume dispatching
	s.resourceManager.ResumeDispatch()
}

func (s *SchedulerManager) AllTasksInQueue() []*structure.TaskInfo {
	// Implement logic to get all tasks in the queue
	return s.taskManager.GetAllTasks()
}

func (s *SchedulerManager) TasksInQueue(space string) []*structure.TaskInfo {
	// Implement logic to get tasks in the queue for a specific space
	return s.taskManager.GetTasksInQueue(space)
}

//

func (s *SchedulerManager) IsTaskOngoing(taskID int32) bool {
	// Check if the task is ongoing
	return s.taskManager.IsTaskOngoing(taskID)
}

func (s *SchedulerManager) RemoveTask(taskID int32) error {
	// Remove a task from the queue
	return s.taskManager.RemoveTask(taskID)
}

func (s *SchedulerManager) GetAgent(taskInfo *structure.TaskInfo) (*Agent, AgentStatus, error) {
	// Get an agent for the given task
	return s.resourceManager.GetAgent(taskInfo)
}
