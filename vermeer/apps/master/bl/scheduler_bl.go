/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

package bl

import (
	"errors"
	"strconv"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/master/schedules"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type ScheduleBl struct {
	structure.MutexLocker
	// resource management
	resourceManager *schedules.ResourceManager
	// algorithm management
	algorithmManager *schedules.AlgorithmManager
	// task management
	taskManager *schedules.TaskManager
	// start channel for tasks to be started
	startChan chan *structure.TaskInfo
}

func (s *ScheduleBl) Init() {
	logrus.Info("Initializing ScheduleBl...")
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

	s.resourceManager = &schedules.ResourceManager{}
	s.resourceManager.Init()
	s.taskManager = &schedules.TaskManager{}
	s.taskManager.Init()
	s.algorithmManager = &schedules.AlgorithmManager{}
	s.algorithmManager.Init()
	go s.startTicker()
	go s.waitingStartedTask()
}

func (s *ScheduleBl) startTicker() {
	// Create a ticker that triggers every 3 seconds
	// TODO: make it configurable
	ticker := time.Tick(3 * time.Second)

	for range ticker {
		logrus.Debug("Ticker ticked")
		s.TryScheduleNextTasks()
	}
}

// this make scheduler manager try to schedule next tasks
func (s *ScheduleBl) TryScheduleNextTasks() {
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
func (s *ScheduleBl) tryScheduleInner(softSchedule bool) error {
	// Implement logic to get the next task in the queue for the given space

	// step 1: make sure all tasks have alloc to a worker group
	// This is done by the TaskManager, which assigns a worker group to each task

	// step 2: get available resources and tasks
	logrus.Debugf("scheduling next tasks, softSchedule: %v", softSchedule)
	availableWorkers := s.resourceManager.GetIdleWorkers()
	allTasks := s.taskManager.GetAllTasksNotRunning()
	if len(allTasks) == 0 || len(availableWorkers) == 0 {
		logrus.Debugf("no available tasks or workers, availableTasks: %d, availableWorkers: %d",
			len(allTasks), len(availableWorkers))
		return nil
	}
	logrus.Debugf("available tasks: %d, available workers: %d", len(allTasks), len(availableWorkers))

	// step 3: return the task with the highest priority or small tasks which can be executed immediately
	workerGroupMap := s.taskManager.GetWorkerGroupMap()
	nextTasks, err := s.algorithmManager.ScheduleNextTasks(allTasks, workerGroupMap, availableWorkers, softSchedule)
	if err != nil {
		logrus.Errorf("failed to schedule next tasks: %v", err)
		return err
	}
	logrus.Debugf("scheduled %d tasks", len(nextTasks))
	// step 4: send to start channel
	for _, task := range nextTasks {
		if task == nil {
			logrus.Warnf("received a nil task from algorithm manager")
			continue
		}
		if task.State != structure.TaskStateWaiting {
			logrus.Warnf("task '%d' is not in waiting state, current state: %s", task.ID, task.State)
			continue
		}
		logrus.Infof("scheduling task '%d' with type '%s' to start channel", task.ID, task.Type)
		select {
		case s.startChan <- task:
			logrus.Infof("task '%d' sent to start channel", task.ID)
		default:
			logrus.Warnf("start channel is full, task '%d' could not be sent", task.ID)
		}
	}

	return nil
}

// QueueTask Add the task to the inner queue.
// The tasks will be executed in order from the queue.
// If the task exists, return false.
func (s *ScheduleBl) QueueTask(taskInfo *structure.TaskInfo) (bool, error) {
	if taskInfo == nil {
		return false, errors.New("the argument `taskInfo` is nil")
	}

	if taskInfo.SpaceName == "" {
		return false, errors.New("the property `SpaceName` of taskInfo is empty")
	}

	//defer s.Unlock(s.Lock())
	if err := taskMgr.SetState(taskInfo, structure.TaskStateWaiting); err != nil {
		return false, err
	}

	// Notice: Ensure successful invocation.
	// make sure all tasks have alloc to a worker group
	ok, err := s.taskManager.QueueTask(taskInfo)
	if err != nil {
		taskMgr.SetError(taskInfo, err.Error())
		return ok, err
	}

	return ok, nil
}

// ******** CloseCurrent ********

func (s *ScheduleBl) CloseCurrent(taskId int32) error {
	// trace tasks need these workers, check if these tasks are available
	s.taskManager.RemoveTask(taskId)
	// release the worker group
	s.resourceManager.ReleaseByTaskID(taskId)

	logrus.Infof("invoke dispatch when task '%d' is closed", taskId)
	s.TryScheduleNextTasks()
	return nil
}

func (s *ScheduleBl) ChangeWorkerStatus(workerName string, status schedules.WorkerOngoingStatus) {
	s.resourceManager.ChangeWorkerStatus(workerName, status)

	logrus.Infof("worker '%s' status changed to '%s'", workerName, status)
	// After changing the worker status, we may need to reschedule tasks
	s.TryScheduleNextTasks()
}

// ******** START TASK ********
func (s *ScheduleBl) waitingStartedTask() {
	for taskInfo := range s.startChan {
		if taskInfo == nil {
			logrus.Warnf("recieved a nil task from startChan")
			return
		}

		logrus.Infof("chan received task '%d' to start", taskInfo.ID)
		s.handleStartTask(taskInfo)
	}
}

// now, start task!
func (s *ScheduleBl) handleStartTask(taskInfo *structure.TaskInfo) {
	agent, status, err := s.resourceManager.GetAgentAndAssignTask(taskInfo)

	if err != nil {
		logrus.Errorf("apply agent error: %v", err)
		taskMgr.SetError(taskInfo, err.Error())
		return
	}

	switch status {
	case schedules.AgentStatusNoWorker:
		fallthrough
	case schedules.AgentStatusWorkerNotReady:
		logrus.Warnf("failed to apply an agent for task '%d', graph: %s/%s, status: %s",
			taskInfo.ID, taskInfo.SpaceName, taskInfo.GraphName, status)
		return
	}

	if agent == nil {
		logrus.Infof("no available agent for task '%d', graph: %s/%s, status: %s",
			taskInfo.ID, taskInfo.SpaceName, taskInfo.GraphName, status)
		return
	}

	logrus.Infof("got an agent '%s' for task '%d', graph: %s/%s",
		agent.GroupName(), taskInfo.ID, taskInfo.SpaceName, taskInfo.GraphName)

	go s.startWaitingTask(agent, taskInfo)
}

func (s *ScheduleBl) startWaitingTask(agent *schedules.Agent, taskInfo *structure.TaskInfo) {
	logrus.Infof("starting a task, id: %v, type: %v, graph: %v", taskInfo.ID, taskInfo.Type, taskInfo.GraphName)

	defer func() {
		if err := recover(); err != nil {
			logrus.Errorln("startWaitingTask() has been recovered:", err)
		}
	}()

	if taskInfo.State != structure.TaskStateWaiting {
		logrus.Errorf("task state is not in 'Waiting' state, taskID: %v", taskInfo)
		return
	}

	err := taskMgr.SetState(taskInfo, structure.TaskStateCreated)
	if err != nil {
		logrus.Errorf("set taskInfo to %s error:%v", structure.TaskStateCreated, err)
		return
	}

	taskStarter, err := NewTaskStarter(taskInfo, agent.GroupName())
	if err != nil {
		logrus.Errorf("failed to construct a TaskStarter with task type: %s, taskID: %d, caused by: %v", taskInfo.Type, taskInfo.ID, err)
		taskMgr.SetError(taskInfo, err.Error())
		return
	}

	taskInfo.StartTime = time.Now()
	err = taskStarter.StartTask()
	if err != nil {
		logrus.Errorf("failed to start a task, type: %s, taskID: %d, caused by: %v", taskInfo.Type, taskInfo.ID, err)
		taskMgr.SetError(taskInfo, err.Error())
	}
}

// ********* CANCEL TASK ********
// handle cancel task

func (s *ScheduleBl) CancelTask(taskInfo *structure.TaskInfo) error {
	if taskInfo == nil {
		return errors.New("the argument `taskInfo` is nil")
	}

	isHeadTask := s.taskManager.IsTaskOngoing(taskInfo.ID)
	task := s.taskManager.RemoveTask(taskInfo.ID)
	// err := s.taskManager.CancelTask(taskInfo)
	isInQueue := false
	if task != nil {
		logrus.Infof("removed task '%d' from space queue", taskInfo.ID)
		isInQueue = true
	}

	if isInQueue && !isHeadTask {
		if err := taskMgr.SetState(taskInfo, structure.TaskStateCanceled); err != nil {
			return err
		}

		logrus.Infof("set task '%d' to TaskStateCanceled", taskInfo.ID)
	} else {
		logrus.Infof("sending task '%d' to task canceler", taskInfo.ID)
		return s.handleCancelTask(taskInfo)
	}

	return nil
}

func (s *ScheduleBl) handleCancelTask(taskInfo *structure.TaskInfo) error {
	logrus.Infof("received task '%d' to cancel", taskInfo.ID)
	canceler, err := NewTaskCanceler(taskInfo)
	if err != nil {
		logrus.Errorf("failed to create new TaskCanceler err: %v", err)
		taskMgr.SetError(taskInfo, err.Error())
		return err
	}

	if err := canceler.CancelTask(); err != nil {
		logrus.Errorf("failed to cancel task '%d', caused by: %v", taskInfo.ID, err)
		taskMgr.SetError(taskInfo, err.Error())
		return err
	}

	return nil
}

// ** Other Methods **

func (s *ScheduleBl) PeekSpaceTail(space string) *structure.TaskInfo {
	return s.taskManager.GetLastTask(space)
}

func (s *ScheduleBl) IsDispatchPaused() bool {
	// Implement logic to check if dispatching is paused
	return s.algorithmManager.IsDispatchPaused()
}

func (s *ScheduleBl) PauseDispatch() {
	// Implement logic to pause dispatching
	s.algorithmManager.PauseDispatch()
}

func (s *ScheduleBl) ResumeDispatch() {
	// Implement logic to resume dispatching
	s.algorithmManager.ResumeDispatch()
}

func (s *ScheduleBl) AllTasksInQueue() []*structure.TaskInfo {
	// Implement logic to get all tasks in the queue
	return s.taskManager.GetAllTasks()
}

func (s *ScheduleBl) TasksInQueue(space string) []*structure.TaskInfo {
	// Implement logic to get tasks in the queue for a specific space
	return s.taskManager.GetTasksInQueue(space)
}
