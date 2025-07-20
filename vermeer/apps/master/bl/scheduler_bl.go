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
	"time"
	"vermeer/apps/master/schedules"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type ScheduleBl struct {
	structure.MutexLocker
	schedulerManager *schedules.SchedulerManager
}

func (s *ScheduleBl) Init() {
	s.schedulerManager = &schedules.SchedulerManager{}
	s.schedulerManager.Init(taskMgr.SetState, taskMgr.SetError)
}

func (s *ScheduleBl) PeekSpaceTail(space string) *structure.TaskInfo {
	return s.schedulerManager.GetLastTask(space)
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
	ok, err := s.schedulerManager.QueueTask(taskInfo)
	if err != nil {
		taskMgr.SetError(taskInfo, err.Error())
		return ok, err
	}

	return ok, nil
}

func (s *ScheduleBl) CancelTask(taskInfo *structure.TaskInfo) error {
	if taskInfo == nil {
		return errors.New("the argument `taskInfo` is nil")
	}

	isHeadTask := s.schedulerManager.IsTaskOngoing(taskInfo.ID)
	task := s.schedulerManager.RemoveTask(taskInfo.ID)
	// err := s.schedulerManager.CancelTask(taskInfo)
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

func (s *ScheduleBl) IsDispatchPaused() bool {
	return s.schedulerManager.IsDispatchPaused()
}
func (s *ScheduleBl) PauseDispatch() {
	s.schedulerManager.PauseDispatch()
}

func (s *ScheduleBl) ResumeDispatch() {
	s.schedulerManager.ResumeDispatch()
}

func (s *ScheduleBl) AllTasksInQueue() []*structure.TaskInfo {
	return s.schedulerManager.AllTasksInQueue()
}

func (s *ScheduleBl) TasksInQueue(space string) []*structure.TaskInfo {
	return s.schedulerManager.TasksInQueue(space)
}

func (s *ScheduleBl) CloseCurrent(taskId int32) error {
	s.schedulerManager.ReleaseByTaskID(taskId)

	logrus.Infof("invoke dispatch when task '%d' is closed", taskId)
	s.schedulerManager.TryScheduleNextTasks()
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

// now, start task!
func (s *ScheduleBl) handleStartTask(taskInfo *structure.TaskInfo) {
	agent, status, err := s.schedulerManager.GetAgent(taskInfo)

	if err != nil {
		logrus.Errorf("apply agent error: %v", err)
		taskMgr.SetError(taskInfo, err.Error())
		return
	}

	// switch status {
	// case schedules.AgentStatusNoWorker:
	// 	fallthrough
	// case schedules.AgentStatusWorkerNotReady:
	// 	logrus.Warnf("failed to apply an agent for task '%d', graph: %s/%s, status: %s",
	// 		taskInfo.ID, taskInfo.SpaceName, taskInfo.GraphName, status)
	// 	return
	// }

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
