package schedules

import (
	"errors"
	"vermeer/apps/structure"

	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

type SchedulerCronManager struct {
	cronTasks map[int32][]*structure.TaskInfo // cron expression to TaskInfo. Origin task ID to copied tasks
	crons     map[int32][]*cron.Cron          // cron expression to cron jobs
	// queueHandler is a function that handles the task queue
	queueHandler func(*structure.TaskInfo) (bool, error)
}

func (t *SchedulerCronManager) Init(queueHandler func(*structure.TaskInfo) (bool, error)) *SchedulerCronManager {
	t.cronTasks = make(map[int32][]*structure.TaskInfo)
	t.crons = make(map[int32][]*cron.Cron)
	t.queueHandler = queueHandler
	return t
}

func (t *SchedulerCronManager) CheckCronExpression(cronExpr string) error {
	if cronExpr == "" {
		return errors.New("cron expression is empty")
	}
	if _, err := cron.ParseStandard(cronExpr); err != nil {
		return errors.New("invalid cron expression: " + err.Error())
	}
	return nil
}

func (t *SchedulerCronManager) AddCronTask(taskInfo *structure.TaskInfo) error {
	if taskInfo == nil {
		return errors.New("the argument `taskInfo` is nil")
	}

	if taskInfo.CronExpr == "" {
		return errors.New("the property `CronExpr` of taskInfo is empty")
	}

	t.cronTasks[taskInfo.ID] = append(t.cronTasks[taskInfo.ID], taskInfo)
	cronJob := cron.New()
	_, err := cronJob.AddFunc(taskInfo.CronExpr, func() {
		if taskInfo == nil {
			return
		}
		if _, err := t.queueHandler(taskInfo); err != nil {
			logrus.Errorf("Failed to queue task %d in cron job: %v", taskInfo.ID, err)
			return
		}
	})
	if err != nil {
		logrus.Errorf("Failed to add cron job for task %d: %v", taskInfo.ID, err)
		return err
	}
	t.crons[taskInfo.ID] = append(t.crons[taskInfo.ID], cronJob)
	return nil
}

func (t *SchedulerCronManager) DeleteTask(taskID int32) error {
	if _, exists := t.cronTasks[taskID]; !exists {
		return errors.New("task not found in cron tasks")
	}

	for _, cronJob := range t.crons[taskID] {
		cronJob.Stop()
	}
	delete(t.cronTasks, taskID)
	delete(t.crons, taskID)
	logrus.Infof("Deleted cron task for task ID %d", taskID)
	return nil
}
