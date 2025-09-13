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
		logrus.Errorf("Failed to parse cron expression: %v", err)
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

	// add to cron tasks
	t.cronTasks[taskInfo.ID] = append(t.cronTasks[taskInfo.ID], taskInfo)
	cronJob := cron.New()
	_, err := cronJob.AddFunc(taskInfo.CronExpr, func() {
		if taskInfo == nil {
			return
		}

		// TODO: CREATE a new task from the original task, using taskbl
		// copy a new taskInfo
		task, err := structure.TaskManager.CreateTask(taskInfo.SpaceName, taskInfo.Type, 0)
		task.CreateType = structure.TaskCreateAsync
		task.GraphName = taskInfo.GraphName
		task.CreateUser = taskInfo.CreateUser
		task.Params = taskInfo.Params
		task.CronExpr = "" // clear cron expression for the new task
		task.Priority = taskInfo.Priority
		task.Preorders = taskInfo.Preorders
		task.Exclusive = taskInfo.Exclusive
		if err != nil {
			logrus.Errorf("Failed to create task from cron job for task %d: %v", taskInfo.ID, err)
			return
		}
		structure.TaskManager.AddTask(task)
		structure.TaskManager.SaveTask(task.ID)
		if _, err := t.queueHandler(task); err != nil {
			logrus.Errorf("Failed to queue task %d in cron job: %v", taskInfo.ID, err)
			return
		}
		logrus.Infof("Successfully queued task %d from cron job", task.ID)
	})
	if err != nil {
		logrus.Errorf("Failed to add cron job for task %d: %v", taskInfo.ID, err)
		return err
	}
	t.crons[taskInfo.ID] = append(t.crons[taskInfo.ID], cronJob)
	cronJob.Start()
	logrus.Infof("Added cron task for task ID %d with expression %s", taskInfo.ID, taskInfo.CronExpr)
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

func (t *SchedulerCronManager) DeleteTaskByGraph(spaceName, graphName string) error {
	if spaceName == "" || graphName == "" {
		return errors.New("the argument `spaceName` or `graphName` is empty")
	}

	var toDelete []int32
	for taskID, tasks := range t.cronTasks {
		for _, task := range tasks {
			if task.SpaceName == spaceName && task.GraphName == graphName {
				toDelete = append(toDelete, taskID)
				break
			}
		}
	}

	for _, taskID := range toDelete {
		if err := t.DeleteTask(taskID); err != nil {
			logrus.Errorf("Failed to delete cron task for task ID %d: %v", taskID, err)
			return err
		}
	}
	logrus.Infof("Deleted cron tasks for space %s and graph %s", spaceName, graphName)
	return nil
}
