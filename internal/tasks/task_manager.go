package tasks

import (
	"context"
	"fmt"
)

// TaskManager will act as queue which will be responsible for handling
// document loading task.
type TaskManager struct {
	taskQueue chan *Task
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewTasKManager  returns an instance of TaskManager
func NewTasKManager(size int) *TaskManager {
	taskQueue := make(chan *Task, size)
	ctx, cancel := context.WithCancel(context.Background())

	return &TaskManager{
		taskQueue: taskQueue,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// AddTask will add task in the taskQueue for scheduling. it returns no error on
// successful addition of task to the queue.
func (tm *TaskManager) AddTask(task *Task) error {
	if err := tm.ctx.Err(); err != nil {
		return err
	}
	select {
	case tm.taskQueue <- task:
	default:
		return fmt.Errorf("task queue is full")
	}
	return nil
}

// StartTaskManager will initiate a scheduling job that will listen for taskQueue until
// TaskManager is closed.
func (tm *TaskManager) StartTaskManager() {
	// TODO : Need to sleep incoming task from same host.bucket.scope.collection if such task from same host exists.
	if tm.ctx.Err() != nil {
		return
	}
	go func() {
		for {
			select {
			case task, ok := <-tm.taskQueue:
				if ok {
					go task.Handler()
				} else {
					return
				}
			case <-tm.ctx.Done():
				return
			}
		}
	}()
}

// StopTaskManager will close the taskQueue abd
func (tm *TaskManager) StopTaskManager() {
	close(tm.taskQueue)
	tm.cancel()
}
