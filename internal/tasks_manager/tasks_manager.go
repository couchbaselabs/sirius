package tasks_manager

import (
	"context"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"log"
)

// TaskManager will act as queue which will be responsible for handling
// document loading task.
type TaskManager struct {
	taskQueue chan interface{}
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewTasKManager  returns an instance of TaskManager
func NewTasKManager(size int) *TaskManager {
	taskQueue := make(chan interface{}, size)
	ctx, cancel := context.WithCancel(context.Background())

	tm := &TaskManager{
		taskQueue: taskQueue,
		ctx:       ctx,
		cancel:    cancel,
	}
	go tm.StartTaskManager()
	return tm
}

// AddTask will add task in the taskQueue for scheduling. it returns no error on
// successful addition of task to the queue.
func (tm *TaskManager) AddTask(task interface{}) error {
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
	if tm.ctx.Err() != nil {
		return
	}
	go func() {
		for {
			select {
			case task, ok := <-tm.taskQueue:
				if ok {
					if t, ok := task.(tasks.Task); ok {
						go func() {
							err := t.Do()
							if err != nil {
								log.Println(err)
							}
						}()
					}
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
