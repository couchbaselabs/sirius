package tasks

import (
	"testing"
	"time"
)

func TestTaskManager(t *testing.T) {
	tm := NewTasKManager(30)
	tm.StartTaskManager()

	time.Sleep(10 * time.Second)
	tm.StopTaskManager()

	if tm.ctx.Err() == nil {
		t.Fail()
	}

	if _, ok := <-tm.taskQueue; ok {
		t.Fail()
	}

}
