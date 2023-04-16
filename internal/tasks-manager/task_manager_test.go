package tasks_manager

import (
	"testing"
	"time"
)

func TestNewTasKManager(t *testing.T) {

	tm := NewTasKManager(30)
	time.Sleep(10 * time.Second)
	tm.StopTaskManager()
	if tm.ctx.Err() == nil {
		t.Fail()
	}
}
