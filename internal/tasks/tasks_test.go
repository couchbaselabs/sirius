package tasks

import (
	"log"
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

func TestSaveTaskStateToFile(t *testing.T) {
	task := Task{
		TaskState: TaskState{
			Host:       "172.23.136.000",
			BUCKET:     "bucket",
			SCOPE:      "scope",
			Collection: "collection",
			Seed:       1678775265835097000,
			SeedEnd:    1678775265835097000,
			InsertTaskError: InsertTaskError{
				Err: make(map[int64]struct{}),
			},
			DeleteTask: DeleteTask{
				Del: make(map[int64]struct{}),
			},
			UpsertTask: UpsertTask{
				Operation: []UpsertTaskOperation{},
			},
		},
	}
	if err := task.saveTaskStateToFile(); err != nil {
		log.Println(err)
		t.Fail()
	}
}

func TestReadTaskStateFromFile(t *testing.T) {
	task := Task{
		UserData: UserData{
			Seed: 1678775265835097000,
		},
		TaskState: TaskState{
			Host:       "172.23.136.000",
			BUCKET:     "bucket",
			SCOPE:      "scope",
			Collection: "collection",
			Seed:       1678775265835097000,
		},
	}
	var err error
	task.TaskState, err = task.readTaskStateFromFile()
	if err != nil {
		t.Fail()
	}
	log.Println(task.TaskState)
}

func TestSaveResultIntoFile(t *testing.T) {
	taskResult := TaskResult{
		UserData: UserData{
			Seed: 1678623796852619000,
		},
		TaskOperationCounter: TaskOperationCounter{
			Failure: 0,
			Success: 1,
		},
	}

	if err := SaveResultIntoFile(taskResult); err != nil {
		t.Fail()
	}

}

func TestReadResultFromFile(t *testing.T) {
	seed := "1678623796852619000"
	if val, err := ReadResultFromFile(seed, false); err != nil {
		log.Println(err.Error())
		t.Fail()
	} else {
		log.Println(val)
	}
}
