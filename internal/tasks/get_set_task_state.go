package tasks

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

// ReadTaskStateFromFile restores  the TaskState as a meta-data of a cluster into a file
func (t *Task) ReadTaskStateFromFile() (TaskState, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return TaskState{}, err
	}
	// For testing purpose
	//fileName := filepath.Join(cwd, "task-state", buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))

	fileName := filepath.Join(cwd, TaskStatePath, buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))
	taskState := TaskState{}
	file, err := os.Open(fileName)
	if err != nil {
		return TaskState{}, fmt.Errorf("no such result found, reasons:[No such Task, In process, Record Deleted]")
	}
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&taskState); err != nil {
		return TaskState{}, err
	}
	if err := file.Close(); err != nil {
		return TaskState{}, err
	}
	return taskState, nil
}

// SaveTaskStateToFile stores the TaskState as a meta-data of a cluster into a file
func (t *Task) SaveTaskStateToFile() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	// For testing purpose
	//fileName := filepath.Join(cwd, "task-state", buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))

	fileName := filepath.Join(cwd, TaskStatePath, buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(&t.TaskState); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return nil
}

// checkForTaskValidity returns if a meta-data for cluster even exists or not.
func (t *Task) checkForTaskValidity() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, TaskStatePath, buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))
	if _, err := os.Stat(fileName); err != nil {
		return err
	}
	return nil
}

// buildTaskName returns the name of the TaskState meta-data file.
func buildTaskName(host, bucket, scope, collection string) string {
	return fmt.Sprintf("%s_%s_%s_%s", host, bucket, scope, collection)
}
