package task_state

import (
	"encoding/gob"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"os"
	"path/filepath"
	"strings"
)

const TaskStatePath = "./internal/task_state/task_state_logs"

type InsertTaskState struct {
	Err []int64
}

type DeleteTaskState struct {
	Del []int64
}

type UpsertTaskState struct {
	Start          int64
	End            int64
	FieldsToChange []string
	Err            []int64
}

type TaskState struct {
	Host            string
	BUCKET          string
	SCOPE           string
	Collection      string
	TemplateName    string
	DocumentSize    int64
	Seed            int64
	SeedEnd         int64
	KeyPrefix       string
	KeySuffix       string
	InsertTaskState InsertTaskState
	DeleteTaskState DeleteTaskState
	UpsertTaskState []UpsertTaskState
}

func ConfigTaskState(host, bucket, scope, collection, templateName, keyPrefix, keySuffix string, docSize, seed, seedEnd int64) (*TaskState, bool) {

	state := &TaskState{
		Host:         host,
		BUCKET:       bucket,
		SCOPE:        scope,
		Collection:   collection,
		TemplateName: templateName,
		DocumentSize: docSize,
		Seed:         seed,
		SeedEnd:      seedEnd,
		KeyPrefix:    keyPrefix,
		KeySuffix:    keySuffix,
	}

	if statExisting, err := state.ReadTaskStateFromFile(); err == nil {
		return statExisting, true
	}
	return state, false

}

// RetracePreviousMutations retraces all previous mutation from the saved sequences of upsert operations.
func (t *TaskState) RetracePreviousMutations(key int64, doc interface{}, gen docgenerator.Generator, fake *faker.Faker) (interface{}, error) {
	for _, u := range t.UpsertTaskState {
		if key >= (u.Start+t.Seed-1) && (key <= u.End+t.Seed-1) {
			flag := true
			for _, e := range u.Err {
				if e == key {
					flag = false
					break
				}
			}
			if flag {
				doc, _ = gen.Template.UpdateDocument(u.FieldsToChange, doc, fake)
			}
		}
	}
	return doc, nil
}

// buildTaskName returns the name of the TaskState meta-data file.
func buildTaskName(host, bucket, scope, collection string) string {
	if strings.Contains(host, "couchbase://") {
		host = strings.ReplaceAll(host, "couchbase://", "")
	}
	if strings.Contains(host, "couchbases://") {
		host = strings.ReplaceAll(host, "couchbases://", "")
	}

	return fmt.Sprintf("%s_%s_%s_%s", host, bucket, scope, collection)
}

// ReadTaskStateFromFile restores  the TaskState as a meta-data of a cluster into a file
func (t *TaskState) ReadTaskStateFromFile() (*TaskState, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	// For testing purpose
	//fileName := filepath.Join(cwd, "task_state_logs", buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))

	fileName := filepath.Join(cwd, TaskStatePath, buildTaskName(t.Host, t.BUCKET, t.SCOPE, t.Collection))
	taskState := &TaskState{}
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("no such result found, reasons:[No such Task, In process, Record Deleted]")
	}
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(taskState); err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return taskState, nil
}

// SaveTaskStateToFile stores the TaskState as a meta-data of a cluster into a file
func (t *TaskState) SaveTaskStateToFile() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	// For testing purpose
	//fileName := filepath.Join(cwd, "task_state_logs", buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))

	fileName := filepath.Join(cwd, TaskStatePath, buildTaskName(t.Host, t.BUCKET, t.SCOPE, t.Collection))
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(t); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return nil
}

func (t *TaskState) DeleteTaskStateFromFile() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	// For testing purpose
	//fileName := filepath.Join(cwd, "task_state_logs", buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))

	fileName := filepath.Join(cwd, TaskStatePath, buildTaskName(t.Host, t.BUCKET, t.SCOPE, t.Collection))
	if err := os.Remove(fileName); err != nil {
		return err
	}
	return nil
}

// CheckForTaskValidity returns if a meta-data for cluster even exists or not.
func (t *TaskState) CheckForTaskValidity() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, TaskStatePath, buildTaskName(t.Host, t.BUCKET, t.SCOPE, t.Collection))
	if _, err := os.Stat(fileName); err != nil {
		return err
	}
	return nil
}
