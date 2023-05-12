package tasks

import (
	"fmt"
	"strings"
)

// ClearTask represents a request structure for retrieving result of the task.
type ClearTask struct {
	ConnectionString string `json:"connectionString"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	Bucket           string `json:"bucket"`
	Scope            string `json:"scope,omitempty"`
	Collection       string `json:"collection,omitempty"`
	TaskPending      bool
}

func (task *ClearTask) Describe() string {
	return `The Task clear operation will remove the metadata from the bucket on the specific Couchbase server where
the test was executed.`
}

func (task *ClearTask) tearUp() error {
	return nil
}

func (task *ClearTask) Do() error {
	task.TaskPending = false
	return nil
}

func (task *ClearTask) Config(req *Request, seed int64, seedEnd int64, index int, rerun bool) (int64, error) {
	task.TaskPending = false
	return 0, nil
}

func (task *ClearTask) BuildIdentifier() string {
	if task.Bucket == "" {
		task.Bucket = DefaultBucket
	}
	if task.Scope == "" {
		task.Scope = DefaultScope
	}
	if task.Collection == "" {
		task.Collection = DefaultCollection
	}
	var host string
	if strings.Contains(task.ConnectionString, "couchbase://") {
		host = strings.ReplaceAll(task.ConnectionString, "couchbase://", "")
	}
	if strings.Contains(task.ConnectionString, "couchbases://") {
		host = strings.ReplaceAll(task.ConnectionString, "couchbases://", "")
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s", task.Username, host, task.Bucket, task.Scope, task.Collection)
}

func (task *ClearTask) CheckIfPending() bool {
	return task.TaskPending
}
