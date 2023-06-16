package tasks

import (
	"fmt"
)

// ClearTask represents a request structure for clearing everything.
type ClearTask struct {
	IdentifierToken string `json:"identifierToken" doc:"true"`
	Username        string `json:"username" doc:"true"`
	Password        string `json:"password" doc:"true"`
	Bucket          string `json:"bucket,omitempty" doc:"true"`
	Scope           string `json:"scope,omitempty" doc:"true"`
	Collection      string `json:"collection,omitempty" doc:"true"`
	TaskPending     bool   `json:"-" doc"false"`
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

func (task *ClearTask) Config(req *Request, seed int, seedEnd int, rerun bool) (int, error) {
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
	return fmt.Sprintf("%s-%s-%s-%s-%s", task.Username, task.IdentifierToken, task.Bucket, task.Scope, task.Collection)
}

func (task *ClearTask) CheckIfPending() bool {
	return task.TaskPending
}
