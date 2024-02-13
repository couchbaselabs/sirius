package util_sirius

import "github.com/couchbaselabs/sirius/internal/tasks"

// ClearTask represents a request structure for clearing everything.
type ClearTask struct {
	IdentifierToken string `json:"identifierToken" doc:"true"`
	tasks.DatabaseInformation
	TaskPending bool `json:"-" doc:"false"`
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

func (task *ClearTask) Config(_ *tasks.Request, _ bool) (int64, error) {
	task.TaskPending = false
	return 0, nil
}
