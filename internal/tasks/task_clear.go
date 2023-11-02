package tasks

import (
	"github.com/couchbaselabs/sirius/internal/sdk"
)

// ClearTask represents a request structure for clearing everything.
type ClearTask struct {
	IdentifierToken string `json:"identifierToken" doc:"true"`
	TaskPending     bool   `json:"-" doc:"false"`
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

func (task *ClearTask) Config(_ *Request, _ bool) (int64, error) {
	task.TaskPending = false
	return 0, nil
}

func (task *ClearTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *ClearTask) CollectionIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *ClearTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *ClearTask) PostTaskExceptionHandling(_ *sdk.CollectionObject) {

}

func (task *ClearTask) MatchResultSeed(_ string) bool {
	return false
}

func (task *ClearTask) GetCollectionObject() ([]*sdk.CollectionObject, error) {
	return []*sdk.CollectionObject{}, nil
}

func (task *ClearTask) SetException(exceptions Exceptions) {
}
