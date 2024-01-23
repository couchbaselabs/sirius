package tasks

import (
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_state"
)

type Task interface {
	Describe() string
	Do() error
	Config(req *Request, reRun bool) (int64, error)
	BuildIdentifier() string
	CollectionIdentifier() string
	CheckIfPending() bool
	PostTaskExceptionHandling(collectionObject *sdk.CollectionObject)
	MatchResultSeed(resultSeed string) bool
	GetCollectionObject() (*sdk.CollectionObject, error)
	SetException(exceptions Exceptions)
	tearUp() error
	GetOperationConfig() (*OperationConfig, *task_state.TaskState)
}
