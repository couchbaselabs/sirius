package bulk_loading_cb

import (
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
)

type BulkTask interface {
	tasks.Task
	PostTaskExceptionHandling(collectionObject *cb_sdk.CollectionObject)
	MatchResultSeed(resultSeed string) (bool, error)
	GetCollectionObject() (*cb_sdk.CollectionObject, error)
	SetException(exceptions tasks.Exceptions)
	GetOperationConfig() (*tasks.OperationConfig, *task_state.TaskState)
	CollectionIdentifier() string
}
