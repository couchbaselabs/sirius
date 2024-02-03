package key_based_loading_cb

import (
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/tasks"
)

type KeyBasedTask interface {
	tasks.Task
	GetCollectionObject() (*cb_sdk.CollectionObject, error)
	CollectionIdentifier() string
}
