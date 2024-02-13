package bulk_loading

import (
	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"sync"
)

type BulkTask interface {
	tasks.Task
	PostTaskExceptionHandling()
	MatchResultSeed(resultSeed string) (bool, error)
	SetException(exceptions Exceptions)
	GetOperationConfig() (*OperationConfig, *task_state.TaskState)
	MetaDataIdentifier() string
}

type loadingTask struct {
	start           int64
	end             int64
	operationConfig *OperationConfig
	seed            int64
	operation       string
	rerun           bool
	gen             *docgenerator.Generator
	state           *task_state.TaskState
	result          *task_result.TaskResult
	databaseInfo    tasks.DatabaseInformation
	extra           db.Extras
	req             *tasks.Request
	identifier      string
	wg              *sync.WaitGroup
}

func newLoadingTask(start, end, seed int64, operationConfig *OperationConfig,
	operation string, rerun bool, gen *docgenerator.Generator,
	state *task_state.TaskState, result *task_result.TaskResult, databaseInfo tasks.DatabaseInformation,
	extra db.Extras, req *tasks.Request, identifier string, wg *sync.WaitGroup) *loadingTask {
	return &loadingTask{
		start:           start,
		end:             end,
		seed:            seed,
		operationConfig: operationConfig,
		operation:       operation,
		rerun:           rerun,
		gen:             gen,
		state:           state,
		result:          result,
		databaseInfo:    databaseInfo,
		extra:           extra,
		req:             req,
		identifier:      identifier,
		wg:              wg,
	}
}

func (l *loadingTask) Run() {
	switch l.operation {
	case tasks.InsertOperation:
		{
			insertDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.wg)
		}
	case tasks.UpsertOperation:
		{
			upsertDocuments(l.start, l.end, l.seed, l.operationConfig, l.rerun, l.gen, l.state, l.result,
				l.databaseInfo, l.extra, l.req, l.identifier, l.wg)
		}
	case tasks.DeleteOperation:
		{

		}
	case tasks.ReadOperation:
		{

		}
	case tasks.TouchOperation:
		{

		}
	case tasks.SubDocInsertOperation:
		{

		}
	case tasks.SubDocDeleteOperation:
		{

		}
	case tasks.SubDocReadOperation:
		{

		}
	case tasks.SubDocReplaceOperation:
		{

		}
	case tasks.SubDocUpsertOperation:
		{

		}
	}
}
