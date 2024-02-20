package bulk_loading

import (
	"fmt"

	"github.com/barkha06/sirius/internal/err_sirius"
	"github.com/barkha06/sirius/internal/task_state"
	"github.com/barkha06/sirius/internal/tasks"
)

type RetryExceptions struct {
	IdentifierToken string         `json:"identifierToken" doc:"true"`
	ResultSeed      string         `json:"resultSeed" doc:"true"`
	Exceptions      Exceptions     `json:"exceptions" doc:"true"`
	Task            BulkTask       `json:"-" doc:"false"`
	req             *tasks.Request `json:"-" doc:"false"`
}

func (r *RetryExceptions) Describe() string {
	return "Retry Exception reties failed operations.\n" +
		"IgnoreExceptions will ignore failed operation occurred in this category. \n" +
		"RetryExceptions will retry failed operation occurred in this category. \n" +
		"RetryAttempts is the number of retry attempts.\n"
}

func (r *RetryExceptions) Do() {
	if r.req.ContextClosed() {
		return
	}

	r.Task.SetException(r.Exceptions)
	r.Task.PostTaskExceptionHandling()
	_ = r.Task.TearUp()
}

func (r *RetryExceptions) Config(req *tasks.Request, reRun bool) (int64, error) {
	r.req = req
	if r.req == nil {
		return 0, err_sirius.RequestIsNil
	}

	if r.req.Tasks == nil {
		return 0, fmt.Errorf("request.Task struct is nil")
	}
	for i := range r.req.Tasks {
		if bulkTask, ok := r.req.Tasks[i].Task.(BulkTask); ok {
			if ok, err := bulkTask.MatchResultSeed(r.ResultSeed); ok {
				if err != nil {
					return 0, err
				} else {
					r.Task = bulkTask
					break
				}
			}
		}
	}

	if r.Task == nil {
		return 0, fmt.Errorf("no bulk loading task found for %s : %s", r.req.Identifier, r.ResultSeed)
	}

	return r.Task.Config(req, true)

}

func (r *RetryExceptions) MetaDataIdentifier() string {
	return r.Task.MetaDataIdentifier()
}

func (r *RetryExceptions) CheckIfPending() bool {
	return r.Task.CheckIfPending()
}

func (r *RetryExceptions) PostTaskExceptionHandling() {

}

func (r *RetryExceptions) TearUp() error {
	return r.Task.TearUp()
}
func (r *RetryExceptions) MatchResultSeed(resultSeed string) (bool, error) {
	return r.Task.MatchResultSeed(resultSeed)
}

func (r *RetryExceptions) SetException(exceptions Exceptions) {
	r.Task.SetException(r.Exceptions)
}

func (r *RetryExceptions) GetOperationConfig() (*OperationConfig, *task_state.TaskState) {
	if r.Task != nil {
		return r.Task.GetOperationConfig()
	}
	return nil, nil
}
