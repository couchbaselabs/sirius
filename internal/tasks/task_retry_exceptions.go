package tasks

import (
	"errors"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_state"
)

type RetryExceptions struct {
	IdentifierToken string     `json:"identifierToken" doc:"true"`
	ResultSeed      string     `json:"resultSeed" doc:"true"`
	Exceptions      Exceptions `json:"exceptions" doc:"true"`
	Task            Task       `json:"-" doc:"false"`
	req             *Request   `json:"-" doc:"false"`
}

func (r *RetryExceptions) Describe() string {
	return "Retry Exception reties failed operations.\n" +
		"IgnoreExceptions will ignore failed operation occurred in this category. \n" +
		"RetryExceptions will retry failed operation occurred in this category. \n" +
		"RetryAttempts is the number of retry attempts.\n"
}

func (r *RetryExceptions) Do() error {
	if r.req.ContextClosed() {
		return errors.New("req is cleared")
	}

	c, e := r.Task.GetCollectionObject()
	if e != nil {
		r.Task.tearUp()
		return nil
	}
	r.Task.SetException(r.Exceptions)
	r.Task.PostTaskExceptionHandling(c)
	r.Task.tearUp()
	return nil
}

func (r *RetryExceptions) Config(req *Request, reRun bool) (int64, error) {
	r.req = req
	if r.req == nil {
		return 0, task_errors.ErrRequestIsNil
	}

	if r.req.Tasks == nil {
		return 0, fmt.Errorf("request.Task struct is nil")
	}
	for i := range r.req.Tasks {
		if ok, err := r.req.Tasks[i].Task.MatchResultSeed(r.ResultSeed); ok {
			if err != nil {
				return 0, err
			}
			r.Task = r.req.Tasks[i].Task
			break
		}
	}

	if r.Task == nil {
		return 0, fmt.Errorf(" no task found for %s : %s ", r.req.Identifier, r.ResultSeed)
	}

	return r.Task.Config(req, true)

}

func (r *RetryExceptions) BuildIdentifier() string {
	if r.IdentifierToken == "" {
		r.IdentifierToken = DefaultIdentifierToken
	}
	return r.IdentifierToken
}

func (r *RetryExceptions) CollectionIdentifier() string {
	return r.Task.CollectionIdentifier()
}

func (r *RetryExceptions) CheckIfPending() bool {
	return r.Task.CheckIfPending()
}

func (r *RetryExceptions) PostTaskExceptionHandling(_ *sdk.CollectionObject) {

}

func (r *RetryExceptions) tearUp() error {
	return r.Task.tearUp()
}
func (r *RetryExceptions) MatchResultSeed(resultSeed string) (bool, error) {
	return r.Task.MatchResultSeed(resultSeed)
}

func (r *RetryExceptions) GetCollectionObject() (*sdk.CollectionObject, error) {
	return r.Task.GetCollectionObject()
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
