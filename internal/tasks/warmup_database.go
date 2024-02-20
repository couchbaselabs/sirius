package tasks

import (
	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"log"
	"time"
)

type BucketWarmUpTask struct {
	IdentifierToken string `json:"identifierToken" doc:"true"`
	DatabaseInformation
	Result      *task_result.TaskResult `json:"-" doc:"false"`
	Operation   string                  `json:"operation" doc:"false"`
	ResultSeed  int64                   `json:"resultSeed" doc:"false"`
	req         *Request                `json:"-" doc:"false"`
	TaskPending bool                    `json:"taskPending" doc:"false"`
}

func (t *BucketWarmUpTask) Describe() string {
	return "Warming up a connection to database."
}

func (t *BucketWarmUpTask) Do() {
	t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed)

	database, err := db.ConfigDatabase(t.DBType)

	if err != nil {
		t.Result.ErrorOther = err.Error()
		_ = t.TearUp()
	}
	if err = database.Connect(t.ConnStr, t.Username, t.Password, t.Extra); err != nil {
		t.Result.ErrorOther = err.Error()
		_ = t.TearUp()
	}

	if err = database.Warmup(t.ConnStr, t.Username, t.Password, t.Extra); err != nil {
		t.Result.ErrorOther = err.Error()
		_ = t.TearUp()
	}

	_ = t.TearUp()
}

func (t *BucketWarmUpTask) Config(req *Request, reRun bool) (int64, error) {
	t.TaskPending = false
	t.req = req

	if t.req == nil {
		return 0, err_sirius.RequestIsNil
	}

	t.ResultSeed = int64(time.Now().UnixNano())
	t.Operation = BucketWarmUpOperation

	return t.ResultSeed, nil
}

func (t *BucketWarmUpTask) CheckIfPending() bool {
	return t.TaskPending
}

func (t *BucketWarmUpTask) TearUp() error {
	t.Result.StopStoringResult()
	if err := t.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", t.ResultSeed, t.Operation)
	}
	t.TaskPending = false
	return nil
}
