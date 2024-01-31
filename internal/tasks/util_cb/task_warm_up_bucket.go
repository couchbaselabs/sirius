package util_cb

import (
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"log"
	"time"
)

type BucketWarmUpTask struct {
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *cb_sdk.ClusterConfig   `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"-" doc:"false"`
	Collection      string                  `json:"-" doc:"false"`
	Result          *task_result.TaskResult `json:"-" doc:"false"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int64                   `json:"resultSeed" doc:"false"`
	req             *tasks.Request          `json:"-" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
}

func (task *BucketWarmUpTask) Describe() string {
	return "This API aids in warming up a Couchbase bucket or establishing connections to KV services."
}

func (task *BucketWarmUpTask) Do() error {
	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	_, err1 := task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
	}
	return task.TearUp()
}

func (task *BucketWarmUpTask) Config(req *tasks.Request, reRun bool) (int64, error) {
	task.TaskPending = false
	task.req = req

	if task.req == nil {
		return 0, task_errors.ErrRequestIsNil
	}

	task.req.ReconnectionManager()
	if _, err := task.req.GetCluster(task.ClusterConfig); err != nil {
		return 0, err
	}

	task.ResultSeed = int64(time.Now().UnixNano())
	task.Operation = tasks.BucketWarmUpOperation

	if task.Bucket == "" {
		task.Bucket = tasks.DefaultBucket
	}
	if task.Scope == "" {
		task.Scope = tasks.DefaultScope
	}
	if task.Collection == "" {
		task.Collection = tasks.DefaultCollection
	}
	return task.ResultSeed, nil
}

func (task *BucketWarmUpTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = tasks.DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *BucketWarmUpTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *BucketWarmUpTask) TearUp() error {
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.TaskPending = false
	return nil
}
