package tasks

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"log"
	"time"
)

type BucketWarmUpTask struct {
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"-" doc:"false"`
	Collection      string                  `json:"-" doc:"false"`
	Result          *task_result.TaskResult `json:"-" doc:"false"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int64                   `json:"resultSeed" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *BucketWarmUpTask) Describe() string {
	return "This API aids in warming up a Couchbase bucket or establishing connections to KV services."
}

func (task *BucketWarmUpTask) Do() error {
	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	_, err1 := task.GetCollectionObject()
	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
	}
	return task.tearUp()
}

func (task *BucketWarmUpTask) Config(req *Request, reRun bool) (int64, error) {
	task.req = req

	if task.req == nil {
		return 0, task_errors.ErrRequestIsNil
	}

	task.req.ReconnectionManager()
	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
		return 0, err
	}

	task.ResultSeed = int64(time.Now().UnixNano())
	task.Operation = BucketWarmUpOperation

	if task.Bucket == "" {
		task.Bucket = DefaultBucket
	}
	if task.Scope == "" {
		task.Scope = DefaultScope
	}
	if task.Collection == "" {
		task.Collection = DefaultCollection
	}
	return task.ResultSeed, nil
}

func (task *BucketWarmUpTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *BucketWarmUpTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *BucketWarmUpTask) CheckIfPending() bool {
	return false
}

func (task *BucketWarmUpTask) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
	return

}

func (task *BucketWarmUpTask) MatchResultSeed(resultSeed string) bool {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true
	}
	return false
}

func (task *BucketWarmUpTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *BucketWarmUpTask) SetException(exceptions Exceptions) {
	return
}

func (task *BucketWarmUpTask) tearUp() error {
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	return nil
}
