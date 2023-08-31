package tasks

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type SingleReadTask struct {
	IdentifierToken       string                  `json:"identifierToken" doc:"true"`
	ClusterConfig         *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket                string                  `json:"bucket" doc:"true"`
	Scope                 string                  `json:"scope,omitempty" doc:"true"`
	Collection            string                  `json:"collection,omitempty" doc:"true"`
	SingleOperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation             string                  `json:"operation" doc:"false"`
	ResultSeed            int64                   `json:"resultSeed" doc:"false"`
	TaskPending           bool                    `json:"taskPending" doc:"false"`
	Result                *task_result.TaskResult `json:"-" doc:"false"`
	req                   *Request                `json:"-" doc:"false"`
}

func (task *SingleReadTask) Describe() string {
	return "Single read task reads key value in couchbase and validates.\n"
}

func (task *SingleReadTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleReadTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleReadTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleReadTask) Config(req *Request, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req

	if task.req == nil {
		task.TaskPending = false
		return 0, task_errors.ErrRequestIsNil
	}

	task.req.ReconnectionManager()
	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
		task.TaskPending = false
		return 0, err
	}

	task.req.ReconfigureDocumentManager()

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = SingleReadOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configSingleOperationConfig(task.SingleOperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}
	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleReadTask) tearUp() error {
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}
	task.Result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleReadTask) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeSingleOperation(task.SingleOperationConfig.Keys, err1)
		return task.tearUp()
	}

	singleReadDocuments(task, collectionObject)

	task.Result.Success = int64(len(task.SingleOperationConfig.Keys)) - task.Result.Failure
	return task.tearUp()
}

// singleDeleteDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleReadDocuments(task *SingleReadTask, collectionObject *sdk.CollectionObject) {

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan string, MaxConcurrentRoutines)

	group := errgroup.Group{}

	for _, data := range task.SingleOperationConfig.Keys {
		routineLimiter <- struct{}{}
		dataChannel <- data

		group.Go(func() error {
			key := <-dataChannel
			task.req.DocumentsMeta.GetDocumentsMetadata(task.CollectionIdentifier(), key, task.SingleOperationConfig.Template,
				task.SingleOperationConfig.DocSize, false)

			initTime := time.Now().UTC().Format(time.RFC850)
			result, err := collectionObject.Collection.Get(key, nil)
			if err != nil {
				task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}

			task.Result.CreateSingleErrorResult(initTime, key, "", true, uint64(result.Cas()))
			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleReadTask) PostTaskExceptionHandling(_ *sdk.CollectionObject) {
}

func (task *SingleReadTask) GetResultSeed() string {
	if task.Result == nil {
		task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	}
	return fmt.Sprintf("%d", task.ResultSeed)
}

func (task *SingleReadTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleReadTask) SetException(exceptions Exceptions) {

}
