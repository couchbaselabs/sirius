package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"log"
	"time"
)

type SingleSubDocRead struct {
	IdentifierToken             string                       `json:"identifierToken" doc:"true"`
	ClusterConfig               *sdk.ClusterConfig           `json:"clusterConfig" doc:"true"`
	Bucket                      string                       `json:"bucket" doc:"true"`
	Scope                       string                       `json:"scope,omitempty" doc:"true"`
	Collection                  string                       `json:"collection,omitempty" doc:"true"`
	SingleSubDocOperationConfig *SingleSubDocOperationConfig `json:"singleSubDocOperationConfig" doc:"true"`
	LookupInOptions             *LookupInOptions             `json:"lookupInOptions" doc:"true"`
	GetSpecOptions              *GetSpecOptions              `json:"getSpecOptions" doc:"true"`
	Operation                   string                       `json:"operation" doc:"false"`
	ResultSeed                  int64                        `json:"resultSeed" doc:"false"`
	TaskPending                 bool                         `json:"taskPending" doc:"false"`
	result                      *task_result.TaskResult      `json:"-" doc:"false"`
	req                         *Request                     `json:"-" doc:"false"`
}

func (task *SingleSubDocRead) Describe() string {
	return "SingleSingleSubDocRead inserts a Sub-Document as per user's input [No Random data]"
}

func (task *SingleSubDocRead) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleSubDocRead) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleSubDocRead) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleSubDocRead) Config(req *Request, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req

	if task.req == nil {
		task.TaskPending = false
		return 0, fmt.Errorf("request.Request struct is nil")
	}

	task.req.ReconnectionManager()
	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
		task.TaskPending = false
		return 0, err
	}

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = SingleSubDocReadOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configSingleSubDocOperationConfig(task.SingleSubDocOperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configGetSpecOptions(task.GetSpecOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configLookupInOptions(task.LookupInOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleSubDocRead) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed, task.Operation)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleSubDocRead) Do() error {

	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		var docIds []string
		for _, data := range task.SingleSubDocOperationConfig.KeyPathValue {
			docIds = append(docIds, data.Key)
		}
		task.result.FailWholeSingleOperation(docIds, err1)
		return task.tearUp()
	}

	singleReadSubDocuments(task, collectionObject)
	task.result.Success = int64(len(task.SingleSubDocOperationConfig.KeyPathValue)) - task.result.Failure
	return task.tearUp()
}

// singleInsertSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleReadSubDocuments(task *SingleSubDocRead, collectionObject *sdk.CollectionObject) {

	for _, data := range task.SingleSubDocOperationConfig.KeyPathValue {

		var iOps []gocb.LookupInSpec
		var paths []string
		for _, pathValue := range data.PathValue {
			paths = append(paths, pathValue.Path)
			iOps = append(iOps, gocb.GetSpec(pathValue.Path, &gocb.GetSpecOptions{
				IsXattr: task.GetSpecOptions.IsXattr,
			}))
		}

		result, err := collectionObject.Collection.LookupIn(data.Key, iOps, &gocb.LookupInOptions{
			Timeout: time.Duration(task.LookupInOptions.Timeout) * time.Second,
		})

		if err != nil {
			task.result.Failure++
			task.result.CreateSingleErrorResult(data.Key, err.Error(), false, 0)
		} else {
			flag := true
			for index, _ := range paths {
				var val interface{}
				if err := result.ContentAt(uint(index), &val); err != nil {
					task.result.Failure++
					task.result.CreateSingleErrorResult(data.Key, err.Error(), false, 0)
					flag = false
					break
				}
			}
			if flag {
				task.result.CreateSingleErrorResult(data.Key, "", true, uint64(result.Cas()))
			}
		}

	}

	task.PostTaskExceptionHandling(collectionObject)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleSubDocRead) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
}

func (task *SingleSubDocRead) GetResultSeed() string {
	if task.result == nil {
		return ""
	}
	return fmt.Sprintf("%d", task.result.ResultSeed)
}

func (task *SingleSubDocRead) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleSubDocRead) SetException(exceptions Exceptions) {
}
