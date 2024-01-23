package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"log"
	"strings"
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
	Result                      *task_result.TaskResult      `json:"-" doc:"false"`
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
	clusterIdentifier, _ := sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
		task.Collection}, ":")
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
		return 0, task_errors.ErrRequestIsNil
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
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.Result.StopStoringResult()
	task.Result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleSubDocRead) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeSingleOperation([]string{task.SingleSubDocOperationConfig.Key}, err1)
		return task.tearUp()
	}

	singleReadSubDocuments(task, collectionObject)
	task.Result.Success = int64(1) - task.Result.Failure
	return task.tearUp()
}

// singleInsertSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleReadSubDocuments(task *SingleSubDocRead, collectionObject *sdk.CollectionObject) {

	if task.req.ContextClosed() {
		return
	}

	var iOps []gocb.LookupInSpec
	key := task.SingleSubDocOperationConfig.Key
	documentMetaData := task.req.DocumentsMeta.GetDocumentsMetadata(task.CollectionIdentifier(), key, "", 0, false)

	for _, path := range task.SingleSubDocOperationConfig.Paths {

		documentMetaData.SubDocument(path, task.GetSpecOptions.IsXattr, task.SingleSubDocOperationConfig.DocSize,
			false)

		iOps = append(iOps, gocb.GetSpec(path, &gocb.GetSpecOptions{
			IsXattr: task.GetSpecOptions.IsXattr,
		}))
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	result, err := collectionObject.Collection.LookupIn(key, iOps, &gocb.LookupInOptions{
		Timeout: time.Duration(task.LookupInOptions.Timeout) * time.Second,
	})

	if err != nil {
		task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
	} else {
		flag := true
		for index := range task.SingleSubDocOperationConfig.Paths {
			var val interface{}
			if err := result.ContentAt(uint(index), &val); err != nil {
				task.Result.Failure++
				task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
				flag = false
				break
			}
		}
		if flag {
			task.Result.CreateSingleErrorResult(initTime, key, "", true, uint64(result.Cas()))
		}
	}

	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleSubDocRead) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
}

func (task *SingleSubDocRead) MatchResultSeed(resultSeed string) bool {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true
	}
	return false
}

func (task *SingleSubDocRead) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleSubDocRead) SetException(exceptions Exceptions) {
}

func (task *SingleSubDocRead) GetOperationConfig() (*OperationConfig, *task_state.TaskState) {
	return nil, nil
}
