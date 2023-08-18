package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"log"
	"time"
)

type SingleSubDocDelete struct {
	IdentifierToken             string                       `json:"identifierToken" doc:"true"`
	ClusterConfig               *sdk.ClusterConfig           `json:"clusterConfig" doc:"true"`
	Bucket                      string                       `json:"bucket" doc:"true"`
	Scope                       string                       `json:"scope,omitempty" doc:"true"`
	Collection                  string                       `json:"collection,omitempty" doc:"true"`
	SingleSubDocOperationConfig *SingleSubDocOperationConfig `json:"singleSubDocOperationConfig" doc:"true"`
	RemoveSpecOptions           *RemoveSpecOptions           `json:"removeSpecOptions" doc:"true"`
	MutateInOptions             *MutateInOptions             `json:"mutateInOptions" doc:"true"`
	Operation                   string                       `json:"operation" doc:"false"`
	ResultSeed                  int64                        `json:"resultSeed" doc:"false"`
	TaskPending                 bool                         `json:"taskPending" doc:"false"`
	result                      *task_result.TaskResult      `json:"-" doc:"false"`
	req                         *Request                     `json:"-" doc:"false"`
}

func (task *SingleSubDocDelete) Describe() string {
	return "SingleSingleSubDocDelete inserts a Sub-Document as per user's input [No Random data]"
}

func (task *SingleSubDocDelete) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleSubDocDelete) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleSubDocDelete) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleSubDocDelete) Config(req *Request, reRun bool) (int64, error) {
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
		task.Operation = SingleSubDocDeleteOperation

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

		if err := configRemoveSpecOptions(task.RemoveSpecOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configMutateInOptions(task.MutateInOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}
	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleSubDocDelete) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed, task.Operation)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleSubDocDelete) Do() error {

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

	singleDeleteSubDocuments(task, collectionObject)

	task.result.Success = int64(len(task.SingleSubDocOperationConfig.KeyPathValue)) - task.result.Failure
	return task.tearUp()
}

// singleInsertSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleDeleteSubDocuments(task *SingleSubDocDelete, collectionObject *sdk.CollectionObject) {

	for _, data := range task.SingleSubDocOperationConfig.KeyPathValue {

		var iOps []gocb.MutateInSpec

		for i := range data.PathValue {
			iOps = append(iOps, gocb.RemoveSpec(data.PathValue[i].Path, &gocb.RemoveSpecOptions{
				IsXattr: task.RemoveSpecOptions.IsXattr,
			}))
		}

		result, err := collectionObject.Collection.MutateIn(data.Key, iOps, &gocb.MutateInOptions{
			Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
			Cas:             gocb.Cas(task.MutateInOptions.Cas),
			PersistTo:       task.MutateInOptions.PersistTo,
			ReplicateTo:     task.MutateInOptions.ReplicateTo,
			DurabilityLevel: getDurability(task.MutateInOptions.Durability),
			StoreSemantic:   getStoreSemantic(task.MutateInOptions.StoreSemantic),
			Timeout:         time.Duration(task.MutateInOptions.Expiry) * time.Second,
			PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
		})

		if err != nil {
			task.result.Failure++
			task.result.CreateSingleErrorResult(data.Key, err.Error(), false, 0)
		} else {
			task.result.CreateSingleErrorResult(data.Key, "", true, uint64(result.Cas()))
		}

	}

	task.PostTaskExceptionHandling(collectionObject)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleSubDocDelete) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
}

func (task *SingleSubDocDelete) GetResultSeed() string {
	if task.result == nil {
		return ""
	}
	return fmt.Sprintf("%d", task.result.ResultSeed)
}

func (task *SingleSubDocDelete) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleSubDocDelete) SetException(exceptions Exceptions) {
}