package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"log"
	"time"
)

type SingleSubDocInsert struct {
	IdentifierToken             string                       `json:"identifierToken" doc:"true"`
	ClusterConfig               *sdk.ClusterConfig           `json:"clusterConfig" doc:"true"`
	Bucket                      string                       `json:"bucket" doc:"true"`
	Scope                       string                       `json:"scope,omitempty" doc:"true"`
	Collection                  string                       `json:"collection,omitempty" doc:"true"`
	SingleSubDocOperationConfig *SingleSubDocOperationConfig `json:"singleSubDocOperationConfig" doc:"true"`
	InsertSpecOptions           *InsertSpecOptions           `json:"insertSpecOptions" doc:"true"`
	MutateInOptions             *MutateInOptions             `json:"mutateInOptions" doc:"true"`
	Operation                   string                       `json:"operation" doc:"false"`
	ResultSeed                  int64                        `json:"resultSeed" doc:"false"`
	TaskPending                 bool                         `json:"taskPending" doc:"false"`
	result                      *task_result.TaskResult      `json:"-" doc:"false"`
	req                         *Request                     `json:"-" doc:"false"`
}

func (task *SingleSubDocInsert) Describe() string {
	return "SingleSingleSubDocInsert inserts a Sub-Document as per user's input [No Random data]"
}

func (task *SingleSubDocInsert) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleSubDocInsert) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleSubDocInsert) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleSubDocInsert) Config(req *Request, reRun bool) (int64, error) {
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
		task.Operation = SingleSubDocInsertOperation

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

		if err := configInsertSpecOptions(task.InsertSpecOptions); err != nil {
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

func (task *SingleSubDocInsert) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed, task.Operation)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleSubDocInsert) Do() error {

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

	singleInsertSubDocuments(task, collectionObject)

	task.result.Success = int64(len(task.SingleSubDocOperationConfig.KeyPathValue)) - task.result.Failure
	return task.tearUp()
}

// singleInsertSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleInsertSubDocuments(task *SingleSubDocInsert, collectionObject *sdk.CollectionObject) {

	for _, data := range task.SingleSubDocOperationConfig.KeyPathValue {

		var iOps []gocb.MutateInSpec

		for i := range data.PathValue {
			iOps = append(iOps, gocb.InsertSpec(data.PathValue[i].Path, data.PathValue[i].Value, &gocb.InsertSpecOptions{
				CreatePath: task.InsertSpecOptions.CreatePath,
				IsXattr:    task.InsertSpecOptions.IsXattr,
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

func (task *SingleSubDocInsert) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
}

func (task *SingleSubDocInsert) GetResultSeed() string {
	if task.result == nil {
		return ""
	}
	return fmt.Sprintf("%d", task.result.ResultSeed)
}

func (task *SingleSubDocInsert) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleSubDocInsert) SetException(exceptions Exceptions) {
}
