package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"log"
	"time"
)

type SingleSubDocIncrement struct {
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

func (task *SingleSubDocIncrement) Describe() string {
	return "SingleSingleSubDocIncrement increment the value of paths by delta as a sub-Document opertion."
}

func (task *SingleSubDocIncrement) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleSubDocIncrement) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleSubDocIncrement) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleSubDocIncrement) Config(req *Request, reRun bool) (int64, error) {
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
		task.Operation = SingleSubDocIncrementOperation

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

func (task *SingleSubDocIncrement) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed, task.Operation)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleSubDocIncrement) Do() error {

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

	singleIncrementSubDocuments(task, collectionObject)

	task.result.Success = int64(len(task.SingleSubDocOperationConfig.KeyPathValue)) - task.result.Failure
	return task.tearUp()
}

// singleInsertSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleIncrementSubDocuments(task *SingleSubDocIncrement, collectionObject *sdk.CollectionObject) {

	for _, data := range task.SingleSubDocOperationConfig.KeyPathValue {

		var iOps []gocb.MutateInSpec

		for i := range data.PathValue {
			v, ok := data.PathValue[i].Value.(float64)
			if !ok {
				task.result.CreateSingleErrorResult(data.Key, "delta to increment is not number", false, 0)
				continue
			}
			iOps = append(iOps, gocb.IncrementSpec(data.PathValue[i].Path, int64(v),
				&gocb.CounterSpecOptions{
					IsXattr:    task.InsertSpecOptions.IsXattr,
					CreatePath: task.InsertSpecOptions.CreatePath,
				}))
		}

		result, err := collectionObject.Collection.MutateIn(data.Key, iOps, &gocb.MutateInOptions{
			Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
			PersistTo:       task.MutateInOptions.PersistTo,
			ReplicateTo:     task.MutateInOptions.ReplicateTo,
			DurabilityLevel: getDurability(task.MutateInOptions.Durability),
			StoreSemantic:   getStoreSemantic(task.MutateInOptions.StoreSemantic),
			Timeout:         time.Duration(task.MutateInOptions.Expiry) * time.Second,
			PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
		})

		if err != nil {
			task.result.CreateSingleErrorResult(data.Key, err.Error(), false, 0)
		} else {
			task.result.CreateSingleErrorResult(data.Key, "", true, uint64(result.Cas()))
		}

	}

	task.PostTaskExceptionHandling(collectionObject)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleSubDocIncrement) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
}

func (task *SingleSubDocIncrement) GetResultSeed() string {
	if task.result == nil {
		return ""
	}
	return fmt.Sprintf("%d", task.result.ResultSeed)
}

func (task *SingleSubDocIncrement) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleSubDocIncrement) SetException(exceptions Exceptions) {
}
