package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"time"
)

type SingleSubDocUpsert struct {
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

func (task *SingleSubDocUpsert) Describe() string {
	return "SingleSingleSubDocUpsert inserts a Sub-Document as per user's input [No Random data]"
}

func (task *SingleSubDocUpsert) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleSubDocUpsert) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleSubDocUpsert) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleSubDocUpsert) Config(req *Request, reRun bool) (int64, error) {
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

	task.req.ReconfigureDocumentManager()

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = SingleSubDocUpsertOperation

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

func (task *SingleSubDocUpsert) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed, task.Operation)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleSubDocUpsert) Do() error {

	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		task.result.FailWholeSingleOperation([]string{task.SingleSubDocOperationConfig.Key}, err1)
		return task.tearUp()
	}

	singleUpsertSubDocuments(task, collectionObject)

	task.result.Success = int64(1) - task.result.Failure
	return task.tearUp()
}

// singleInsertSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleUpsertSubDocuments(task *SingleSubDocUpsert, collectionObject *sdk.CollectionObject) {

	var iOps []gocb.MutateInSpec
	key := task.SingleSubDocOperationConfig.Key
	documentMetaData := task.req.documentsMeta.GetDocumentsMetadata(key, "", 0, false)

	for _, path := range task.SingleSubDocOperationConfig.Paths {
		subDocument := documentMetaData.SubDocument(path, task.SingleSubDocOperationConfig.Template,
			task.SingleSubDocOperationConfig.DocSize, false)

		fake := faker.NewWithSeed(rand.NewSource(int64(subDocument.Seed)))

		value := subDocument.GenerateValue(&fake)

		value = subDocument.RetracePreviousMutations(value, &fake)

		value = subDocument.UpdateValue(value, &fake)

		iOps = append(iOps, gocb.UpsertSpec(path, value, &gocb.UpsertSpecOptions{
			CreatePath: task.InsertSpecOptions.CreatePath,
			IsXattr:    task.InsertSpecOptions.IsXattr,
		}))
	}

	if !task.InsertSpecOptions.IsXattr {
		iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
			int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
				CreatePath: true,
				IsXattr:    false,
			}))
	}

	result, err := collectionObject.Collection.MutateIn(key, iOps, &gocb.MutateInOptions{
		Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
		Cas:             gocb.Cas(task.MutateInOptions.Cas),
		PersistTo:       task.MutateInOptions.PersistTo,
		ReplicateTo:     task.MutateInOptions.ReplicateTo,
		DurabilityLevel: getDurability(task.MutateInOptions.Durability),
		StoreSemantic:   getStoreSemantic(task.MutateInOptions.StoreSemantic),
		Timeout:         time.Duration(task.MutateInOptions.Timeout) * time.Second,
		PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
	})

	if err != nil {
		for _, path := range task.SingleSubDocOperationConfig.Paths {

			subDocument := documentMetaData.SubDocument(path, task.SingleSubDocOperationConfig.Template,
				task.SingleSubDocOperationConfig.DocSize, false)

			subDocument.DecrementCount()
		}
		task.result.CreateSingleErrorResult(key, err.Error(), false, 0)
	} else {
		documentMetaData.IncrementMutationCount()
		task.result.CreateSingleErrorResult(key, "", true, uint64(result.Cas()))
	}

	task.PostTaskExceptionHandling(collectionObject)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleSubDocUpsert) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
}

func (task *SingleSubDocUpsert) GetResultSeed() string {
	if task.result == nil {
		return ""
	}
	return fmt.Sprintf("%d", task.result.ResultSeed)
}

func (task *SingleSubDocUpsert) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleSubDocUpsert) SetException(exceptions Exceptions) {
}
