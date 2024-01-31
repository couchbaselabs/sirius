package key_based_loading_cb

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"
	"log"
	"strings"
	"time"
)

type SingleSubDocDelete struct {
	IdentifierToken             string                             `json:"identifierToken" doc:"true"`
	ClusterConfig               *cb_sdk.ClusterConfig              `json:"clusterConfig" doc:"true"`
	Bucket                      string                             `json:"bucket" doc:"true"`
	Scope                       string                             `json:"scope,omitempty" doc:"true"`
	Collection                  string                             `json:"collection,omitempty" doc:"true"`
	SingleSubDocOperationConfig *tasks.SingleSubDocOperationConfig `json:"singleSubDocOperationConfig" doc:"true"`
	RemoveSpecOptions           *tasks.RemoveSpecOptions           `json:"removeSpecOptions" doc:"true"`
	MutateInOptions             *tasks.MutateInOptions             `json:"mutateInOptions" doc:"true"`
	Operation                   string                             `json:"operation" doc:"false"`
	ResultSeed                  int64                              `json:"resultSeed" doc:"false"`
	TaskPending                 bool                               `json:"taskPending" doc:"false"`
	Result                      *task_result.TaskResult            `json:"-" doc:"false"`
	req                         *tasks.Request                     `json:"-" doc:"false"`
}

func (task *SingleSubDocDelete) Describe() string {
	return "SingleSingleSubDocDelete inserts a Sub-Document as per user's input [No Random data]"
}

func (task *SingleSubDocDelete) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = tasks.DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleSubDocDelete) CollectionIdentifier() string {
	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
		task.Collection}, ":")
}

func (task *SingleSubDocDelete) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleSubDocDelete) Config(req *tasks.Request, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req

	if task.req == nil {
		task.TaskPending = false
		return 0, task_errors.ErrRequestIsNil
	}

	task.req.ReconnectionManager()
	if _, err := task.req.GetCluster(task.ClusterConfig); err != nil {
		task.TaskPending = false
		return 0, err
	}

	task.req.ReconfigureDocumentManager()

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = tasks.SingleSubDocDeleteOperation

		if task.Bucket == "" {
			task.Bucket = tasks.DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = tasks.DefaultScope
		}
		if task.Collection == "" {
			task.Collection = tasks.DefaultCollection
		}

		if err := tasks.ConfigSingleSubDocOperationConfig(task.SingleSubDocOperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := tasks.ConfigRemoveSpecOptions(task.RemoveSpecOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := tasks.ConfigMutateInOptions(task.MutateInOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}
	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleSubDocDelete) TearUp() error {
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.Result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleSubDocDelete) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeSingleOperation([]string{task.SingleSubDocOperationConfig.Key}, err1)
		return task.TearUp()
	}

	singleDeleteSubDocuments(task, collectionObject)

	task.Result.Success = 1 - task.Result.Failure
	return task.TearUp()
}

// singleInsertSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleDeleteSubDocuments(task *SingleSubDocDelete, collectionObject *cb_sdk.CollectionObject) {

	if task.req.ContextClosed() {
		return
	}

	var iOps []gocb.MutateInSpec
	key := task.SingleSubDocOperationConfig.Key
	documentMetaData := task.req.DocumentsMeta.GetDocumentsMetadata(task.CollectionIdentifier(), key, "", 0, false)

	for _, path := range task.SingleSubDocOperationConfig.Paths {
		_ = documentMetaData.SubDocument(path, task.RemoveSpecOptions.IsXattr, task.SingleSubDocOperationConfig.DocSize,
			false)

		documentMetaData.RemovePath(path)

		iOps = append(iOps, gocb.RemoveSpec(path, &gocb.RemoveSpecOptions{
			IsXattr: task.RemoveSpecOptions.IsXattr,
		}))
	}

	if !task.RemoveSpecOptions.IsXattr {
		iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
			int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
				CreatePath: true,
				IsXattr:    false,
			}))
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	result, err := collectionObject.Collection.MutateIn(key, iOps, &gocb.MutateInOptions{
		Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
		Cas:             gocb.Cas(task.MutateInOptions.Cas),
		PersistTo:       task.MutateInOptions.PersistTo,
		ReplicateTo:     task.MutateInOptions.ReplicateTo,
		DurabilityLevel: tasks.GetDurability(task.MutateInOptions.Durability),
		StoreSemantic:   tasks.GetStoreSemantic(task.MutateInOptions.StoreSemantic),
		Timeout:         time.Duration(task.MutateInOptions.Timeout) * time.Second,
		PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
	})

	if err != nil {
		task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
	} else {
		if !task.RemoveSpecOptions.IsXattr {
			documentMetaData.IncrementMutationCount()
		}
		task.Result.CreateSingleErrorResult(initTime, key, "", true, uint64(result.Cas()))
	}

	task.PostTaskExceptionHandling(collectionObject)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleSubDocDelete) PostTaskExceptionHandling(collectionObject *cb_sdk.CollectionObject) {
}

func (task *SingleSubDocDelete) MatchResultSeed(resultSeed string) (bool, error) {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.TaskPending {
			return true, task_errors.ErrTaskInPendingState
		}
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true, nil
	}
	return false, nil
}
