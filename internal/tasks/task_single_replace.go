package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"time"
)

type SingleReplaceTask struct {
	IdentifierToken       string                  `json:"identifierToken" doc:"true"`
	ClusterConfig         *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket                string                  `json:"bucket" doc:"true"`
	Scope                 string                  `json:"scope,omitempty" doc:"true"`
	Collection            string                  `json:"collection,omitempty" doc:"true"`
	ReplaceOptions        *ReplaceOptions         `json:"replaceOptions,omitempty" doc:"true"`
	SingleOperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation             string                  `json:"operation" doc:"false"`
	ResultSeed            int64                   `json:"resultSeed" doc:"false"`
	TaskPending           bool                    `json:"taskPending" doc:"false"`
	Result                *task_result.TaskResult `json:"-" doc:"false"`
	req                   *Request                `json:"-" doc:"false"`
}

func (task *SingleReplaceTask) Describe() string {
	return "Single replace task a document in the collection in Couchbase.\n"
}

func (task *SingleReplaceTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleReplaceTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleReplaceTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleReplaceTask) Config(req *Request, reRun bool) (int64, error) {
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
		task.Operation = SingleReplaceOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configReplaceOptions(task.ReplaceOptions); err != nil {
			task.TaskPending = false
			return 0, err
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

func (task *SingleReplaceTask) tearUp() error {
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}
	task.Result.StopStoringResult()
	task.Result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleReplaceTask) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeSingleOperation(task.SingleOperationConfig.Keys, err1)
		return task.tearUp()
	}

	singleReplaceDocuments(task, collectionObject)

	task.Result.Success = int64(len(task.SingleOperationConfig.Keys)) - task.Result.Failure
	return task.tearUp()
}

// singleReplaceDocuments uploads new documents in a bucket.scope.
// collection in a defined batch size at multiple iterations.
func singleReplaceDocuments(task *SingleReplaceTask, collectionObject *sdk.CollectionObject) {

	if task.req.ContextClosed() {
		return
	}

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan string, MaxConcurrentRoutines)

	group := errgroup.Group{}

	for _, data := range task.SingleOperationConfig.Keys {

		if task.req.ContextClosed() {
			close(routineLimiter)
			close(dataChannel)
			return
		}

		routineLimiter <- struct{}{}
		dataChannel <- data

		group.Go(func() error {
			key := <-dataChannel

			documentMetaData := task.req.DocumentsMeta.GetDocumentsMetadata(task.CollectionIdentifier(), key, task.SingleOperationConfig.Template,
				task.SingleOperationConfig.DocSize, true)

			fake := faker.NewWithSeed(rand.NewSource(int64(documentMetaData.Seed)))

			t := template.InitialiseTemplate(documentMetaData.Template)

			doc, _ := t.GenerateDocument(&fake, documentMetaData.DocSize)

			initTime := time.Now().UTC().Format(time.RFC850)
			result, err := collectionObject.Collection.Replace(key, doc, &gocb.ReplaceOptions{
				Expiry:          time.Duration(task.ReplaceOptions.Expiry) * time.Second,
				Cas:             gocb.Cas(task.ReplaceOptions.Cas),
				PersistTo:       task.ReplaceOptions.PersistTo,
				ReplicateTo:     task.ReplaceOptions.ReplicateTo,
				DurabilityLevel: getDurability(task.ReplaceOptions.Durability),
				Timeout:         time.Duration(task.ReplaceOptions.Timeout) * time.Second,
			})

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

func (task *SingleReplaceTask) PostTaskExceptionHandling(_ *sdk.CollectionObject) {
}

func (task *SingleReplaceTask) MatchResultSeed(resultSeed string) bool {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true
	}
	return false
}

func (task *SingleReplaceTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleReplaceTask) SetException(exceptions Exceptions) {
}
