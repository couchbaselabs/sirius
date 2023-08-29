package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"time"
)

type SingleInsertTask struct {
	IdentifierToken       string                  `json:"identifierToken" doc:"true"`
	ClusterConfig         *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket                string                  `json:"bucket" doc:"true"`
	Scope                 string                  `json:"scope,omitempty" doc:"true"`
	Collection            string                  `json:"collection,omitempty" doc:"true"`
	InsertOptions         *InsertOptions          `json:"insertOptions,omitempty" doc:"true"`
	SingleOperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation             string                  `json:"operation" doc:"false"`
	ResultSeed            int64                   `json:"resultSeed" doc:"false"`
	TaskPending           bool                    `json:"taskPending" doc:"false"`
	result                *task_result.TaskResult `json:"result" doc:"false"`
	req                   *Request                `json:"-" doc:"false"`
}

func (task *SingleInsertTask) Describe() string {
	return "Single insert task create key value in Couchbase.\n"
}

func (task *SingleInsertTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleInsertTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleInsertTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleInsertTask) Config(req *Request, reRun bool) (int64, error) {
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
		task.Operation = SingleInsertOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configInsertOptions(task.InsertOptions); err != nil {
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

func (task *SingleInsertTask) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleInsertTask) Do() error {

	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		task.result.FailWholeSingleOperation(task.SingleOperationConfig.Keys, err1)
		return task.tearUp()
	}

	singleInsertDocuments(task, collectionObject)

	task.result.Success = int64(len(task.SingleOperationConfig.Keys)) - task.result.Failure
	return task.tearUp()
}

// singleInsertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleInsertDocuments(task *SingleInsertTask, collectionObject *sdk.CollectionObject) {

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan string, MaxConcurrentRoutines)

	group := errgroup.Group{}

	for _, data := range task.SingleOperationConfig.Keys {
		routineLimiter <- struct{}{}
		dataChannel <- data

		group.Go(func() error {
			key := <-dataChannel

			documentMetaData := task.req.documentsMeta.GetDocumentsMetadata(task.CollectionIdentifier(), key,
				task.SingleOperationConfig.Template,
				task.SingleOperationConfig.DocSize, false)

			fake := faker.NewWithSeed(rand.NewSource(int64(documentMetaData.Seed)))

			t := template.InitialiseTemplate(documentMetaData.Template)

			doc, _ := t.GenerateDocument(&fake, documentMetaData.DocSize)

			m, err := collectionObject.Collection.Insert(key, doc, &gocb.InsertOptions{
				DurabilityLevel: getDurability(task.InsertOptions.Durability),
				PersistTo:       task.InsertOptions.PersistTo,
				ReplicateTo:     task.InsertOptions.ReplicateTo,
				Timeout:         time.Duration(task.InsertOptions.Timeout) * time.Second,
				Expiry:          time.Duration(task.InsertOptions.Expiry) * time.Second,
			})

			if err != nil {
				task.result.CreateSingleErrorResult(key, err.Error(), false, 0)
				<-routineLimiter
				return err

			}

			task.result.CreateSingleErrorResult(key, "", true, uint64(m.Cas()))
			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleInsertTask) PostTaskExceptionHandling(_ *sdk.CollectionObject) {
}

func (task *SingleInsertTask) GetResultSeed() string {
	if task.result == nil {
		task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	}
	return fmt.Sprintf("%d", task.ResultSeed)
}

func (task *SingleInsertTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleInsertTask) SetException(exceptions Exceptions) {

}
