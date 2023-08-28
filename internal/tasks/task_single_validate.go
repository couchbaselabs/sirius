package tasks

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"time"
)

type SingleValidate struct {
	IdentifierToken       string                  `json:"identifierToken" doc:"true"`
	ClusterConfig         *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket                string                  `json:"bucket" doc:"true"`
	Scope                 string                  `json:"scope,omitempty" doc:"true"`
	Collection            string                  `json:"collection,omitempty" doc:"true"`
	SingleOperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation             string                  `json:"operation" doc:"false"`
	ResultSeed            int64                   `json:"resultSeed" doc:"false"`
	TaskPending           bool                    `json:"taskPending" doc:"false"`
	result                *task_result.TaskResult `json:"result" doc:"false"`
	req                   *Request                `json:"-" doc:"false"`
}

func (task *SingleValidate) Describe() string {
	return "validate the document integrity by document ID"
}

func (task *SingleValidate) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleValidate) Config(req *Request, reRun bool) (int64, error) {
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
		task.Operation = SingleDocValidateOperation

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

func (task *SingleValidate) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleValidate) Do() error {
	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		task.result.FailWholeSingleOperation(task.SingleOperationConfig.Keys, err1)
		return task.tearUp()
	}

	validateSingleDocuments(task, collectionObject)

	task.result.Success = int64(len(task.SingleOperationConfig.Keys)) - task.result.Failure
	return task.tearUp()
}

// validateSingleDocuments validates the document integrity as per meta-data stored in Sirius
func validateSingleDocuments(task *SingleValidate, collectionObject *sdk.CollectionObject) {
	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan string, MaxConcurrentRoutines)

	group := errgroup.Group{}

	for _, data := range task.SingleOperationConfig.Keys {
		routineLimiter <- struct{}{}
		dataChannel <- data

		group.Go(func() error {
			key := <-dataChannel

			documentMetaData := task.req.documentsMeta.GetDocumentsMetadata(key, task.SingleOperationConfig.Template,
				task.SingleOperationConfig.DocSize, false)

			fake := faker.NewWithSeed(rand.NewSource(int64(documentMetaData.Seed)))

			t := template.InitialiseTemplate(documentMetaData.Template)

			doc, err := t.GenerateDocument(&fake, documentMetaData.DocSize)
			if err != nil {
				task.result.CreateSingleErrorResult(key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}
			doc = documentMetaData.RetracePreviousMutations(t, doc, &fake)

			docBytes, err := json.Marshal(&doc)
			if err != nil {
				task.result.CreateSingleErrorResult(key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}

			var docMap map[string]any
			if err := json.Unmarshal(docBytes, &docMap); err != nil {
				task.result.CreateSingleErrorResult(key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}

			subDocumentMap := make(map[string]any)

			for path, subDocument := range documentMetaData.SubDocMutations {

				fakeSub := faker.NewWithSeed(rand.NewSource(int64(subDocument.Seed)))

				value := subDocument.GenerateValue(&fakeSub)

				value = subDocument.RetracePreviousMutations(value, &fakeSub)

				subDocumentMap[path] = value
			}

			docMap[template.MutatedPath] = documentMetaData.SubDocMutationCount()

			result, err := collectionObject.Collection.Get(key, nil)
			if err != nil {
				task.result.CreateSingleErrorResult(key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}
			var resultFromHostMap map[string]any
			if err = result.Content(&resultFromHostMap); err != nil {
				task.result.CreateSingleErrorResult(key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}

			if !compareDocumentsIsSame(resultFromHostMap, docMap, subDocumentMap) {
				task.result.CreateSingleErrorResult(key, "integrity lost", false, 0)
				<-routineLimiter
				return err
			}

			task.result.CreateSingleErrorResult(key, "", true, uint64(result.Cas()))
			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleValidate) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleValidate) CheckIfPending() bool {
	return task.TaskPending
}

func (task *SingleValidate) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
}

func (task *SingleValidate) GetResultSeed() string {
	if task.result == nil {
		task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	}
	return fmt.Sprintf("%d", task.ResultSeed)
}

func (task *SingleValidate) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleValidate) SetException(exceptions Exceptions) {

}
