package key_based_loading_cb

import (
	"encoding/json"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"strings"
	"time"
)

type SingleValidate struct {
	IdentifierToken       string                  `json:"identifierToken" doc:"true"`
	ClusterConfig         *cb_sdk.ClusterConfig   `json:"clusterConfig" doc:"true"`
	Bucket                string                  `json:"bucket" doc:"true"`
	Scope                 string                  `json:"scope,omitempty" doc:"true"`
	Collection            string                  `json:"collection,omitempty" doc:"true"`
	SingleOperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation             string                  `json:"operation" doc:"false"`
	ResultSeed            int64                   `json:"resultSeed" doc:"false"`
	TaskPending           bool                    `json:"taskPending" doc:"false"`
	Result                *task_result.TaskResult `json:"Result" doc:"false"`
	req                   *tasks.Request          `json:"-" doc:"false"`
}

func (task *SingleValidate) Describe() string {
	return "validate the document integrity by document ID"
}

func (task *SingleValidate) TearUp() error {
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}
	task.Result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleValidate) Config(req *tasks.Request, reRun bool) (int64, error) {
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
		task.Operation = tasks.SingleDocValidateOperation

		if task.Bucket == "" {
			task.Bucket = cb_sdk.DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = cb_sdk.DefaultScope
		}
		if task.Collection == "" {
			task.Collection = cb_sdk.DefaultCollection
		}

		if err := ConfigSingleOperationConfig(task.SingleOperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}

	} else {
		log.Println("retrying :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleValidate) Do() error {
	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeSingleOperation(task.SingleOperationConfig.Keys, err1)
		return task.TearUp()
	}

	validateSingleDocuments(task, collectionObject)

	task.Result.Success = int64(len(task.SingleOperationConfig.Keys)) - task.Result.Failure
	return task.TearUp()
}

// validateSingleDocuments validates the document integrity as per meta-data stored in Sirius
func validateSingleDocuments(task *SingleValidate, collectionObject *cb_sdk.CollectionObject) {

	if task.req.ContextClosed() {
		return
	}

	routineLimiter := make(chan struct{}, tasks.MaxConcurrentRoutines)
	dataChannel := make(chan string, tasks.MaxConcurrentRoutines)

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

			initTime := time.Now().UTC().Format(time.RFC850)

			documentMetaData := task.req.DocumentsMeta.GetDocumentsMetadata(task.CollectionIdentifier(), key, task.SingleOperationConfig.Template,
				task.SingleOperationConfig.DocSize, false)

			fake := faker.NewWithSeed(rand.NewSource(int64(documentMetaData.Seed)))

			t := template.InitialiseTemplate(documentMetaData.Template)

			doc, err := t.GenerateDocument(&fake, documentMetaData.DocSize)
			if err != nil {
				task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}
			doc = documentMetaData.RetracePreviousMutations(t, doc, task.SingleOperationConfig.DocSize, &fake)

			docBytes, err := json.Marshal(&doc)
			if err != nil {
				task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}

			var docMap map[string]any
			if err := json.Unmarshal(docBytes, &docMap); err != nil {
				task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}

			subDocumentMap := make(map[string]any)
			xAttrFromHostMap := make(map[string]any)

			for path, subDocument := range documentMetaData.SubDocMutations {

				fakeSub := faker.NewWithSeed(rand.NewSource(int64(subDocument.Seed)))

				value := subDocument.GenerateValue(&fakeSub)

				value = subDocument.RetracePreviousMutations(value, &fakeSub)

				subDocumentMap[path] = value

				if subDocument.IsXattr() {
					result, err := collectionObject.Collection.LookupIn(key, []gocb.LookupInSpec{
						gocb.GetSpec(path, &gocb.GetSpecOptions{IsXattr: true}),
					}, nil)
					if err != nil {
						task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
						<-routineLimiter
						return err
					}
					var tempResult string
					if err = result.ContentAt(0, &tempResult); err != nil {
						task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
						<-routineLimiter
						return err
					}
					xAttrFromHostMap[path] = tempResult
				}
			}

			docMap[template.MutatedPath] = documentMetaData.SubDocMutationCount()

			initTime = time.Now().UTC().Format(time.RFC850)
			result, err := collectionObject.Collection.Get(key, nil)
			if err != nil {
				task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}

			var resultFromHostMap map[string]any
			if err = result.Content(&resultFromHostMap); err != nil {
				task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
				<-routineLimiter
				return err
			}

			for k, v := range xAttrFromHostMap {
				resultFromHostMap[k] = v
			}

			if !tasks.CompareDocumentsIsSame(resultFromHostMap, docMap, subDocumentMap) {
				task.Result.CreateSingleErrorResult(initTime, key, "integrity lost", false, 0)
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
	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
}

func (task *SingleValidate) CollectionIdentifier() string {
	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
		task.Collection}, ":")
}

func (task *SingleValidate) CheckIfPending() bool {
	return task.TaskPending
}
