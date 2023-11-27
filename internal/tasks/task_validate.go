package tasks

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_meta_data"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math"
	"math/rand"
	"time"
)

type ValidateTask struct {
	IdentifierToken string                             `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig                 `json:"clusterConfig" doc:"true"`
	Bucket          string                             `json:"bucket" doc:"true"`
	Scope           string                             `json:"scope,omitempty" doc:"true"`
	Collection      string                             `json:"collection,omitempty" doc:"true"`
	OperationConfig *OperationConfig                   `json:"operationConfig,omitempty" doc:"true"`
	Operation       string                             `json:"operation" doc:"false"`
	ResultSeed      int64                              `json:"resultSeed" doc:"false"`
	TaskPending     bool                               `json:"taskPending" doc:"false"`
	MetaData        *task_meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	State           *task_state.TaskState              `json:"State" doc:"false"`
	Result          *task_result.TaskResult            `json:"-" doc:"false"`
	gen             *docgenerator.Generator            `json:"-" doc:"false"`
	req             *Request                           `json:"-" doc:"false"`
	rerun           bool                               `json:"-" doc:"false"`
}

func (task *ValidateTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *ValidateTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *ValidateTask) Describe() string {
	return "Validates every document in the cluster's bucket"
}

func (task *ValidateTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *ValidateTask) tearUp() error {
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.Result.StopStoringResult()
	task.Result = nil
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *ValidateTask) Config(req *Request, reRun bool) (int64, error) {
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

	task.rerun = false

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = ValidateOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configureOperationConfig(task.OperationConfig); err != nil {
			task.TaskPending = false
			return 0, fmt.Errorf(err.Error())
		}

		task.MetaData = task.req.MetaData.GetCollectionMetadata(task.CollectionIdentifier(),
			task.OperationConfig.KeySize, task.OperationConfig.DocSize, task.OperationConfig.DocType, task.OperationConfig.KeyPrefix,
			task.OperationConfig.KeySuffix, task.OperationConfig.TemplateName)

		task.req.lock.Lock()
		task.State = task_state.ConfigTaskState(task.MetaData.Seed, task.MetaData.SeedEnd, task.ResultSeed)
		task.req.lock.Unlock()

	} else {
		if task.State == nil {
			return task.ResultSeed, task_errors.ErrTaskStateIsNil
		}
		task.State.SetupStoringKeys()
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *ValidateTask) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	task.gen = docgenerator.ConfigGenerator(task.MetaData.DocType, task.MetaData.KeyPrefix,
		task.MetaData.KeySuffix, task.State.SeedStart, task.State.SeedEnd,
		template.InitialiseTemplate(task.MetaData.TemplateName))

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeBulkOperation(0, task.MetaData.Seed-task.MetaData.SeedEnd,
			task.MetaData.DocSize, task.gen, err1, task.State)
		return task.tearUp()
	}

	validateDocuments(task, collectionObject)

	task.Result.Success = task.State.SeedEnd - task.State.SeedStart - task.Result.Failure

	return task.tearUp()
}

// ValidateDocuments return the validity of the collection using TaskState
func validateDocuments(task *ValidateTask, collectionObject *sdk.CollectionObject) {

	if task.req.ContextClosed() {
		return
	}

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}
	deletedOffset, err1 := task.req.retracePreviousDeletions(task.CollectionIdentifier(), task.ResultSeed)
	if err1 != nil {
		log.Println(err1)
		return
	}

	deletedOffsetSubDoc, err2 := task.req.retracePreviousSubDocDeletions(task.CollectionIdentifier(), task.ResultSeed)
	if err2 != nil {
		log.Println(err2)
		return
	}

	group := errgroup.Group{}
	for offset := int64(0); offset < (task.MetaData.SeedEnd - task.MetaData.Seed); offset++ {

		if task.req.ContextClosed() {
			close(routineLimiter)
			close(dataChannel)
			return
		}

		routineLimiter <- struct{}{}
		dataChannel <- offset
		group.Go(func() error {
			offset := <-dataChannel
			key := task.State.SeedStart + offset
			docId := task.gen.BuildKey(key)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}

			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			fakeSub := faker.NewWithSeed(rand.NewSource(int64(key)))
			initTime := time.Now().UTC().Format(time.RFC850)
			originalDocument, err := task.gen.Template.GenerateDocument(&fake, task.MetaData.DocSize)
			if err != nil {
				task.Result.IncrementFailure(initTime, docId, originalDocument, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			updatedDocument, err := task.req.retracePreviousMutations(task.CollectionIdentifier(), offset,
				originalDocument, *task.gen,
				&fake,
				task.ResultSeed)
			if err != nil {
				task.Result.IncrementFailure(initTime, docId, updatedDocument, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}

			task.gen.Template.GenerateSubPathAndValue(&fakeSub)
			subDocumentMap := task.req.retracePreviousSubDocMutations(task.CollectionIdentifier(), offset, *task.gen, &fakeSub,
				task.ResultSeed)

			mutationCount := task.req.countMutation(task.CollectionIdentifier(), offset, task.ResultSeed)

			updatedDocumentBytes, err := json.Marshal(updatedDocument)
			if err != nil {
				log.Println(err)
				<-routineLimiter
				return err
			}

			var updatedDocumentMap map[string]any
			if err := json.Unmarshal(updatedDocumentBytes, &updatedDocumentMap); err != nil {
				log.Println(err)
				<-routineLimiter
				return err
			}
			updatedDocumentMap[template.MutatedPath] = float64(mutationCount)

			result := &gocb.GetResult{}
			var resultFromHost map[string]any
			resultFromHostTemplate := template.InitialiseTemplate(task.MetaData.TemplateName)

			initTime = time.Now().UTC().Format(time.RFC850)
			for retry := 0; retry < int(math.Max(float64(1), float64(task.OperationConfig.Exceptions.
				RetryAttempts))); retry++ {
				result, err = collectionObject.Collection.Get(docId, nil)
				if err == nil {
					break
				}
			}

			if err != nil {
				if errors.Is(err, gocb.ErrDocumentNotFound) {
					if _, ok := deletedOffset[offset]; ok {
						task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
						<-routineLimiter
						return nil
					}
					if _, ok := deletedOffsetSubDoc[offset]; ok {
						task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
						<-routineLimiter
						return nil
					}
				}
				task.Result.IncrementFailure(initTime, docId, updatedDocument, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}

			if err := result.Content(&resultFromHost); err != nil {
				task.Result.IncrementFailure(initTime, docId, updatedDocument, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}

			resultFromHostBytes, _ := json.Marshal(resultFromHost)
			json.Unmarshal(resultFromHostBytes, &resultFromHostTemplate)

			if !compareDocumentsIsSame(resultFromHost, updatedDocumentMap, subDocumentMap) {
				ok, err := task.gen.Template.Compare(resultFromHostTemplate, updatedDocument)
				if err != nil || !ok {
					task.Result.IncrementFailure(initTime, docId, updatedDocument, errors.New("integrity Lost"),
						false, 0, offset)
					task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
					<-routineLimiter
					return err
				}
			}

			task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
			<-routineLimiter
			return nil
		})
	}
	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	task.PostTaskExceptionHandling(collectionObject)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)

}

func (task *ValidateTask) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
	task.Result.StopStoringResult()
	task.State.StopStoringState()
}

func (task *ValidateTask) MatchResultSeed(resultSeed string) bool {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true
	}
	return false
}

func (task *ValidateTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *ValidateTask) SetException(exceptions Exceptions) {
}
