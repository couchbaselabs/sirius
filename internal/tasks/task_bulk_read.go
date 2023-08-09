package tasks

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
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

type ReadTask struct {
	IdentifierToken string                             `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig                 `json:"clusterConfig" doc:"true"`
	Bucket          string                             `json:"bucket" doc:"true"`
	Scope           string                             `json:"scope,omitempty" doc:"true"`
	Collection      string                             `json:"collection,omitempty" doc:"true"`
	OperationConfig *OperationConfig                   `json:"operationConfig,omitempty" doc:"true"`
	Operation       string                             `json:"operation" doc:"false"`
	ResultSeed      int64                              `json:"resultSeed" doc:"false"`
	TaskPending     bool                               `json:"taskPending" doc:"false"`
	State           *task_state.TaskState              `json:"State" doc:"false"`
	MetaData        *task_meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	result          *task_result.TaskResult            `json:"result" doc:"false"`
	gen             *docgenerator.Generator            `json:"-" doc:"false"`
	req             *Request                           `json:"-" doc:"false"`
}

func (task *ReadTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *ReadTask) Describe() string {
	return "Read Task get documents from bucket and validate them with the expected ones"
}

func (task *ReadTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *ReadTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *ReadTask) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed, task.Operation)
	}
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *ReadTask) Config(req *Request, reRun bool) (int64, error) {
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
		task.Operation = ReadOperation

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
			return task.ResultSeed, fmt.Errorf("task State is nil")
		}

		task.State.SetupStoringKeys()

		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *ReadTask) Do() error {

	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	task.gen = docgenerator.ConfigGenerator(task.MetaData.DocType, task.MetaData.KeyPrefix,
		task.MetaData.KeySuffix, task.State.SeedStart, task.State.SeedEnd,
		template.InitialiseTemplate(task.MetaData.TemplateName))

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		task.result.FailWholeBulkOperation(task.OperationConfig.Start, task.OperationConfig.End,
			task.MetaData.DocSize, task.gen, err1, task.State)
		return task.tearUp()
	}

	getDocuments(task, collectionObject)
	task.result.Success = task.OperationConfig.End - task.OperationConfig.Start - task.result.Failure

	return task.tearUp()
}

// getDocuments reads the documents in the bucket
func getDocuments(task *ReadTask, collectionObject *sdk.CollectionObject) {
	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	insertErrorOffset, err2 := task.req.retracePreviousFailedInsertions(task.CollectionIdentifier(), task.ResultSeed)
	if err2 != nil {
		return
	}

	group := errgroup.Group{}
	for i := task.OperationConfig.Start; i < task.OperationConfig.End; i++ {
		routineLimiter <- struct{}{}
		dataChannel <- i
		group.Go(func() error {
			offset := <-dataChannel
			key := task.State.SeedStart + offset
			docId := task.gen.BuildKey(key)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}

			if _, ok := insertErrorOffset[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("error in insertion of docID on " + docId)
			}

			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			originalDocument, err := task.gen.Template.GenerateDocument(&fake, task.MetaData.DocSize)
			if err != nil {
				task.result.IncrementFailure(docId, originalDocument, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			originalDocument, err = task.req.retracePreviousMutations(task.CollectionIdentifier(), offset,
				originalDocument, *task.gen,
				&fake,
				task.ResultSeed)
			if err != nil {
				task.result.IncrementFailure(docId, originalDocument, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}

			var resultFromHost map[string]any
			documentFromHost := template.InitialiseTemplate(task.MetaData.TemplateName)

			result := &gocb.GetResult{}

			for retry := 0; retry <= int(math.Max(float64(1), float64(task.OperationConfig.Exceptions.
				RetryAttempts))); retry++ {
				result, err = collectionObject.Collection.Get(docId, nil)
				if err == nil {
					break
				}
			}

			if err != nil {
				task.result.IncrementFailure(docId, originalDocument, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			if err := result.Content(&resultFromHost); err != nil {
				task.result.IncrementFailure(docId, originalDocument, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			resultBytes, err := json.Marshal(resultFromHost)
			err = json.Unmarshal(resultBytes, &documentFromHost)
			if err != nil {
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				task.result.IncrementFailure(docId, originalDocument, err, false, 0, offset)
				<-routineLimiter
				return err
			}

			ok, err := task.gen.Template.Compare(documentFromHost, originalDocument)
			if err != nil || !ok {
				task.result.IncrementFailure(docId, documentFromHost, errors.New("integrity lost"), false, 0, offset)
				<-routineLimiter
				return err
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

func (task *ReadTask) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
	task.State.StopStoringState()

	// Get all the errorOffset
	errorOffsetMaps := task.State.ReturnErrOffset()
	// Get all the completed offset
	completedOffsetMaps := task.State.ReturnCompletedOffset()

	// For the offset in ignore exceptions :-> move them from error to completed
	shiftErrToCompletedOnIgnore(task.OperationConfig.Exceptions.IgnoreExceptions, task.result, errorOffsetMaps, completedOffsetMaps)

	if task.OperationConfig.Exceptions.RetryAttempts > 0 {

		exceptionList := getExceptions(task.result, task.OperationConfig.Exceptions.RetryExceptions)

		// For the retry exceptions :-> move them on success after retrying from err to completed
		for _, exception := range exceptionList {

			errorOffsetListMap := make([]map[int64]RetriedResult, 0)
			for _, failedDocs := range task.result.BulkError[exception] {
				m := make(map[int64]RetriedResult)
				m[failedDocs.Offset] = RetriedResult{}
				errorOffsetListMap = append(errorOffsetListMap, m)
			}

			routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
			dataChannel := make(chan map[int64]RetriedResult, MaxConcurrentRoutines)
			wg := errgroup.Group{}
			for _, x := range errorOffsetListMap {
				dataChannel <- x
				routineLimiter <- struct{}{}
				wg.Go(func() error {
					m := <-dataChannel
					var offset = int64(-1)
					for k, _ := range m {
						offset = k
					}
					key := task.State.SeedStart + offset
					docId := task.gen.BuildKey(key)
					fake := faker.NewWithSeed(rand.NewSource(int64(key)))

					originalDoc, err := task.gen.Template.GenerateDocument(&fake, task.MetaData.DocSize)
					if err != nil {
						<-routineLimiter
						return err
					}
					originalDoc, err = task.req.retracePreviousMutations(task.CollectionIdentifier(), offset, originalDoc, *task.gen, &fake,
						task.ResultSeed)
					if err != nil {
						task.result.IncrementFailure(docId, originalDoc, err, false, 0, offset)
						<-routineLimiter
						return err
					}

					retry := 0
					result := &gocb.GetResult{}
					for retry = 0; retry <= task.OperationConfig.Exceptions.RetryAttempts; retry++ {
						result, err = collectionObject.Collection.Get(docId, nil)

						if err == nil {
							break
						}
					}

					if err == nil {
						var resultFromHost map[string]any
						documentFromHost := template.InitialiseTemplate(task.MetaData.TemplateName)
						if err := result.Content(&resultFromHost); err != nil {
							<-routineLimiter
							return err
						}
						resultBytes, err := json.Marshal(resultFromHost)
						err = json.Unmarshal(resultBytes, &documentFromHost)
						if err != nil {
							<-routineLimiter
							return err
						}

						ok, err := task.gen.Template.Compare(documentFromHost, originalDoc)
						if err != nil || !ok {
							<-routineLimiter
							return err
						} else {
							m[offset] = RetriedResult{
								Status: true,
								CAS:    uint64(result.Cas()),
							}
						}
					}

					<-routineLimiter
					return nil
				})
			}
			_ = wg.Wait()

			shiftErrToCompletedOnRetrying(exception, task.result, errorOffsetListMap, errorOffsetMaps, completedOffsetMaps)
		}
	}

	task.State.MakeCompleteKeyFromMap(completedOffsetMaps)
	task.State.MakeErrorKeyFromMap(errorOffsetMaps)
	task.result.Failure = int64(len(task.State.KeyStates.Err))
}

func (task *ReadTask) GetResultSeed() string {
	return fmt.Sprintf("%d", task.result.ResultSeed)
}

func (task *ReadTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *ReadTask) SetException(exceptions Exceptions) {
	task.OperationConfig.Exceptions = exceptions
}
