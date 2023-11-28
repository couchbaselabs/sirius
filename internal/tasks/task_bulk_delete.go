package tasks

import (
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
	"golang.org/x/sync/errgroup"
	"log"
	"math"
	"time"
)

type DeleteTask struct {
	IdentifierToken string                             `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig                 `json:"clusterConfig" doc:"true"`
	Bucket          string                             `json:"bucket" doc:"true"`
	Scope           string                             `json:"scope,omitempty" doc:"true"`
	Collection      string                             `json:"collection,omitempty" doc:"true"`
	RemoveOptions   *RemoveOptions                     `json:"removeOptions,omitempty" doc:"true"`
	OperationConfig *OperationConfig                   `json:"operationConfig,omitempty" doc:"true"`
	Operation       string                             `json:"operation" doc:"false"`
	ResultSeed      int64                              `json:"resultSeed" doc:"false"`
	TaskPending     bool                               `json:"taskPending" doc:"false"`
	State           *task_state.TaskState              `json:"State" doc:"false"`
	MetaData        *task_meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	Result          *task_result.TaskResult            `json:"-" doc:"false"`
	gen             *docgenerator.Generator            `json:"-" doc:"false"`
	req             *Request                           `json:"-" doc:"false"`
	rerun           bool                               `json:"-" doc:"false"`
}

func (task *DeleteTask) Describe() string {
	return `Delete task deletes documents in bulk into a bucket.
The task will delete documents from [start,end] inclusive.`
}

func (task *DeleteTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *DeleteTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *DeleteTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config checks the validity of DeleteTask
func (task *DeleteTask) Config(req *Request, reRun bool) (int64, error) {
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

	task.rerun = reRun

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = DeleteOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configRemoveOptions(task.RemoveOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configureOperationConfig(task.OperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
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
		_ = DeleteResultFile(task.ResultSeed)
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *DeleteTask) tearUp() error {
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.Result = nil
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *DeleteTask) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	task.gen = docgenerator.ConfigGenerator(task.MetaData.DocType, task.MetaData.KeyPrefix,
		task.MetaData.KeySuffix, task.State.SeedStart, task.State.SeedEnd,
		template.InitialiseTemplate(task.MetaData.TemplateName))

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeBulkOperation(task.OperationConfig.Start, task.OperationConfig.End,
			task.MetaData.DocSize, task.gen, err1, task.State)
		return task.tearUp()
	}

	deleteDocuments(task, collectionObject)
	task.Result.Success = task.OperationConfig.End - task.OperationConfig.Start - task.Result.Failure

	return task.tearUp()
}

// deleteDocuments delete the document stored on a host from start to end.
func deleteDocuments(task *DeleteTask, collectionObject *sdk.CollectionObject) {

	if task.req.ContextClosed() {
		return
	}

	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	group := errgroup.Group{}

	for i := task.OperationConfig.Start; i < task.OperationConfig.End; i++ {

		if task.req.ContextClosed() {
			close(routineLimiter)
			close(dataChannel)
			return
		}

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

			var err error
			initTime := time.Now().UTC().Format(time.RFC850)
			for retry := 0; retry < int(math.Max(float64(1), float64(task.OperationConfig.Exceptions.
				RetryAttempts))); retry++ {
				initTime = time.Now().UTC().Format(time.RFC850)
				_, err = collectionObject.Collection.Remove(docId, &gocb.RemoveOptions{
					Cas:             gocb.Cas(task.RemoveOptions.Cas),
					PersistTo:       task.RemoveOptions.PersistTo,
					ReplicateTo:     task.RemoveOptions.ReplicateTo,
					DurabilityLevel: getDurability(task.RemoveOptions.Durability),
					Timeout:         time.Duration(task.RemoveOptions.Timeout) * time.Second,
				})
				if err == nil {
					break
				}
			}
			if err != nil {
				if errors.Is(err, gocb.ErrDocumentNotFound) && task.rerun {
					task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
					<-routineLimiter
					return nil
				} else {
					task.Result.IncrementFailure(initTime, docId, nil, err, false, uint64(0), offset)
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

func (task *DeleteTask) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
	task.Result.StopStoringResult()
	task.State.StopStoringState()
	if task.OperationConfig.Exceptions.RetryAttempts <= 0 {
		return
	}

	// Get all the errorOffset
	errorOffsetMaps := task.State.ReturnErrOffset()
	// Get all the completed offset
	completedOffsetMaps := task.State.ReturnCompletedOffset()

	// For the offset in ignore exceptions :-> move them from error to completed
	shiftErrToCompletedOnIgnore(task.OperationConfig.Exceptions.IgnoreExceptions, task.Result, errorOffsetMaps, completedOffsetMaps)

	if task.OperationConfig.Exceptions.RetryAttempts > 0 {

		exceptionList := getExceptions(task.Result, task.OperationConfig.Exceptions.RetryExceptions)

		// For the retry exceptions :-> move them on success after retrying from err to completed
		for _, exception := range exceptionList {

			errorOffsetListMap := make([]map[int64]RetriedResult, 0)
			for _, failedDocs := range task.Result.BulkError[exception] {
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

					retry := 0
					var err error
					result := &gocb.MutationResult{}

					initTime := time.Now().UTC().Format(time.RFC850)
					for retry = 0; retry <= task.OperationConfig.Exceptions.RetryAttempts; retry++ {
						result, err = collectionObject.Collection.Remove(docId, &gocb.RemoveOptions{
							Cas:             gocb.Cas(task.RemoveOptions.Cas),
							PersistTo:       task.RemoveOptions.PersistTo,
							ReplicateTo:     task.RemoveOptions.ReplicateTo,
							DurabilityLevel: getDurability(task.RemoveOptions.Durability),
							Timeout:         time.Duration(task.RemoveOptions.Timeout) * time.Second,
						})

						if err == nil {
							break
						}
					}

					if err != nil {
						if errors.Is(err, gocb.ErrDocumentNotFound) {
							m[offset] = RetriedResult{
								Status:   true,
								CAS:      0,
								InitTime: initTime,
								AckTime:  time.Now().UTC().Format(time.RFC850),
							}
						} else {
							m[offset] = RetriedResult{
								InitTime: initTime,
								AckTime:  time.Now().UTC().Format(time.RFC850),
							}
						}
					} else {
						m[offset] = RetriedResult{
							Status:   true,
							CAS:      uint64(result.Cas()),
							InitTime: initTime,
							AckTime:  time.Now().UTC().Format(time.RFC850),
						}
					}

					<-routineLimiter
					return nil
				})
			}
			_ = wg.Wait()

			shiftErrToCompletedOnRetrying(exception, task.Result, errorOffsetListMap, errorOffsetMaps, completedOffsetMaps)
		}
	}

	task.State.MakeCompleteKeyFromMap(completedOffsetMaps)
	task.State.MakeErrorKeyFromMap(errorOffsetMaps)
	task.Result.Failure = int64(len(task.State.KeyStates.Err))
	task.Result.Success = task.OperationConfig.End - task.OperationConfig.Start - task.Result.Failure
	log.Println("completed retrying:- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)

}

func (task *DeleteTask) MatchResultSeed(resultSeed string) bool {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true
	}
	return false
}

func (task *DeleteTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *DeleteTask) SetException(exceptions Exceptions) {
	task.OperationConfig.Exceptions = exceptions
}
