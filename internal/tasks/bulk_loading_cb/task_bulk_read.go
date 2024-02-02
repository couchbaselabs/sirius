package bulk_loading_cb

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_meta_data"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"
	"golang.org/x/sync/errgroup"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

type ReadTask struct {
	IdentifierToken string                             `json:"identifierToken" doc:"true"`
	ClusterConfig   *cb_sdk.ClusterConfig              `json:"clusterConfig" doc:"true"`
	Bucket          string                             `json:"bucket" doc:"true"`
	Scope           string                             `json:"scope,omitempty" doc:"true"`
	Collection      string                             `json:"collection,omitempty" doc:"true"`
	OperationConfig *tasks.OperationConfig             `json:"operationConfig,omitempty" doc:"true"`
	Operation       string                             `json:"operation" doc:"false"`
	ResultSeed      int64                              `json:"resultSeed" doc:"false"`
	TaskPending     bool                               `json:"taskPending" doc:"false"`
	State           *task_state.TaskState              `json:"State" doc:"false"`
	MetaData        *task_meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	Result          *task_result.TaskResult            `json:"Result" doc:"false"`
	gen             *docgenerator.Generator            `json:"-" doc:"false"`
	req             *tasks.Request                     `json:"-" doc:"false"`
	rerun           bool                               `json:"-" doc:"false"`
	lock            sync.Mutex                         `json:"-" doc:"false"`
}

func (task *ReadTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = tasks.DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *ReadTask) Describe() string {
	return "Read BulkTask get documents from bucket and validate them with the expected ones"
}

func (task *ReadTask) CollectionIdentifier() string {
	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
		task.Collection}, ":")
}

func (task *ReadTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *ReadTask) TearUp() error {
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.Result = nil
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *ReadTask) Config(req *tasks.Request, reRun bool) (int64, error) {
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

	task.lock = sync.Mutex{}
	task.rerun = reRun

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = tasks.ReadOperation

		if task.Bucket == "" {
			task.Bucket = tasks.DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = tasks.DefaultScope
		}
		if task.Collection == "" {
			task.Collection = tasks.DefaultCollection
		}

		if err := tasks.ConfigureOperationConfig(task.OperationConfig); err != nil {
			task.TaskPending = false
			return 0, fmt.Errorf(err.Error())
		}

		task.MetaData = task.req.MetaData.GetCollectionMetadata(task.CollectionIdentifier())
		task.req.Lock()
		task.State = task_state.ConfigTaskState(task.MetaData.Seed, task.MetaData.SeedEnd, task.ResultSeed)
		task.req.Unlock()

	} else {
		if task.State == nil {
			return task.ResultSeed, task_errors.ErrTaskStateIsNil
		}

		task.State.SetupStoringKeys()
		_ = tasks.DeleteResultFile(task.ResultSeed)
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *ReadTask) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	task.gen = docgenerator.ConfigGenerator(
		task.OperationConfig.KeySize,
		task.OperationConfig.DocSize,
		task.OperationConfig.DocType,
		task.OperationConfig.KeyPrefix,
		task.OperationConfig.KeySuffix,
		template.InitialiseTemplate(task.OperationConfig.TemplateName))

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeBulkOperation(task.OperationConfig.Start, task.OperationConfig.End,
			err1, task.State, task.gen, task.MetaData.Seed)
		return task.TearUp()
	}

	getDocuments(task, collectionObject)
	task.Result.Success = task.OperationConfig.End - task.OperationConfig.Start - task.Result.Failure

	return task.TearUp()
}

// getDocuments reads the documents in the bucket
func getDocuments(task *ReadTask, collectionObject *cb_sdk.CollectionObject) {

	if task.req.ContextClosed() {
		return
	}

	routineLimiter := make(chan struct{}, tasks.MaxConcurrentRoutines)
	dataChannel := make(chan int64, tasks.MaxConcurrentRoutines)
	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}

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
			key := task.MetaData.Seed + offset
			docId := task.gen.BuildKey(key)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}

			initTime := time.Now().UTC().Format(time.RFC850)
			var err error
			for retry := 0; retry < int(math.Max(float64(1), float64(task.OperationConfig.Exceptions.
				RetryAttempts))); retry++ {
				initTime = time.Now().UTC().Format(time.RFC850)
				_, err = collectionObject.Collection.Get(docId, nil)
				if err == nil {
					break
				}
			}

			if err != nil {
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				task.Result.IncrementFailure(initTime, docId, err, false, 0, offset)
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

func (task *ReadTask) PostTaskExceptionHandling(collectionObject *cb_sdk.CollectionObject) {
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
	tasks.ShiftErrToCompletedOnIgnore(task.OperationConfig.Exceptions.IgnoreExceptions, task.Result, errorOffsetMaps,
		completedOffsetMaps)

	if task.OperationConfig.Exceptions.RetryAttempts > 0 {

		exceptionList := tasks.GetExceptions(task.Result, task.OperationConfig.Exceptions.RetryExceptions)

		// For the retry exceptions :-> move them on success after retrying from err to completed
		for _, exception := range exceptionList {

			errorOffsetListMap := make([]map[int64]tasks.RetriedResult, 0)
			for _, failedDocs := range task.Result.BulkError[exception] {
				m := make(map[int64]tasks.RetriedResult)
				m[failedDocs.Offset] = tasks.RetriedResult{}
				errorOffsetListMap = append(errorOffsetListMap, m)
			}

			routineLimiter := make(chan struct{}, tasks.MaxConcurrentRoutines)
			dataChannel := make(chan map[int64]tasks.RetriedResult, tasks.MaxConcurrentRoutines)
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
					key := task.MetaData.Seed + offset
					docId := task.gen.BuildKey(key)

					retry := 0
					result := &gocb.GetResult{}
					var err error
					initTime := time.Now().UTC().Format(time.RFC850)
					for retry = 0; retry <= task.OperationConfig.Exceptions.RetryAttempts; retry++ {
						result, err = collectionObject.Collection.Get(docId, nil)

						if err == nil {
							break
						}
					}

					if err == nil {
						m[offset] = tasks.RetriedResult{
							Status:   true,
							CAS:      uint64(result.Cas()),
							InitTime: initTime,
							AckTime:  time.Now().UTC().Format(time.RFC850),
						}
					} else {
						m[offset] = tasks.RetriedResult{
							InitTime: initTime,
							AckTime:  time.Now().UTC().Format(time.RFC850),
						}
					}

					<-routineLimiter
					return nil
				})
			}
			_ = wg.Wait()

			tasks.ShiftErrToCompletedOnRetrying(exception, task.Result, errorOffsetListMap, errorOffsetMaps,
				completedOffsetMaps)
		}
	}

	task.State.MakeCompleteKeyFromMap(completedOffsetMaps)
	task.State.MakeErrorKeyFromMap(errorOffsetMaps)
	task.Result.Failure = int64(len(task.State.KeyStates.Err))
	task.Result.Success = task.OperationConfig.End - task.OperationConfig.Start - task.Result.Failure
	log.Println("completed retrying:- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *ReadTask) MatchResultSeed(resultSeed string) (bool, error) {
	defer task.lock.Unlock()
	task.lock.Lock()
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

func (task *ReadTask) GetCollectionObject() (*cb_sdk.CollectionObject, error) {
	return task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *ReadTask) SetException(exceptions tasks.Exceptions) {
	task.OperationConfig.Exceptions = exceptions
}

func (task *ReadTask) GetOperationConfig() (*tasks.OperationConfig, *task_state.TaskState) {
	return task.OperationConfig, task.State
}
