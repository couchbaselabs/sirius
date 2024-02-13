package bulk_loading

import (
	"errors"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"github.com/couchbaselabs/sirius/internal/meta_data"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"
	"golang.org/x/sync/errgroup"
	"log"
	"strings"
	"sync"
	"time"
)

type GenericLoadingTask struct {
	IdentifierToken string `json:"identifierToken" doc:"true"`
	tasks.DatabaseInformation
	ResultSeed      int64                         `json:"resultSeed" doc:"false"`
	TaskPending     bool                          `json:"taskPending" doc:"false"`
	State           *task_state.TaskState         `json:"State" doc:"false"`
	MetaData        *meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	OperationConfig *OperationConfig              `json:"operationConfig" doc:"true"`
	Operation       string                        `json:"operation" doc:"false"`
	Result          *task_result.TaskResult       `json:"-" doc:"false"`
	gen             *docgenerator.Generator       `json:"-" doc:"false"`
	req             *tasks.Request                `json:"-" doc:"false"`
	rerun           bool                          `json:"-" doc:"false"`
	lock            sync.Mutex                    `json:"-" doc:"false"`
}

func (t *GenericLoadingTask) Describe() string {
	return " Insert t uploads documents in bulk into a bucket.\n" +
		"The durability while inserting a document can be set using following values in the 'durability' JSON tag :-\n" +
		"1. MAJORITY\n" +
		"2. MAJORITY_AND_PERSIST_TO_ACTIVE\n" +
		"3. PERSIST_TO_MAJORITY\n"
}

func (t *GenericLoadingTask) MetaDataIdentifier() string {
	if t.DBType == db.COUCHBASE_DB {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr, t.Extra.Bucket, t.Extra.Scope,
			t.Extra.Collection}, ":")
	} else if t.DBType == db.MONGO_DB {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr, t.Extra.Collection}, ":")
	} else {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr}, ":")
	}
}

func (t *GenericLoadingTask) CheckIfPending() bool {
	return t.TaskPending
}

// Config configures  the insert task
func (t *GenericLoadingTask) Config(req *tasks.Request, reRun bool) (int64, error) {
	t.TaskPending = true
	t.req = req

	if t.req == nil {
		t.TaskPending = false
		return 0, err_sirius.RequestIsNil
	}

	if database, err := db.ConfigDatabase(t.DBType); err != nil {
		return 0, err
	} else {
		if err = database.Connect(t.ConnStr, t.Username, t.Password, t.Extra); err != nil {
			return 0, err
		}
	}

	t.lock = sync.Mutex{}
	t.rerun = reRun

	if t.Operation == "" {
		return 0, err_sirius.InternalErrorSetOperationType
	}

	if !reRun {
		t.ResultSeed = int64(time.Now().UnixNano())

		if err := ConfigureOperationConfig(t.OperationConfig); err != nil {
			t.TaskPending = false
			return 0, err
		}

		if err := configExtraParameters(t.DBType, &t.Extra); err != nil {
			return 0, err
		}

		t.MetaData = t.req.MetaData.GetCollectionMetadata(t.MetaDataIdentifier())

		t.req.Lock()
		if t.OperationConfig.End+t.MetaData.Seed > t.MetaData.SeedEnd {
			t.req.AddToSeedEnd(t.MetaData, (t.OperationConfig.End+t.MetaData.Seed)-(t.MetaData.SeedEnd))
		}
		t.req.Unlock()
		t.State = task_state.ConfigTaskState(t.MetaData.Seed, t.ResultSeed)

	} else {
		//if t.State == nil {
		//	return t.ResultSeed, err_sirius.TaskStateIsNil
		//}
		//t.State.SetupStoringKeys()
		t.State = task_state.ConfigTaskState(t.MetaData.Seed, t.ResultSeed)
		_ = task_result.DeleteResultFile(t.ResultSeed)
		log.Println("retrying :- ", t.Operation, t.IdentifierToken, t.ResultSeed)
	}
	return t.ResultSeed, nil
}

func (t *GenericLoadingTask) TearUp() error {

	t.Result.StopStoringResult()
	if err := t.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", t.ResultSeed, t.Operation)
	}
	t.Result = nil

	t.State.StopStoringState()
	if err := t.State.SaveTaskSateOnDisk(); err != nil {
		log.Println("Error in storing TASK State on DISK")
	}
	t.State = nil

	t.TaskPending = false
	return t.req.SaveRequestIntoFile()
}

func (t *GenericLoadingTask) Do() {

	t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed)
	t.gen = docgenerator.ConfigGenerator(
		t.OperationConfig.KeySize,
		t.OperationConfig.DocSize,
		template.InitialiseTemplate(t.OperationConfig.TemplateName))

	database, err := db.ConfigDatabase(t.DBType)
	if err != nil {
		t.Result.ErrorOther = err.Error()
		t.Result.FailWholeBulkOperation(t.OperationConfig.Start, t.OperationConfig.End,
			err, t.State, t.gen, t.MetaData.Seed)
		_ = t.TearUp()
	}
	err = database.Warmup(t.ConnStr, t.Username, t.Password, t.Extra)
	if err != nil {
		t.Result.ErrorOther = err.Error()
		t.Result.FailWholeBulkOperation(t.OperationConfig.Start, t.OperationConfig.End,
			err, t.State, t.gen, t.MetaData.Seed)
		_ = t.TearUp()
	}

	loadDocumentsInBatches(t)

	t.Result.Success = t.OperationConfig.End - t.OperationConfig.Start - t.Result.Failure

	_ = t.TearUp()
}

// loadDocumentsInBatches divides the load into batches and will allocate to one of go routine.
func loadDocumentsInBatches(task *GenericLoadingTask) {

	// This is stop processing loading operation if user has already cancelled all the tasks from sirius
	if task.req.ContextClosed() {
		return
	}

	batchSize := (task.OperationConfig.End - task.OperationConfig.Start) / int64(tasks.MaxThreads)
	numOfBatches := (task.OperationConfig.End - task.OperationConfig.Start) / batchSize

	wg := &sync.WaitGroup{}

	for i := int64(0); i < numOfBatches; i++ {
		batchStart := i * batchSize
		batchEnd := (i + 1) * batchSize
		t := newLoadingTask(batchStart,
			batchEnd,
			task.MetaData.Seed,
			task.OperationConfig,
			task.Operation,
			task.rerun,
			task.gen,
			task.State,
			task.Result,
			task.DatabaseInformation,
			task.Extra,
			task.req,
			task.MetaDataIdentifier(),
			wg)
		loadBatch(task, t, batchStart, batchEnd)
		wg.Add(1)
	}

	remainingItems := (task.OperationConfig.End - task.OperationConfig.Start) - (numOfBatches * batchSize)
	if remainingItems > 0 {
		t := newLoadingTask(
			numOfBatches*batchSize,
			task.OperationConfig.End,
			task.MetaData.Seed,
			task.OperationConfig,
			task.Operation,
			task.rerun,
			task.gen,
			task.State,
			task.Result,
			task.DatabaseInformation,
			task.Extra,
			task.req,
			task.MetaDataIdentifier(),
			wg)
		loadBatch(task, t, numOfBatches*batchSize, task.OperationConfig.End)
		wg.Add(1)
	}

	wg.Wait()
	task.PostTaskExceptionHandling()
	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
}

// loadBatch will enqueue the batch to thread pool. if the queue is full,
// it will wait for sometime any thread to pick it up.
func loadBatch(task *GenericLoadingTask, t *loadingTask, batchStart int64, batchEnd int64) {
	retryBatchCounter := 10
	for ; retryBatchCounter > 0; retryBatchCounter-- {
		if err := tasks.Pool.Execute(t); err == nil {
			break
		}
		time.Sleep(1 * time.Minute)
	}
	if retryBatchCounter == 0 {
		task.Result.FailWholeBulkOperation(batchStart, batchEnd, errors.New("internal error, "+
			"sirius is overloaded"), task.State, task.gen, task.MetaData.Seed)
	}
}

func (t *GenericLoadingTask) PostTaskExceptionHandling() {
	t.Result.StopStoringResult()
	t.State.StopStoringState()

	if t.OperationConfig.Exceptions.RetryAttempts <= 0 {
		return
	}

	// Get all the errorOffset
	errorOffsetMaps := t.State.ReturnErrOffset()
	// Get all the completed offset
	completedOffsetMaps := t.State.ReturnCompletedOffset()

	// For the offset in ignore exceptions :-> move them from error to completed
	shiftErrToCompletedOnIgnore(t.OperationConfig.Exceptions.IgnoreExceptions, t.Result, errorOffsetMaps,
		completedOffsetMaps)

	if t.OperationConfig.Exceptions.RetryAttempts > 0 {

		exceptionList := GetExceptions(t.Result, t.OperationConfig.Exceptions.RetryExceptions)

		// For the retry exceptions :-> move them on success after retrying from err_sirius to completed
		for _, exception := range exceptionList {

			errorOffsetListMap := make([]map[int64]RetriedResult, 0)
			for _, failedDocs := range t.Result.BulkError[exception] {
				m := make(map[int64]RetriedResult)
				m[failedDocs.Offset] = RetriedResult{}
				errorOffsetListMap = append(errorOffsetListMap, m)
			}

			routineLimiter := make(chan struct{}, tasks.MaxConcurrentRoutines)
			dataChannel := make(chan map[int64]RetriedResult, tasks.MaxConcurrentRoutines)
			wg := errgroup.Group{}
			for _, x := range errorOffsetListMap {
				dataChannel <- x
				routineLimiter <- struct{}{}
				wg.Go(func() error {
					//m := <-dataChannel
					//var offset = int64(-1)
					//for k, _ := range m {
					//	offset = k
					//}
					//key := offset + t.MetaData.Seed
					//docId := t.gen.BuildKey(key)
					//
					//fake := faker.NewWithSeed(rand.NewSource(int64(key)))
					//doc, _ := t.gen.Template.GenerateDocument(&fake, t.OperationConfig.DocSize)
					//
					//retry := 0
					//var err error
					//result := &gocb.MutationResult{}
					//
					//initTime := time.Now().UTC().Format(time.RFC850)
					//
					//for retry = 0; retry <= t.OperationConfig.Exceptions.RetryAttempts; retry++ {
					//	result, err = collectionObject.Collection.Insert(docId, doc, &gocb.InsertOptions{
					//		DurabilityLevel: cb_sdk.GetDurability(t.InsertOptions.Durability),
					//		PersistTo:       t.InsertOptions.PersistTo,
					//		ReplicateTo:     t.InsertOptions.ReplicateTo,
					//		Timeout:         time.Duration(t.InsertOptions.Timeout) * time.Second,
					//		Expiry:          time.Duration(t.InsertOptions.Expiry) * time.Second,
					//	})
					//
					//	if err == nil {
					//		break
					//	}
					//}
					//
					//if err != nil {
					//	if errors.Is(err, gocb.ErrDocumentExists) {
					//		if tempResult, err1 := collectionObject.Collection.Get(docId, &gocb.GetOptions{
					//			Timeout: 5 * time.Second,
					//		}); err1 == nil {
					//			m[offset] = RetriedResult{
					//				Status:   true,
					//				CAS:      uint64(tempResult.Cas()),
					//				InitTime: initTime,
					//				AckTime:  time.Now().UTC().Format(time.RFC850),
					//			}
					//		} else {
					//			m[offset] = RetriedResult{
					//				Status:   true,
					//				InitTime: initTime,
					//				AckTime:  time.Now().UTC().Format(time.RFC850),
					//			}
					//		}
					//	} else {
					//		m[offset] = RetriedResult{
					//			InitTime: initTime,
					//			AckTime:  time.Now().UTC().Format(time.RFC850),
					//		}
					//	}
					//} else {
					//	m[offset] = RetriedResult{
					//		Status:   true,
					//		CAS:      uint64(result.Cas()),
					//		InitTime: initTime,
					//		AckTime:  time.Now().UTC().Format(time.RFC850),
					//	}
					//}
					<-dataChannel
					<-routineLimiter
					return nil
				})
			}
			_ = wg.Wait()

			shiftErrToCompletedOnRetrying(exception, t.Result, errorOffsetListMap, errorOffsetMaps,
				completedOffsetMaps)
		}
	}

	t.State.MakeCompleteKeyFromMap(completedOffsetMaps)
	t.State.MakeErrorKeyFromMap(errorOffsetMaps)
	t.Result.Failure = int64(len(t.State.KeyStates.Err))
	t.Result.Success = t.OperationConfig.End - t.OperationConfig.Start - t.Result.Failure
	log.Println("completed retrying:- ", t.Operation, t.IdentifierToken, t.ResultSeed)
}

func (t *GenericLoadingTask) MatchResultSeed(resultSeed string) (bool, error) {
	defer t.lock.Unlock()
	t.lock.Lock()
	if fmt.Sprintf("%d", t.ResultSeed) == resultSeed {
		if t.TaskPending {
			return true, err_sirius.TaskInPendingState
		}
		if t.Result == nil {
			t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed)
		}
		return true, nil
	}
	return false, nil
}

func (t *GenericLoadingTask) SetException(exceptions Exceptions) {
	t.OperationConfig.Exceptions = exceptions
}

func (t *GenericLoadingTask) GetOperationConfig() (*OperationConfig, *task_state.TaskState) {
	return t.OperationConfig, t.State
}
