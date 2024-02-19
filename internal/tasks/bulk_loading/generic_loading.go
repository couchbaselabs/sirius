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
	"os"
	"strconv"
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
	return "Do operation between range from [start,end)"
}

func (t *GenericLoadingTask) MetaDataIdentifier() string {
	if t.DBType == db.CouchbaseDb {
		return strings.Join([]string{t.IdentifierToken, t.ConnStr, t.Extra.Bucket, t.Extra.Scope,
			t.Extra.Collection}, ":")
	} else if t.DBType == db.MongoDb {
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

		t.req.Lock()
		t.MetaData = t.req.MetaData.GetCollectionMetadata(t.MetaDataIdentifier())
		if t.OperationConfig.End+t.MetaData.Seed > t.MetaData.SeedEnd {
			t.req.AddToSeedEnd(t.MetaData, (t.OperationConfig.End+t.MetaData.Seed)-(t.MetaData.SeedEnd))
		}
		t.req.Unlock()

	} else {
		_ = task_result.DeleteResultFile(t.ResultSeed)
		log.Println("retrying :- ", t.Operation, t.IdentifierToken, t.ResultSeed)
	}

	t.State = task_state.ConfigTaskState(t.ResultSeed)
	t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed)
	t.gen = docgenerator.ConfigGenerator(
		t.OperationConfig.KeySize,
		t.OperationConfig.DocSize,
		template.InitialiseTemplate(t.OperationConfig.TemplateName))

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

	t.TaskPending = false
	return t.req.SaveRequestIntoFile()
}

func (t *GenericLoadingTask) Do() {

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

	fmt.Println()
	log.Println("identifier : ", task.MetaDataIdentifier())
	log.Println("operation :", task.Operation)
	log.Println("result ", task.ResultSeed)
	fmt.Println(task.OperationConfig)

	// This is stop processing loading operation if user has already cancelled all the tasks from sirius
	if task.req.ContextClosed() {
		return
	}

	wg := &sync.WaitGroup{}
	numOfBatches := int64(0)

	// default batch size is calculated by dividing the total operations in equal quantity to each thread.
	batchSize := (task.OperationConfig.End - task.OperationConfig.Start) / int64(tasks.MaxThreads)

	// if we are using sdk Batching call, then fetch the batch size from extras.
	// current default value of a batch for SDK batching is 100 but will be picked from os.env
	if tasks.CheckBulkOperation(task.Operation) {
		if task.Extra.SDKBatchSize > 0 {
			batchSize = (task.OperationConfig.End - task.OperationConfig.Start) / int64(task.Extra.SDKBatchSize)
		} else {
			envBatchSize := os.Getenv("sirius_sdk_batch_size")
			if len(envBatchSize) == 0 {
				batchSize = 100
			} else {
				if x, e := strconv.Atoi(envBatchSize); e != nil {
					batchSize = int64(x)
				}
			}
		}
	}

	if batchSize > 0 {
		numOfBatches = (task.OperationConfig.End - task.OperationConfig.Start) / batchSize
	}
	remainingItems := (task.OperationConfig.End - task.OperationConfig.Start) - (numOfBatches * batchSize)

	for i := int64(0); i < numOfBatches; i++ {
		batchStart := i * batchSize
		batchEnd := (i + 1) * batchSize
		t := newLoadingTask(batchStart+task.OperationConfig.Start,
			batchEnd+task.OperationConfig.Start,
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

	if remainingItems > 0 {
		t := newLoadingTask(
			numOfBatches*batchSize+task.OperationConfig.Start,
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

	if t.Result == nil {
		t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed)
	}
	if t.State == nil {
		t.State = task_state.ConfigTaskState(t.ResultSeed)
	}
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
					//	m := <-dataChannel
					//	var offset = int64(-1)
					//	for k, _ := range m {
					//		offset = k
					//	}
					//
					//	l := loadingTask{
					//		start:           offset,
					//		end:             offset + 1,
					//		operationConfig: t.OperationConfig,
					//		seed:            t.MetaData.Seed,
					//		operation:       t.Operation,
					//		rerun:           true,
					//		gen:             t.gen,
					//		state:           t.State,
					//		result:          t.Result,
					//		databaseInfo:    tasks.DatabaseInformation{},
					//		extra:           db.Extras{},
					//		req:             t.req,
					//		identifier:      t.IdentifierToken,
					//		wg:              nil,}
					//
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
