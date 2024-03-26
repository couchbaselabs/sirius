package data_loading

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"github.com/couchbaselabs/sirius/internal/meta_data"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"
	"golang.org/x/sync/errgroup"
)

type GenericLoadingTask struct {
	IdentifierToken string `json:"identifierToken" doc:"true"`
	tasks.DatabaseInformation
	ResultSeed      int64                         `json:"resultSeed" doc:"false"`
	TaskPending     bool                          `json:"taskPending" doc:"false"`
	State           *task_state.TaskState         `json:"State" doc:"false"`
	MetaData        *meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	OperationConfig *OperationConfig              `json:"operationConfig" doc:"true"`
	Operation       string                        `json:"-" doc:"false"`
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

		t.State = task_state.ConfigTaskState(t.ResultSeed)

	} else {
		if t.State == nil {
			t.State = task_state.ConfigTaskState(t.ResultSeed)
		} else {
			t.State.SetupStoringKeys()
		}
		_ = task_result.DeleteResultFile(t.ResultSeed)
		log.Println("retrying :- ", t.Operation, t.IdentifierToken, t.ResultSeed)
	}

	t.gen = docgenerator.ConfigGenerator(
		t.OperationConfig.KeySize,
		t.OperationConfig.DocSize,
		template.InitialiseTemplate(t.OperationConfig.TemplateName))

	return t.ResultSeed, nil
}

func (t *GenericLoadingTask) TearUp() error {

	t.Result.StopStoringResult()
	t.Result.Success = t.OperationConfig.End - t.OperationConfig.Start - t.Result.Failure
	if err := t.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", t.MetaDataIdentifier(), " ", t.ResultSeed, " ", t.Operation)
	}
	t.Result = nil

	t.State.StopStoringState()
	if err := t.State.SaveTaskSateOnDisk(); err != nil {
		log.Println("not able to save state into ", t.MetaDataIdentifier(), " ", t.ResultSeed, " ", t.Operation)
	}

	t.TaskPending = false
	return t.req.SaveRequestIntoFile()
}

func (t *GenericLoadingTask) Do() {

	t.Result = task_result.ConfigTaskResult(t.Operation, t.ResultSeed)

	database, err := db.ConfigDatabase(t.DBType)
	if err != nil {
		t.Result.ErrorOther = err.Error()
		t.Result.FailWholeBulkOperation(t.OperationConfig.Start, t.OperationConfig.End,
			err, t.State, t.gen, t.MetaData.Seed)
		_ = t.TearUp()
		return
	}
	err = database.Warmup(t.ConnStr, t.Username, t.Password, t.Extra)
	if err != nil {
		t.Result.ErrorOther = err.Error()
		t.Result.FailWholeBulkOperation(t.OperationConfig.Start, t.OperationConfig.End,
			err, t.State, t.gen, t.MetaData.Seed)
		_ = t.TearUp()
		return
	}

	loadDocumentsInBatches(t)

	_ = t.TearUp()
}

// loadDocumentsInBatches divides the load into batches and will allocate to one of go routine.
func loadDocumentsInBatches(task *GenericLoadingTask) {

	// This is stop processing loading operation if user has already cancelled all the tasks from sirius
	if task.req.ContextClosed() {
		return
	}

	wg := &sync.WaitGroup{}
	numOfBatches := int64(0)

	//default batch size is calculated by dividing the total operations in equal quantity to each thread.
	batchSize := (task.OperationConfig.End - task.OperationConfig.Start) / int64(tasks.MaxThreads)

	//batchSize := int64(0)
	// if we are using sdk Batching call, then fetch the batch size from extras.
	// current default value of a batch for SDK batching is 100 but will be picked from os.env
	if tasks.CheckBulkOperation(task.Operation) {
		if task.Extra.SDKBatchSize > 0 {
			batchSize = int64(task.Extra.SDKBatchSize)
		} else {
			envBatchSize := os.Getenv("sirius_sdk_batch_size")
			if len(envBatchSize) == 0 {
				batchSize = 500
			} else {
				if x, e := strconv.Atoi(envBatchSize); e != nil {
					batchSize = int64(x)
				}
			}
		}
		if batchSize > (task.OperationConfig.End-task.OperationConfig.Start)/int64(tasks.MaxThreads) {
			batchSize = (task.OperationConfig.End - task.OperationConfig.Start) / int64(tasks.MaxThreads)
		}
	}

	if task.DBType == "dynamodb" {
		batchSize = 25
	}

	if batchSize > 0 {
		numOfBatches = (task.OperationConfig.End - task.OperationConfig.Start) / (batchSize)
	}
	remainingItems := (task.OperationConfig.End - task.OperationConfig.Start) - (numOfBatches * batchSize)

	fmt.Println()
	log.Println("identifier : ", task.MetaDataIdentifier())
	log.Println("operation :", task.Operation, " result ", task.ResultSeed)
	log.Println("#batches:", numOfBatches, " batch-size:", batchSize, " remaining:", remainingItems)
	fmt.Println(task.OperationConfig)

	t1 := time.Now()
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
		loadBatch(task, t, batchStart, batchEnd, nil)
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
		loadBatch(task, t, numOfBatches*batchSize, task.OperationConfig.End, nil)
		wg.Add(1)
	}

	wg.Wait()
	log.Println("result ", task.ResultSeed, " time took: ", time.Now().Sub(t1))
	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)

}

func (t *GenericLoadingTask) PostTaskExceptionHandling() {

	shiftErrToCompletedOnIgnore(t.OperationConfig.Exceptions.IgnoreExceptions, t.Result, t.State)

	_ = t.State.SaveTaskSateOnDisk()

	exceptionList := GetExceptions(t.Result, t.OperationConfig.Exceptions.RetryExceptions)

	for _, exception := range exceptionList {
		routineLimiter := make(chan struct{}, tasks.MaxRetryingRoutines)
		dataChannel := make(chan int64, tasks.MaxRetryingRoutines)

		failedDocuments := t.Result.BulkError[exception]
		delete(t.Result.BulkError, exception)

		wg := errgroup.Group{}
		for _, x := range failedDocuments {

			t.Result.Failure--
			t.State.RemoveOffsetFromErrSet(x.Offset)

			dataChannel <- x.Offset
			routineLimiter <- struct{}{}

			wg.Go(func() error {

				offset := <-dataChannel
				l := loadingTask{
					start:           offset,
					end:             offset + 1,
					operationConfig: t.OperationConfig,
					seed:            t.MetaData.Seed,
					operation:       t.Operation,
					rerun:           true,
					gen:             t.gen,
					state:           t.State,
					result:          t.Result,
					databaseInfo:    t.DatabaseInformation,
					extra:           t.Extra,
					req:             t.req,
					identifier:      t.IdentifierToken,
					wg:              nil}
				l.Run()

				<-routineLimiter
				return nil
			})
		}
		close(routineLimiter)
		close(dataChannel)
		_ = wg.Wait()
	}

	log.Println("completed retrying:- ", t.Operation, t.IdentifierToken, t.ResultSeed)
	_ = t.TearUp()
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
