package tasks

import (
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

type UpsertTask struct {
	IdentifierToken string                             `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig                 `json:"clusterConfig" doc:"true"`
	Bucket          string                             `json:"bucket" doc:"true"`
	Scope           string                             `json:"scope,omitempty" doc:"true"`
	Collection      string                             `json:"collection,omitempty" doc:"true"`
	InsertOptions   *InsertOptions                     `json:"insertOptions,omitempty" doc:"true"`
	OperationConfig *OperationConfig                   `json:"operationConfig,omitempty" doc:"true"`
	Operation       string                             `json:"operation" doc:"false"`
	ResultSeed      int64                              `json:"resultSeed" doc:"false"`
	TaskPending     bool                               `json:"taskPending" doc:"false"`
	State           *task_state.TaskState              `json:"State" doc:"false"`
	MetaData        *task_meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	Result          *task_result.TaskResult            `json:"-" doc:"false"`
	gen             *docgenerator.Generator            `json:"-" doc:"false"`
	req             *Request                           `json:"-" doc:"false"`
}

func (task *UpsertTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *UpsertTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *UpsertTask) Describe() string {
	return `Upsert task mutates documents in bulk into a bucket.
The task will update the fields in a documents ranging from [start,end] inclusive.
We need to share the fields we want to update in a json document using SQL++ syntax.`
}

func (task *UpsertTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *UpsertTask) Config(req *Request, reRun bool) (int64, error) {
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

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = UpsertOperation

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

		if err := configureOperationConfig(task.OperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}

		task.MetaData = task.req.MetaData.GetCollectionMetadata(task.CollectionIdentifier(),
			task.OperationConfig.KeySize, task.OperationConfig.DocSize, task.OperationConfig.DocType, task.OperationConfig.KeyPrefix,
			task.OperationConfig.KeySuffix, task.OperationConfig.TemplateName)

		task.req.lock.Lock()
		if task.OperationConfig.End+task.MetaData.Seed > task.MetaData.SeedEnd {
			task.req.AddToSeedEnd(task.MetaData, (task.OperationConfig.End+task.MetaData.Seed)-(task.MetaData.SeedEnd))
		}
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

func (task *UpsertTask) tearUp() error {
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *UpsertTask) Do() error {

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

	upsertDocuments(task, collectionObject)
	task.Result.Success = task.OperationConfig.End - task.OperationConfig.Start - task.Result.Failure

	return task.tearUp()
}

func upsertDocuments(task *UpsertTask, collectionObject *sdk.CollectionObject) {
	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)

	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	group := errgroup.Group{}
	for i := task.OperationConfig.Start; i < task.OperationConfig.End; i++ {
		routineLimiter <- struct{}{}
		dataChannel <- i

		group.Go(func() error {
			var err error
			offset := <-dataChannel
			key := task.State.SeedStart + offset
			docId := task.gen.BuildKey(key)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}

			initTime := time.Now().UTC().Format(time.RFC850)
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			originalDoc, err := task.gen.Template.GenerateDocument(&fake, task.MetaData.DocSize)
			if err != nil {
				<-routineLimiter
				return err
			}
			originalDoc, err = task.req.retracePreviousMutations(task.CollectionIdentifier(), offset, originalDoc, *task.gen, &fake,
				task.ResultSeed)
			if err != nil {
				task.Result.IncrementFailure(initTime, docId, originalDoc, err, false, 0, offset)
				<-routineLimiter
				return err
			}
			docUpdated, err := task.gen.Template.UpdateDocument(task.OperationConfig.FieldsToChange, originalDoc, &fake)

			for retry := 0; retry < int(math.Max(float64(1), float64(task.OperationConfig.Exceptions.
				RetryAttempts))); retry++ {
				initTime = time.Now().UTC().Format(time.RFC850)
				_, err = collectionObject.Collection.Upsert(docId, docUpdated, &gocb.UpsertOptions{
					DurabilityLevel: getDurability(task.InsertOptions.Durability),
					PersistTo:       task.InsertOptions.PersistTo,
					ReplicateTo:     task.InsertOptions.ReplicateTo,
					Timeout:         time.Duration(task.InsertOptions.Timeout) * time.Second,
					Expiry:          time.Duration(task.InsertOptions.Expiry) * time.Second,
				})

				if err == nil {
					break
				}
			}

			if err != nil {
				task.Result.IncrementFailure(initTime, docId, docUpdated, err, false, 0, offset)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
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

func (task *UpsertTask) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
	task.State.StopStoringState()

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
					fake := faker.NewWithSeed(rand.NewSource(int64(key)))

					originalDoc, err := task.gen.Template.GenerateDocument(&fake, task.MetaData.DocSize)
					if err != nil {
						<-routineLimiter
						return err
					}
					originalDoc, err = task.req.retracePreviousMutations(task.CollectionIdentifier(), offset, originalDoc, *task.gen, &fake,
						task.ResultSeed)
					if err != nil {
						initTime := time.Now().UTC().Format(time.RFC850)
						task.Result.IncrementFailure(initTime, docId, originalDoc, err, false, 0, offset)
						<-routineLimiter
						return err
					}
					docUpdated, err := task.gen.Template.UpdateDocument(task.OperationConfig.FieldsToChange, originalDoc, &fake)

					retry := 0
					result := &gocb.MutationResult{}

					initTime := time.Now().UTC().Format(time.RFC850)
					for retry = 0; retry <= task.OperationConfig.Exceptions.RetryAttempts; retry++ {
						result, err = collectionObject.Collection.Upsert(docId, docUpdated, &gocb.UpsertOptions{
							DurabilityLevel: getDurability(task.InsertOptions.Durability),
							PersistTo:       task.InsertOptions.PersistTo,
							ReplicateTo:     task.InsertOptions.ReplicateTo,
							Timeout:         time.Duration(task.InsertOptions.Timeout) * time.Second,
							Expiry:          time.Duration(task.InsertOptions.Expiry) * time.Second,
						})

						if err == nil {
							break
						}
					}

					if err == nil {
						m[offset] = RetriedResult{
							Status:   true,
							CAS:      uint64(result.Cas()),
							InitTime: initTime,
							AckTime:  time.Now().UTC().Format(time.RFC850),
						}
					} else {
						m[offset] = RetriedResult{
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
}

func (task *UpsertTask) GetResultSeed() string {
	if task.Result == nil {
		task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	}
	return fmt.Sprintf("%d", task.ResultSeed)
}

func (task *UpsertTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *UpsertTask) SetException(exceptions Exceptions) {
	task.OperationConfig.Exceptions = exceptions
}
