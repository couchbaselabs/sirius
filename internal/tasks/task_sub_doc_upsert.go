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
	"strings"
	"sync"
	"time"
)

type SubDocUpsert struct {
	IdentifierToken   string                             `json:"identifierToken" doc:"true"`
	ClusterConfig     *sdk.ClusterConfig                 `json:"clusterConfig" doc:"true"`
	Bucket            string                             `json:"bucket" doc:"true"`
	Scope             string                             `json:"scope,omitempty" doc:"true"`
	Collection        string                             `json:"collection,omitempty" doc:"true"`
	OperationConfig   *OperationConfig                   `json:"operationConfig" doc:"true"`
	InsertSpecOptions *InsertSpecOptions                 `json:"insertSpecOptions" doc:"true"`
	MutateInOptions   *MutateInOptions                   `json:"mutateInOptions" doc:"true"`
	Operation         string                             `json:"operation" doc:"false"`
	ResultSeed        int64                              `json:"resultSeed" doc:"false"`
	TaskPending       bool                               `json:"taskPending" doc:"false"`
	State             *task_state.TaskState              `json:"State" doc:"false"`
	MetaData          *task_meta_data.CollectionMetaData `json:"metaData" doc:"false"`
	Result            *task_result.TaskResult            `json:"-" doc:"false"`
	gen               *docgenerator.Generator            `json:"-" doc:"false"`
	req               *Request                           `json:"-" doc:"false"`
	rerun             bool                               `json:"-" doc:"false"`
	lock              sync.Mutex                         `json:"-" doc:"false"`
}

func (task *SubDocUpsert) Describe() string {
	return " SubDocUpsert upserts a Sub-Document"
}

func (task *SubDocUpsert) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SubDocUpsert) CollectionIdentifier() string {
	clusterIdentifier, _ := sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
		task.Collection}, ":")
}

func (task *SubDocUpsert) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SubDocUpsert) Config(req *Request, reRun bool) (int64, error) {
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

	task.lock = sync.Mutex{}
	task.rerun = reRun

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = SubDocUpsertOperation

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
			return 0, err
		}

		if err := configInsertSpecOptions(task.InsertSpecOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configMutateInOptions(task.MutateInOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		task.MetaData = task.req.MetaData.GetCollectionMetadata(task.CollectionIdentifier())

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

func (task *SubDocUpsert) tearUp() error {
	//Use this case to store task's state on disk when required
	//if err := task.State.SaveTaskSateOnDisk(); err != nil {
	//	log.Println("Error in storing TASK State on DISK")
	//}
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
	}
	task.Result = nil
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SubDocUpsert) Do() error {

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
		return task.tearUp()
	}

	upsertSubDocuments(task, collectionObject)
	task.Result.Success = (task.OperationConfig.End - task.OperationConfig.Start) - task.Result.Failure

	return task.tearUp()
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func upsertSubDocuments(task *SubDocUpsert, collectionObject *sdk.CollectionObject) {

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
	group := errgroup.Group{}
	for iteration := task.OperationConfig.Start; iteration < task.OperationConfig.End; iteration++ {

		if task.req.ContextClosed() {
			close(routineLimiter)
			close(dataChannel)
			return
		}

		routineLimiter <- struct{}{}
		dataChannel <- iteration
		group.Go(func() error {
			offset := <-dataChannel
			key := offset + task.MetaData.Seed
			docId := task.gen.BuildKey(key)

			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))

			task.gen.Template.GenerateSubPathAndValue(&fake)
			task.req.retracePreviousSubDocMutations(task.CollectionIdentifier(), offset, *task.gen, &fake,
				task.ResultSeed)

			var err error
			initTime := time.Now().UTC().Format(time.RFC850)
			for retry := 0; retry < int(math.Max(float64(1), float64(task.OperationConfig.Exceptions.
				RetryAttempts))); retry++ {

				var iOps []gocb.MutateInSpec
				for path, value := range task.gen.Template.GenerateSubPathAndValue(&fake) {
					iOps = append(iOps, gocb.UpsertSpec(path, value, &gocb.UpsertSpecOptions{
						CreatePath: task.InsertSpecOptions.CreatePath,
						IsXattr:    task.InsertSpecOptions.IsXattr,
					}))
				}

				if !task.InsertSpecOptions.IsXattr {
					iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
						int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
							CreatePath: false,
							IsXattr:    false,
						}))
				}

				initTime = time.Now().UTC().Format(time.RFC850)
				_, err = collectionObject.Collection.MutateIn(docId, iOps, &gocb.MutateInOptions{
					Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
					PersistTo:       task.MutateInOptions.PersistTo,
					ReplicateTo:     task.MutateInOptions.ReplicateTo,
					DurabilityLevel: getDurability(task.MutateInOptions.Durability),
					StoreSemantic:   getStoreSemantic(task.MutateInOptions.StoreSemantic),
					Timeout:         time.Duration(task.MutateInOptions.Expiry) * time.Second,
					PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
				})

				if err == nil {
					break
				}
			}
			if err != nil {
				task.Result.IncrementFailure(initTime, docId, err, false, 0, offset)
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

func (task *SubDocUpsert) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
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
					key := offset + task.MetaData.Seed
					docId := task.gen.BuildKey(key)
					fake := faker.NewWithSeed(rand.NewSource(int64(key)))

					task.gen.Template.GenerateSubPathAndValue(&fake)
					_ = task.req.retracePreviousSubDocMutations(task.CollectionIdentifier(), offset, *task.gen, &fake,
						task.ResultSeed)

					retry := 0
					var err error
					result := &gocb.MutateInResult{}

					for retry = 0; retry <= task.OperationConfig.Exceptions.RetryAttempts; retry++ {

						var iOps []gocb.MutateInSpec
						for path, value := range task.gen.Template.GenerateSubPathAndValue(&fake) {
							iOps = append(iOps, gocb.UpsertSpec(path, value, &gocb.UpsertSpecOptions{
								CreatePath: task.InsertSpecOptions.CreatePath,
								IsXattr:    task.InsertSpecOptions.IsXattr,
							}))
						}

						if !task.InsertSpecOptions.IsXattr {
							iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
								int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
									CreatePath: true,
									IsXattr:    false,
								}))
						}

						result, err = collectionObject.Collection.MutateIn(docId, iOps, &gocb.MutateInOptions{
							Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
							PersistTo:       task.MutateInOptions.PersistTo,
							ReplicateTo:     task.MutateInOptions.ReplicateTo,
							DurabilityLevel: getDurability(task.MutateInOptions.Durability),
							StoreSemantic:   getStoreSemantic(task.MutateInOptions.StoreSemantic),
							Timeout:         time.Duration(task.MutateInOptions.Expiry) * time.Second,
							PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
						})

						if err == nil {
							break
						}
					}

					if err != nil {
						m[offset] = RetriedResult{
							Status: true,
							CAS:    0,
						}
					} else {
						m[offset] = RetriedResult{
							Status: true,
							CAS:    uint64(result.Cas()),
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
	log.Println("completed retrying:- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)

}

func (task *SubDocUpsert) MatchResultSeed(resultSeed string) (bool, error) {
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

func (task *SubDocUpsert) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SubDocUpsert) SetException(exceptions Exceptions) {
	task.OperationConfig.Exceptions = exceptions
}

func (task *SubDocUpsert) GetOperationConfig() (*OperationConfig, *task_state.TaskState) {
	return task.OperationConfig, task.State
}
