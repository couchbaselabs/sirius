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

type InsertTask struct {
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
	result          *task_result.TaskResult            `json:"-" doc:"false"`
	gen             *docgenerator.Generator            `json:"-" doc:"false"`
	req             *Request                           `json:"-" doc:"false"`
}

func (task *InsertTask) Describe() string {
	return " Insert task uploads documents in bulk into a bucket.\n" +
		"The durability while inserting a document can be set using following values in the 'durability' JSON tag :-\n" +
		"1. MAJORITY\n" +
		"2. MAJORITY_AND_PERSIST_TO_ACTIVE\n" +
		"3. PERSIST_TO_MAJORITY\n"
}

func (task *InsertTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *InsertTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *InsertTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *InsertTask) Config(req *Request, reRun bool) (int64, error) {
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
		task.Operation = InsertOperation

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
		task.State = task_state.ConfigTaskState(task.MetaData.Seed, task.MetaData.SeedEnd, task.ResultSeed)
		task.req.AddToSeedEnd(task.MetaData, task.OperationConfig.Count)
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

func (task *InsertTask) tearUp() error {
	//Use this case to store task's state on disk when required
	//if err := task.State.SaveTaskSateOnDisk(); err != nil {
	//	log.Println("Error in storing TASK State on DISK")
	//}

	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed, task.Operation)
	}
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *InsertTask) Do() error {

	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.GetCollectionObject()

	task.gen = docgenerator.ConfigGenerator(task.MetaData.DocType, task.MetaData.KeyPrefix,
		task.MetaData.KeySuffix, task.State.SeedStart, task.State.SeedEnd,
		template.InitialiseTemplate(task.MetaData.TemplateName))

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		task.result.FailWholeBulkOperation(0, task.OperationConfig.Count,
			task.OperationConfig.DocSize, task.gen, err1, task.State)
		return task.tearUp()
	}

	insertDocuments(task, collectionObject)
	task.result.Success = task.OperationConfig.Count - task.result.Failure

	return task.tearUp()
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func insertDocuments(task *InsertTask, collectionObject *sdk.CollectionObject) {

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
	for iteration := int64(0); iteration < task.OperationConfig.Count; iteration++ {

		routineLimiter <- struct{}{}
		dataChannel <- iteration

		group.Go(func() error {
			offset := <-dataChannel
			docId, key := task.gen.GetDocIdAndKey(offset)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			doc, err := task.gen.Template.GenerateDocument(&fake, task.OperationConfig.DocSize)
			if err != nil {
				task.result.IncrementFailure(docId, doc, err, false, 0, offset)
				<-routineLimiter
				return err
			}

			for retry := 0; retry <= int(math.Max(float64(1), float64(task.OperationConfig.Exceptions.
				RetryAttempts))); retry++ {
				_, err = collectionObject.Collection.Insert(docId, doc, &gocb.InsertOptions{
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

			if task.OperationConfig.ReadYourOwnWrite {
				var resultFromHost map[string]any
				documentFromHost := template.InitialiseTemplate(task.OperationConfig.TemplateName)
				result, err := collectionObject.Collection.Get(docId, nil)
				if err != nil {
					task.result.IncrementFailure(docId, doc, err, false, 0, offset)
					<-routineLimiter
					return err
				}
				if err := result.Content(&resultFromHost); err != nil {
					task.result.IncrementFailure(docId, doc, err, false, 0, offset)
					<-routineLimiter
					return err
				}
				resultBytes, err := json.Marshal(resultFromHost)
				err = json.Unmarshal(resultBytes, &documentFromHost)
				if err != nil {
					task.result.IncrementFailure(docId, doc, err, false, 0, offset)
					<-routineLimiter
					return err
				}
				ok, err := task.gen.Template.Compare(documentFromHost, doc)
				if err != nil || !ok {
					task.result.IncrementFailure(docId, documentFromHost, errors.New("integrity lost"), false, 0, offset)
					<-routineLimiter
					return err
				}
			} else {
				if err != nil {
					if errors.Is(err, gocb.ErrDocumentExists) {
						task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
						<-routineLimiter
						return nil
					} else {
						task.result.IncrementFailure(docId, doc, err, false, 0, offset)
						task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
						<-routineLimiter
						return err
					}
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

func (task *InsertTask) PostTaskExceptionHandling(collectionObject *sdk.CollectionObject) {
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
					docId, key := task.gen.GetDocIdAndKey(offset)
					fake := faker.NewWithSeed(rand.NewSource(int64(key)))
					doc, _ := task.gen.Template.GenerateDocument(&fake, task.OperationConfig.DocSize)

					retry := 0
					var err error
					result := &gocb.MutationResult{}

					for retry = 0; retry <= task.OperationConfig.Exceptions.RetryAttempts; retry++ {
						result, err = collectionObject.Collection.Insert(docId, doc, &gocb.InsertOptions{
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
						if errors.Is(err, gocb.ErrDocumentExists) {
							if tempResult, err1 := collectionObject.Collection.Get(docId, &gocb.GetOptions{
								Timeout: 5 * time.Second,
							}); err1 == nil {
								m[offset] = RetriedResult{
									Status: true,
									CAS:    uint64(tempResult.Cas()),
								}
							} else {
								m[offset] = RetriedResult{
									Status: true,
									CAS:    0,
								}
							}
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

			shiftErrToCompletedOnRetrying(exception, task.result, errorOffsetListMap, errorOffsetMaps, completedOffsetMaps)
		}
	}

	task.State.MakeCompleteKeyFromMap(completedOffsetMaps)
	task.State.MakeErrorKeyFromMap(errorOffsetMaps)
	task.result.Failure = int64(len(task.State.KeyStates.Err))

}

func (task *InsertTask) GetResultSeed() string {
	return fmt.Sprintf("%d", task.result.ResultSeed)
}

func (task *InsertTask) GetCollectionObject() (*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *InsertTask) SetException(exceptions Exceptions) {
	task.OperationConfig.Exceptions = exceptions
}
