package tasks

import (
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type SingleReplaceTask struct {
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"scope,omitempty" doc:"true"`
	Collection      string                  `json:"collection,omitempty" doc:"true"`
	ReplaceOptions  *ReplaceOptions         `json:"replaceOptions,omitempty" doc:"true"`
	OperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int                     `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	result          *task_result.TaskResult `json:"-" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *SingleReplaceTask) Describe() string {
	return "Single replace task a document in the collection in Couchbase.\n"
}

func (task *SingleReplaceTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleReplaceTask) CollectionIdentifier() string {
	return task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleReplaceTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleReplaceTask) Config(req *Request, reRun bool) (int, error) {
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
		task.ResultSeed = int(time.Now().UnixNano())
		task.Operation = SingleReplaceOperation

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configReplaceOptions(task.ReplaceOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configSingleOperationConfig(task.OperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}
	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleReplaceTask) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleReplaceTask) Do() error {

	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	collectionObject, err1 := task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		var docIds []string
		for _, kV := range task.OperationConfig.KeyValue {
			docIds = append(docIds, kV.Key)
		}
		task.result.FailWholeSingleOperation(docIds, err1)
		return task.tearUp()
	}

	singleReplaceDocuments(task, collectionObject)

	task.result.Success = (len(task.OperationConfig.KeyValue)) - task.result.Failure
	return task.tearUp()
}

// singleReplaceDocuments uploads new documents in a bucket.scope.
// collection in a defined batch size at multiple iterations.
func singleReplaceDocuments(task *SingleReplaceTask, collectionObject *sdk.CollectionObject) {

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan interface{}, MaxConcurrentRoutines)

	group := errgroup.Group{}

	for _, data := range task.OperationConfig.KeyValue {
		routineLimiter <- struct{}{}
		dataChannel <- data

		group.Go(func() error {
			keyValue := <-dataChannel
			kV, ok := keyValue.(KeyValue)
			if !ok {
				task.result.IncrementFailure("unknownDocId", struct{}{},
					errors.New("unable to decode Key Value for single crud"))
				<-routineLimiter
				return errors.New("unable to decode Key Value for single crud")
			}

			result, err := collectionObject.Collection.Replace(kV.Key, kV.Doc, &gocb.ReplaceOptions{
				Expiry:          time.Duration(task.ReplaceOptions.Expiry) * time.Second,
				Cas:             gocb.Cas(task.ReplaceOptions.Cas),
				PersistTo:       task.ReplaceOptions.PersistTo,
				ReplicateTo:     task.ReplaceOptions.ReplicateTo,
				DurabilityLevel: getDurability(task.ReplaceOptions.Durability),
				Timeout:         time.Duration(task.ReplaceOptions.Timeout) * time.Second,
			})

			if err != nil {
				task.result.CreateSingleErrorResult(kV.Key, err.Error(), false, 0)
				task.result.IncrementFailure(kV.Key, kV.Doc, err)
				<-routineLimiter
				return err
			}

			task.result.CreateSingleErrorResult(kV.Key, "", true, uint64(result.Cas()))
			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}
