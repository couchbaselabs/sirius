package tasks

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type SingleInsertTask struct {
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"scope,omitempty" doc:"true"`
	Collection      string                  `json:"collection,omitempty" doc:"true"`
	InsertOptions   *InsertOptions          `json:"insertOptions,omitempty" doc:"true"`
	OperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int                     `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	result          *task_result.TaskResult `json:"result" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *SingleInsertTask) Describe() string {
	return "Single insert task create key value in Couchbase.\n"
}

func (task *SingleInsertTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleInsertTask) CollectionIdentifier() string {
	return task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *SingleInsertTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleInsertTask) Config(req *Request, reRun bool) (int, error) {
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
		task.Operation = SingleInsertOperation

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

		if err := configSingleOperationConfig(task.OperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}

	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleInsertTask) tearUp() error {
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	task.result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleInsertTask) Do() error {

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

	singleInsertDocuments(task, collectionObject)

	task.result.Success = (len(task.OperationConfig.KeyValue)) - task.result.Failure
	return task.tearUp()
}

// singleInsertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleInsertDocuments(task *SingleInsertTask, collectionObject *sdk.CollectionObject) {

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

			m, err := collectionObject.Collection.Insert(kV.Key, kV.Doc, &gocb.InsertOptions{
				DurabilityLevel: getDurability(task.InsertOptions.Durability),
				PersistTo:       task.InsertOptions.PersistTo,
				ReplicateTo:     task.InsertOptions.ReplicateTo,
				Timeout:         time.Duration(task.InsertOptions.Timeout) * time.Second,
				Expiry:          time.Duration(task.InsertOptions.Expiry) * time.Second,
			})
			if task.OperationConfig.ReadYourOwnWrite {
				var resultFromHost map[string]any
				result, err := collectionObject.Collection.Get(kV.Key, nil)
				if err != nil {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					task.result.IncrementFailure(kV.Key, kV.Doc, err)
					<-routineLimiter
					return err
				}
				if err := result.Content(&resultFromHost); err != nil {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					task.result.IncrementFailure(kV.Key, kV.Doc, err)
					<-routineLimiter
					return err
				}

				resultFromHostBytes, err := json.Marshal(resultFromHost)
				if err != nil {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					task.result.IncrementFailure(kV.Key, kV.Doc, err)
					<-routineLimiter
					return err
				}
				resultFromDocBytes, err := json.Marshal(kV.Doc)
				if err != nil {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					task.result.IncrementFailure(kV.Key, kV.Doc, err)
					<-routineLimiter
					return err
				}

				if !bytes.Equal(resultFromHostBytes, resultFromDocBytes) {
					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
						false, 0)
					task.result.IncrementFailure(kV.Key, kV.Doc, errors.New("document validation failed on read your own write"))
					<-routineLimiter
					return err
				}
			} else {
				if err != nil {
					if errors.Is(err, gocb.ErrDocumentExists) {
						if tempResult, err1 := collectionObject.Collection.Get(kV.Key, nil); err1 != nil {
							task.result.CreateSingleErrorResult(kV.Key, "unable to get CAS Value", false,
								0)
						} else {
							task.result.CreateSingleErrorResult(kV.Key, "", true, uint64(tempResult.Cas()))
						}
						<-routineLimiter
						return nil
					} else {
						task.result.CreateSingleErrorResult(kV.Key, err.Error(), false, 0)
						task.result.IncrementFailure(kV.Key, kV.Doc, err)
						<-routineLimiter
						return err
					}
				}
			}

			task.result.CreateSingleErrorResult(kV.Key, "", true, uint64(m.Cas()))
			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}
