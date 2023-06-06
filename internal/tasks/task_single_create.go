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
	ResultSeed      int64                   `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	Result          *task_result.TaskResult `json:"Result" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *SingleInsertTask) Describe() string {
	return "Single insert task uploads key value in CB.\n"
}

func (task *SingleInsertTask) BuildIdentifier() string {
	if task.ClusterConfig == nil {
		task.ClusterConfig = &sdk.ClusterConfig{}
		log.Println("build Identifier have received nil ClusterConfig")
	}
	if task.Bucket == "" {
		task.Bucket = DefaultBucket
	}
	if task.Scope == "" {
		task.Scope = DefaultScope
	}
	if task.Collection == "" {
		task.Collection = DefaultCollection
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s", task.ClusterConfig.Username, task.IdentifierToken, task.Bucket, task.Scope,
		task.Collection)
}

func (task *SingleInsertTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleInsertTask) Config(req *Request, seed int64, seedEnd int64, reRun bool) (int64, error) {
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
		task.ResultSeed = time.Now().UnixNano()
		task.Operation = SingleInsertOperation
		task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

		if task.IdentifierToken == "" {
			task.Result.ErrorOther = "identifier token is missing"
		}

		if err := configInsertOptions(task.InsertOptions); err != nil {
			task.Result.ErrorOther = err.Error()
		}

		if err := configSingleOperationConfig(task.OperationConfig); err != nil {
			task.Result.ErrorOther = err.Error()
		}
	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleInsertTask) tearUp() error {
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleInsertTask) Do() error {

	if task.Result != nil && task.Result.ErrorOther != "" {
		log.Println(task.Result.ErrorOther)
		if err := task.Result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save Result into ", task.ResultSeed)
			return err
		}
		return task.tearUp()
	} else {
		task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	}

	collection, err1 := task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		if err := task.Result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save Result into ", task.ResultSeed)
			return err
		}
		return task.tearUp()
	}

	singleInsertDocuments(task, collection)

	task.Result.Success = int64(len(task.OperationConfig.KeyValue)) - task.Result.Failure

	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}

	return task.tearUp()
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func singleInsertDocuments(task *SingleInsertTask, collection *gocb.Collection) {

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
				task.Result.IncrementFailure("unknownDocId", struct{}{},
					errors.New("unable to decode Key Value for single crud"))
				<-routineLimiter
				return errors.New("unable to decode Key Value for single crud")
			}

			_, err := collection.Insert(kV.Key, kV.Doc, &gocb.InsertOptions{
				DurabilityLevel: getDurability(task.InsertOptions.Durability),
				PersistTo:       task.InsertOptions.PersistTo,
				ReplicateTo:     task.InsertOptions.ReplicateTo,
				Timeout:         time.Duration(task.InsertOptions.Timeout) * time.Second,
				Expiry:          time.Duration(task.InsertOptions.Expiry) * time.Second,
			})
			if task.OperationConfig.ReadYourOwnWrite {

				var resultFromHost map[string]interface{}
				result, err := collection.Get(kV.Key, nil)
				if err != nil {
					task.Result.IncrementFailure(kV.Key, kV.Doc, err)
					<-routineLimiter
					return err
				}
				if err := result.Content(&resultFromHost); err != nil {
					task.Result.IncrementFailure(kV.Key, kV.Doc, err)
					<-routineLimiter
					return err
				}

				resultFromHostBytes, err := json.Marshal(resultFromHost)
				if err != nil {
					task.Result.IncrementFailure(kV.Key, kV.Doc, err)
					<-routineLimiter
					return err
				}
				resultFromDocBytes, err := json.Marshal(kV.Doc)
				if err != nil {
					task.Result.IncrementFailure(kV.Key, kV.Doc, err)
					<-routineLimiter
					return err
				}

				if !bytes.Equal(resultFromHostBytes, resultFromDocBytes) {
					task.Result.IncrementFailure(kV.Key, kV.Doc, errors.New("document mismatch on RYOW"))
					<-routineLimiter
					return err
				}
			} else {
				if err != nil {
					if errors.Is(err, gocb.ErrDocumentExists) {
						<-routineLimiter
						return nil
					} else {
						task.Result.IncrementFailure(kV.Key, kV.Doc, err)
						<-routineLimiter
						return err
					}
				}
			}

			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}
