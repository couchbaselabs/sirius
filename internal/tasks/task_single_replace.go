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
	ResultSeed      int64                   `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	Result          *task_result.TaskResult `json:"Result" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *SingleReplaceTask) Describe() string {
	return "Single replace task a document in the collection in Couchbase.\n"
}

func (task *SingleReplaceTask) BuildIdentifier() string {
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

func (task *SingleReplaceTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *SingleReplaceTask) Config(req *Request, seed int64, seedEnd int64, reRun bool) (int64, error) {
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
		task.Operation = SingleReplaceOperation
		task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

		if task.IdentifierToken == "" {
			task.Result.ErrorOther = "identifier token is missing"
		}

		if err := configReplaceOptions(task.ReplaceOptions); err != nil {
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

func (task *SingleReplaceTask) tearUp() error {
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleReplaceTask) Do() error {

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

	singleReplaceDocuments(task, collection)

	task.Result.Success = int64(len(task.OperationConfig.KeyValue)) - task.Result.Failure

	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}

	return task.tearUp()
}

// singleReplaceDocuments uploads new documents in a bucket.scope.
// collection in a defined batch size at multiple iterations.
func singleReplaceDocuments(task *SingleReplaceTask, collection *gocb.Collection) {

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

			_, err := collection.Replace(kV.Key, kV.Doc, &gocb.ReplaceOptions{
				Expiry:          time.Duration(task.ReplaceOptions.Expiry) * time.Second,
				Cas:             gocb.Cas(task.ReplaceOptions.Cas),
				PersistTo:       task.ReplaceOptions.PersistTo,
				ReplicateTo:     task.ReplaceOptions.ReplicateTo,
				DurabilityLevel: getDurability(task.ReplaceOptions.Durability),
				Timeout:         time.Duration(task.ReplaceOptions.Timeout) * time.Second,
			})

			if err != nil {
				task.Result.IncrementFailure(kV.Key, kV.Doc, err)
				<-routineLimiter
				return err
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
