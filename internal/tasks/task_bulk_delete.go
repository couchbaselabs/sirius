package tasks

import (
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/template"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type DeleteTask struct {
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"scope,omitempty" doc:"true"`
	Collection      string                  `json:"collection,omitempty" doc:"true"`
	RemoveOptions   *RemoveOptions          `json:"removeOptions,omitempty" doc:"true"`
	OperationConfig *OperationConfig        `json:"operationConfig,omitempty" doc:"true"`
	Template        interface{}             `json:"-" doc:"false"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int64                   `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	State           *task_state.TaskState   `json:"State" doc:"false"`
	result          *task_result.TaskResult `json:"-" doc:"false"`
	gen             *docgenerator.Generator `json:"-" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *DeleteTask) Describe() string {
	return `Delete task deletes documents in bulk into a bucket.
The task will delete documents from [start,end] inclusive.`
}

func (task *DeleteTask) BuildIdentifier() string {
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

func (task *DeleteTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config checks the validity of DeleteTask
func (task *DeleteTask) Config(req *Request, seed int64, seedEnd int64, reRun bool) (int64, error) {
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
		task.Operation = DeleteOperation
		task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

		if task.IdentifierToken == "" {
			task.result.ErrorOther = "identifier token is missing"
		}

		if err := configRemoveOptions(task.RemoveOptions); err != nil {
			task.result.ErrorOther = err.Error()
		}

		if err := configureOperationConfig(task.OperationConfig); err != nil {
			task.result.ErrorOther = err.Error()
		}

		task.Template = template.InitialiseTemplate(task.OperationConfig.TemplateName)

		task.State = task_state.ConfigTaskState(task.OperationConfig.TemplateName, task.OperationConfig.KeyPrefix,
			task.OperationConfig.KeySuffix, task.OperationConfig.DocSize, seed, seedEnd, task.ResultSeed)

	} else {
		if task.State == nil {
			return task.ResultSeed, fmt.Errorf("task State is nil")
		}

		task.State.SetupStoringKeys()

		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *DeleteTask) tearUp() error {
	task.result = nil
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *DeleteTask) Do() error {

	if task.result != nil && task.result.ErrorOther != "" {
		log.Println(task.result.ErrorOther)
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.ResultSeed)
			return err
		}
		return task.tearUp()
	} else {
		task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	}

	collection, err1 := task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.ResultSeed)
			return err
		}
		return task.tearUp()
	}

	task.gen = docgenerator.ConfigGenerator("", task.State.KeyPrefix, task.State.KeySuffix,
		task.State.SeedStart, task.State.SeedEnd, template.InitialiseTemplate(task.State.TemplateName))

	deleteDocuments(task, collection)
	task.result.Success = task.OperationConfig.End - task.OperationConfig.Start - task.result.Failure

	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}

	task.State.ClearErrorKeyStates()
	return task.tearUp()
}

// deleteDocuments delete the document stored on a host from start to end.
func deleteDocuments(task *DeleteTask, collection *gocb.Collection) {
	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}
	deletedOffset, err := task.req.retracePreviousDeletions(task.ResultSeed)
	if err != nil {
		return
	}
	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	group := errgroup.Group{}

	for i := task.OperationConfig.Start; i < task.OperationConfig.End; i++ {
		routineLimiter <- struct{}{}
		dataChannel <- i

		group.Go(func() error {
			offset := <-dataChannel
			key := task.req.Seed + offset
			docId := task.gen.BuildKey(key)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}
			if _, ok := deletedOffset[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday deleted docID on " + docId)
			}
			if key > task.req.SeedEnd || key < task.req.Seed {
				task.result.IncrementFailure(docId, nil, errors.New("docId out of bound"))
				<-routineLimiter
				return fmt.Errorf("docId out of bound")
			}
			_, err := collection.Remove(docId, &gocb.RemoveOptions{
				Cas:             gocb.Cas(task.RemoveOptions.Cas),
				PersistTo:       task.RemoveOptions.PersistTo,
				ReplicateTo:     task.RemoveOptions.ReplicateTo,
				DurabilityLevel: getDurability(task.RemoveOptions.Durability),
				Timeout:         time.Duration(task.RemoveOptions.Timeout) * time.Second,
			})
			if err != nil {
				task.result.IncrementFailure(docId, nil, err)
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
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}
