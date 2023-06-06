package tasks

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"time"
)

type ValidateTask struct {
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"scope,omitempty" doc:"true"`
	Collection      string                  `json:"collection,omitempty" doc:"true"`
	OperationConfig *OperationConfig        `json:"operationConfig,omitempty" doc:"true"`
	Template        interface{}             `json:"-" doc:"false"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int64                   `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	State           *task_state.TaskState   `json:"State" doc:"false"`
	Result          *task_result.TaskResult `json:"Result" doc:"false"`
	gen             *docgenerator.Generator `json:"-" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *ValidateTask) BuildIdentifier() string {
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

func (task *ValidateTask) Describe() string {
	return "Validates every document in the cluster's bucket"
}

func (task *ValidateTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *ValidateTask) tearUp() error {
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *ValidateTask) Config(req *Request, seed int64, seedEnd int64, reRun bool) (int64, error) {
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
		task.Operation = ValidateOperation
		task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

		if task.IdentifierToken == "" {
			return 0, fmt.Errorf("identifier token is missing")
		}

		if err := configureOperationConfig(task.OperationConfig); err != nil {
			task.Result.ErrorOther = err.Error()
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

func (task *ValidateTask) Do() error {

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

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

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

	task.gen = docgenerator.ConfigGenerator(task.OperationConfig.DocType, task.OperationConfig.KeyPrefix,
		task.OperationConfig.KeySuffix, task.State.SeedStart, task.State.SeedEnd,
		template.InitialiseTemplate(task.OperationConfig.TemplateName))

	validateDocuments(task, collection)

	task.Result.Success = task.State.SeedEnd - task.State.SeedStart - task.Result.Failure - int64(len(task.Result.
		ValidationError))

	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}
	task.State.ClearErrorKeyStates()
	task.State.ClearCompletedKeyStates()
	return task.tearUp()
}

// ValidateDocuments return the validity of the collection using TaskState
func validateDocuments(task *ValidateTask, collection *gocb.Collection) {
	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}
	deletedOffset, err1 := task.req.retracePreviousDeletions(task.ResultSeed)
	if err1 != nil {
		return
	}
	insertErrorOffset, err2 := task.req.retracePreviousFailedInsertions(task.ResultSeed)
	if err2 != nil {
		return
	}

	group := errgroup.Group{}
	for offset := int64(0); offset < (task.State.SeedEnd - task.State.SeedStart); offset++ {
		routineLimiter <- struct{}{}
		dataChannel <- offset
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
			if _, ok := insertErrorOffset[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("error in insertion of docID on " + docId)
			}

			fake := faker.NewWithSeed(rand.NewSource(key))
			originalDocument, err := task.gen.Template.GenerateDocument(&fake, task.State.DocumentSize)
			if err != nil {
				task.Result.IncrementFailure(docId, originalDocument, err)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			originalDocument, err = task.req.retracePreviousMutations(offset, originalDocument, *task.gen, &fake,
				task.ResultSeed)
			if err != nil {
				task.Result.IncrementFailure(docId, originalDocument, err)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}

			var resultFromHost map[string]interface{}
			documentFromHost := template.InitialiseTemplate(task.State.TemplateName)
			result, err := collection.Get(docId, nil)
			if err != nil {
				task.Result.IncrementFailure(docId, originalDocument, err)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			if err := result.Content(&resultFromHost); err != nil {
				task.Result.IncrementFailure(docId, originalDocument, err)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			resultBytes, err := json.Marshal(resultFromHost)
			err = json.Unmarshal(resultBytes, &documentFromHost)
			if err != nil {
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				task.Result.IncrementFailure(docId, documentFromHost, err)
				<-routineLimiter
				return err
			}

			ok, err := task.gen.Template.Compare(documentFromHost, originalDocument)
			if err != nil || !ok {
				task.Result.IncrementFailure(docId, documentFromHost, err)
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
