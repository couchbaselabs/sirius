package tasks

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"strings"
	"time"
)

type ValidateTask struct {
	ConnectionString string `json:"connectionString"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	Bucket           string `json:"bucket"`
	Scope            string `json:"scope,omitempty"`
	Collection       string `json:"collection,omitempty"`
	TemplateName     string `json:"template"`
	DocSize          int64  `json:"docSize"`
	KeyPrefix        string `json:"keyPrefix"`
	KeySuffix        string `json:"keySuffix"`
	ResultSeed       int64
	Operation        string
	TaskPending      bool
	State            *task_state.TaskState
	result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
	req              *Request
	index            int
}

func (task *ValidateTask) BuildIdentifier() string {
	if task.Scope == "" {
		task.Scope = DefaultScope
	}
	if task.Collection == "" {
		task.Collection = DefaultCollection
	}
	var host string
	if strings.Contains(task.ConnectionString, "couchbase://") {
		host = strings.ReplaceAll(task.ConnectionString, "couchbase://", "")
	}
	if strings.Contains(task.ConnectionString, "couchbases://") {
		host = strings.ReplaceAll(task.ConnectionString, "couchbases://", "")
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s", task.Username, host, task.Bucket, task.Scope, task.Collection)
}

func (task *ValidateTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *ValidateTask) tearUp() error {
	_ = task.connection.Close()
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *ValidateTask) Config(req *Request, seed int64, seedEnd int64, index int, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req
	if task.req == nil {
		return 0, fmt.Errorf("request.Request struct is nil")
	}
	task.index = index
	if !reRun {
		if task.ConnectionString == "" {
			return 0, fmt.Errorf("empty connection string")
		}
		if task.Username == "" || task.Password == "" {
			return 0, fmt.Errorf("cluster's credentials are missing ")
		}
		if task.Bucket == "" {
			return 0, fmt.Errorf("bucker is missing")
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}
		task.Operation = ValidateOperation
		time.Sleep(1 * time.Microsecond)
		task.ResultSeed = time.Now().UnixNano()
		task.State = task_state.ConfigTaskState(task.TemplateName, task.KeyPrefix, task.KeySuffix, task.DocSize, seed,
			seedEnd, task.ResultSeed)
	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
		if task.State == nil {
			return task.ResultSeed, fmt.Errorf("task State is nil")
		}
		task.State.SetupStoringKeys()
	}
	log.Println(task.req.Seed, task.req.SeedEnd)
	return task.ResultSeed, nil
}

func (task *ValidateTask) Describe() string {
	return "validate every document in the cluster's bucket"
}
func (task *ValidateTask) Do() error {
	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	task.connection = sdk.ConfigConnectionManager(task.ConnectionString, task.Username, task.Password,
		task.Bucket, task.Scope, task.Collection)

	if err := task.connection.Connect(); err != nil {
		task.result.ErrorOther = err.Error()
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.ResultSeed)
		}
		return task.tearUp()
	}

	task.gen = docgenerator.ConfigGenerator("", task.State.KeyPrefix, task.State.KeySuffix,
		task.State.SeedStart, task.State.SeedEnd, template.InitialiseTemplate(task.State.TemplateName))

	validateDocuments(task)

	task.result.Success = task.State.SeedEnd - task.State.SeedStart - task.result.Failure

	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	task.State.ClearErrorKeyStates()
	task.State.ClearCompletedKeyStates()
	return task.tearUp()
}

// ValidateDocuments return the validity of the collection using TaskState
func validateDocuments(task *ValidateTask) {
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

			var resultFromHost map[string]interface{}
			documentFromHost := template.InitialiseTemplate(task.State.TemplateName)
			result, err := task.connection.Collection.Get(docId, nil)
			if err != nil {
				task.result.IncrementFailure(docId, err.Error())
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			if err := result.Content(&resultFromHost); err != nil {
				task.result.ValidationFailures(docId)
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			resultBytes, err := json.Marshal(resultFromHost)
			err = json.Unmarshal(resultBytes, &documentFromHost)
			if err != nil {
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				task.result.ValidationFailures(docId)
				<-routineLimiter
				return err
			}
			fake := faker.NewWithSeed(rand.NewSource(key))
			originalDocument, err := task.gen.Template.GenerateDocument(&fake, task.State.DocumentSize)
			if err != nil {
				task.result.IncrementFailure(docId, err.Error())
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			originalDocument, err = task.req.retracePreviousMutations(offset, originalDocument, *task.gen, &fake,
				task.ResultSeed)
			if err != nil {
				task.result.IncrementFailure(docId, err.Error())
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}
			ok, err := task.gen.Template.Compare(documentFromHost, originalDocument)
			if err != nil || !ok {
				task.result.ValidationFailures(docId)
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
