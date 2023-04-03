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
)

type ValidateTask struct {
	ConnectionString string `json:"connectionString"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	Bucket           string `json:"bucket"`
	Scope            string `json:"scope,omitempty"`
	Collection       string `json:"collection,omitempty"`
	Operation        string
	State            *task_state.TaskState
	Result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
}

func (task *ValidateTask) Config() (int64, error) {
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
	// restore the original cluster state which should exist for operation, returns an error if task state don't exist else returns nil
	state, ok := task_state.ConfigTaskState(task.ConnectionString, task.Bucket, task.Scope, task.Collection, "", "", "",
		0, 0, 0)
	if !ok {
		return 0, fmt.Errorf("no such cluster's state exists for validation")
	}
	task.State = state
	return task.State.Seed, nil
}

func (task *ValidateTask) Describe() string {
	return "validate every document in the cluster's bucket"
}
func (task *ValidateTask) Do() error {
	// prepare a result for the task
	task.Result = task_result.ConfigTaskResult(task.Operation, task.State.Seed)

	// establish a connection
	task.connection = sdk.ConfigConnectionManager(task.ConnectionString, task.Username, task.Password,
		task.Bucket, task.Scope, task.Collection)

	if err := task.connection.Connect(); err != nil {
		task.Result.ErrorOther = err.Error()
		if err := task.Result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.State.Seed)
			return err
		}
		return err
	}

	// Prepare generator
	task.gen = docgenerator.ConfigGenerator("", task.State.KeyPrefix, task.State.KeySuffix,
		task.State.Seed, task.State.SeedEnd, template.InitialiseTemplate(task.State.TemplateName))

	// do bulk loading
	validateDocuments(task)

	// close the sdk connection
	_ = task.connection.Close()

	// save the cluster result into the file
	if err := task.State.SaveTaskStateToFile(); err != nil {
		task.Result.ErrorOther = err.Error()
	}

	// calculated result success here to prevent late update in failure due to locking.
	task.Result.Success = task.State.SeedEnd - task.State.Seed - task.Result.Failure - int64(len(task.State.InsertTaskState.Err)) - int64(len(task.State.DeleteTaskState.Del))

	// save the result into a file
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.State.Seed)
		return err
	}
	return nil
}

// ValidateDocuments return the validity of the collection using TaskState
func validateDocuments(task *ValidateTask) {

	if err := task.State.CheckForTaskValidity(); err != nil {
		log.Println(err)
		return
	}

	insertErrorCheck := make(map[int64]struct{})
	for _, k := range task.State.InsertTaskState.Err {
		insertErrorCheck[k] = struct{}{}
	}

	deleteCheck := make(map[int64]struct{})
	for _, k := range task.State.DeleteTaskState.Del {
		deleteCheck[k] = struct{}{}
	}

	rateLimiter := make(chan struct{}, MaxConcurrentOps)
	dataChannel := make(chan int64, MaxConcurrentOps)
	group := errgroup.Group{}

	for key := task.State.Seed; key < task.State.SeedEnd; key++ {
		rateLimiter <- struct{}{}
		dataChannel <- key

		group.Go(func() error {
			key := <-dataChannel
			if _, ok := insertErrorCheck[key]; ok {
				<-rateLimiter
				return fmt.Errorf("key is in InsertErrorCheck")
			}
			if _, ok := deleteCheck[key]; ok {
				<-rateLimiter
				return fmt.Errorf("key is delete from the server")
			}
			docId := task.gen.BuildKey(key)
			var resultFromHost map[string]interface{}
			documentFromHost := template.InitialiseTemplate(task.State.TemplateName)
			result, err := task.connection.Collection.Get(docId, nil)
			if err != nil {
				task.Result.IncrementFailure(docId, err.Error())
				<-rateLimiter
				return err
			}
			if err := result.Content(&resultFromHost); err != nil {
				task.Result.IncrementFailure(docId, err.Error())
				<-rateLimiter
				return err
			}
			resultBytes, err := json.Marshal(resultFromHost)
			err = json.Unmarshal(resultBytes, &documentFromHost)
			if err != nil {
				task.Result.ValidationFailures(docId)
				<-rateLimiter
				return err
			}
			fake := faker.NewWithSeed(rand.NewSource(key))
			originalDocument, err := task.gen.Template.GenerateDocument(&fake, task.State.DocumentSize)
			if err != nil {
				task.Result.ValidationFailures(docId)
				<-rateLimiter
				return err
			}
			originalDocument, err = task.State.RetracePreviousMutations(key, originalDocument, *task.gen, &fake)
			if err != nil {
				task.Result.ValidationFailures(docId)
				<-rateLimiter
				return err
			}
			ok, err := task.gen.Template.Compare(documentFromHost, originalDocument)
			if err != nil || !ok {
				task.Result.ValidationFailures(docId)
				<-rateLimiter
				return err
			}

			<-rateLimiter
			return nil
		})
	}
	_ = group.Wait()
	close(rateLimiter)
	close(dataChannel)
	log.Println(task.Operation, task.Bucket, task.Scope, task.Collection)
}
