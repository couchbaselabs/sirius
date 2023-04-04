package tasks

import (
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
	"sync"
)

type UpsertTask struct {
	ConnectionString string   `json:"connectionString"`
	Username         string   `json:"username"`
	Password         string   `json:"password"`
	Bucket           string   `json:"bucket"`
	Scope            string   `json:"scope,omitempty"`
	Collection       string   `json:"collection,omitempty"`
	Start            int64    `json:"start"`
	End              int64    `json:"end"`
	FieldsToChange   []string `json:"fieldsToChange,omitempty"`
	Operation        string
	State            *task_state.TaskState
	Result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
}

func (task *UpsertTask) Config() (int64, error) {
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
	if task.Start == 0 {
		task.Start = 1
		task.End = 1
	}
	if task.Start > task.End {
		return 0, fmt.Errorf("delete operation start to end range is malformed")
	}
	task.Operation = UpsertOperation

	// restore the original cluster state which should exist for operation, returns an error if task state don't exist else returns nil
	state, ok := task_state.ConfigTaskState(task.ConnectionString, task.Bucket, task.Scope, task.Collection, "", "", "",
		0, 0, 0)
	if !ok {
		return 0, fmt.Errorf("no such cluster's state exists for validation")
	}
	task.State = state
	return task.State.Seed, nil
}

func (task *UpsertTask) Describe() string {
	return `Upsert task mutates documents in bulk into a bucket.
The task will update the fields in a documents ranging from [start,end] inclusive.
We need to share the fields we want to update in a json document using SQL++ sytax.`
}

func (task *UpsertTask) Do() error {
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
	upsertDocuments(task)

	// close the sdk connection
	_ = task.connection.Close()

	// save the cluster result into the file
	if err := task.State.SaveTaskStateToFile(); err != nil {
		task.Result.ErrorOther = err.Error()
	}

	// calculated result success here to prevent late update in failure due to locking.
	task.Result.Success = task.End - task.Start - task.Result.Failure + 1

	// save the result into a file
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.State.Seed)
		return err
	}
	return nil
}

func upsertDocuments(task *UpsertTask) {
	var l sync.Mutex
	u := task_state.UpsertTaskState{
		Start:          task.Start,
		End:            task.End,
		FieldsToChange: task.FieldsToChange,
	}

	insertErrorCheck := make(map[int64]struct{})
	for _, k := range task.State.InsertTaskState.Err {
		insertErrorCheck[k] = struct{}{}
	}

	deleteCheck := make(map[int64]struct{})
	for _, k := range task.State.DeleteTaskState.Del {
		deleteCheck[k] = struct{}{}
	}

	rateLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	group := errgroup.Group{}

	for i := task.Start; i <= task.End; i++ {
		rateLimiter <- struct{}{}
		dataChannel <- i

		group.Go(func() error {
			var err error
			offset := (<-dataChannel) - 1
			key := task.gen.Seed + offset
			if _, ok := insertErrorCheck[key]; ok {
				<-rateLimiter
				return fmt.Errorf("key is in InsertErrorCheck")
			}
			if _, ok := deleteCheck[key]; ok {
				<-rateLimiter
				return fmt.Errorf("key is delete from the server")
			}
			docId := task.gen.BuildKey(key)

			fake := faker.NewWithSeed(rand.NewSource(key))
			originalDoc, err := task.gen.Template.GenerateDocument(&fake, task.State.DocumentSize)
			if err != nil {
				<-rateLimiter
				task.Result.IncrementFailure(docId, err.Error())
				return err
			}
			originalDoc, err = task.State.RetracePreviousMutations(key, originalDoc, *task.gen, &fake)
			if err != nil {
				<-rateLimiter
				return err
			}
			docUpdated, err := task.gen.Template.UpdateDocument(task.FieldsToChange, originalDoc, &fake)
			_, err = task.connection.Collection.Upsert(docId, docUpdated, nil)
			if err != nil {
				task.Result.IncrementFailure(docId, err.Error())
				l.Lock()
				u.Err = append(u.Err, offset+1)
				l.Unlock()
				<-rateLimiter
				return err
			}
			if key > task.State.SeedEnd {
				l.Lock()
				task.State.SeedEnd = key
				l.Unlock()
			}

			<-rateLimiter
			return nil
		})
	}
	_ = group.Wait()
	close(rateLimiter)
	close(dataChannel)
	task.State.UpsertTaskState = append(task.State.UpsertTaskState, u)
	log.Println(task.Operation, task.Bucket, task.Scope, task.Collection)
}
