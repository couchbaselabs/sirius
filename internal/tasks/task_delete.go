package tasks

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/template"
	"golang.org/x/sync/errgroup"
	"log"
	"sync"
)

type DeleteTask struct {
	ConnectionString string `json:"connectionString"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	Host             string `json:"host"`
	Bucket           string `json:"bucket"`
	Scope            string `json:"scope,omitempty"`
	Collection       string `json:"collection,omitempty"`
	Start            int64  `json:"start"`
	End              int64  `json:"end"`
	Operation        string
	State            *task_state.TaskState
	Result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
}

// Config checks the validity of DeleteTask
func (task *DeleteTask) Config() (int64, error) {
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
	task.Operation = DeleteOperation

	// restore the original cluster state which should exist for operation, returns an error if task state don't exist else returns nil
	state, ok := task_state.ConfigTaskState(task.ConnectionString, task.Bucket, task.Scope, task.Collection, "", "", "",
		0, 0, 0)
	if !ok {
		return 0, fmt.Errorf("no such cluster's state exists for deletion")
	}
	task.State = state
	return task.State.Seed, nil
}

func (task *DeleteTask) Describe() string {
	return `Delete task deletes documents in bulk into a bucket.
The task will delete documents from [start,end] inclusive.`
}

func (task *DeleteTask) Do() error {
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
	deleteDocuments(task)

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

// deleteDocuments delete the document stored on a host from start to end.
func deleteDocuments(task *DeleteTask) {
	d := task_state.DeleteTaskState{
		Start: task.Start,
		End:   task.End,
	}

	var l sync.Mutex
	insertErrorCheck := make(map[int64]struct{})
	for _, k := range task.State.InsertTaskState.Err {
		insertErrorCheck[k] = struct{}{}
	}

	deleteCheck := task.State.RetracePreviousDeletions()

	rateLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	group := errgroup.Group{}

	for i := task.Start; i <= task.End; i++ {
		rateLimiter <- struct{}{}
		dataChannel <- i

		group.Go(func() error {
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
			if key > task.State.SeedEnd || key < task.State.Seed {
				task.Result.IncrementFailure(docId, "docId out of bound")
				<-rateLimiter
				return fmt.Errorf("docId out of bound")
			}
			_, err := task.connection.Collection.Remove(docId, nil)
			if err != nil {
				task.Result.IncrementFailure(docId, err.Error())
				l.Lock()
				d.Err = append(d.Err, offset)
				l.Unlock()
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
	task.State.DeleteTaskState = append(task.State.DeleteTaskState, d)
	log.Println(task.Operation, task.Bucket, task.Scope, task.Collection)
}
