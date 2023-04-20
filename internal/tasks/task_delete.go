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
	"strings"
	"time"
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
	ResultSeed       int64
	Operation        string
	TaskPending      bool
	State            *task_state.TaskState
	result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
	req              *Request
}

func (task *DeleteTask) BuildIdentifier() string {
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

func (task *DeleteTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config checks the validity of DeleteTask
func (task *DeleteTask) Config(req *Request, seed int64, seedEnd int64, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req
	if task.req == nil {
		return 0, fmt.Errorf("request.Request struct is nil")
	}
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
		if task.Start == 0 {
			task.Start = 1
			task.End = 1
		}
		if task.Start > task.End {
			return 0, fmt.Errorf("delete operation start to end range is malformed")
		}
		task.Operation = DeleteOperation
		time.Sleep(1 * time.Microsecond)
		task.ResultSeed = time.Now().UnixNano()
		task.State = task_state.ConfigTaskState("", "", "", 0, seed,
			seedEnd, task.ResultSeed)
	} else {
		if task.State != nil {
			task.State.SetupStoringKeys()
			task.State.StoreState()
		}
		log.Println("Retrying " + task.Operation + " " + task.req.Identifier + " " + string(task.ResultSeed))
	}

	return task.ResultSeed, nil
}

func (task *DeleteTask) Describe() string {
	return `Delete task deletes documents in bulk into a bucket.
The task will delete documents from [start,end] inclusive.`
}

func (task *DeleteTask) tearUp() error {
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}
func (task *DeleteTask) Do() error {
	// prepare a result for the task
	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	// establish a connection
	task.connection = sdk.ConfigConnectionManager(task.ConnectionString, task.Username, task.Password,
		task.Bucket, task.Scope, task.Collection)

	if err := task.connection.Connect(); err != nil {
		task.result.ErrorOther = err.Error()
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.State.SeedStart)
		}
		task.tearUp()
	}

	// Prepare generator
	task.gen = docgenerator.ConfigGenerator("", task.State.KeyPrefix, task.State.KeySuffix,
		task.State.SeedStart, task.State.SeedEnd, template.InitialiseTemplate(task.State.TemplateName))

	// do bulk loading
	if err := deleteDocuments(task); err != nil {
		task.result.ErrorOther = err.Error()
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.ResultSeed)
		}
		task.tearUp()
	}

	// close the sdk connection
	_ = task.connection.Close()

	// calculated result success here to prevent late update in failure due to locking.
	task.result.Success = task.End - task.Start - task.result.Failure + 1

	// save the result into a file
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
		return task.tearUp()
	}
	return task.tearUp()
}

// deleteDocuments delete the document stored on a host from start to end.
func deleteDocuments(task *DeleteTask) error {
	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}
	deletedOffset, err := task.req.retracePreviousDeletions(task.ResultSeed)
	if err != nil {
		return nil
	}
	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	group := errgroup.Group{}

	for i := task.Start; i <= task.End; i++ {
		routineLimiter <- struct{}{}
		dataChannel <- i

		group.Go(func() error {
			offset := (<-dataChannel) - 1
			key := task.req.Seed + offset
			docId := task.gen.BuildKey(key)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}
			if _, ok := deletedOffset[offset+1]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday deleted docID on " + docId)
			}
			if key > task.req.SeedEnd || key < task.req.Seed {
				task.result.IncrementFailure(docId, "docId out of bound")
				<-routineLimiter
				return fmt.Errorf("docId out of bound")
			}
			_, err := task.connection.Collection.Remove(docId, nil)
			if err != nil {
				task.result.IncrementFailure(docId, err.Error())
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
	log.Println(task.Operation, task.Bucket, task.Scope, task.Collection, task.ResultSeed)
	return task.tearUp()
}
