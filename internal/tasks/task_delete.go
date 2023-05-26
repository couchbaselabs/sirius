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
	IdentifierToken  string `json:"identifierToken"`
	ConnectionString string `json:"connectionString"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	Host             string `json:"host"`
	Bucket           string `json:"bucket"`
	Scope            string `json:"scope,omitempty"`
	Collection       string `json:"collection,omitempty"`
	Start            int64  `json:"start"`
	End              int64  `json:"end"`
	PersistTo        uint   `json:"persistTo,omitempty"`
	ReplicateTo      uint   `json:"replicateTo,omitempty"`
	Durability       string `json:"durability,omitempty"`
	Timeout          int    `json:"timeout,omitempty"`
	ResultSeed       int64
	Operation        string
	DurabilityLevel  gocb.DurabilityLevel
	TaskPending      bool
	State            *task_state.TaskState
	result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
	req              *Request
	index            int
}

func (task *DeleteTask) BuildIdentifier() string {
	if task.Bucket == "" {
		task.Bucket = DefaultBucket
	}
	if task.Scope == "" {
		task.Scope = DefaultScope
	}
	if task.Collection == "" {
		task.Collection = DefaultCollection
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s", task.Username, task.IdentifierToken, task.Bucket, task.Scope, task.Collection)
}

func (task *DeleteTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config checks the validity of DeleteTask
func (task *DeleteTask) Config(req *Request, seed int64, seedEnd int64, index int, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req
	if task.req == nil {
		return 0, fmt.Errorf("request.Request struct is nil")
	}
	task.index = index
	if !reRun {
		if task.IdentifierToken == "" {
			return 0, fmt.Errorf("identifier token is missing")
		}
		if task.ConnectionString == "" {
			return 0, fmt.Errorf("empty connection string")
		}
		if task.Username == "" || task.Password == "" {
			return 0, fmt.Errorf("cluster's credentials are missing ")
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
		if task.Start < 0 {
			task.Start = 0
			task.End = 0
		}
		if task.Start > task.End {
			return 0, fmt.Errorf("delete operation start to end range is malformed")
		}
		task.Operation = DeleteOperation
		if task.Timeout == 0 {
			task.Timeout = 10
		}
		switch task.Durability {
		case DurabilityLevelMajority:
			task.DurabilityLevel = gocb.DurabilityLevelMajority
		case DurabilityLevelMajorityAndPersistToActive:
			task.DurabilityLevel = gocb.DurabilityLevelMajorityAndPersistOnMaster
		case DurabilityLevelPersistToMajority:
			task.DurabilityLevel = gocb.DurabilityLevelPersistToMajority
		default:
			task.DurabilityLevel = gocb.DurabilityLevelNone
		}
		time.Sleep(1 * time.Microsecond)
		task.ResultSeed = time.Now().UnixNano()
		task.State = task_state.ConfigTaskState("", "", "", 0, seed,
			seedEnd, task.ResultSeed)
	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
		if task.State == nil {
			return task.ResultSeed, fmt.Errorf("task State is nil")
		}
		task.State.SetupStoringKeys()
	}
	return task.ResultSeed, nil
}

func (task *DeleteTask) Describe() string {
	return `Delete task deletes documents in bulk into a bucket.
The task will delete documents from [start,end] inclusive.`
}

func (task *DeleteTask) tearUp() error {
	_ = task.connection.Close()
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}
func (task *DeleteTask) Do() error {
	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	task.connection = sdk.ConfigConnectionManager(task.ConnectionString, task.Username, task.Password,
		task.Bucket, task.Scope, task.Collection)

	if err := task.connection.Connect(); err != nil {
		task.result.ErrorOther = err.Error()
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.ResultSeed)
		}
		task.State.AddRangeToErrSet(task.Start, task.End)
		return task.tearUp()
	}

	// Prepare generator
	task.gen = docgenerator.ConfigGenerator("", task.State.KeyPrefix, task.State.KeySuffix,
		task.State.SeedStart, task.State.SeedEnd, template.InitialiseTemplate(task.State.TemplateName))

	deleteDocuments(task)

	// calculated result success here to prevent late update in failure due to locking.
	task.result.Success = task.End - task.Start - task.result.Failure

	// save the result into a file
	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	task.State.ClearErrorKeyStates()
	return task.tearUp()
}

// deleteDocuments delete the document stored on a host from start to end.
func deleteDocuments(task *DeleteTask) {
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

	for i := task.Start; i < task.End; i++ {
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
			_, err := task.connection.Collection.Remove(docId, &gocb.RemoveOptions{
				PersistTo:       task.PersistTo,
				ReplicateTo:     task.ReplicateTo,
				DurabilityLevel: task.DurabilityLevel,
				Timeout:         time.Duration(task.Timeout) * time.Second,
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
