package tasks

import (
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
	"strings"
	"time"
)

type UpsertTask struct {
	ConnectionString string        `json:"connectionString"`
	Username         string        `json:"username"`
	Password         string        `json:"password"`
	Bucket           string        `json:"bucket"`
	Scope            string        `json:"scope,omitempty"`
	Collection       string        `json:"collection,omitempty"`
	Start            int64         `json:"start"`
	End              int64         `json:"end"`
	FieldsToChange   []string      `json:"fieldsToChange"`
	Expiry           time.Duration `json:"expiry,omitempty"`
	PersistTo        uint          `json:"persistTo,omitempty"`
	ReplicateTo      uint          `json:"replicateTo,omitempty"`
	Durability       string        `json:"durability,omitempty"`
	Timeout          int           `json:"timeout,omitempty"`
	TemplateName     string        `json:"template"`
	DocSize          int64         `json:"docSize"`
	KeyPrefix        string        `json:"keyPrefix"`
	KeySuffix        string        `json:"keySuffix"`
	ResultSeed       int64
	DurabilityLevel  gocb.DurabilityLevel
	Operation        string
	TaskPending      bool
	State            *task_state.TaskState
	result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
	req              *Request
	index            int
}

func (task *UpsertTask) BuildIdentifier() string {
	if task.Bucket == "" {
		task.Bucket = DefaultBucket
	}
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

func (task *UpsertTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *UpsertTask) Config(req *Request, seed int64, seedEnd int64, index int, reRun bool) (int64, error) {
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
		if task.DocSize == 0 {
			task.DocSize = docgenerator.DefaultDocSize
		}
		if task.Start > task.End {
			return 0, fmt.Errorf("delete operation start to end range is malformed")
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
		if task.Timeout == 0 {
			task.Timeout = 10
		}
		task.Operation = UpsertOperation
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
	return task.ResultSeed, nil
}

func (task *UpsertTask) Describe() string {
	return `Upsert task mutates documents in bulk into a bucket.
The task will update the fields in a documents ranging from [start,end] inclusive.
We need to share the fields we want to update in a json document using SQL++ syntax.`
}

func (task *UpsertTask) tearUp() error {
	_ = task.connection.Close()
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *UpsertTask) Do() error {

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

	task.gen = docgenerator.ConfigGenerator("", task.State.KeyPrefix, task.State.KeySuffix,
		task.State.SeedStart, task.State.SeedEnd, template.InitialiseTemplate(task.State.TemplateName))

	upsertDocuments(task)

	task.result.Success = task.End - task.Start - task.result.Failure

	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	task.State.ClearCompletedKeyStates()
	return task.tearUp()
}

func upsertDocuments(task *UpsertTask) {
	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	maxKey := int64(-1)
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

	group := errgroup.Group{}
	for i := task.Start; i < task.End; i++ {
		routineLimiter <- struct{}{}
		dataChannel <- i

		group.Go(func() error {
			var err error
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
			fake := faker.NewWithSeed(rand.NewSource(key))
			originalDoc, err := task.gen.Template.GenerateDocument(&fake, task.State.DocumentSize)
			if err != nil {
				<-routineLimiter
				task.result.IncrementFailure(docId, err.Error())
				return err
			}
			originalDoc, err = task.req.retracePreviousMutations(offset, originalDoc, *task.gen, &fake, task.ResultSeed)
			if err != nil {
				<-routineLimiter
				return err
			}
			docUpdated, err := task.gen.Template.UpdateDocument(task.FieldsToChange, originalDoc, &fake)
			_, err = task.connection.Collection.Upsert(docId, docUpdated, &gocb.UpsertOptions{
				Expiry:          task.Expiry,
				PersistTo:       task.PersistTo,
				ReplicateTo:     task.ReplicateTo,
				DurabilityLevel: task.DurabilityLevel,
				Timeout:         time.Duration(task.Timeout) * time.Second,
			})
			if err != nil {
				task.result.IncrementFailure(docId, err.Error())
				task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
				<-routineLimiter
				return err
			}

			task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
			if offset > maxKey {
				maxKey = offset
			}
			<-routineLimiter
			return nil
		})
	}
	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	task.req.checkAndUpdateSeedEnd(maxKey)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}
