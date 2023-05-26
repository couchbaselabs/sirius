package tasks

import (
	"encoding/json"
	"errors"
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

type InsertTask struct {
	IdentifierToken  string        `json:"identifierToken"`
	ConnectionString string        `json:"connectionString"`
	Username         string        `json:"username"`
	Password         string        `json:"password"`
	Bucket           string        `json:"bucket"`
	Scope            string        `json:"scope,omitempty"`
	Collection       string        `json:"collection,omitempty"`
	Count            int64         `json:"count,omitempty"`
	DocSize          int64         `json:"docSize"`
	DocType          string        `json:"docType,omitempty"`
	KeySize          int           `json:"keySize,omitempty"`
	KeyPrefix        string        `json:"keyPrefix"`
	KeySuffix        string        `json:"keySuffix"`
	RandomDocSize    bool          `json:"randomDocSize,omitempty"`
	RandomKeySize    bool          `json:"randomKeySize,omitempty"`
	Expiry           time.Duration `json:"expiry,omitempty"`
	PersistTo        uint          `json:"persistTo,omitempty"`
	ReplicateTo      uint          `json:"replicateTo,omitempty"`
	Durability       string        `json:"durability,omitempty"`
	Timeout          int           `json:"timeout,omitempty"`
	ReadYourOwnWrite bool          `json:"readYourOwnWrite,omitempty"`
	TemplateName     string        `json:"template"`
	Template         interface{}
	DurabilityLevel  gocb.DurabilityLevel
	Operation        string
	ResultSeed       int64
	TaskPending      bool
	State            *task_state.TaskState
	result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
	req              *Request
	index            int
}

func (task *InsertTask) BuildIdentifier() string {
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

func (task *InsertTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *InsertTask) Config(req *Request, seed int64, seedEnd int64, index int, reRun bool) (int64, error) {
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
		if task.DocType == "" {
			task.DocType = docgenerator.JsonDocument
		}

		if task.KeySize == 0 || task.KeySize > docgenerator.DefaultKeySize {
			task.KeySize = docgenerator.DefaultKeySize
		}
		if task.Count == 0 {
			task.Count = 1
		}
		if task.DocSize == 0 {
			task.DocSize = docgenerator.DefaultDocSize
		}
		if task.Timeout == 0 {
			task.Timeout = 10
		}
		task.Operation = InsertOperation
		task.Template = template.InitialiseTemplate(task.TemplateName)
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
		time.Sleep(1 * time.Microsecond) // this sleep ensures that seed generated is always different.
		task.ResultSeed = time.Now().UnixNano()
		task.State = task_state.ConfigTaskState(task.TemplateName, task.KeyPrefix, task.KeySuffix, task.DocSize, seed,
			seedEnd, task.ResultSeed)
		if err := task.req.AddToSeedEnd(task.Count); err != nil {
			return 0, err
		}
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

func (task *InsertTask) Describe() string {
	return " Insert task uploads documents in bulk into a bucket.\n" +
		"The durability while inserting a document can be set using following values in the 'durability' JSON tag :-\n" +
		"1. MAJORITY\n" +
		"2. MAJORITY_AND_PERSIST_TO_ACTIVE\n" +
		"3. PERSIST_TO_MAJORITY\n"
}

func (task *InsertTask) tearUp() error {
	_ = task.connection.Close()
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *InsertTask) Do() error {

	task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	task.connection = sdk.ConfigConnectionManager(task.ConnectionString, task.Username, task.Password,
		task.Bucket, task.Scope, task.Collection)

	if err := task.connection.Connect(); err != nil {
		task.result.ErrorOther = err.Error()
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.ResultSeed)
			return err
		}
		_ = task.req.RemoveFromSeedEnd(task.Count)
		return task.tearUp()
	}

	task.gen = docgenerator.ConfigGenerator(task.DocType, task.KeyPrefix, task.KeySuffix, task.State.SeedStart,
		task.State.SeedEnd, template.InitialiseTemplate(task.TemplateName))

	insertDocuments(task)
	task.result.Success = task.Count - task.result.Failure

	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}
	// Inserted Has been completed, We can remove successful completions.
	if err := task.State.SaveTaskSateOnDisk(); err != nil {
		log.Println("Error in storing TASK state on DISK")
	}
	task.State.ClearCompletedKeyStates()
	return task.tearUp()
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func insertDocuments(task *InsertTask) {

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)

	skip := make(map[int64]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}
	group := errgroup.Group{}
	for iteration := int64(0); iteration < task.Count; iteration++ {

		routineLimiter <- struct{}{}
		dataChannel <- iteration

		group.Go(func() error {
			offset := <-dataChannel
			docId, key := task.gen.GetDocIdAndKey(offset)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}
			fake := faker.NewWithSeed(rand.NewSource(key))
			doc, err := task.gen.Template.GenerateDocument(&fake, task.State.DocumentSize)
			if err != nil {
				task.result.IncrementFailure(docId, doc, err)
				<-routineLimiter
				return err
			}
			_, err = task.connection.Collection.Insert(docId, doc, &gocb.InsertOptions{
				DurabilityLevel: task.DurabilityLevel,
				PersistTo:       task.PersistTo,
				ReplicateTo:     task.ReplicateTo,
				Timeout:         time.Duration(task.Timeout) * time.Second,
			})
			if task.ReadYourOwnWrite {
				var resultFromHost map[string]interface{}
				documentFromHost := template.InitialiseTemplate(task.TemplateName)
				result, err := task.connection.Collection.Get(docId, nil)
				if err != nil {
					task.result.IncrementFailure(docId, doc, err)
					<-routineLimiter
					return err
				}
				if err := result.Content(&resultFromHost); err != nil {
					task.result.IncrementFailure(docId, doc, err)
					<-routineLimiter
					return err
				}
				resultBytes, err := json.Marshal(resultFromHost)
				err = json.Unmarshal(resultBytes, &documentFromHost)
				if err != nil {
					task.result.ValidationFailures(docId)
					<-routineLimiter
					return err
				}
				ok, err := task.gen.Template.Compare(documentFromHost, doc)
				if err != nil || !ok {
					task.result.ValidationFailures(docId)
					<-routineLimiter
					return err
				}
			} else {
				if err != nil {
					if errors.Is(err, gocb.ErrDocumentExists) {
						task.State.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
						<-routineLimiter
						return nil
					} else {
						task.result.IncrementFailure(docId, doc, err)
						task.State.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
						<-routineLimiter
						return err
					}
				}
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
