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
	"sync"
	"time"
)

type InsertTask struct {
	ConnectionString string        `json:"connectionString"`
	Username         string        `json:"username"`
	Password         string        `json:"password"`
	Bucket           string        `json:"bucket"`
	Scope            string        `json:"scope,omitempty"`
	Collection       string        `json:"collection,omitempty"`
	Count            int64         `json:"count,omitempty"`
	DocSize          int64         `json:"docSize,omitempty"`
	DocType          string        `json:"docType,omitempty"`
	KeySize          int           `json:"keySize,omitempty"`
	KeyPrefix        string        `json:"keyPrefix,omitempty"`
	KeySuffix        string        `json:"keySuffix,omitempty"`
	RandomDocSize    bool          `json:"randomDocSize,omitempty"`
	RandomKeySize    bool          `json:"randomKeySize,omitempty"`
	Expiry           time.Duration `json:"expiry,omitempty"`
	PersistTo        uint          `json:"persistTo,omitempty"`
	ReplicateTo      uint          `json:"replicateTo,omitempty"`
	Durability       string        `json:"durability,omitempty"`
	Timeout          int           `json:"timeout,omitempty"`
	ReadYourOwnWrite bool          `json:"readYourOwnWrite,omitempty"`
	TemplateName     string        `json:"template,omitempty"`
	Template         template.Template
	Seed             int64
	DurabilityLevel  gocb.DurabilityLevel
	Operation        string
	State            *task_state.TaskState
	Result           *task_result.TaskResult
	connection       *sdk.ConnectionManager
	gen              *docgenerator.Generator
}

// Config configures  the insert task
func (task *InsertTask) Config() (int64, error) {
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
	task.Seed = time.Now().UnixNano()
	// restore the original cluster state
	task.State, _ = task_state.ConfigTaskState(task.ConnectionString, task.Bucket, task.Scope, task.Collection, task.TemplateName, task.KeyPrefix, task.KeySuffix,
		task.DocSize, task.Seed, task.Seed)
	return task.State.Seed, nil
}

func (task *InsertTask) Describe() string {
	return " Insert task uploads documents in bulk into a bucket.\n" +
		"The durability while inserting a document can be set using following values in the 'durability' JSON tag :-\n" +
		"1. MAJORITY\n" +
		"2. MAJORITY_AND_PERSIST_TO_ACTIVE\n" +
		"3. PERSIST_TO_MAJORITY\n"
}

func (task *InsertTask) Do() error {
	//prepare a result for the task
	task.Result = task_result.ConfigTaskResult(task.Operation, task.State.Seed)

	// establish a connection
	task.connection = sdk.ConfigConnectionManager(task.ConnectionString, task.Username, task.Password,
		task.Bucket, task.Scope, task.Collection)

	if err := task.connection.Connect(); err != nil {
		task.Result.ErrorOther = err.Error()
		if err := task.Result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.Seed)
			return err
		}
		return err
	}

	// Prepare generator
	task.gen = docgenerator.ConfigGenerator(task.DocType, task.KeyPrefix, task.KeySuffix, task.State.Seed, task.State.SeedEnd, task.Template)

	// do bulk loading
	insertDocuments(task)

	// close the connection
	_ = task.connection.Close()

	// save the cluster result into the file
	if err := task.State.SaveTaskStateToFile(); err != nil {
		task.Result.ErrorOther = err.Error()
	}

	// calculated result success here to prevent late update in failure due to locking.
	task.Result.Success = task.Count - task.Result.Failure

	// save the result into a file
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.State.Seed)
		return err
	}
	return nil
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func insertDocuments(task *InsertTask) {
	var l sync.Mutex
	rateLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)
	group := errgroup.Group{}

	for iteration := int64(0); iteration < task.Count; iteration++ {

		rateLimiter <- struct{}{}
		dataChannel <- iteration

		group.Go(func() error {
			iteration := <-dataChannel
			docId, key := task.gen.GetDocIdAndKey(iteration)
			fake := faker.NewWithSeed(rand.NewSource(key))
			doc, err := task.gen.Template.GenerateDocument(&fake, task.State.DocumentSize)
			if err != nil {
				task.Result.IncrementFailure(docId, err.Error())
				<-rateLimiter
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
				ok, err := task.gen.Template.Compare(documentFromHost, doc)
				if err != nil || !ok {
					task.Result.ValidationFailures(docId)
					<-rateLimiter
					return err
				}
			} else {
				if err != nil {
					task.Result.IncrementFailure(docId, err.Error())
					l.Lock()
					task.State.InsertTaskState.Err = append(task.State.InsertTaskState.Err, key)
					l.Unlock()
					<-rateLimiter
					return err
				}
			}
			<-rateLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(rateLimiter)
	close(dataChannel)

	task.State.SeedEnd += task.Count
	log.Println(task.Operation, task.Bucket, task.Scope, task.Collection)
}
