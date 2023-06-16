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
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"scope,omitempty" doc:"true"`
	Collection      string                  `json:"collection,omitempty" doc:"true"`
	InsertOptions   *InsertOptions          `json:"insertOptions,omitempty" doc:"true"`
	OperationConfig *OperationConfig        `json:"operationConfig,omitempty" doc:"true"`
	Template        interface{}             `json:"-" doc:"false"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int                     `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	State           *task_state.TaskState   `json:"State" doc:"false"`
	result          *task_result.TaskResult `json:"-" doc:"false"`
	gen             *docgenerator.Generator `json:"-" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *InsertTask) Describe() string {
	return " Insert task uploads documents in bulk into a bucket.\n" +
		"The durability while inserting a document can be set using following values in the 'durability' JSON tag :-\n" +
		"1. MAJORITY\n" +
		"2. MAJORITY_AND_PERSIST_TO_ACTIVE\n" +
		"3. PERSIST_TO_MAJORITY\n"
}

func (task *InsertTask) BuildIdentifier() string {
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

func (task *InsertTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *InsertTask) Config(req *Request, seed int, seedEnd int, reRun bool) (int, error) {
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
		task.ResultSeed = int(time.Now().UnixNano())
		task.Operation = InsertOperation
		task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

		if task.IdentifierToken == "" {
			task.result.ErrorOther = "identifier token is missing"
		}

		if err := configInsertOptions(task.InsertOptions); err != nil {
			task.result.ErrorOther = err.Error()
		}

		if err := configureOperationConfig(task.OperationConfig); err != nil {
			task.result.ErrorOther = err.Error()
		}

		task.Template = template.InitialiseTemplate(task.OperationConfig.TemplateName)

		task.State = task_state.ConfigTaskState(task.OperationConfig.TemplateName, task.OperationConfig.KeyPrefix,
			task.OperationConfig.KeySuffix, task.OperationConfig.DocSize, seed, seedEnd, task.ResultSeed)

		if err := task.req.AddToSeedEnd(task.OperationConfig.Count); err != nil {
			return 0, err
		}
	} else {
		if task.State == nil {
			return task.ResultSeed, fmt.Errorf("task State is nil")
		}

		task.State.SetupStoringKeys()

		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *InsertTask) tearUp() error {
	task.result = nil
	task.State.StopStoringState()
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *InsertTask) Do() error {

	if task.result != nil && task.result.ErrorOther != "" {
		log.Println(task.result.ErrorOther)
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.ResultSeed)
			return err
		}
		return task.tearUp()
	} else {
		task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	}

	collection, err1 := task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)

	if err1 != nil {
		task.result.ErrorOther = err1.Error()
		if err := task.result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save result into ", task.ResultSeed)
			return err
		}
		_ = task.req.RemoveFromSeedEnd(task.OperationConfig.Count)
		return task.tearUp()
	}

	task.gen = docgenerator.ConfigGenerator(task.OperationConfig.DocType, task.OperationConfig.KeyPrefix,
		task.OperationConfig.KeySuffix, task.State.SeedStart, task.State.SeedEnd,
		template.InitialiseTemplate(task.OperationConfig.TemplateName))

	insertDocuments(task, collection)
	task.result.Success = task.OperationConfig.Count - task.result.Failure

	if err := task.result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save result into ", task.ResultSeed)
	}

	// Use this case to store task's state on disk when required
	//if err := task.State.SaveTaskSateOnDisk(); err != nil {
	//	log.Println("Error in storing TASK State on DISK")
	//}

	task.State.ClearCompletedKeyStates()
	return task.tearUp()
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func insertDocuments(task *InsertTask, collection *gocb.Collection) {

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int, MaxConcurrentRoutines)

	skip := make(map[int]struct{})
	for _, offset := range task.State.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range task.State.KeyStates.Err {
		skip[offset] = struct{}{}
	}
	group := errgroup.Group{}
	for iteration := int(0); iteration < task.OperationConfig.Count; iteration++ {

		routineLimiter <- struct{}{}
		dataChannel <- iteration

		group.Go(func() error {
			offset := <-dataChannel
			docId, key := task.gen.GetDocIdAndKey(offset)
			if _, ok := skip[offset]; ok {
				<-routineLimiter
				return fmt.Errorf("alreday performed operation on " + docId)
			}
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			doc, err := task.gen.Template.GenerateDocument(&fake, task.State.DocumentSize)
			if err != nil {
				task.result.IncrementFailure(docId, doc, err)
				<-routineLimiter
				return err
			}
			_, err = collection.Insert(docId, doc, &gocb.InsertOptions{
				DurabilityLevel: getDurability(task.InsertOptions.Durability),
				PersistTo:       task.InsertOptions.PersistTo,
				ReplicateTo:     task.InsertOptions.ReplicateTo,
				Timeout:         time.Duration(task.InsertOptions.Timeout) * time.Second,
				Expiry:          time.Duration(task.InsertOptions.Expiry) * time.Second,
			})
			if task.OperationConfig.ReadYourOwnWrite {
				var resultFromHost map[string]interface{}
				documentFromHost := template.InitialiseTemplate(task.OperationConfig.TemplateName)
				result, err := collection.Get(docId, nil)
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
