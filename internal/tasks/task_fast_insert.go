package tasks

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"time"
)

type FastInsertTask struct {
	IdentifierToken string                  `json:"identifierToken" doc:"true"`
	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
	Bucket          string                  `json:"bucket" doc:"true"`
	Scope           string                  `json:"scope,omitempty" doc:"true"`
	Collection      string                  `json:"collection,omitempty" doc:"true"`
	InsertOptions   *InsertOptions          `json:"insertOptions,omitempty" doc:"true"`
	OperationConfig *OperationConfig        `json:"operationConfig,omitempty" doc:"true"`
	Template        interface{}             `json:"-" doc:"false"`
	Operation       string                  `json:"operation" doc:"false"`
	ResultSeed      int64                   `json:"resultSeed" doc:"false"`
	TaskPending     bool                    `json:"taskPending" doc:"false"`
	Result          *task_result.TaskResult `json:"Result" doc:"false"`
	gen             *docgenerator.Generator `json:"-" doc:"false"`
	req             *Request                `json:"-" doc:"false"`
}

func (task *FastInsertTask) Describe() string {
	return "Fast Insert task uploads documents in bulk into a bucket without maintaining intermediate state of task" +
		" \n" +
		"During fast operations, " +
		"An incomplete task will be retied as whole if server dies in between of the operation.\n "
}

func (task *FastInsertTask) BuildIdentifier() string {
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

func (task *FastInsertTask) CheckIfPending() bool {
	return task.TaskPending
}

// Config configures  the insert task
func (task *FastInsertTask) Config(req *Request, seed int64, seedEnd int64, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req

	if task.req == nil {
		return 0, fmt.Errorf("request.Request struct is nil")
	}

	task.req.ReconnectionManager()
	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
		return 0, err
	}

	if !reRun {
		task.ResultSeed = time.Now().UnixNano()
		task.Operation = InsertOperation
		task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

		if task.IdentifierToken == "" {
			task.Result.ErrorOther = "identifier token is missing"
		}

		if err := configInsertOptions(task.InsertOptions); err != nil {
			task.Result.ErrorOther = err.Error()
		}

		if err := configureOperationConfig(task.OperationConfig); err != nil {
			task.Result.ErrorOther = err.Error()
		}

		task.Operation = InsertOperation

		task.Template = template.InitialiseTemplate(task.OperationConfig.TemplateName)

		if err := task.req.AddToSeedEnd(task.OperationConfig.Count); err != nil {
			return 0, err
		}
	}
	log.Println(req.SeedEnd)
	return task.ResultSeed, nil
}

func (task *FastInsertTask) tearUp() error {
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *FastInsertTask) Do() error {

	if task.Result != nil && task.Result.ErrorOther != "" {
		log.Println(task.Result.ErrorOther)
		if err := task.Result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save Result into ", task.ResultSeed)
			return err
		}
		return task.tearUp()
	} else {
		task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
	}

	collection, err1 := task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		if err := task.Result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save Result into ", task.ResultSeed)
			return err
		}
		_ = task.req.RemoveFromSeedEnd(task.OperationConfig.Count)
		return task.tearUp()
	}

	task.gen = docgenerator.ConfigGenerator(task.OperationConfig.DocType, task.OperationConfig.KeyPrefix,
		task.OperationConfig.KeySuffix, task.req.Seed, task.req.SeedEnd,
		template.InitialiseTemplate(task.OperationConfig.TemplateName))

	fastInsertDocuments(task, collection)
	task.Result.Success = task.OperationConfig.Count - task.Result.Failure

	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}

	return task.tearUp()
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func fastInsertDocuments(task *FastInsertTask, collection *gocb.Collection) {

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan int64, MaxConcurrentRoutines)

	group := errgroup.Group{}
	for iteration := int64(0); iteration < task.OperationConfig.Count; iteration++ {

		routineLimiter <- struct{}{}
		dataChannel <- iteration

		group.Go(func() error {
			offset := <-dataChannel
			docId, key := task.gen.GetDocIdAndKey(offset)
			fake := faker.NewWithSeed(rand.NewSource(key))
			doc, err := task.gen.Template.GenerateDocument(&fake, task.OperationConfig.DocSize)
			if err != nil {
				task.Result.IncrementFailure(docId, doc, err)
				<-routineLimiter
				return err
			}
			_, err = collection.Insert(docId, doc, &gocb.InsertOptions{
				DurabilityLevel: getDurability(task.InsertOptions.Durability),
				PersistTo:       task.InsertOptions.PersistTo,
				ReplicateTo:     task.InsertOptions.ReplicateTo,
				Timeout:         time.Duration(task.InsertOptions.Timeout) * time.Second,
			})
			if task.OperationConfig.ReadYourOwnWrite {
				var resultFromHost map[string]interface{}
				documentFromHost := template.InitialiseTemplate(task.OperationConfig.TemplateName)
				result, err := collection.Get(docId, nil)
				if err != nil {
					task.Result.IncrementFailure(docId, doc, err)
					<-routineLimiter
					return err
				}
				if err := result.Content(&resultFromHost); err != nil {
					task.Result.IncrementFailure(docId, doc, err)
					<-routineLimiter
					return err
				}
				resultBytes, err := json.Marshal(resultFromHost)
				err = json.Unmarshal(resultBytes, &documentFromHost)
				if err != nil {
					task.Result.IncrementFailure(docId, doc, err)
					<-routineLimiter
					return err
				}
				ok, err := task.gen.Template.Compare(documentFromHost, doc)
				if err != nil || !ok {
					task.Result.IncrementFailure(docId, doc, errors.New("read your own write failure"))
					<-routineLimiter
					return err
				}
			} else {
				if err != nil {
					task.Result.IncrementFailure(docId, doc, err)
					<-routineLimiter
					return err
				}
			}

			<-routineLimiter
			return nil
		})
	}

	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}
