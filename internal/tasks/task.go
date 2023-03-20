package tasks

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"sync"
	"time"
)

const ResultPath = "./internal/tasks/result-logs"
const TaskStatePath = "./internal/tasks/task-state"

type UserData struct {
	Seed int64 `json:"seed"`
}

type InsertTaskError struct {
	Err map[int64]struct{}
}

type UpsertTaskError struct {
	Err map[int64]struct{}
}

type DeleteTask struct {
	Del map[int64]struct{}
}

type UpsertTaskOperation struct {
	Start           int64
	End             int64
	FieldToChange   []string
	UpsertTaskError UpsertTaskError
}

type UpsertTask struct {
	Operation []UpsertTaskOperation
}

type TaskState struct {
	Host            string
	BUCKET          string
	SCOPE           string
	Collection      string
	Seed            int64
	SeedEnd         int64
	KeyPrefix       string
	KeySuffix       string
	InsertTaskError InsertTaskError
	DeleteTask      DeleteTask
	UpsertTask      UpsertTask
}

type TaskOperationCounter struct {
	Success int64      `json:"success"`
	Failure int64      `json:"failure"`
	lock    sync.Mutex `json:"-"`
}

type TaskResult struct {
	UserData             UserData             `json:"-"`
	error                error                `json:"error,omitempty"`
	TaskOperationCounter TaskOperationCounter `json:"task_operation_counter"`
}

type Task struct {
	UserData    UserData
	Request     *communication.TaskRequest
	TaskState   TaskState
	Result      TaskResult
	clientError error
}

func (t *Task) Handler() error {

	var connectionString string
	switch t.Request.Service {
	case communication.OnPremService:
		connectionString = "couchbase://" + t.Request.Host
	case communication.CapellaService:
		connectionString = "couchbases://" + t.Request.Host
	}

	cluster, err := gocb.Connect(connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: t.Request.Username,
			Password: t.Request.Password,
		},
	})
	if err != nil {
		t.clientError = err
		log.Println(err)
		return err
	}

	bucket := cluster.Bucket(t.Request.Bucket)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		t.clientError = err
		log.Println(err)
		return err
	}
	col := bucket.Scope(t.Request.Scope).Collection(t.Request.Collection)

	// Retrieve the original collection state if it existed.
	// TODO : Retrieved the original state of collection.
	taskState, err := t.readTaskStateFromFile()
	if err == nil {
		t.TaskState = taskState
		if t.Request.Operation == communication.InsertOperation {
			t.TaskState.SeedEnd += t.Request.Iteration * t.Request.BatchSize
		}
	} else {
		t.TaskState.SeedEnd = t.Request.Seed
	}

	// initialise a doc generator
	gen := docgenerator.Generator{
		DocType:   t.Request.DocType,
		KeyPrefix: t.Request.KeyPrefix,
		KeySuffix: t.Request.KeyPrefix,
		Seed:      t.Request.Seed,
		SeedEnd:   t.TaskState.SeedEnd,
		Template:  t.Request.Template,
	}

	// Call the doc loading operation
	switch t.Request.Operation {
	case communication.InsertOperation:
		t.insertDocuments(gen, col)
	case communication.UpsertOperation:
		log.Println("upsert")
	case communication.DeleteOperation:
		t.deleteDocument(gen, col)
	case communication.GetRangeOpertaion:
		log.Println("validated")
	}

	// Close connection and cluster.
	err = cluster.Close(nil)
	if err != nil {
		t.clientError = err
		log.Println(err)
		return err
	}

	// save the task-state
	if err := t.saveTaskStateToFile(); err != nil {
		t.Result.error = err
	}

	// save the task result-logs into a file
	if err := SaveResultIntoFile(t.Result); err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func (t *Task) insertDocuments(gen docgenerator.Generator, col *gocb.Collection) {

	for i := int64(0); i < t.Request.Iteration; i++ {
		wg := sync.WaitGroup{}
		wg.Add(int(t.Request.BatchSize))
		for index := int64(0); index < t.Request.BatchSize; index++ {
			go func(iteration, batchSize, index int64) {
				defer wg.Done()
				docId, key := gen.GetDocIdAndKey(iteration, t.Request.BatchSize, index)
				fake := faker.NewWithSeed(rand.NewSource(key))
				doc := gen.Template.GenerateDocument(&fake)
				_, err := col.Insert(docId, doc, nil)
				if err != nil {
					log.Println(err)
					t.incrementFailure()
					return
				}
			}(i, t.Request.BatchSize, index)
		}
		wg.Wait()
	}
	t.Result.TaskOperationCounter.Success = (t.Request.Iteration * t.Request.BatchSize) - t.Result.TaskOperationCounter.Failure
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection, t.Result.TaskOperationCounter.Success)
}

func (t *Task) deleteDocument(gen docgenerator.Generator, col *gocb.Collection) {
	// TODO : Delete Documents will  delete documents.
	// Ensure that document which we are going to delete should be present in the collection.
}

func (t *Task) incrementFailure() {
	t.Result.TaskOperationCounter.lock.Lock()
	t.Result.TaskOperationCounter.Failure++
	t.Result.TaskOperationCounter.lock.Unlock()
}
