package tasks

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"log"
	"sync"
	"time"
)

const ResultPath = "./internal/tasks/result-logs"
const TaskStatePath = "./internal/tasks/task-state"
const MaxConcurrentOps = 1500

type UserData struct {
	Seed int64 `json:"seed"`
}

type InsertTask struct {
	Err []int64
}

type DeleteTask struct {
	Del []int64
}

type UpsertTask struct {
	Start         int64
	End           int64
	FieldToChange []string
	Err           []int64
}

type TaskState struct {
	Host         string
	BUCKET       string
	SCOPE        string
	Collection   string
	DocumentSize int64
	Seed         int64
	SeedEnd      int64
	KeyPrefix    string
	KeySuffix    string
	InsertTask   InsertTask
	DeleteTask   DeleteTask
	UpsertTask   []UpsertTask
}

type TaskOperationCounter struct {
}

type TaskResult struct {
	UserData        UserData                        `json:"-"`
	Operation       communication.DocumentOperation `json:"operation"`
	ErrorOther      string                          `json:"errorOther,omitempty"`
	Success         int64                           `json:"success"`
	Failure         int64                           `json:"failure"`
	ValidationError []string                        `json:"validationError,omitempty"`
	Error           map[string][]string             `json:"errors"`
	lock            sync.Mutex
}

type Task struct {
	UserData    UserData
	Request     *communication.TaskRequest
	TaskState   TaskState
	Result      TaskResult
	clientError error
}

// Handler is executed after a task is scheduled and proceeds  with the doc type operation.
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
	t.clientError = err
	if t.clientError != nil {
		t.Result.ErrorOther = t.clientError.Error()
		if err := SaveResultIntoFile(t.Result); err != nil {
			log.Println(err.Error())
			return err
		}
		return t.clientError
	}

	bucket := cluster.Bucket(t.Request.Bucket)
	t.clientError = bucket.WaitUntilReady(5*time.Second, nil)
	if t.clientError != nil {
		t.Result.ErrorOther = t.clientError.Error()
		if err := SaveResultIntoFile(t.Result); err != nil {
			log.Println(err.Error())
			return err
		}
		return t.clientError
	}

	col := bucket.Scope(t.Request.Scope).Collection(t.Request.Collection)
	gen := docgenerator.Generator{
		DocType:   t.Request.DocType,
		KeyPrefix: t.TaskState.KeyPrefix,
		KeySuffix: t.TaskState.KeySuffix,
		Seed:      t.TaskState.Seed,
		SeedEnd:   t.TaskState.SeedEnd,
		Template:  t.Request.Template,
	}

	switch t.Request.Operation {
	case communication.InsertOperation:
		t.insertDocuments(gen, col)
	case communication.UpsertOperation:
		t.upsertDocuments(gen, col)
	case communication.DeleteOperation:
		t.deleteDocuments(gen, col)
	case communication.ValidateOperation:
		t.ValidateDocuments(gen, col)
	}

	t.clientError = cluster.Close(nil)
	if t.clientError != nil {
		t.Result.ErrorOther = t.clientError.Error()
		if err := SaveResultIntoFile(t.Result); err != nil {
			log.Println("not able to save result into ", t.TaskState.Seed)
		}
	}

	if err := t.SaveTaskStateToFile(); err != nil {
		log.Println("not able to save task state for ", buildTaskName(t.TaskState.Host, t.TaskState.BUCKET, t.TaskState.SCOPE, t.TaskState.Collection))
		t.Result.ErrorOther = err.Error()
	}

	t.calculateSuccess()
	if err := SaveResultIntoFile(t.Result); err != nil {
		log.Println("not able to save result into ", t.TaskState.Seed)
		return err
	}

	log.Println(t.TaskState.Seed, t.TaskState.SeedEnd, t.TaskState.SeedEnd-t.TaskState.Seed)
	return nil
}
