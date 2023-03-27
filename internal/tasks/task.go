package tasks

import (
	"encoding/json"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"sync"
	"time"
)

const ResultPath = "./internal/tasks/result-logs"
const TaskStatePath = "./internal/tasks/task-state"
const MaxConcurrentOps = 500

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
	Success int64      `json:"success"`
	Failure int64      `json:"failure"`
	lock    sync.Mutex `json:"-"`
}

type TaskResult struct {
	UserData             UserData                        `json:"-"`
	Operation            communication.DocumentOperation `json:"operation"`
	Error                error                           `json:"error,omitempty"`
	TaskOperationCounter TaskOperationCounter            `json:"task_operation_counter"`
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

	err = cluster.Close(nil)
	if err != nil {
		t.clientError = err
		log.Println(err)
		return err
	}

	if err := t.SaveTaskStateToFile(); err != nil {
		t.Result.Error = err
	}

	if err := SaveResultIntoFile(t.Result); err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func (t *Task) insertDocuments(gen docgenerator.Generator, col *gocb.Collection) {
	for i := int64(0); i < t.Request.Iteration; i++ {
		wg := sync.WaitGroup{}
		wg.Add(int(t.Request.BatchSize))
		for index := int64(0); index < t.Request.BatchSize; index++ {
			go func(iteration, batchSize, index int64) {
				defer wg.Done()
				docId, key := gen.GetDocIdAndKey(iteration, t.Request.BatchSize, index)
				fake := faker.NewWithSeed(rand.NewSource(key))
				doc, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
				if err != nil {
					t.incrementFailure(key)
					return
				}
				_, err = col.Insert(docId, doc, nil)
				if err != nil {
					t.incrementFailure(key)
					return
				}
			}(i, t.Request.BatchSize, index)
		}
		wg.Wait()
	}
	t.Result.TaskOperationCounter.Success = (t.Request.Iteration * t.Request.BatchSize) - t.Result.TaskOperationCounter.Failure
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection, t.Result.TaskOperationCounter.Success)
}

// deleteDocuments delete the document stored on a host from start to end.
func (t *Task) deleteDocuments(gen docgenerator.Generator, col *gocb.Collection) {
	var l sync.Mutex
	insertErrorCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.InsertTask.Err {
		insertErrorCheck[k] = struct{}{}
	}
	deleteCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.DeleteTask.Del {
		deleteCheck[k] = struct{}{}
	}

	if t.Request.End < t.Request.Start {
		return
	}

	deleteQueue := make([]int64, 0, MaxConcurrentOps)
	for i := t.Request.Start; i <= t.Request.End; i++ {
		deleteQueue = append(deleteQueue, i)
		if len(deleteQueue) == MaxConcurrentOps || i == t.Request.End {
			var wg sync.WaitGroup
			wg.Add(len(deleteQueue))
			for _, offset := range deleteQueue {
				go func(offset int64) {
					defer wg.Done()
					key := gen.Seed + offset
					if _, ok := insertErrorCheck[key]; ok {
						return
					}
					if _, ok := deleteCheck[key]; ok {
						return
					}
					docId := gen.BuildKey(key)
					_, err := col.Remove(docId, nil)
					if err != nil {
						log.Println(err, offset, docId)
						t.incrementFailure(key)
						return
					}
					l.Lock()
					t.TaskState.DeleteTask.Del = append(t.TaskState.DeleteTask.Del, key)
					l.Unlock()
				}(offset - 1)
			}
			wg.Wait()
			deleteQueue = deleteQueue[:0]
		}
	}
	t.Result.TaskOperationCounter.Success = t.Request.End - t.Request.Start - t.Result.TaskOperationCounter.Failure + 1
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection, t.Result.TaskOperationCounter.Success)
}

// upsertDocuments updates the fields of a template a described by user request from start to end.
func (t *Task) upsertDocuments(gen docgenerator.Generator, col *gocb.Collection) {
	u := UpsertTask{
		Start:         t.Request.Start,
		End:           t.Request.End,
		FieldToChange: t.Request.FieldsToChange,
	}

	insertErrorCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.InsertTask.Err {
		insertErrorCheck[k] = struct{}{}
	}

	deleteCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.DeleteTask.Del {
		deleteCheck[k] = struct{}{}
	}

	upsertQueue := make([]int64, 0, MaxConcurrentOps)
	for i := t.Request.Start; i <= t.Request.End; i++ {
		upsertQueue = append(upsertQueue, i)
		if len(upsertQueue) == MaxConcurrentOps || i == t.Request.End {
			var wg sync.WaitGroup
			wg.Add(len(upsertQueue))
			for _, offset := range upsertQueue {
				go func(offset int64) {
					defer wg.Done()
					var err error
					key := gen.Seed + offset
					if _, ok := insertErrorCheck[key]; ok {
						return
					}
					if _, ok := deleteCheck[key]; ok {
						return
					}
					docId := gen.BuildKey(key)
					fake := faker.NewWithSeed(rand.NewSource(key))
					originalDoc, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
					if err != nil {
						t.incrementFailure(key)
						return
					}
					originalDoc, err = t.retracePreviousMutations(key, originalDoc, gen, &fake)
					if err != nil {
						return
					}
					docUpdated, err := gen.Template.UpdateDocument(t.Request.FieldsToChange, originalDoc, &fake)

					_, err = col.Upsert(docId, docUpdated, nil)
					if err != nil {
						t.incrementFailure(key)
						return
					}
					if key > t.TaskState.SeedEnd {
						t.TaskState.SeedEnd = key
					}
				}(offset - 1)
			}
			wg.Wait()
			upsertQueue = upsertQueue[:0]
		}
	}
	t.TaskState.UpsertTask = append(t.TaskState.UpsertTask, u)
	t.Result.TaskOperationCounter.Success = t.Request.End - t.Request.Start - t.Result.TaskOperationCounter.Failure + 1
}

// ValidateDocuments return the validity of the collection using TaskState
func (t *Task) ValidateDocuments(gen docgenerator.Generator, col *gocb.Collection) {

	if err := t.checkForTaskValidity(); err != nil {
		log.Println(err)
		return
	}

	insertErrorCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.InsertTask.Err {
		insertErrorCheck[k] = struct{}{}
	}

	deleteCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.DeleteTask.Del {
		deleteCheck[k] = struct{}{}
	}

	validateQueue := make([]int64, 0, MaxConcurrentOps)
	for key := t.TaskState.Seed; key <= t.TaskState.SeedEnd; key++ {
		validateQueue = append(validateQueue, key)
		if len(validateQueue) == MaxConcurrentOps || key == t.TaskState.SeedEnd {
			var wg sync.WaitGroup
			wg.Add(len(validateQueue))
			for _, key := range validateQueue {
				go func(key int64) {
					defer wg.Done()
					if _, ok := insertErrorCheck[key]; ok {
						return
					}
					if _, ok := deleteCheck[key]; ok {
						return
					}

					docId := gen.BuildKey(key)
					var resultFromHost map[string]interface{}
					documentFromHost, err := template.InitialiseTemplate(t.Request.TemplateToken)
					if err != nil {
						t.incrementFailure(key)
						return
					}
					result, err := col.Get(docId, nil)
					if err != nil {
						t.incrementFailure(key)
						return
					}
					if err := result.Content(&resultFromHost); err != nil {
						t.incrementFailure(key)
						return
					}
					resultBytes, err := json.Marshal(resultFromHost)
					err = json.Unmarshal(resultBytes, &documentFromHost)
					if err != nil {
						t.incrementFailure(key)
						return
					}
					fake := faker.NewWithSeed(rand.NewSource(key))
					originalDocument, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
					if err != nil {
						t.incrementFailure(key)
						return
					}
					originalDocument, err = t.retracePreviousMutations(key, originalDocument, gen, &fake)
					if err != nil {
						t.incrementFailure(key)
						return
					}
					ok, err := gen.Template.Compare(documentFromHost, originalDocument)
					if err != nil || !ok {
						t.incrementFailure(key)
						return
					}
				}(key)
			}
			wg.Wait()
			validateQueue = validateQueue[:0]
		}
	}
	t.Result.TaskOperationCounter.Success = t.TaskState.SeedEnd - t.TaskState.Seed - t.Result.TaskOperationCounter.Failure + 1
}

// incrementFailure saves the failure count of doc loading operation.
func (t *Task) incrementFailure(key int64) {
	t.Result.TaskOperationCounter.lock.Lock()
	t.Result.TaskOperationCounter.Failure++
	switch t.Request.Operation {
	case communication.InsertOperation:
		t.TaskState.InsertTask.Err = append(t.TaskState.InsertTask.Err, key)
	}
	t.Result.TaskOperationCounter.lock.Unlock()
}

// retracePreviousMutations retraces all previous mutation from the saved sequences of upsert operations.
func (t *Task) retracePreviousMutations(key int64, doc interface{}, gen docgenerator.Generator, fake *faker.Faker) (interface{}, error) {
	for _, u := range t.TaskState.UpsertTask {
		if key >= (u.Start+t.TaskState.Seed-1) && (key <= u.End+t.TaskState.Seed-1) {
			flag := true
			for _, e := range u.Err {
				if e == key {
					flag = false
					break
				}
			}
			if flag {
				doc, _ = gen.Template.UpdateDocument(u.FieldToChange, doc, fake)
			}
		}
	}
	return doc, nil
}
