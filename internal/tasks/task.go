package tasks

import (
	"encoding/gob"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const ResultPath = "./results/result-logs"

type UserData struct {
	Seed int64 `json:"seed"`
}

type TaskError struct {
	Key string      `json:"key"`
	Doc interface{} `json:"doc"`
	Err string      `json:"string"`
}

type TaskOperationCounter struct {
	Success int64      `json:"success"`
	Failure int64      `json:"failure"`
	lock    sync.Mutex `json:"-"`
}

type TaskResult struct {
	UserData             UserData             `json:"-"`
	TaskError            []*TaskError         `json:"taskError"`
	TaskOperationCounter TaskOperationCounter `json:"task_operation_counter"`
}

type Task struct {
	UserData             UserData
	Request              *communication.TaskRequest
	taskError            []*TaskError
	clientError          error
	taskOperationCounter TaskOperationCounter
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
	// initialise a doc generator
	gen := docgenerator.Generator{
		Itr:           0,
		End:           t.Request.Iteration,
		BatchSize:     t.Request.BatchSize,
		DocType:       t.Request.DocType,
		KeySize:       t.Request.KeySize,
		DocSize:       0,
		RandomDocSize: false,
		RandomKeySize: false,
		Seed:          t.Request.Seed,
		Fake:          faker.NewWithSeed(rand.NewSource(t.Request.Seed)),
		Template:      nil,
	}
	switch t.Request.Operation {
	case communication.InsertOperation, communication.UpsertOperation:
		t.insertUpsert(gen, col)
	case communication.DeleteOperation:
		log.Println("delete")
	case communication.GetOperation:
		log.Println("get")
	}

	// Close connection and cluster.
	err = cluster.Close(nil)
	if err != nil {
		t.clientError = err
		log.Println(err)
		return err
	}

	// save the task result-logs into a file
	if err := t.saveResultIntoFile(); err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func (t *Task) insertUpsert(gen docgenerator.Generator, col *gocb.Collection) {
	for i := gen.Itr; i < gen.End; i++ {
		personsTemplate := gen.Next(gen.BatchSize)
		wg := sync.WaitGroup{}
		wg.Add(len(personsTemplate))
		for index, person := range personsTemplate {
			go func(iteration, batchSize, index int64, doc interface{}) {
				defer wg.Done()
				var err error
				key := gen.GetKey(iteration, batchSize, index, gen.Seed)
				switch t.Request.Operation {
				case communication.InsertOperation:
					_, err = col.Insert(key, person, nil)
				case communication.UpsertOperation:
					_, err = col.Upsert(key, person, nil)

				}
				if err != nil {
					log.Println(err)
					t.taskOperationCounter.lock.Lock()
					t.taskError = append(t.taskError, &TaskError{
						Key: key,
						Doc: doc,
						Err: err.Error(),
					})
					t.taskOperationCounter.Failure++
					t.taskOperationCounter.lock.Unlock()
				}
			}(i, gen.BatchSize, int64(index), person)
		}
		wg.Wait()
	}
	t.taskOperationCounter.Success = (gen.End * gen.BatchSize) - t.taskOperationCounter.Failure
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection, t.taskOperationCounter.Success)
}

// Save the results into a file
func (t *Task) saveResultIntoFile() error {
	tr := TaskResult{
		UserData:  t.UserData,
		TaskError: t.taskError,
		TaskOperationCounter: TaskOperationCounter{
			Success: t.taskOperationCounter.Success,
			Failure: t.taskOperationCounter.Failure,
		},
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	fileName := filepath.Join(cwd, ResultPath, fmt.Sprintf("%d", tr.UserData.Seed))

	// save the value to a file
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(tr); err != nil {
		return err
	}
	file.Close()
	return nil
}
