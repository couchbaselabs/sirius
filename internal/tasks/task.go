package tasks

import (
	"encoding/gob"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const ResultPath = "./results/result-logs"

type UserData struct {
	Seed []int64 `json:"seed"`
}

type TaskError struct {
	Key string      `json:"key"`
	Doc interface{} `json:"doc"`
	Err string      `json:"string"`
}

type TaskOperationCounter struct {
	Success int
	Failure int
	lock    sync.Mutex
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
		keys, personsTemplate := gen.Next(gen.Seed[i])
		wg := sync.WaitGroup{}
		wg.Add(len(keys))
		for index, key := range keys {
			go func(key string, doc interface{}) {
				defer wg.Done()
				var err error
				switch t.Request.Operation {
				case communication.InsertOperation:
					_, err = col.Insert(key, doc, nil)
				case communication.UpsertOperation:
					_, err = col.Upsert(key, doc, nil)

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
			}(key, *personsTemplate[index])
		}
		wg.Wait()
	}
	t.taskOperationCounter.Success = (gen.End * gen.BatchSize) - t.taskOperationCounter.Failure
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection, t.taskOperationCounter.Success)
}

// Save the results into a file
func (t *Task) saveResultIntoFile() error {
	tr := TaskResult{
		UserData:             t.UserData,
		TaskError:            t.taskError,
		TaskOperationCounter: t.taskOperationCounter,
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	fileName := filepath.Join(cwd, ResultPath, fmt.Sprintf("%d", tr.UserData.Seed[0]))
	// save the value to a file
	log.Println(fileName)
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
