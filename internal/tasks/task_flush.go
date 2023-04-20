package tasks

//import (
//	"fmt"
//	"github.com/couchbaselabs/sirius/internal/docgenerator"
//	"github.com/couchbaselabs/sirius/internal/sdk"
//	"github.com/couchbaselabs/sirius/internal/task_result"
//	"github.com/couchbaselabs/sirius/internal/task_state"
//	"github.com/couchbaselabs/sirius/internal/template"
//	"golang.org/x/sync/errgroup"
//	"log"
//)
//
//type FlushTask struct {
//	ConnectionString string `json:"connectionString"`
//	Username         string `json:"username"`
//	Password         string `json:"password"`
//	Host             string `json:"host"`
//	Bucket           string `json:"bucket"`
//	Scope            string `json:"scope,omitempty"`
//	Collection       string `json:"collection,omitempty"`
//	Operation        string
//	State            *task_state.TaskState
//	result           *task_result.TaskResult
//	connection       *sdk.ConnectionManager
//	gen              *docgenerator.Generator
//}
//
//// Config checks the validity of DeleteTask
//func (task *FlushTask) Config() (int64, error) {
//	if task.ConnectionString == "" {
//		return 0, fmt.Errorf("empty connection string")
//	}
//	if task.Username == "" || task.Password == "" {
//		return 0, fmt.Errorf("cluster's credentials are missing ")
//	}
//	if task.Bucket == "" {
//		return 0, fmt.Errorf("bucker is missing")
//	}
//	if task.Scope == "" {
//		task.Scope = DefaultScope
//	}
//	if task.Collection == "" {
//		task.Collection = DefaultCollection
//	}
//	task.Operation = FlushOperation
//	// restore the original cluster state which should exist for operation, returns an error if task state don't exist else returns nil
//	state, ok := task_state.ConfigTaskState(task.ConnectionString, task.Bucket, task.Scope, task.Collection, "", "", "",
//		0, 0, 0)
//	if !ok {
//		return 0, fmt.Errorf("no such cluster's state exists for flush")
//	}
//	task.State = state
//	return task.State.SeedStart, nil
//}
//
//func (task *FlushTask) Describe() string {
//	return `Flush task tries to clear a bucket by deleting all documents in a bucket.
//The current state of that bucket would be lost. Hence, we need to initiate insert task again
//for creating a state of a bucket.`
//}
//
//func (task *FlushTask) Do() error {
//	// prepare a result for the task
//	task.result = task_result.ConfigTaskResult(task.Operation, task.State.SeedStart)
//
//	// establish a connection
//	task.connection = sdk.ConfigConnectionManager(task.ConnectionString, task.Username, task.Password,
//		task.Bucket, task.Scope, task.Collection)
//
//	if err := task.connection.Connect(); err != nil {
//		task.result.ErrorOther = err.Error()
//		if err := task.result.SaveResultIntoFile(); err != nil {
//			log.Println("not able to save result into ", task.State.SeedStart)
//			return err
//		}
//		return err
//	}
//
//	// Prepare generator
//	task.gen = docgenerator.ConfigGenerator("", task.State.KeyPrefix, task.State.KeySuffix,
//		task.State.SeedStart, task.State.SeedEnd, template.InitialiseTemplate(task.State.TemplateName))
//
//	// do bulk loading
//	flushBucket(task)
//
//	// close the sdk connection
//	_ = task.connection.Close()
//
//	// save the cluster result into the file
//	if err := task.State.DeleteTaskStateFromFile(); err != nil {
//		task.result.ErrorOther = err.Error()
//	}
//
//	// calculated result success here to prevent late update in failure due to locking.
//	task.result.Success = task.State.SeedEnd - task.State.SeedStart
//
//	// save the result into a file
//	if err := task.result.SaveResultIntoFile(); err != nil {
//		log.Println("not able to save result into ", task.State.SeedStart)
//		return err
//	}
//	return nil
//}
//
//// deleteDocuments delete the document stored on a host from start to end.
//func flushBucket(task *FlushTask) {
//
//	deleteCheck := task.State.RetracePreviousDeletions()
//
//	rateLimiter := make(chan struct{}, MaxConcurrentRoutines)
//	dataChannel := make(chan int64, MaxConcurrentRoutines)
//	group := errgroup.Group{}
//
//	for key := task.State.SeedStart; key < task.State.SeedEnd; key++ {
//		rateLimiter <- struct{}{}
//		dataChannel <- key
//
//		group.Go(func() error {
//			key := <-dataChannel
//			if _, ok := deleteCheck[key]; ok {
//				<-rateLimiter
//				return fmt.Errorf("key is delete from the server")
//			}
//			docId := task.gen.BuildKey(key)
//
//			_, err := task.connection.Collection.Remove(docId, nil)
//			if err != nil {
//				task.result.IncrementFailure(docId, err.Error())
//				<-rateLimiter
//				return err
//			}
//			<-rateLimiter
//			return nil
//		})
//	}
//	_ = group.Wait()
//	close(rateLimiter)
//	close(dataChannel)
//	log.Println(task.Operation, task.Bucket, task.Scope, task.Collection)
//}
