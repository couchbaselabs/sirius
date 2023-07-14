package tasks

//
//import (
//	"bytes"
//	"encoding/json"
//	"errors"
//	"fmt"
//	"github.com/couchbase/gocb/v2"
//	"github.com/couchbaselabs/sirius/internal/sdk"
//	"github.com/couchbaselabs/sirius/internal/task_result"
//	"golang.org/x/sync/errgroup"
//	"log"
//	"time"
//)
//
//type SingleReadTask struct {
//	IdentifierToken string                  `json:"identifierToken" doc:"true"`
//	ClusterConfig   *sdk.ClusterConfig      `json:"clusterConfig" doc:"true"`
//	Bucket          string                  `json:"bucket" doc:"true"`
//	Scope           string                  `json:"scope,omitempty" doc:"true"`
//	Collection      string                  `json:"collection,omitempty" doc:"true"`
//	OperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
//	Operation       string                  `json:"operation" doc:"false"`
//	ResultSeed      int                     `json:"resultSeed" doc:"false"`
//	TaskPending     bool                    `json:"taskPending" doc:"false"`
//	result          *task_result.TaskResult `json:"-" doc:"false"`
//	req             *Request                `json:"-" doc:"false"`
//}
//
//func (task *SingleReadTask) Describe() string {
//	return "Single read task reads key value in couchbase and validates.\n"
//}
//
//func (task *SingleReadTask) BuildIdentifier() string {
//	if task.IdentifierToken == "" {
//		task.IdentifierToken = DefaultIdentifierToken
//	}
//	return task.IdentifierToken
//}
//
//func (task *SingleReadTask) CheckIfPending() bool {
//	return task.TaskPending
//}
//
//// Config configures  the insert task
//func (task *SingleReadTask) Config(req *Request, seed int, seedEnd int, reRun bool) (int, error) {
//	task.TaskPending = true
//	task.req = req
//
//	if task.req == nil {
//		task.TaskPending = false
//		return 0, fmt.Errorf("request.Request struct is nil")
//	}
//
//	task.req.ReconnectionManager()
//	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
//		task.TaskPending = false
//		return 0, err
//	}
//
//	if !reRun {
//		task.ResultSeed = int(time.Now().UnixNano())
//		task.Operation = SingleReadOperation
//		task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
//
//		if task.Bucket == "" {
//			task.Bucket = DefaultBucket
//		}
//		if task.Scope == "" {
//			task.Scope = DefaultScope
//		}
//		if task.Collection == "" {
//			task.Collection = DefaultCollection
//		}
//
//		if err := configSingleOperationConfig(task.OperationConfig); err != nil {
//			task.TaskPending = false
//			return 0, fmt.Errorf(err.Error())
//		}
//	} else {
//		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
//	}
//	return task.ResultSeed, nil
//}
//
//func (task *SingleReadTask) tearUp() error {
//	task.result = nil
//	task.TaskPending = false
//	return task.req.SaveRequestIntoFile()
//}
//
//func (task *SingleReadTask) Do() error {
//
//	if task.result != nil && task.result.ErrorOther != "" {
//		log.Println(task.result.ErrorOther)
//		if err := task.result.SaveResultIntoFile(); err != nil {
//			log.Println("not able to save result into ", task.ResultSeed)
//			return err
//		}
//		return task.tearUp()
//	} else {
//		task.result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
//	}
//
//	collection, err1 := task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
//		task.Collection)
//
//	if err1 != nil {
//		task.result.ErrorOther = err1.Error()
//		var docIds []string
//		for _, kV := range task.OperationConfig.KeyValue {
//			docIds = append(docIds, kV.Key)
//		}
//		task.result.FailWholeSingleOperation(docIds, err1)
//		if err := task.result.SaveResultIntoFile(); err != nil {
//			log.Println("not able to save result into ", task.ResultSeed)
//			return err
//		}
//		return task.tearUp()
//	}
//
//	singleReadDocuments(task, collection)
//
//	task.result.Success = (len(task.OperationConfig.KeyValue)) - task.result.Failure
//
//	if err := task.result.SaveResultIntoFile(); err != nil {
//		log.Println("not able to save result into ", task.ResultSeed)
//	}
//
//	return task.tearUp()
//}
//
//// singleDeleteDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
//func singleReadDocuments(task *SingleReadTask, collection *gocb.Collection) {
//
//	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
//	dataChannel := make(chan interface{}, MaxConcurrentRoutines)
//
//	group := errgroup.Group{}
//
//	for _, data := range task.OperationConfig.KeyValue {
//		routineLimiter <- struct{}{}
//		dataChannel <- data
//
//		group.Go(func() error {
//			keyValue := <-dataChannel
//			kV, ok := keyValue.(KeyValue)
//			if !ok {
//				task.result.IncrementFailure("unknownDocId", struct{}{},
//					errors.New("unable to decode Key Value for single crud"))
//				<-routineLimiter
//				return errors.New("unable to decode Key Value for single crud")
//			}
//
//			result, err := collection.Get(kV.Key, nil)
//			if err != nil {
//				task.result.CreateSingleErrorResult(kV.Key, err.Error(),
//					false, 0)
//				task.result.IncrementFailure(kV.Key, kV.Doc, err)
//				<-routineLimiter
//				return err
//			}
//
//			if task.OperationConfig.ReadYourOwnWrite {
//
//				var resultFromHost map[string]any
//				if err := result.Content(&resultFromHost); err != nil {
//					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
//						false, 0)
//					task.result.IncrementFailure(kV.Key, kV.Doc, err)
//					<-routineLimiter
//					return err
//				}
//
//				resultFromHostBytes, err := json.Marshal(resultFromHost)
//				if err != nil {
//					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
//						false, 0)
//					task.result.IncrementFailure(kV.Key, kV.Doc, err)
//					<-routineLimiter
//					return err
//				}
//				resultFromDocBytes, err := json.Marshal(kV.Doc)
//				if err != nil {
//					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
//						false, 0)
//					task.result.IncrementFailure(kV.Key, kV.Doc, err)
//					<-routineLimiter
//					return err
//				}
//
//				if !bytes.Equal(resultFromHostBytes, resultFromDocBytes) {
//					task.result.CreateSingleErrorResult(kV.Key, "document validation failed on read your own write",
//						false, 0)
//					task.result.IncrementFailure(kV.Key, kV.Doc, errors.New("document validation failed on read your own write"))
//					<-routineLimiter
//					return err
//				}
//			}
//
//			task.result.CreateSingleErrorResult(kV.Key, "", true, uint64(result.Cas()))
//			<-routineLimiter
//			return nil
//		})
//	}
//
//	_ = group.Wait()
//	close(routineLimiter)
//	close(dataChannel)
//	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
//}
