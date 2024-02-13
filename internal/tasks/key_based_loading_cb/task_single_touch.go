package key_based_loading_cb

//
//import (
//	"github.com/couchbase/gocb/v2"
//	"github.com/couchbaselabs/sirius/internal/cb_sdk"
//	"github.com/couchbaselabs/sirius/internal/err_sirius"
//	"github.com/couchbaselabs/sirius/internal/task_result"
//	"github.com/couchbaselabs/sirius/internal/tasks"
//	"golang.org/x/sync/errgroup"
//	"log"
//	"strings"
//	"time"
//)
//
//type SingleTouchTask struct {
//	IdentifierToken       string                  `json:"identifierToken" doc:"true"`
//	ClusterConfig         *cb_sdk.ClusterConfig   `json:"clusterConfig" doc:"true"`
//	Bucket                string                  `json:"bucket" doc:"true"`
//	Scope                 string                  `json:"scope,omitempty" doc:"true"`
//	Collection            string                  `json:"collection,omitempty" doc:"true"`
//	InsertOptions         *cb_sdk.InsertOptions   `json:"insertOptions,omitempty" doc:"true"`
//	SingleOperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
//	Operation             string                  `json:"operation" doc:"false"`
//	ResultSeed            int64                   `json:"resultSeed" doc:"false"`
//	TaskPending           bool                    `json:"taskPending" doc:"false"`
//	Result                *task_result.TaskResult `json:"-" doc:"false"`
//	req                   *tasks.Request          `json:"-" doc:"false"`
//}
//
//func (task *SingleTouchTask) Describe() string {
//	return "Single touch task specifies a new expiry time for a document in Couchbase.\n"
//}
//
//func (task *SingleTouchTask) MetaDataIdentifier() string {
//	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
//	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
//		task.Collection}, ":")
//}
//
//func (task *SingleTouchTask) CheckIfPending() bool {
//	return task.TaskPending
//}
//
//// Config configures  the insert task
//func (task *SingleTouchTask) Config(req *tasks.Request, reRun bool) (int64, error) {
//	task.TaskPending = true
//	task.req = req
//
//	if task.req == nil {
//		task.TaskPending = false
//		return 0, err_sirius.RequestIsNil
//	}
//
//	task.req.ReconnectionManager()
//	if _, err_sirius := task.req.GetCluster(task.ClusterConfig); err_sirius != nil {
//		task.TaskPending = false
//		return 0, err_sirius
//	}
//
//	task.req.ReconfigureDocumentManager()
//
//	if !reRun {
//		task.ResultSeed = int64(time.Now().UnixNano())
//		task.Operation = tasks.SingleTouchOperation
//
//		if task.Bucket == "" {
//			task.Bucket = cb_sdk.DefaultBucket
//		}
//		if task.Scope == "" {
//			task.Scope = cb_sdk.DefaultScope
//		}
//		if task.Collection == "" {
//			task.Collection = cb_sdk.DefaultCollection
//		}
//
//		if err_sirius := cb_sdk.ConfigInsertOptions(task.InsertOptions); err_sirius != nil {
//			task.TaskPending = false
//			return 0, err_sirius
//		}
//
//		if err_sirius := ConfigSingleOperationConfig(task.SingleOperationConfig); err_sirius != nil {
//			task.TaskPending = false
//			return 0, err_sirius
//		}
//	} else {
//		log.Println("retrying :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//	}
//	return task.ResultSeed, nil
//}
//
//func (task *SingleTouchTask) TearUp() error {
//	task.Result.StopStoringResult()
//	if err_sirius := task.Result.SaveResultIntoFile(); err_sirius != nil {
//		log.Println("not able to save Result into ", task.ResultSeed)
//	}
//	task.Result = nil
//	task.TaskPending = false
//	return task.req.SaveRequestIntoFile()
//}
//
//func (task *SingleTouchTask) Do() error {
//
//	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
//
//	collectionObject, err1 := task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
//		task.Collection)
//
//	if err1 != nil {
//		task.Result.ErrorOther = err1.Error()
//		task.Result.FailWholeSingleOperation(task.SingleOperationConfig.Keys, err1)
//		return task.TearUp()
//	}
//
//	singleTouchDocuments(task, collectionObject)
//
//	task.Result.Success = int64(len(task.SingleOperationConfig.Keys)) - task.Result.Failure
//	return task.TearUp()
//}
//
//// singleTouchDocuments uploads new documents in a bucket.scope.
//// collection in a defined batch size at multiple iterations.
//func singleTouchDocuments(task *SingleTouchTask, collectionObject *cb_sdk.CollectionObject) {
//
//	if task.req.ContextClosed() {
//		return
//	}
//
//	routineLimiter := make(chan struct{}, tasks.MaxConcurrentRoutines)
//	dataChannel := make(chan string, tasks.MaxConcurrentRoutines)
//
//	group := errgroup.Group{}
//
//	for _, data := range task.SingleOperationConfig.Keys {
//
//		if task.req.ContextClosed() {
//			close(routineLimiter)
//			close(dataChannel)
//			return
//		}
//
//		routineLimiter <- struct{}{}
//		dataChannel <- data
//
//		group.Go(func() error {
//			key := <-dataChannel
//
//			task.req.DocumentsMeta.GetDocumentsMetadata(task.MetaDataIdentifier(), key, task.SingleOperationConfig.Template,
//				task.SingleOperationConfig.DocSize, false)
//
//			initTime := time.Now().UTC().Format(time.RFC850)
//			result, err_sirius := collectionObject.Collection.Touch(key, time.Duration(task.InsertOptions.Timeout)*time.Second,
//				&gocb.TouchOptions{
//					Timeout: time.Duration(task.InsertOptions.Timeout) * time.Second,
//				})
//
//			if err_sirius != nil {
//				task.Result.CreateSingleErrorResult(initTime, key, err_sirius.Error(), false, 0)
//				<-routineLimiter
//				return err_sirius
//			}
//
//			task.Result.CreateSingleErrorResult(initTime, key, "", true, uint64(result.Cas()))
//			<-routineLimiter
//			return nil
//		})
//	}
//
//	_ = group.Wait()
//	close(routineLimiter)
//	close(dataChannel)
//	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//}
