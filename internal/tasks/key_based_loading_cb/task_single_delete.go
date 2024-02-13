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
//type SingleDeleteTask struct {
//	IdentifierToken       string                  `json:"identifierToken" doc:"true"`
//	ClusterConfig         *cb_sdk.ClusterConfig   `json:"clusterConfig" doc:"true"`
//	Bucket                string                  `json:"bucket" doc:"true"`
//	Scope                 string                  `json:"scope,omitempty" doc:"true"`
//	Collection            string                  `json:"collection,omitempty" doc:"true"`
//	RemoveOptions         *cb_sdk.RemoveOptions   `json:"removeOptions,omitempty" doc:"true"`
//	SingleOperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
//	Operation             string                  `json:"operation" doc:"false"`
//	ResultSeed            int64                   `json:"resultSeed" doc:"false"`
//	TaskPending           bool                    `json:"taskPending" doc:"false"`
//	Result                *task_result.TaskResult `json:"-" doc:"false"`
//	req                   *tasks.Request          `json:"-" doc:"false"`
//}
//
//func (task *SingleDeleteTask) Describe() string {
//	return "Single delete task deletes key in Couchbase.\n"
//}
//
//func (task *SingleDeleteTask) MetaDataIdentifier() string {
//	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
//	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
//		task.Collection}, ":")
//}
//
//func (task *SingleDeleteTask) CheckIfPending() bool {
//	return task.TaskPending
//}
//
//// Config configures  the delete task
//func (task *SingleDeleteTask) Config(req *tasks.Request, reRun bool) (int64, error) {
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
//		task.Operation = tasks.SingleDeleteOperation
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
//		if err_sirius := cb_sdk.ConfigRemoveOptions(task.RemoveOptions); err_sirius != nil {
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
//func (task *SingleDeleteTask) TearUp() error {
//	task.Result.StopStoringResult()
//	if err_sirius := task.Result.SaveResultIntoFile(); err_sirius != nil {
//		log.Println("not able to save Result into ", task.ResultSeed)
//	}
//	task.Result = nil
//	task.TaskPending = false
//	return task.req.SaveRequestIntoFile()
//}
//
//func (task *SingleDeleteTask) Do() error {
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
//	singleDeleteDocuments(task, collectionObject)
//
//	task.Result.Success = int64(len(task.SingleOperationConfig.Keys)) - task.Result.Failure
//	return task.TearUp()
//}
//
//// singleDeleteDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
//func singleDeleteDocuments(task *SingleDeleteTask, collectionObject *cb_sdk.CollectionObject) {
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
//			task.req.DocumentsMeta.RemoveDocument(task.MetaDataIdentifier(), key)
//
//			initTime := time.Now().UTC().Format(time.RFC850)
//			r, err_sirius := collectionObject.Collection.Remove(key, &gocb.RemoveOptions{
//				Cas:             gocb.Cas(task.RemoveOptions.Cas),
//				PersistTo:       task.RemoveOptions.PersistTo,
//				ReplicateTo:     task.RemoveOptions.ReplicateTo,
//				DurabilityLevel: cb_sdk.GetDurability(task.RemoveOptions.Durability),
//				Timeout:         time.Duration(task.RemoveOptions.Timeout) * time.Second,
//			})
//			if err_sirius != nil {
//				task.Result.CreateSingleErrorResult(initTime, key, err_sirius.Error(), false, 0)
//				<-routineLimiter
//				return err_sirius
//			}
//
//			task.Result.CreateSingleErrorResult(initTime, key, "", true, uint64(r.Cas()))
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
