package key_based_loading_cb

//
//import (
//	"github.com/couchbase/gocb/v2"
//	"github.com/couchbaselabs/sirius/internal/cb_sdk"
//	"github.com/couchbaselabs/sirius/internal/err_sirius"
//	"github.com/couchbaselabs/sirius/internal/task_result"
//	"github.com/couchbaselabs/sirius/internal/tasks"
//	"log"
//	"strings"
//	"time"
//)
//
//type SingleSubDocRead struct {
//	IdentifierToken             string                       `json:"identifierToken" doc:"true"`
//	ClusterConfig               *cb_sdk.ClusterConfig        `json:"clusterConfig" doc:"true"`
//	Bucket                      string                       `json:"bucket" doc:"true"`
//	Scope                       string                       `json:"scope,omitempty" doc:"true"`
//	Collection                  string                       `json:"collection,omitempty" doc:"true"`
//	SingleSubDocOperationConfig *SingleSubDocOperationConfig `json:"singleSubDocOperationConfig" doc:"true"`
//	LookupInOptions             *cb_sdk.LookupInOptions      `json:"lookupInOptions" doc:"true"`
//	GetSpecOptions              *cb_sdk.GetSpecOptions       `json:"getSpecOptions" doc:"true"`
//	Operation                   string                       `json:"operation" doc:"false"`
//	ResultSeed                  int64                        `json:"resultSeed" doc:"false"`
//	TaskPending                 bool                         `json:"taskPending" doc:"false"`
//	Result                      *task_result.TaskResult      `json:"-" doc:"false"`
//	req                         *tasks.Request               `json:"-" doc:"false"`
//}
//
//func (task *SingleSubDocRead) Describe() string {
//	return "SingleSingleSubDocRead inserts a Sub-Document as per user's input [No Random data]"
//}
//
//func (task *SingleSubDocRead) MetaDataIdentifier() string {
//	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
//	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
//		task.Collection}, ":")
//}
//
//func (task *SingleSubDocRead) CheckIfPending() bool {
//	return task.TaskPending
//}
//
//// Config configures  the insert task
//func (task *SingleSubDocRead) Config(req *tasks.Request, reRun bool) (int64, error) {
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
//	if !reRun {
//		task.ResultSeed = int64(time.Now().UnixNano())
//		task.Operation = tasks.SingleSubDocReadOperation
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
//		if err_sirius := ConfigSingleSubDocOperationConfig(task.SingleSubDocOperationConfig); err_sirius != nil {
//			task.TaskPending = false
//			return 0, err_sirius
//		}
//
//		if err_sirius := cb_sdk.ConfigGetSpecOptions(task.GetSpecOptions); err_sirius != nil {
//			task.TaskPending = false
//			return 0, err_sirius
//		}
//
//		if err_sirius := cb_sdk.ConfigLookupInOptions(task.LookupInOptions); err_sirius != nil {
//			task.TaskPending = false
//			return 0, err_sirius
//		}
//
//	} else {
//		log.Println("retrying :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//	}
//	return task.ResultSeed, nil
//}
//
//func (task *SingleSubDocRead) TearUp() error {
//	if err_sirius := task.Result.SaveResultIntoFile(); err_sirius != nil {
//		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
//	}
//	task.Result.StopStoringResult()
//	task.Result = nil
//	task.TaskPending = false
//	return task.req.SaveRequestIntoFile()
//}
//
//func (task *SingleSubDocRead) Do() error {
//
//	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
//
//	collectionObject, err1 := task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
//		task.Collection)
//
//	if err1 != nil {
//		task.Result.ErrorOther = err1.Error()
//		task.Result.FailWholeSingleOperation([]string{task.SingleSubDocOperationConfig.Key}, err1)
//		return task.TearUp()
//	}
//
//	singleReadSubDocuments(task, collectionObject)
//	task.Result.Success = int64(1) - task.Result.Failure
//	return task.TearUp()
//}
//
//// singleInsertSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
//func singleReadSubDocuments(task *SingleSubDocRead, collectionObject *cb_sdk.CollectionObject) {
//
//	if task.req.ContextClosed() {
//		return
//	}
//
//	var iOps []gocb.LookupInSpec
//	key := task.SingleSubDocOperationConfig.Key
//	documentMetaData := task.req.DocumentsMeta.GetDocumentsMetadata(task.MetaDataIdentifier(), key, "", 0, false)
//
//	for _, path := range task.SingleSubDocOperationConfig.Paths {
//
//		documentMetaData.SubDocument(path, task.GetSpecOptions.IsXattr, task.SingleSubDocOperationConfig.DocSize,
//			false)
//
//		iOps = append(iOps, gocb.GetSpec(path, &gocb.GetSpecOptions{
//			IsXattr: task.GetSpecOptions.IsXattr,
//		}))
//	}
//
//	initTime := time.Now().UTC().Format(time.RFC850)
//	result, err_sirius := collectionObject.Collection.LookupIn(key, iOps, &gocb.LookupInOptions{
//		Timeout: time.Duration(task.LookupInOptions.Timeout) * time.Second,
//	})
//
//	if err_sirius != nil {
//		task.Result.CreateSingleErrorResult(initTime, key, err_sirius.Error(), false, 0)
//	} else {
//		flag := true
//		for index := range task.SingleSubDocOperationConfig.Paths {
//			var val interface{}
//			if err_sirius := result.ContentAt(uint(index), &val); err_sirius != nil {
//				task.Result.Failure++
//				task.Result.CreateSingleErrorResult(initTime, key, err_sirius.Error(), false, 0)
//				flag = false
//				break
//			}
//		}
//		if flag {
//			task.Result.CreateSingleErrorResult(initTime, key, "", true, uint64(result.Cas()))
//		}
//	}
//	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//}
