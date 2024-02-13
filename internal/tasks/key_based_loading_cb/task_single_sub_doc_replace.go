package key_based_loading_cb

//
//import (
//	"github.com/couchbase/gocb/v2"
//	"github.com/couchbaselabs/sirius/internal/cb_sdk"
//	"github.com/couchbaselabs/sirius/internal/err_sirius"
//	"github.com/couchbaselabs/sirius/internal/task_result"
//	"github.com/couchbaselabs/sirius/internal/tasks"
//	"github.com/couchbaselabs/sirius/internal/template"
//	"github.com/jaswdr/faker"
//	"log"
//	"math/rand"
//	"strings"
//	"time"
//)
//
//type SingleSubDocReplace struct {
//	IdentifierToken             string                       `json:"identifierToken" doc:"true"`
//	ClusterConfig               *cb_sdk.ClusterConfig        `json:"clusterConfig" doc:"true"`
//	Bucket                      string                       `json:"bucket" doc:"true"`
//	Scope                       string                       `json:"scope,omitempty" doc:"true"`
//	Collection                  string                       `json:"collection,omitempty" doc:"true"`
//	SingleSubDocOperationConfig *SingleSubDocOperationConfig `json:"singleSubDocOperationConfig" doc:"true"`
//	ReplaceSpecOptions          *cb_sdk.ReplaceSpecOptions   `json:"replaceSpecOptions" doc:"true"`
//	MutateInOptions             *cb_sdk.MutateInOptions      `json:"mutateInOptions" doc:"true"`
//	Operation                   string                       `json:"operation" doc:"false"`
//	ResultSeed                  int64                        `json:"resultSeed" doc:"false"`
//	TaskPending                 bool                         `json:"taskPending" doc:"false"`
//	Result                      *task_result.TaskResult      `json:"-" doc:"false"`
//	req                         *tasks.Request               `json:"-" doc:"false"`
//}
//
//func (task *SingleSubDocReplace) Describe() string {
//	return "SingleSingleSubDocReplace inserts a Sub-Document as per user's input [No Random data]"
//}
//
//func (task *SingleSubDocReplace) MetaDataIdentifier() string {
//	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
//	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
//		task.Collection}, ":")
//}
//
//func (task *SingleSubDocReplace) CheckIfPending() bool {
//	return task.TaskPending
//}
//
//// Config configures  the insert task
//func (task *SingleSubDocReplace) Config(req *tasks.Request, reRun bool) (int64, error) {
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
//		task.Operation = tasks.SingleSubDocReplaceOperation
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
//		if err_sirius := cb_sdk.ConfigReplaceSpecOptions(task.ReplaceSpecOptions); err_sirius != nil {
//			task.TaskPending = false
//			return 0, err_sirius
//		}
//
//		if err_sirius := cb_sdk.ConfigMutateInOptions(task.MutateInOptions); err_sirius != nil {
//			task.TaskPending = false
//			return 0, err_sirius
//		}
//	} else {
//		log.Println("retrying :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//	}
//	return task.ResultSeed, nil
//}
//
//func (task *SingleSubDocReplace) TearUp() error {
//	task.Result.StopStoringResult()
//	if err_sirius := task.Result.SaveResultIntoFile(); err_sirius != nil {
//		log.Println("not able to save Result into ", task.ResultSeed, task.Operation)
//	}
//	task.Result = nil
//	task.TaskPending = false
//	return task.req.SaveRequestIntoFile()
//}
//
//func (task *SingleSubDocReplace) Do() error {
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
//	singleReplaceSubDocuments(task, collectionObject)
//
//	task.Result.Success = int64(1) - task.Result.Failure
//	return task.TearUp()
//}
//
//// singleReplaceSubDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
//func singleReplaceSubDocuments(task *SingleSubDocReplace, collectionObject *cb_sdk.CollectionObject) {
//
//	if task.req.ContextClosed() {
//		return
//	}
//
//	var iOps []gocb.MutateInSpec
//	key := task.SingleSubDocOperationConfig.Key
//	documentMetaData := task.req.DocumentsMeta.GetDocumentsMetadata(task.MetaDataIdentifier(), key, "", 0, false)
//
//	for _, path := range task.SingleSubDocOperationConfig.Paths {
//		subDocument := documentMetaData.SubDocument(path, task.ReplaceSpecOptions.IsXattr, task.SingleSubDocOperationConfig.
//			DocSize, true)
//
//		fake := faker.NewWithSeed(rand.NewSource(int64(subDocument.Seed)))
//
//		value := subDocument.GenerateValue(&fake)
//
//		iOps = append(iOps, gocb.ReplaceSpec(path, value, &gocb.ReplaceSpecOptions{
//			IsXattr: task.ReplaceSpecOptions.IsXattr,
//		}))
//	}
//
//	if !task.ReplaceSpecOptions.IsXattr {
//		iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
//			int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
//				CreatePath: true,
//				IsXattr:    false,
//			}))
//	}
//
//	initTime := time.Now().UTC().Format(time.RFC850)
//	result, err_sirius := collectionObject.Collection.MutateIn(key, iOps, &gocb.MutateInOptions{
//		Expiry:          time.Duration(task.MutateInOptions.Expiry) * time.Second,
//		Cas:             gocb.Cas(task.MutateInOptions.Cas),
//		PersistTo:       task.MutateInOptions.PersistTo,
//		ReplicateTo:     task.MutateInOptions.ReplicateTo,
//		DurabilityLevel: cb_sdk.GetDurability(task.MutateInOptions.Durability),
//		StoreSemantic:   cb_sdk.GetStoreSemantic(task.MutateInOptions.StoreSemantic),
//		Timeout:         time.Duration(task.MutateInOptions.Timeout) * time.Second,
//		PreserveExpiry:  task.MutateInOptions.PreserveExpiry,
//	})
//
//	if err_sirius != nil {
//		task.Result.CreateSingleErrorResult(initTime, key, err_sirius.Error(), false, 0)
//	} else {
//		if !task.ReplaceSpecOptions.IsXattr {
//			documentMetaData.IncrementMutationCount()
//		}
//		task.Result.CreateSingleErrorResult(initTime, key, "", true, uint64(result.Cas()))
//	}
//
//	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//}
