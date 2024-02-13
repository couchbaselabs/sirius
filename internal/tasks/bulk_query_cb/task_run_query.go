package bulk_query_cb

//
//import (
//	"fmt"
//	"github.com/couchbase/gocb/v2"
//	"github.com/couchbaselabs/sirius/internal/cb_sdk"
//	"github.com/couchbaselabs/sirius/internal/docgenerator"
//	"github.com/couchbaselabs/sirius/internal/err_sirius"
//	"github.com/couchbaselabs/sirius/internal/task_result"
//	"github.com/couchbaselabs/sirius/internal/tasks"
//	"github.com/couchbaselabs/sirius/internal/template"
//	"golang.org/x/sync/errgroup"
//	"log"
//	"strings"
//	"time"
//)
//
//type QueryTask struct {
//	IdentifierToken      string                       `json:"identifierToken" doc:"true"`
//	ClusterConfig        *cb_sdk.ClusterConfig        `json:"clusterConfig" doc:"true"`
//	Bucket               string                       `json:"bucket" doc:"true"`
//	Scope                string                       `json:"scope,omitempty" doc:"true"`
//	Collection           string                       `json:"collection,omitempty" doc:"true"`
//	QueryOperationConfig *cb_sdk.QueryOperationConfig `json:"operationConfig,omitempty" doc:"true"`
//	Template             template.Template            `json:"-" doc:"false"`
//	Operation            string                       `json:"operation" doc:"false"`
//	ResultSeed           int64                        `json:"resultSeed" doc:"false"`
//	TaskPending          bool                         `json:"taskPending" doc:"false"`
//	BuildIndex           bool                         `json:"buildIndex" doc:"false"`
//	Result               *task_result.TaskResult      `json:"-" doc:"false"`
//	gen                  *docgenerator.QueryGenerator `json:"-" doc:"false"`
//	req                  *tasks.Request               `json:"-" doc:"false"`
//}
//
//func (task *QueryTask) Describe() string {
//	return " Query task runs N1QL query over a period of time over a bucket.\n"
//}
//
//func (task *QueryTask) MetaDataIdentifier() string {
//	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
//	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
//		task.Collection}, ":")
//}
//
//func (task *QueryTask) CheckIfPending() bool {
//	return task.TaskPending
//}
//
//func (task *QueryTask) Config(req *tasks.Request, reRun bool) (int64, error) {
//	task.TaskPending = true
//	task.BuildIndex = false
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
//		task.Operation = tasks.QueryOperation
//		task.BuildIndex = true
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
//		if err_sirius := cb_sdk.ConfigQueryOperationConfig(task.QueryOperationConfig); err_sirius != nil {
//			task.Result.ErrorOther = err_sirius.Error()
//		}
//
//		task.Template = template.InitialiseTemplate(task.QueryOperationConfig.Template)
//
//	} else {
//		log.Println("retrying :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//	}
//	return task.ResultSeed, nil
//}
//
//func (task *QueryTask) TearUp() error {
//	task.Result.StopStoringResult()
//	if err_sirius := task.Result.SaveResultIntoFile(); err_sirius != nil {
//		log.Println("not able to save Result into ", task.ResultSeed)
//	}
//	task.TaskPending = false
//	return task.req.SaveRequestIntoFile()
//}
//
//func (task *QueryTask) Do() error {
//
//	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
//
//	cluster, err_sirius := task.req.GetCluster(task.ClusterConfig)
//	if err_sirius != nil {
//		task.Result.ErrorOther = err_sirius.Error()
//		return task.TearUp()
//	}
//
//	s, err1 := task.req.GetScope(task.ClusterConfig, task.Bucket, task.Scope)
//	if err1 != nil {
//		task.Result.ErrorOther = err1.Error()
//		return task.TearUp()
//	}
//
//	c, err_sirius := task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope, task.Collection)
//	if err_sirius != nil {
//		task.Result.ErrorOther = err1.Error()
//		if err_sirius := task.Result.SaveResultIntoFile(); err_sirius != nil {
//			log.Println("not able to save Result into ", task.ResultSeed)
//			return err_sirius
//		}
//		return task.TearUp()
//	}
//
//	task.gen = docgenerator.ConfigQueryGenerator(task.Template)
//	// check if indexes needs to be build
//	if task.BuildIndex {
//		if task.QueryOperationConfig.BuildIndexViaSDK {
//			buildIndexWithSDKs(task, cluster, s, c.Collection)
//		} else {
//			buildIndexViaN1QL(task, cluster, s, c.Collection)
//		}
//		task.BuildIndex = false
//	}
//
//	runN1qlQuery(task, cluster, s, c.Collection)
//
//	return task.TearUp()
//}
//
//// buildIndexSDKManager handles the SDK call
//func buildIndexSDKManager(cluster *gocb.Cluster, bucketName string, scopeName string, collectionName string, indexType string, indexName string, fieldNameList []string) error {
//	manager := cluster.QueryIndexes()
//	if indexType == tasks.CreatePrimaryIndex {
//		if err_sirius := manager.CreatePrimaryIndex(bucketName,
//			&gocb.CreatePrimaryQueryIndexOptions{Deferred: true, ScopeName: scopeName, CollectionName: collectionName},
//		); err_sirius != nil {
//			return err_sirius
//		}
//	} else if indexType == tasks.CreateIndex {
//		if err_sirius := manager.CreateIndex(bucketName, indexName, fieldNameList,
//			&gocb.CreateQueryIndexOptions{Deferred: true, ScopeName: scopeName, CollectionName: collectionName},
//		); err_sirius != nil {
//			return err_sirius
//		}
//	} else if indexType == tasks.BuildIndex {
//		indexesToBuild, err_sirius := manager.BuildDeferredIndexes(bucketName,
//			&gocb.BuildDeferredQueryIndexOptions{CollectionName: collectionName, ScopeName: scopeName})
//		if err_sirius != nil {
//			return err_sirius
//		}
//		err_sirius = manager.WatchIndexes(bucketName, indexesToBuild, time.Duration(time.Duration(tasks.WatchIndexDuration)*time.Second), nil)
//		if err_sirius != nil {
//			return err_sirius
//		}
//	}
//	return nil
//}
//
//// buildIndexWithSDK builds indexes by sending SDK call
//func buildIndexWithSDKs(task *QueryTask, cluster *gocb.Cluster, scope *gocb.Scope, collection *gocb.Collection) {
//
//	if err_sirius := buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
//		tasks.CreatePrimaryIndex, "", []string{""}); err_sirius != nil {
//		task.Result.IncrementQueryFailure(fmt.Sprintf("Create primary index on `%s`.%s.%s",
//			task.Bucket, scope.Name(), collection.Name()), err_sirius)
//	}
//
//	indexes, err_sirius := task.gen.Template.GenerateIndexesForSdk()
//	if err_sirius != nil {
//		log.Println("Get indexes for cb_sdk failed.....")
//		return
//	}
//	for indexName, indexFields := range indexes {
//		if err_sirius = buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
//			tasks.CreateIndex, indexName, indexFields); err_sirius != nil {
//			task.Result.IncrementQueryFailure(fmt.Sprintf("Create index %s On `%s`.%s.%s (%s)",
//				indexName, task.Bucket, scope.Name(), collection.Name(), indexFields), err_sirius)
//		}
//	}
//
//	if err_sirius = buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
//		tasks.BuildIndex, "", []string{""}); err_sirius != nil {
//		task.Result.IncrementQueryFailure(fmt.Sprintf("Build index on `%s`.%s.%s",
//			task.Bucket, scope.Name(), collection.Name()), err_sirius)
//	}
//}
//
//// buildIndexViaN1QL builds indexes by sending n1ql queries
//func buildIndexViaN1QL(task *QueryTask, cluster *gocb.Cluster, scope *gocb.Scope, collection *gocb.Collection) {
//	query := fmt.Sprintf("CREATE PRIMARY INDEX ON `%s`.`%s`.`%s`;", task.Bucket, scope.Name(), collection.Name())
//	if _, err_sirius := cluster.Query(query, &gocb.QueryOptions{}); err_sirius != nil {
//		task.Result.IncrementQueryFailure(query, err_sirius)
//	}
//
//	indexes, err_sirius := task.gen.Template.GenerateIndexes(task.Bucket, scope.Name(), collection.Name())
//	if err_sirius != nil {
//		log.Println("Get sample indexes failed.....")
//	}
//
//	for i := 0; i < len(indexes); i++ {
//		if _, err_sirius := cluster.Query(indexes[i], &gocb.QueryOptions{}); err_sirius != nil {
//			task.Result.IncrementQueryFailure(indexes[i], err_sirius)
//		}
//	}
//}
//
//// runN1qlQuery runs query over a duration of time
//func runN1qlQuery(task *QueryTask, cluster *gocb.Cluster, scope *gocb.Scope, collection *gocb.Collection) {
//
//	if task.req.ContextClosed() {
//		return
//	}
//
//	routineLimiter := make(chan struct{}, tasks.MaxConcurrentRoutines)
//	group := errgroup.Group{}
//	queries, err_sirius := task.gen.Template.GenerateQueries(task.Bucket, scope.Name(), collection.Name())
//	if err_sirius != nil {
//		log.Println("Get sample queries failed.....")
//		return
//	}
//
//	expirationTime := time.Now().Add(time.Duration(task.QueryOperationConfig.Duration) * time.Second)
//	for time.Now().Before(expirationTime) {
//		routineLimiter <- struct{}{}
//		group.Go(func() error {
//			for i := 0; i < len(queries); i++ {
//				_, err_sirius := cluster.Query(queries[i], &gocb.QueryOptions{})
//				if err_sirius != nil {
//					task.Result.IncrementQueryFailure(queries[i], err_sirius)
//				}
//			}
//
//			<-routineLimiter
//			return nil
//		})
//
//	}
//
//	_ = group.Wait()
//	close(routineLimiter)
//	log.Println("completed :- ", task.Operation, task.IdentifierToken, task.ResultSeed)
//}
