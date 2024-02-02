package bulk_query_cb

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"
	"golang.org/x/sync/errgroup"
	"log"
	"strings"
	"time"
)

type QueryTask struct {
	IdentifierToken      string                       `json:"identifierToken" doc:"true"`
	ClusterConfig        *cb_sdk.ClusterConfig        `json:"clusterConfig" doc:"true"`
	Bucket               string                       `json:"bucket" doc:"true"`
	Scope                string                       `json:"scope,omitempty" doc:"true"`
	Collection           string                       `json:"collection,omitempty" doc:"true"`
	QueryOperationConfig *tasks.QueryOperationConfig  `json:"operationConfig,omitempty" doc:"true"`
	Template             template.Template            `json:"-" doc:"false"`
	Operation            string                       `json:"operation" doc:"false"`
	ResultSeed           int64                        `json:"resultSeed" doc:"false"`
	TaskPending          bool                         `json:"taskPending" doc:"false"`
	BuildIndex           bool                         `json:"buildIndex" doc:"false"`
	Result               *task_result.TaskResult      `json:"-" doc:"false"`
	gen                  *docgenerator.QueryGenerator `json:"-" doc:"false"`
	req                  *tasks.Request               `json:"-" doc:"false"`
}

func (task *QueryTask) Describe() string {
	return " Query task runs N1QL query over a period of time over a bucket.\n"
}

func (task *QueryTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = tasks.DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *QueryTask) CollectionIdentifier() string {
	clusterIdentifier, _ := cb_sdk.GetClusterIdentifier(task.ClusterConfig.ConnectionString)
	return strings.Join([]string{task.IdentifierToken, clusterIdentifier, task.Bucket, task.Scope,
		task.Collection}, ":")
}

func (task *QueryTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *QueryTask) Config(req *tasks.Request, reRun bool) (int64, error) {
	task.TaskPending = true
	task.BuildIndex = false
	task.req = req

	if task.req == nil {
		task.TaskPending = false
		return 0, task_errors.ErrRequestIsNil
	}

	task.req.ReconnectionManager()
	if _, err := task.req.GetCluster(task.ClusterConfig); err != nil {
		task.TaskPending = false
		return 0, err
	}

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = tasks.QueryOperation
		task.BuildIndex = true

		if task.Bucket == "" {
			task.Bucket = tasks.DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = tasks.DefaultScope
		}
		if task.Collection == "" {
			task.Collection = tasks.DefaultCollection
		}

		if err := tasks.ConfigQueryOperationConfig(task.QueryOperationConfig); err != nil {
			task.Result.ErrorOther = err.Error()
		}

		task.Template = template.InitialiseTemplate(task.QueryOperationConfig.Template)

	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *QueryTask) TearUp() error {
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *QueryTask) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	cluster, err := task.req.GetCluster(task.ClusterConfig)
	if err != nil {
		task.Result.ErrorOther = err.Error()
		return task.TearUp()
	}

	s, err1 := task.req.GetScope(task.ClusterConfig, task.Bucket, task.Scope)
	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		return task.TearUp()
	}

	c, err := task.req.GetCollection(task.ClusterConfig, task.Bucket, task.Scope, task.Collection)
	if err != nil {
		task.Result.ErrorOther = err1.Error()
		if err := task.Result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save Result into ", task.ResultSeed)
			return err
		}
		return task.TearUp()
	}

	task.gen = docgenerator.ConfigQueryGenerator(task.Template)
	// check if indexes needs to be build
	if task.BuildIndex {
		if task.QueryOperationConfig.BuildIndexViaSDK {
			buildIndexWithSDKs(task, cluster, s, c.Collection)
		} else {
			buildIndexViaN1QL(task, cluster, s, c.Collection)
		}
		task.BuildIndex = false
	}

	runN1qlQuery(task, cluster, s, c.Collection)

	return task.TearUp()
}

// buildIndexSDKManager handles the SDK call
func buildIndexSDKManager(cluster *gocb.Cluster, bucketName string, scopeName string, collectionName string, indexType string, indexName string, fieldNameList []string) error {
	manager := cluster.QueryIndexes()
	if indexType == tasks.CreatePrimaryIndex {
		if err := manager.CreatePrimaryIndex(bucketName,
			&gocb.CreatePrimaryQueryIndexOptions{Deferred: true, ScopeName: scopeName, CollectionName: collectionName},
		); err != nil {
			return err
		}
	} else if indexType == tasks.CreateIndex {
		if err := manager.CreateIndex(bucketName, indexName, fieldNameList,
			&gocb.CreateQueryIndexOptions{Deferred: true, ScopeName: scopeName, CollectionName: collectionName},
		); err != nil {
			return err
		}
	} else if indexType == tasks.BuildIndex {
		indexesToBuild, err := manager.BuildDeferredIndexes(bucketName,
			&gocb.BuildDeferredQueryIndexOptions{CollectionName: collectionName, ScopeName: scopeName})
		if err != nil {
			return err
		}
		err = manager.WatchIndexes(bucketName, indexesToBuild, time.Duration(time.Duration(tasks.WatchIndexDuration)*time.Second), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// buildIndexWithSDK builds indexes by sending SDK call
func buildIndexWithSDKs(task *QueryTask, cluster *gocb.Cluster, scope *gocb.Scope, collection *gocb.Collection) {

	if err := buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
		tasks.CreatePrimaryIndex, "", []string{""}); err != nil {
		task.Result.IncrementQueryFailure(fmt.Sprintf("Create primary index on `%s`.%s.%s",
			task.Bucket, scope.Name(), collection.Name()), err)
	}

	indexes, err := task.gen.Template.GenerateIndexesForSdk()
	if err != nil {
		log.Println("Get indexes for cb_sdk failed.....")
		return
	}
	for indexName, indexFields := range indexes {
		if err = buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
			tasks.CreateIndex, indexName, indexFields); err != nil {
			task.Result.IncrementQueryFailure(fmt.Sprintf("Create index %s On `%s`.%s.%s (%s)",
				indexName, task.Bucket, scope.Name(), collection.Name(), indexFields), err)
		}
	}

	if err = buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
		tasks.BuildIndex, "", []string{""}); err != nil {
		task.Result.IncrementQueryFailure(fmt.Sprintf("Build index on `%s`.%s.%s",
			task.Bucket, scope.Name(), collection.Name()), err)
	}
}

// buildIndexViaN1QL builds indexes by sending n1ql queries
func buildIndexViaN1QL(task *QueryTask, cluster *gocb.Cluster, scope *gocb.Scope, collection *gocb.Collection) {
	query := fmt.Sprintf("CREATE PRIMARY INDEX ON `%s`.`%s`.`%s`;", task.Bucket, scope.Name(), collection.Name())
	if _, err := cluster.Query(query, &gocb.QueryOptions{}); err != nil {
		task.Result.IncrementQueryFailure(query, err)
	}

	indexes, err := task.gen.Template.GenerateIndexes(task.Bucket, scope.Name(), collection.Name())
	if err != nil {
		log.Println("Get sample indexes failed.....")
	}

	for i := 0; i < len(indexes); i++ {
		if _, err := cluster.Query(indexes[i], &gocb.QueryOptions{}); err != nil {
			task.Result.IncrementQueryFailure(indexes[i], err)
		}
	}
}

// runN1qlQuery runs query over a duration of time
func runN1qlQuery(task *QueryTask, cluster *gocb.Cluster, scope *gocb.Scope, collection *gocb.Collection) {

	if task.req.ContextClosed() {
		return
	}

	routineLimiter := make(chan struct{}, tasks.MaxConcurrentRoutines)
	group := errgroup.Group{}
	queries, err := task.gen.Template.GenerateQueries(task.Bucket, scope.Name(), collection.Name())
	if err != nil {
		log.Println("Get sample queries failed.....")
		return
	}

	expirationTime := time.Now().Add(time.Duration(task.QueryOperationConfig.Duration) * time.Second)
	for time.Now().Before(expirationTime) {
		routineLimiter <- struct{}{}
		group.Go(func() error {
			for i := 0; i < len(queries); i++ {
				_, err := cluster.Query(queries[i], &gocb.QueryOptions{})
				if err != nil {
					task.Result.IncrementQueryFailure(queries[i], err)
				}
			}

			<-routineLimiter
			return nil
		})

	}

	_ = group.Wait()
	close(routineLimiter)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}
