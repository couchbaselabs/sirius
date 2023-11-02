package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/template"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"time"
)

type QueryTask struct {
	IdentifierToken      string                       `json:"identifierToken" doc:"true"`
	ClusterConfig        *sdk.ClusterConfig           `json:"clusterConfig" doc:"true"`
	Bucket               string                       `json:"bucket" doc:"true"`
	Scope                string                       `json:"scope,omitempty" doc:"true"`
	Collection           string                       `json:"collection,omitempty" doc:"true"`
	QueryOperationConfig *QueryOperationConfig        `json:"operationConfig,omitempty" doc:"true"`
	Template             template.Template            `json:"-" doc:"false"`
	Operation            string                       `json:"operation" doc:"false"`
	ResultSeed           int64                        `json:"resultSeed" doc:"false"`
	TaskPending          bool                         `json:"taskPending" doc:"false"`
	BuildIndex           bool                         `json:"buildIndex" doc:"false"`
	Result               *task_result.TaskResult      `json:"-" doc:"false"`
	gen                  *docgenerator.QueryGenerator `json:"-" doc:"false"`
	req                  *Request                     `json:"-" doc:"false"`
}

func (task *QueryTask) Describe() string {
	return " Query task runs N1QL query over a period of time over a bucket.\n"
}

func (task *QueryTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *QueryTask) CollectionIdentifier() string {
	return task.IdentifierToken + task.ClusterConfig.ConnectionString + task.Bucket + task.Scope + task.Collection
}

func (task *QueryTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *QueryTask) Config(req *Request, reRun bool) (int64, error) {
	task.TaskPending = true
	task.BuildIndex = false
	task.req = req

	if task.req == nil {
		task.TaskPending = false
		return 0, task_errors.ErrRequestIsNil
	}

	task.req.ReconnectionManager()
	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
		task.TaskPending = false
		return 0, err
	}

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = QueryOperation
		task.BuildIndex = true

		if task.Bucket == "" {
			task.Bucket = DefaultBucket
		}
		if task.Scope == "" {
			task.Scope = DefaultScope
		}
		if task.Collection == "" {
			task.Collection = DefaultCollection
		}

		if err := configQueryOperationConfig(task.QueryOperationConfig); err != nil {
			task.Result.ErrorOther = err.Error()
		}

		task.Template = template.InitialiseTemplate(task.QueryOperationConfig.Template)

	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *QueryTask) tearUp() error {
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *QueryTask) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	cluster, err := task.req.connectionManager.GetCluster(task.ClusterConfig)
	if err != nil {
		task.Result.ErrorOther = err.Error()
		return task.tearUp()
	}

	s, err1 := task.req.connectionManager.GetScope(task.ClusterConfig, task.Bucket, task.Scope)
	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		return task.tearUp()
	}

	cLsit, err := task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope, task.Collection)
	// Picking out collection's connection out of random connections.
	c := cLsit[rand.Intn(len(cLsit))]

	if err != nil {
		task.Result.ErrorOther = err1.Error()
		if err := task.Result.SaveResultIntoFile(); err != nil {
			log.Println("not able to save Result into ", task.ResultSeed)
			return err
		}
		return task.tearUp()
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

	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}
	return task.tearUp()
}

// buildIndexSDKManager handles the SDK call
func buildIndexSDKManager(cluster *gocb.Cluster, bucketName string, scopeName string, collectionName string, indexType string, indexName string, fieldNameList []string) error {
	manager := cluster.QueryIndexes()
	if indexType == CreatePrimaryIndex {
		if err := manager.CreatePrimaryIndex(bucketName,
			&gocb.CreatePrimaryQueryIndexOptions{Deferred: true, ScopeName: scopeName, CollectionName: collectionName},
		); err != nil {
			return err
		}
	} else if indexType == CreateIndex {
		if err := manager.CreateIndex(bucketName, indexName, fieldNameList,
			&gocb.CreateQueryIndexOptions{Deferred: true, ScopeName: scopeName, CollectionName: collectionName},
		); err != nil {
			return err
		}
	} else if indexType == BuildIndex {
		indexesToBuild, err := manager.BuildDeferredIndexes(bucketName,
			&gocb.BuildDeferredQueryIndexOptions{CollectionName: collectionName, ScopeName: scopeName})
		if err != nil {
			return err
		}
		err = manager.WatchIndexes(bucketName, indexesToBuild, time.Duration(time.Duration(WatchIndexDuration)*time.Second), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// buildIndexWithSDK builds indexes by sending SDK call
func buildIndexWithSDKs(task *QueryTask, cluster *gocb.Cluster, scope *gocb.Scope, collection *gocb.Collection) {

	if err := buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
		CreatePrimaryIndex, "", []string{""}); err != nil {
		task.Result.IncrementQueryFailure(fmt.Sprintf("Create primary index on `%s`.%s.%s",
			task.Bucket, scope.Name(), collection.Name()), err)
	}

	indexes, err := task.gen.Template.GenerateIndexesForSdk()
	if err != nil {
		log.Println("Get indexes for sdk failed.....")
		return
	}
	for indexName, indexFields := range indexes {
		if err = buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
			CreateIndex, indexName, indexFields); err != nil {
			task.Result.IncrementQueryFailure(fmt.Sprintf("Create index %s On `%s`.%s.%s (%s)",
				indexName, task.Bucket, scope.Name(), collection.Name(), indexFields), err)
		}
	}

	if err = buildIndexSDKManager(cluster, task.Bucket, scope.Name(), collection.Name(),
		BuildIndex, "", []string{""}); err != nil {
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
	routineLimiter := make(chan struct{}, NumberOfBatches)
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

func (task *QueryTask) PostTaskExceptionHandling(_ *sdk.CollectionObject) {
	//TODO implement me
}

func (task *QueryTask) MatchResultSeed(resultSeed string) bool {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true
	}
	return false
}

func (task *QueryTask) GetCollectionObject() ([]*sdk.CollectionObject, error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig, task.Bucket, task.Scope,
		task.Collection)
}

func (task *QueryTask) SetException(exceptions Exceptions) {
	//TODO implement me
}
