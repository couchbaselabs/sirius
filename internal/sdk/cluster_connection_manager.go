package sdk

import (
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	ClusterConnectionLimit = 1
)

// ConnectionManager contains different cluster information and connections to them.
type ConnectionManager struct {
	clusters map[string][]*ClusterObject
	lock     sync.Mutex
}

// ConfigConnectionManager returns an instance of ConnectionManager.
func ConfigConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		clusters: make(map[string][]*ClusterObject),
		lock:     sync.Mutex{},
	}
}

func addKVPoolSize(connStr string) string {
	if !strings.Contains(connStr, "kv_pool_size") {
		if !strings.Contains(connStr, "?") {
			return connStr + "?kv_pool_size=25"
		}
	}
	return connStr
}

// DisconnectAll disconnect all the clusters used in a tasks.Request
func (cm *ConnectionManager) DisconnectAll() {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	for cS, cObj := range cm.clusters {
		for index, c := range cObj {
			if err := c.Cluster.Close(nil); err != nil {
				log.Println(fmt.Sprintf("error in closing connection at %d of %s", index, cS))
			}
			c.Cluster = nil
		}
		delete(cm.clusters, cS)
	}
}

// setClusterObject maps a couchbase Cluster via connection string to *gocb.Cluster
func (cm *ConnectionManager) setClusterObject(clusterIdentifier string, c []*ClusterObject) {
	cm.clusters[clusterIdentifier] = c
}

func getClusterIdentifierHelper(connStr string, x string) string {
	startIndex := len(x)
	i := startIndex
	for i = startIndex; i < len(connStr); i++ {
		if connStr[i] == '?' {
			break
		}
	}
	return connStr[startIndex:i]
}

// getClusterIdentifier get the ip address and build a cluster Identifier
func getClusterIdentifier(connStr string) (string, error) {
	if strings.Contains(connStr, "couchbases://") {
		return getClusterIdentifierHelper(connStr, "couchbases://"), nil
	} else if strings.Contains(connStr, "couchbase://") {
		return getClusterIdentifierHelper(connStr, "couchbase://"), nil
	} else {
		return "", task_errors.ErrInvalidConnectionString
	}
}

// getClusterObject returns ClusterObject if cluster is already setup.
// If not, then set up a ClusterObject using ClusterConfig.
func (cm *ConnectionManager) getClusterObject(clusterConfig *ClusterConfig) ([]*ClusterObject,
	error) {

	if clusterConfig == nil {
		return []*ClusterObject{}, fmt.Errorf("unable to parse clusterConfig | %w", errors.New("clusterConfig is nil"))
	}

	clusterIdentifier, err := getClusterIdentifier(clusterConfig.ConnectionString)
	if err != nil {
		return []*ClusterObject{}, err
	}

	clusterConfig.ConnectionString = addKVPoolSize(clusterConfig.ConnectionString)

	_, ok := cm.clusters[clusterIdentifier]
	if !ok {
		var clusterObjectList []*ClusterObject
		for i := 0; i < ClusterConnectionLimit; i++ {
			cluster, err := gocb.Connect(clusterConfig.ConnectionString, gocb.ClusterOptions{
				Authenticator: gocb.PasswordAuthenticator{
					Username: clusterConfig.Username,
					Password: clusterConfig.Password,
				},
				Username: clusterConfig.Username,
				Password: clusterConfig.Password,
				TimeoutsConfig: gocb.TimeoutsConfig{
					ConnectTimeout:   time.Duration(clusterConfig.TimeoutsConfig.ConnectTimeout) * time.Second,
					KVTimeout:        time.Duration(clusterConfig.TimeoutsConfig.KVTimeout) * time.Second,
					KVDurableTimeout: time.Duration(clusterConfig.TimeoutsConfig.KVDurableTimeout) * time.Second,
				},
				CircuitBreakerConfig: gocb.CircuitBreakerConfig{
					Disabled: true,
				},
				CompressionConfig: gocb.CompressionConfig{
					Disabled: clusterConfig.CompressionConfig.Disabled,
					MinSize:  clusterConfig.CompressionConfig.MinSize,
					MinRatio: clusterConfig.CompressionConfig.MinRatio,
				},
				SecurityConfig: gocb.SecurityConfig{
					TLSSkipVerify: true,
				},
				InternalConfig: gocb.InternalConfig{
					ConnectionBufferSize: 1048576,
				},
			})
			if err != nil {
				continue
			}

			clusterObjectList = append(clusterObjectList, &ClusterObject{
				Cluster: cluster,
				Buckets: make(map[string]*BucketObject),
			})
		}

		cm.setClusterObject(clusterIdentifier, clusterObjectList)
	}

	return cm.clusters[clusterIdentifier], nil
}

// GetCollection return a *gocb.Collection which represents a single Collection.
func (cm *ConnectionManager) GetCollection(clusterConfig *ClusterConfig, bucketName, scopeName,
	collectionName string) ([]*CollectionObject,
	error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	var collectionObjectList []*CollectionObject
	cObj, err1 := cm.getClusterObject(clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	if len(cObj) == 0 {
		return nil, errors.New("Unable to connect to cluster")
	}
	for i := 0; i < len(cObj); i++ {
		bObj, err2 := cObj[i].getBucketObject(bucketName)
		if err2 != nil {
			log.Println("unable to get bucket for" + clusterConfig.ConnectionString)
			continue
		}
		sObj, err3 := bObj.getScopeObject(scopeName)
		if err3 != nil {
			log.Println("unable to get scope for" + clusterConfig.ConnectionString)
			continue
		}
		c, err4 := sObj.getCollection(collectionName)
		if err4 != nil {
			log.Println("unable to get collection for" + clusterConfig.ConnectionString)
			continue
		}
		collectionObjectList = append(collectionObjectList, c)
	}

	if len(collectionObjectList) == 0 {
		return []*CollectionObject{}, errors.New("empty list of collectionObject")
	}
	return collectionObjectList, nil

}

// GetScope return a *gocb.Scope which represents  a single scope within a bucket.
func (cm *ConnectionManager) GetScope(clusterConfig *ClusterConfig, bucketName, scopeName string) (*gocb.Scope,
	error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getClusterObject(clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	bObj, err2 := cObj[0].getBucketObject(bucketName)
	if err2 != nil {
		return nil, err2
	}
	sObj, err3 := bObj.getScopeObject(scopeName)
	if err3 != nil {
		return nil, err3
	}
	return sObj.scope, nil
}

// GetBucket return a *gocb.Bucket which represents a single bucket within a Cluster.
func (cm *ConnectionManager) GetBucket(clusterConfig *ClusterConfig, bucketName string) (*gocb.Bucket,
	error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getClusterObject(clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	bObj, err2 := cObj[0].getBucketObject(bucketName)
	if err2 != nil {
		return nil, err2
	}
	return bObj.bucket, nil
}

// GetCluster return a *gocb.Cluster which represents connection to a specific Couchbase Cluster.
func (cm *ConnectionManager) GetCluster(clusterConfig *ClusterConfig) (*gocb.Cluster,
	error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getClusterObject(clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	return cObj[0].Cluster, nil
}
