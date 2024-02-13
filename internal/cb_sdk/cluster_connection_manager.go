package cb_sdk

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"strings"
	"sync"
	"time"
)

// ConnectionManager contains different cluster information and connections to them.
type ConnectionManager struct {
	Clusters map[string]*ClusterObject
	lock     sync.Mutex
}

// ConfigConnectionManager returns an instance of ConnectionManager.
func ConfigConnectionManager() *ConnectionManager {

	return &ConnectionManager{
		Clusters: make(map[string]*ClusterObject),
		lock:     sync.Mutex{},
	}
}

func addKVPoolSize(connStr string) string {
	if !strings.Contains(connStr, "kv_pool_size") {
		if !strings.Contains(connStr, "?") {
			return connStr + "?kv_pool_size=32"
		}
	}
	return connStr
}

// Disconnect disconnect a particular Clusters
func (cm *ConnectionManager) Disconnect(connstr string) error {
	clusterIdentifier, err := GetClusterIdentifier(connstr)
	if err != nil {
		return err
	}
	clusterObj, ok := cm.Clusters[clusterIdentifier]
	if ok {
		return clusterObj.Cluster.Close(nil)
	}
	return nil
}

// DisconnectAll disconnect all the Clusters used in a tasks.Request
func (cm *ConnectionManager) DisconnectAll() {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	for cS, v := range cm.Clusters {
		if v.Cluster != nil {
			_ = v.Cluster.Close(nil)
			delete(cm.Clusters, cS)
		}
		v = nil
	}
}

// setClusterObject maps a couchbase Cluster via connection string to *gocb.Cluster
func (cm *ConnectionManager) setClusterObject(clusterIdentifier string, c *ClusterObject) {
	cm.Clusters[clusterIdentifier] = c
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

// GetClusterIdentifier get the ip address and build a cluster Identifier
func GetClusterIdentifier(connStr string) (string, error) {
	if strings.Contains(connStr, "couchbases://") {
		return getClusterIdentifierHelper(connStr, "couchbases://"), nil
	} else if strings.Contains(connStr, "couchbase://") {
		return getClusterIdentifierHelper(connStr, "couchbase://"), nil
	} else {
		return "", err_sirius.InvalidConnectionString
	}
}

// getClusterObject returns ClusterObject if cluster is already setup.
// If not, then set up a ClusterObject using ClusterConfig.
func (cm *ConnectionManager) getClusterObject(connStr, username, password string,
	clusterConfig *ClusterConfig) (*ClusterObject, error) {

	clusterIdentifier, err := GetClusterIdentifier(connStr)
	if err != nil {
		return nil, err
	}

	connStr = addKVPoolSize(connStr)

	_, ok := cm.Clusters[clusterIdentifier]
	if !ok {
		if err := ValidateClusterConfig(connStr, username, password, clusterConfig); err != nil {
			return nil, err
		}
		cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
			Username: username,
			Password: password,
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
			return nil, err
		}

		c := &ClusterObject{
			Cluster: cluster,
			Buckets: make(map[string]*BucketObject),
		}
		cm.setClusterObject(clusterIdentifier, c)
	}

	return cm.Clusters[clusterIdentifier], nil
}

// GetCollection return a *gocb.Collection which represents a single Collection.
func (cm *ConnectionManager) GetCollection(connStr, username, password string, clusterConfig *ClusterConfig,
	bucketName, scopeName, collectionName string) (*CollectionObject,
	error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	bObj, err2 := cObj.getBucketObject(bucketName)
	if err2 != nil {
		return nil, err2
	}
	sObj, err3 := bObj.getScopeObject(scopeName)
	if err3 != nil {
		return nil, err3
	}
	c, err4 := sObj.getCollection(collectionName)
	if err4 != nil {
		return nil, err4
	}
	return c, nil
}

// GetScope return a *gocb.Scope which represents  a single scope within a bucket.
func (cm *ConnectionManager) GetScope(connStr, username, password string, clusterConfig *ClusterConfig, bucketName,
	scopeName string) (*gocb.Scope, error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	bObj, err2 := cObj.getBucketObject(bucketName)
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
func (cm *ConnectionManager) GetBucket(connStr, username, password string, clusterConfig *ClusterConfig,
	bucketName string) (*gocb.Bucket,
	error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	bObj, err2 := cObj.getBucketObject(bucketName)
	if err2 != nil {
		return nil, err2
	}
	return bObj.bucket, nil
}

// GetCluster return a *gocb.Cluster which represents connection to a specific Couchbase Cluster.
func (cm *ConnectionManager) GetCluster(connStr, username, password string, clusterConfig *ClusterConfig) (*gocb.Cluster,
	error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	return cObj.Cluster, nil
}
