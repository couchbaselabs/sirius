package sdk_columnar

import (
	"github.com/couchbase/gocb/v2"
	"sync"
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

// Disconnect disconnect a particular Clusters
func (cm *ConnectionManager) Disconnect(connStr string) error {
	clusterIdentifier := connStr
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

// getClusterObject returns ClusterObject if cluster is already setup.
// If not, then set up a ClusterObject using ClusterConfig.
func (cm *ConnectionManager) getClusterObject(connStr, username, password string,
	clusterConfig *ClusterConfig) (*ClusterObject, error) {

	clusterIdentifier := connStr

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
		})
		if err != nil {
			return nil, err
		}

		c := &ClusterObject{
			Cluster: cluster,
		}
		cm.setClusterObject(clusterIdentifier, c)
	}

	return cm.Clusters[clusterIdentifier], nil
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
