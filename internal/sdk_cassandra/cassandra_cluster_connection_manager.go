package sdk_cassandra

import (
	"log"
	"sync"

	"github.com/gocql/gocql"
)

// CassandraConnectionManager contains different cluster information and connections to them.
type CassandraConnectionManager struct {
	Clusters map[string]*CassandraClusterObject
	lock     sync.Mutex
}

// ConfigCassandraConnectionManager returns an instance of CassandraConnectionManager.
func ConfigCassandraConnectionManager() *CassandraConnectionManager {
	return &CassandraConnectionManager{
		Clusters: make(map[string]*CassandraClusterObject),
		lock:     sync.Mutex{},
	}
}

// setCassandraClusterObject maps a CassandraClusterObject (having Cluster *gocql.Session) using connection string to CassandraConnectionManager.Clusters
func (cm *CassandraConnectionManager) setCassandraClusterObject(clusterIdentifier string, c *CassandraClusterObject) {
	cm.Clusters[clusterIdentifier] = c
}

// getCassandraClusterObject returns CassandraClusterObject if cluster is already setup.
// If cluster is not setup, then it sets it up using setCassandraClusterObject.
func (cm *CassandraConnectionManager) getCassandraClusterObject(connStr, username, password string,
	clusterConfig *CassandraClusterConfig) (*CassandraClusterObject, error) {

	clusterIdentifier := connStr
	_, ok := cm.Clusters[clusterIdentifier]
	if !ok {
		if err := ValidateClusterConfig(connStr, username, password, clusterConfig); err != nil {
			return nil, err
		}

		cassClusterConfig := gocql.NewCluster(connStr)
		cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}

		cassandraSession, err := cassClusterConfig.CreateSession()
		if err != nil {
			log.Println("Unable to connect to Cassandra!")
			log.Println(err)
			return nil, err
		}

		c := &CassandraClusterObject{
			CassandraClusterConfig: cassClusterConfig,
			CassandraClusterClient: cassandraSession,
			CassandraKeyspaces:     make(map[string]*CassandraKeyspaceObject),
		}
		cm.setCassandraClusterObject(clusterIdentifier, c)
	}

	return cm.Clusters[clusterIdentifier], nil
}

// GetCassandraCluster return a *gocql.Session which represents connection to a specific Cassandra Cluster.
func (cm *CassandraConnectionManager) GetCassandraCluster(connStr, username, password string,
	clusterConfig *CassandraClusterConfig) (*gocql.Session, error) {

	defer cm.lock.Unlock()
	cm.lock.Lock()

	cObj, err1 := cm.getCassandraClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	return cObj.CassandraClusterClient, nil
}

// GetCassandraKeyspace return a *gocql.Session which includes the Keyspace parameter along with Cluster details.
func (cm *CassandraConnectionManager) GetCassandraKeyspace(connStr, username, password string,
	clusterConfig *CassandraClusterConfig, cassKeyspaceName string) (*gocql.Session, error) {

	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getCassandraClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	bObj, err2 := cObj.getCassandraKeyspaceObject(cassKeyspaceName)
	if err2 != nil {
		return nil, err2
	}
	return bObj.CassandraKeyspace, nil
}

// Disconnect disconnects a particular Cluster
func (cm *CassandraConnectionManager) Disconnect(connStr string) error {
	clusterIdentifier := connStr
	cassClusterObj, ok := cm.Clusters[clusterIdentifier]
	if ok {
		cassClusterObj.CassandraClusterClient.Close()
	}
	return nil
}

// DisconnectAll disconnects all the Cassandra Clusters used in a tasks.Request
func (cm *CassandraConnectionManager) DisconnectAll() {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	for cS, v := range cm.Clusters {
		if v.CassandraClusterClient != nil {
			v.CassandraClusterClient.Close()
			delete(cm.Clusters, cS)
		}
		v = nil
	}
}
