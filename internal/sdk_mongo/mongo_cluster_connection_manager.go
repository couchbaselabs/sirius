package sdk_mongo

import (
	"context"
	"fmt"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoConnectionManager contains different cluster information and connections to them.
type MongoConnectionManager struct {
	Clusters map[string]*MongoClusterObject
	lock     sync.Mutex
}

// ConfigMongoConnectionManager returns an instance of MongoConnectionManager.
func ConfigMongoConnectionManager() *MongoConnectionManager {

	return &MongoConnectionManager{
		Clusters: make(map[string]*MongoClusterObject),
		lock:     sync.Mutex{},
	}
}

// Disconnect disconnect a particular Clusters
func (cm *MongoConnectionManager) Disconnect(connstr string) error {
	clusterIdentifier := connstr
	clusterObj, ok := cm.Clusters[clusterIdentifier]
	if ok {
		if err := clusterObj.MongoClusterClient.Disconnect(context.TODO()); err != nil {
			fmt.Println("Disconnect failed!")
			log.Fatal(err)
			return err
		}
	}
	return nil
}

// // DisconnectAll disconnect all the Clusters used in a tasks.Request
// func (cm *MongoConnectionManager) DisconnectAll() {
// 	defer cm.lock.Unlock()
// 	cm.lock.Lock()
// 	for cS, v := range cm.Clusters {
// 		if v.Cluster != nil {
// 			_ = v.Cluster.Close(nil)
// 			delete(cm.Clusters, cS)
// 		}
// 		v = nil
// 	}
// }

// setMongoClusterObject maps a couchbase Cluster via connection string to *gocb.Cluster
func (cm *MongoConnectionManager) setMongoClusterObject(clusterIdentifier string, c *MongoClusterObject) {
	cm.Clusters[clusterIdentifier] = c
}

// // getClusterIdentifierHelper helps to get the IP of cb conn str
// func getClusterIdentifierHelper(connStr string, x string) string {
// 	startIndex := len(x)
// 	i := startIndex
// 	for i = startIndex; i < len(connStr); i++ {
// 		if connStr[i] == '?' {
// 			break
// 		}
// 	}
// 	return connStr[startIndex:i]
// }

// // GetClusterIdentifier get the ip address and build a cluster Identifier
// func GetClusterIdentifier(connStr string) (string, error) {
// 	if strings.Contains(connStr, "couchbases://") {
// 		return getClusterIdentifierHelper(connStr, "couchbases://"), nil
// 	} else if strings.Contains(connStr, "couchbase://") {
// 		return getClusterIdentifierHelper(connStr, "couchbase://"), nil
// 	} else {
// 		return "", err_sirius.InvalidConnectionString
// 	}
// }

// getMongoClusterObject returns MongoClusterObject if cluster is already setup.
// If not, then set up a MongoClusterObject using MongoClusterObject.
func (cm *MongoConnectionManager) getMongoClusterObject(connStr, username, password string,
	clusterConfig *MongoClusterConfig) (*MongoClusterObject, error) {

	clusterIdentifier := connStr

	_, ok := cm.Clusters[clusterIdentifier]
	if !ok {
		if err := ValidateClusterConfig(connStr, username, password, clusterConfig); err != nil {
			return nil, err
		}

		cluster, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(connStr))
		if err != nil {
			fmt.Println("Unable to connect to MongoDB!")
			log.Fatal(err)
			return nil, err
		}

		c := &MongoClusterObject{
			MongoClusterClient: cluster,
			MongoDatabases:     make(map[string]*MongoDatabaseObject),
		}
		cm.setMongoClusterObject(clusterIdentifier, c)
	}

	return cm.Clusters[clusterIdentifier], nil
}

// GetMongoCollection return a *mongo.Collection which represents a single Collection.
func (cm *MongoConnectionManager) GetMongoCollection(connStr, username, password string, clusterConfig *MongoClusterConfig,
	mongoDbName, collectionName string) (*MongoCollectionObject, error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getMongoClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	bObj, err2 := cObj.getMongoDatabaseObject(mongoDbName)
	if err2 != nil {
		return nil, err2
	}
	c, err3 := bObj.getCollectionObject(collectionName)
	if err3 != nil {
		return nil, err3
	}
	return c, nil
}

// GetMongoDatabase return a *mongo.Database which represents a single bucket within a Cluster.
func (cm *MongoConnectionManager) GetMongoDatabase(connStr, username, password string, clusterConfig *MongoClusterConfig,
	mongoDbName string) (*mongo.Database, error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getMongoClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	bObj, err2 := cObj.getMongoDatabaseObject(mongoDbName)
	if err2 != nil {
		return nil, err2
	}
	return bObj.MongoDatabase, nil
}

// GetMongoCluster return a *mongo.Client which represents connection to a specific Couchbase Cluster.
func (cm *MongoConnectionManager) GetMongoCluster(connStr, username, password string, clusterConfig *MongoClusterConfig) (*mongo.Client,
	error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getMongoClusterObject(connStr, username, password, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	return cObj.MongoClusterClient, nil
}
