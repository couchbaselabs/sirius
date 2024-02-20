package sdk_mongo

import (
	"context"
	"fmt"
	"log"

	"github.com/barkha06/sirius/internal/err_sirius"
	"go.mongodb.org/mongo-driver/mongo"
)

//const WaitUnityReadyTime = 10
//const WaitUntilReadyTimeRetries = 5
//const (
//	ConnectTimeout      = "connectTimeout"
//	KVTimeout           = "kvTimeout"
//	KVDurableTimeout    = "kvDurableTimeout"
//	CompressionDisabled = "compressionDisabled"
//	CompressionMinSize  = "compressionMinSize"
//	CompressionMaxSize  = "compressionMinSize"
//)

// MongoConnectionOptions defines the Connection Options to MongoDB
type MongoConnectionOptions struct {
	Compressors          string // zlib, snappy, and zstd.
	ZlibCompressionLevel int    // -1 to 9, 0 means no compression
	ConnectTimoutMS      int
	RetryWrites          bool
	RetryReads           bool
}

// MongoClusterConfig maintains the Config Settings for Connection to the MongoDB Cluster
type MongoClusterConfig struct {
	ConnectionString       string                 `json:"connectionString,omitempty"`
	Username               string                 `json:"username,omitempty"`
	Password               string                 `json:"password,omitempty"`
	MongoConnectionOptions MongoConnectionOptions `json:"mongoConnectionOptions,omitempty"`
}

func ValidateClusterConfig(connStr, username, password string, c *MongoClusterConfig) error {
	if c == nil {
		c = &MongoClusterConfig{}
	}
	if connStr == "" {
		return err_sirius.InvalidConnectionString
	}
	if username == "" || password == "" {
		return fmt.Errorf("connection string : %s | %w", connStr, err_sirius.CredentialMissing)
	}
	return nil
}

type MongoClusterObject struct {
	MongoClusterClient *mongo.Client                   `json:"-"`
	MongoDatabases     map[string]*MongoDatabaseObject `json:"-"`
}

func (mongoClusterObj *MongoClusterObject) setMongoDatabaseObject(mongoDbName string, mongoDatabaseObj *MongoDatabaseObject) {
	mongoClusterObj.MongoDatabases[mongoDbName] = mongoDatabaseObj
}

func (mongoClusterObj *MongoClusterObject) getMongoDatabaseObject(mongoDbName string) (*MongoDatabaseObject, error) {
	_, ok := mongoClusterObj.MongoDatabases[mongoDbName]

	if !ok {
		mongoDatabase := mongoClusterObj.MongoClusterClient.Database(mongoDbName)

		mongoDatabaseObj := &MongoDatabaseObject{
			MongoDatabase:    mongoDatabase,
			MongoCollections: make(map[string]*MongoCollectionObject),
		}
		mongoClusterObj.setMongoDatabaseObject(mongoDbName, mongoDatabaseObj)
	}

	return mongoClusterObj.MongoDatabases[mongoDbName], nil
}

// Close closes the MongoDB Client connection.
func Close(mongoClusterObj *MongoClusterObject) error {
	// _ = mongoClusterObj.MongoClusterClient.Close(nil)
	if err := mongoClusterObj.MongoClusterClient.Disconnect(context.TODO()); err != nil {
		log.Println("Disconnect failed!")
		return err
	}
	return nil
}
