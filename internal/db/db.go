package db

import (
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"sync"
)

const (
	CouchbaseDb       = "couchbase"
	MongoDb           = "mongodb"
	CouchbaseColumnar = "columnar"
	DynamoDb          = "dynamodb"
	CassandraDb       = "cassandra"
	MySql             = "mysql"
)

type OperationResult interface {
	Key() string
	Value() interface{}
	GetStatus() bool
	GetError() error
	GetExtra() map[string]any
	GetOffset() int64
}

type BulkOperationResult interface {
	Value(key string) interface{}
	GetStatus(key string) bool
	GetError(key string) error
	GetExtra(key string) map[string]any
	GetOffset(key string) int64
	GetSize() int
}

type SubDocOperationResult interface {
	Key() string
	Value(subPath string) (interface{}, int64)
	Values() []KeyValue
	GetError() error
	GetExtra() map[string]any
	GetOffset() int64
}

type Database interface {
	Connect(connStr, username, password string, extra Extras) error
	Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult
	Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult
	Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult
	Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult
	Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult
	InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult
	UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult
	Increment(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult
	ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult
	ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult
	DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult
	CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	Warmup(connStr, username, password string, extra Extras) error
	CreateDatabase(connStr, username, password string, extra Extras, templateName string, docSize int) (string, error)
	DeleteDatabase(connStr, username, password string, extra Extras) (string, error)
	Count(connStr, username, password string, extra Extras) (int64, error)
	ListDatabase(connStr, username, password string, extra Extras) (any, error)
	Close(connStr string, extra Extras) error
}

var couchbase *Couchbase
var mongodb *Mongo
var cbcolumnar *Columnar
var dynamo *Dynamo
var mysql *Sql

var lock = &sync.Mutex{}

func ConfigDatabase(dbType string) (Database, error) {
	switch dbType {
	case MongoDb:
		if mongodb == nil {
			lock.Lock()
			defer lock.Unlock()
			if mongodb == nil {
				mongodb = NewMongoConnectionManager()
			}
		}
		return mongodb, nil
	case CouchbaseDb:
		if couchbase == nil {
			lock.Lock()
			defer lock.Unlock()
			if couchbase == nil {
				couchbase = NewCouchbaseConnectionManager()
			}
		}
		return couchbase, nil
	case CouchbaseColumnar:
		if cbcolumnar == nil {
			lock.Lock()
			defer lock.Unlock()
			if cbcolumnar == nil {
				cbcolumnar = NewColumnarConnectionManager()
			}
		}
		return cbcolumnar, nil
	case DynamoDb:
		if dynamo == nil {
			lock.Lock()
			defer lock.Unlock()
			if dynamo == nil {
				dynamo = NewDynamoConnectionManager()
			}
		}
		return dynamo, nil
	case MySql:
		if mysql == nil {
			lock.Lock()
			defer lock.Unlock()
			if mysql == nil {
				mysql = NewSqlConnectionManager()
			}
		}
		return mysql, nil
	default:
		return nil, err_sirius.InvalidDatabase
	}
}
