package db

import (
	"sync"

	"github.com/barkha06/sirius/internal/err_sirius"
)

const (
	CouchbaseDb = "couchbase"
	MongoDb     = "mongodb"
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
	InsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult
	UpsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult
	Increment(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult
	ReplaceSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult
	ReadSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult
	DeleteSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult
	CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult
	Warmup(connStr, username, password string, extra Extras) error
	Close(connStr string) error
}

var couchbase *Couchbase
var mongodb *Mongo
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
	default:
		return nil, err_sirius.InvalidDatabase
	}
}
