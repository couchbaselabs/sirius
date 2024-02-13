package db

import (
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"sync"
)

const (
	COUCHBASE_DB = "couchbase"
	MONGO_DB     = "Mongo"
)

type OperationResult interface {
	Key() string
	Value() interface{}
	GetStatus() bool
	GetError() error
	GetExtra() map[string]any
}

type Database interface {
	Connect(connStr, username, password string, extra Extras) error
	Create(connStr, username, password, key string, doc interface{}, extra Extras) OperationResult
	Read(connStr, username, password, key string, extra Extras) OperationResult
	Update(connStr, username, password, key string, doc interface{}, extra Extras) OperationResult
	Delete(connStr, username, password, key string, extra Extras) OperationResult
	Touch(connStr, username, password, key string, extra Extras) OperationResult
	InsertSubDoc(connStr, username, password, key string, subPathValues map[string]any,
		extra Extras) OperationResult
	UpsertSubDoc(connStr, username, password, key string, subPathValues map[string]any,
		extra Extras) OperationResult
	ReplaceSubDoc(connStr, username, password, key string, subPathValues map[string]any,
		extra Extras) OperationResult
	ReadSubDoc(connStr, username, password, key string, subPathValues map[string]any,
		extra Extras) OperationResult
	DeleteSubDoc(connStr, username, password, key string, subPathValues map[string]any, extra Extras) OperationResult
	IncrementMutationCount(connStr, username, password, key string, subPathValues map[string]any,
		extra Extras) OperationResult
	Warmup(connStr, username, password string, extra Extras) error
	Close(connStr string) error
}

var couchbase *Couchbase
var mongo *Mongo
var lock = &sync.Mutex{}

func ConfigDatabase(dbType string) (Database, error) {
	switch dbType {
	case MONGO_DB:
		if mongo == nil {
			lock.Lock()
			defer lock.Unlock()
			if mongo == nil {
				mongo = &Mongo{}
			}
		}
		return mongo, nil
	case COUCHBASE_DB:
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
