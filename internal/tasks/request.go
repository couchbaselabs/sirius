package tasks

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/meta_data"
	"os"
	"path/filepath"
	"sync"
)

const RequestPath = "./internal/tasks/request_logs"

type TaskWithIdentifier struct {
	Operation string `json:"operation" doc:"false"`
	Task      Task   `json:"task" doc:"false"`
}

type Request struct {
	Identifier        string                       `json:"identifier" doc:"false" `
	Tasks             []TaskWithIdentifier         `json:"tasks" doc:"false"`
	MetaData          *meta_data.MetaData          `json:"metaData" doc:"false"`
	DocumentsMeta     *meta_data.DocumentsMetaData `json:"documentMeta" doc:"false"`
	connectionManager *cb_sdk.ConnectionManager    `json:"-" doc:"false"`
	lock              sync.Mutex                   `json:"-" doc:"false"`
	ctx               context.Context              `json:"-"`
	cancel            context.CancelFunc           `json:"-"`
}

// NewRequest return  an instance of Request
func NewRequest(identifier string) *Request {
	ctx, cancel := context.WithCancel(context.Background())
	return &Request{
		Identifier:        identifier,
		MetaData:          meta_data.NewMetaData(),
		DocumentsMeta:     meta_data.NewDocumentsMetaData(),
		connectionManager: cb_sdk.ConfigConnectionManager(),
		lock:              sync.Mutex{},
		ctx:               ctx,
		cancel:            cancel,
	}
}

// Cancel cancels the context of request
func (r *Request) Cancel() {
	r.cancel()
}

// ContextClosed return true if request's context channel is closed else return false
func (r *Request) ContextClosed() bool {
	if r.ctx.Err() != nil {
		return true
	}
	return false
}

// InitializeContext is used to sirius_documentation new contextWithCancel for request upon restart of sirius.
func (r *Request) InitializeContext() {
	ctx, cancel := context.WithCancel(context.Background())
	r.ctx = ctx
	r.cancel = cancel
}

// ReconnectionManager setups again cb_sdk.ConnectionManager
func (r *Request) ReconnectionManager() {
	defer r.lock.Unlock()
	r.lock.Lock()
	if r.connectionManager == nil {
		r.connectionManager = cb_sdk.ConfigConnectionManager()
	}
}

// ReconfigureDocumentManager setups again cb_sdk.ConnectionManager
func (r *Request) ReconfigureDocumentManager() {
	defer r.lock.Unlock()
	r.lock.Lock()
	if r.DocumentsMeta == nil {
		r.DocumentsMeta = meta_data.NewDocumentsMetaData()
	}
}

// DisconnectConnectionManager disconnect all the cluster connections.
func (r *Request) DisconnectConnectionManager() {
	defer r.lock.Unlock()
	r.lock.Lock()
	if r.connectionManager == nil {
		return
	}
	r.connectionManager.DisconnectAll()
}

// ClearAllTask will remove all task
func (r *Request) ClearAllTask() {
	for i := range r.Tasks {
		r.Tasks[i].Task = nil
	}
}

// AddTask will add tasks.Task with operation type.
func (r *Request) AddTask(o string, t Task) error {
	defer r.lock.Unlock()
	r.lock.Lock()
	r.Tasks = append(r.Tasks, TaskWithIdentifier{
		Operation: o,
		Task:      t,
	})
	err := r.saveRequestIntoFile()
	return err
}

// AddToSeedEnd will update the Request.SeedEnd by  adding count into it.
func (r *Request) AddToSeedEnd(collectionMetaData *meta_data.CollectionMetaData, count int64) {
	collectionMetaData.SeedEnd += count
	_ = r.saveRequestIntoFile()
}

// checkAndUpdateSeedEnd will store the max seed value that may occur in upsert operations.
func (r *Request) checkAndUpdateSeedEnd(collectionMetaData *meta_data.CollectionMetaData, key int64) {
	defer r.lock.Unlock()
	r.lock.Lock()
	if key > collectionMetaData.SeedEnd {
		collectionMetaData.SeedEnd = key
	}
}

// RemoveRequestFromFile will remove Request from the disk.
func RemoveRequestFromFile(identifier string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, RequestPath, identifier)
	return os.Remove(fileName)
}

func (r *Request) saveRequestIntoFile() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, RequestPath, r.Identifier)
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(r); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return nil

}

// SaveRequestIntoFile will save request into disk
func (r *Request) SaveRequestIntoFile() error {
	defer r.lock.Unlock()
	r.lock.Lock()
	return r.saveRequestIntoFile()
}

func (r *Request) GetCluster(config *cb_sdk.ClusterConfig) (*gocb.Cluster, error) {
	return r.connectionManager.GetCluster(config)
}

func (r *Request) GetBucket(clusterConfig *cb_sdk.ClusterConfig, bucketName string) (*gocb.Bucket,
	error) {
	return r.connectionManager.GetBucket(clusterConfig, bucketName)
}

func (r *Request) GetCollection(config *cb_sdk.ClusterConfig, bucket string, scope string, collection string) (*cb_sdk.CollectionObject, error) {
	return r.connectionManager.GetCollection(config, bucket, scope, collection)
}

func (r *Request) GetScope(config *cb_sdk.ClusterConfig, bucket string, scope string) (*gocb.Scope,
	error) {
	return r.connectionManager.GetScope(config, bucket, scope)
}

// ReadRequestFromFile will return Request from the disk.
func ReadRequestFromFile(identifier string) (*Request, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	fileName := filepath.Join(cwd, RequestPath, identifier)
	r := &Request{}
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("no such file (request) found for an Identifier" + identifier)
	}
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(r); err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Request) Lock() {
	r.lock.Lock()
}

func (r *Request) Unlock() {
	r.lock.Unlock()
}
