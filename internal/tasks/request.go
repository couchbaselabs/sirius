package tasks

import (
	"encoding/gob"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_meta_data"
	"github.com/jaswdr/faker"
	"os"
	"path/filepath"
	"sync"
)

const RequestPath = "./internal/tasks/request_logs"

type TaskWithIdentifier struct {
	Operation string `json:"operation" doc:"true"`
	Task      Task   `json:"task" doc:"true"`
}

type Request struct {
	Identifier        string                   `json:"identifier" doc:"false" `
	Tasks             []TaskWithIdentifier     `json:"tasks" doc:"false"`
	MetaData          *task_meta_data.MetaData `json:"metaData" doc:"false"`
	connectionManager *sdk.ConnectionManager   `json:"-" doc:"false"`
	lock              sync.Mutex               `json:"-" doc:"false"`
}

// NewRequest return  an instance of Request
func NewRequest(identifier string) *Request {
	return &Request{
		Identifier:        identifier,
		MetaData:          task_meta_data.NewMetaData(),
		connectionManager: sdk.ConfigConnectionManager(),
		lock:              sync.Mutex{},
	}
}

// ReconnectionManager setups again sdk.ConnectionManager
func (r *Request) ReconnectionManager() {
	defer r.lock.Unlock()
	r.lock.Lock()
	if r.connectionManager == nil {
		r.connectionManager = sdk.ConfigConnectionManager()
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

// retracePreviousMutations returns an updated document after mutating the original documents.
func (r *Request) retracePreviousMutations(collectionIdentifier string, offset int64, doc interface{},
	gen docgenerator.Generator, fake *faker.Faker, resultSeed int64) (interface{}, error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == UpsertOperation {
			u, ok := td.Task.(*UpsertTask)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
					ResultSeed {
					if u.State == nil {
						return doc, fmt.Errorf("Unable to retrace previous mutations on sirius for " + u.CollectionIdentifier())
					}
					errOffset := u.State.ReturnErrOffset()
					if _, ok := errOffset[offset]; ok {
						continue
					} else {
						doc, _ = gen.Template.UpdateDocument(u.OperationConfig.FieldsToChange, doc, fake)
					}
				}
			}
		}
	}
	return doc, nil
}

func (r *Request) retracePreviousSubDocMutations(collectionIdentifier string, offset int64, gen docgenerator.Generator,
	fake *faker.Faker, resultSeed int64) map[string]any {
	defer r.lock.Unlock()
	r.lock.Lock()
	var result map[string]any
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == SubDocUpsertOperation {
			u, ok := td.Task.(*SubDocUpsert)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if offset >= (u.SubDocOperationConfig.Start) && (offset < u.SubDocOperationConfig.End) && resultSeed != u.
					ResultSeed {
					errOffset := u.State.ReturnErrOffset()
					if _, ok := errOffset[offset]; ok {
						continue
					} else {
						result = gen.Template.GenerateSubPathAndValue(fake)
					}
				}
			}
		}
	}
	return result
}

// countMutation return the number of mutation happened on a offset
func (r *Request) countMutation(collectionIdentifier string, offset int64, resultSeed int64) int {
	defer r.lock.Unlock()
	r.lock.Lock()
	var result int = 0
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == SubDocUpsertOperation {
			u, ok := td.Task.(*SubDocUpsert)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if offset >= (u.SubDocOperationConfig.Start) && (offset < u.SubDocOperationConfig.End) && resultSeed != u.
					ResultSeed {
					completeOffset := u.State.ReturnCompletedOffset()
					if _, ok := completeOffset[offset]; ok {
						result++
					}
				}
			}
		} else if td.Operation == SubDocDeleteOperation {
			u, ok := td.Task.(*SubDocDelete)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if offset >= (u.SubDocOperationConfig.Start) && (offset < u.SubDocOperationConfig.End) && resultSeed != u.
					ResultSeed {
					completeOffset := u.State.ReturnCompletedOffset()
					if _, ok := completeOffset[offset]; ok {
						result++
					}
				}
			}
		} else if td.Operation == SubDocReplaceOperation {
			u, ok := td.Task.(*SubDocReplace)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if offset >= (u.SubDocOperationConfig.Start) && (offset < u.SubDocOperationConfig.End) && resultSeed != u.
					ResultSeed {
					completeOffset := u.State.ReturnCompletedOffset()
					if _, ok := completeOffset[offset]; ok {
						result++
					}
				}
			}
		} else if td.Operation == SubDocInsertOperation {
			u, ok := td.Task.(*SubDocInsert)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if offset >= (u.SubDocOperationConfig.Start) && (offset < u.SubDocOperationConfig.End) && resultSeed != u.
					ResultSeed {
					completeOffset := u.State.ReturnCompletedOffset()
					if _, ok := completeOffset[offset]; ok {
						result++
					}
				}
			}
		}
	}
	return result

}

// retracePreviousDeletions returns a lookup table representing the offsets which are successfully deleted.
func (r *Request) retracePreviousDeletions(collectionIdentifier string, resultSeed int64) (map[int64]struct{}, error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	result := make(map[int64]struct{})
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == DeleteOperation {
			u, ok := td.Task.(*DeleteTask)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if resultSeed != u.ResultSeed {
					completedOffSet := u.State.ReturnCompletedOffset()
					for deletedOffset, _ := range completedOffSet {
						result[deletedOffset] = struct{}{}
					}
				}
			}
		}
	}
	return result, nil
}

// retracePreviousDeletions returns a lookup table representing the offsets which are successfully deleted.
func (r *Request) retracePreviousSubDocDeletions(collectionIdentifier string, resultSeed int64) (map[int64]struct{},
	error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	result := make(map[int64]struct{})
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == SubDocDeleteOperation {
			u, ok := td.Task.(*SubDocDelete)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if resultSeed != u.ResultSeed {
					completedOffSet := u.State.ReturnCompletedOffset()
					for deletedOffset, _ := range completedOffSet {
						result[deletedOffset] = struct{}{}
					}
				}
			}
		}
	}
	return result, nil
}

// returns a lookup table representing the offsets which are not inserted properly..
func (r *Request) retracePreviousFailedInsertions(collectionIdentifier string, resultSeed int64) (map[int64]struct{},
	error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	result := make(map[int64]struct{})
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == InsertOperation {
			u, ok := td.Task.(*InsertTask)
			if ok {
				if collectionIdentifier != u.CollectionIdentifier() {
					continue
				}
				if resultSeed != u.ResultSeed {
					errorOffSet := u.State.ReturnErrOffset()
					for offSet, _ := range errorOffSet {
						result[offSet] = struct{}{}
					}
				}
			}
		}
	}
	return result, nil
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
func (r *Request) AddToSeedEnd(collectionMetaData *task_meta_data.CollectionMetaData, count int64) {
	collectionMetaData.SeedEnd += count
	_ = r.saveRequestIntoFile()
}

// checkAndUpdateSeedEnd will store the max seed value that may occur in upsert operations.
func (r *Request) checkAndUpdateSeedEnd(collectionMetaData *task_meta_data.CollectionMetaData, key int64) {
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
		return nil, fmt.Errorf("no such file (request) found for an Identifier")
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
