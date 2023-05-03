package tasks

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const RequestPath = "./internal/tasks/request_logs"
const RETRY = 5

type TaskWithIdentifier struct {
	Operation string `json:"operation"`
	Task      Task   `json:"task"`
}

type Request struct {
	Identifier     string               `json:"identifier"`
	Seed           int64                `json:"seed"`
	SeedEnd        int64                `json:"seedEnd"`
	Tasks          []TaskWithIdentifier `json:"tasks"`
	lock           sync.Mutex           `json:"-"`
	IndexChannel   chan int             `json:"-"`
	ctx            context.Context      `json:"-"`
	cancel         context.CancelFunc   `json:"-"`
	Retry          int                  `json:"retry"`
	Timeout        int64                `json:"timeout"`
	IndexCompleted int                  `json:"indexCompleted"`
}

// NewRequest return  a instance of Request
func NewRequest(identifier string) *Request {
	ctx, cancel := context.WithCancel(context.Background())
	seed := time.Now().UnixNano()
	return &Request{
		Identifier:     identifier,
		Seed:           seed,
		SeedEnd:        seed,
		lock:           sync.Mutex{},
		IndexChannel:   make(chan int, 1),
		ctx:            ctx,
		cancel:         cancel,
		Retry:          RETRY,
		Timeout:        300,
		IndexCompleted: -1,
	}
}

// retracePreviousMutations returns a updated document after mutating the original documents.
func (r *Request) retracePreviousMutations(offset int64, doc interface{}, gen docgenerator.Generator,
	fake *faker.Faker, resultSeed int64) (interface{}, error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	for _, td := range r.Tasks {
		if td.Operation == UpsertOperation {
			u, ok := td.Task.(*UpsertTask)
			if !ok {
				return nil, fmt.Errorf("unable to decode upsert task from backlog")
			} else {
				if offset >= (u.Start-1) && (offset <= u.End-1) && resultSeed != u.ResultSeed {
					errOffset := u.State.ReturnErrOffset()
					if _, ok := errOffset[offset]; ok {
						continue
					} else {
						doc, _ = gen.Template.UpdateDocument(u.FieldsToChange, doc, fake)
					}
				}
			}
		}
	}
	return doc, nil
}

// retracePreviousDeletions returns a lookup table representing the offsets which are successfully deleted.
func (r *Request) retracePreviousDeletions(resultSeed int64) (map[int64]struct{}, error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	result := make(map[int64]struct{})
	for _, td := range r.Tasks {
		if td.Operation == DeleteOperation {
			u, ok := td.Task.(*DeleteTask)
			if !ok {
				return map[int64]struct{}{}, fmt.Errorf("unable to decode delete task from backlog")
			} else {
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
func (r *Request) retracePreviousFailedInsertions(resultSeed int64) (map[int64]struct{}, error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	result := make(map[int64]struct{})
	for _, td := range r.Tasks {
		if td.Operation == InsertOperation {
			u, ok := td.Task.(*InsertTask)
			if !ok {
				return map[int64]struct{}{}, fmt.Errorf("unable to decode delete task from backlog")
			} else {
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
func (r *Request) AddTask(o string, t Task) (error, int) {
	defer r.lock.Unlock()
	r.lock.Lock()
	r.Tasks = append(r.Tasks, TaskWithIdentifier{
		Operation: o,
		Task:      t,
	})
	err := r.saveRequestIntoFile()
	return err, len(r.Tasks) - 1
}

// AddToSeedEnd will update the Request.SeedEnd by  adding count into it.
func (r *Request) AddToSeedEnd(count int64) error {
	defer r.lock.Unlock()
	r.lock.Lock()
	r.SeedEnd += count
	err := r.saveRequestIntoFile()
	return err
}

// RemoveFromSeedEnd will update the Request.SeedEnd by  subtracting count into it.
func (r *Request) RemoveFromSeedEnd(count int64) error {
	defer r.lock.Unlock()
	r.lock.Lock()
	r.SeedEnd -= count
	err := r.saveRequestIntoFile()
	return err
}

// checkAndUpdateSeedEnd will store the max seed value that may occur in upsert operations.
func (r *Request) checkAndUpdateSeedEnd(key int64) {
	defer r.lock.Unlock()
	r.lock.Lock()
	if key > r.SeedEnd {
		r.SeedEnd = key
	}
}

// SendOverIndexChannel  :-> Future proof
func (r *Request) SendOverIndexChannel(index int) error {
	defer r.lock.Unlock()
	r.lock.Lock()
	r.IndexChannel <- index
	err := r.saveRequestIntoFile()
	return err
}

// UpdateIndexCompleted  :-> Future proof
func (r *Request) UpdateIndexCompleted(index int) error {
	defer r.lock.Lock()
	r.IndexCompleted = index
	err := r.SaveRequestIntoFile()
	return err
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
