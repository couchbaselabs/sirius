package task_result

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
)

const ResultPath = "./internal/task_result/task_result_logs"

// TaskResult defines the type of result stored in a response after an operation.

type FailedDocument struct {
	DocId       string      `json:"key" doc:"true"`
	Doc         interface{} `json:"value"  doc:"true"`
	ErrorString string      `json:"errorString"  doc:"true"`
}

type SingleOperationResult struct {
	ErrorString string `json:"errorString"  doc:"true"`
	Status      bool   `json:"status"  doc:"true"`
	Cas         uint64 `json:"cas"  doc:"true"`
}

type FailedQuery struct {
	Query       string `json:"query" doc:"true"`
	ErrorString string `json:"errorString" doc:"true"`
}

type TaskResult struct {
	ResultSeed      int                              `json:"resultSeed"`
	Operation       string                           `json:"operation"`
	ErrorOther      string                           `json:"otherErrors"`
	Success         int                              `json:"success"`
	Failure         int                              `json:"failure"`
	ValidationError []string                         `json:"validationErrors"`
	BulkError       map[string][]FailedDocument      `json:"bulkErrors"`
	QueryError      map[string][]FailedQuery         `json:"queryErrors"`
	SingleResult    map[string]SingleOperationResult `json:"singleResult"`
	lock            sync.Mutex                       `json:"-"`
}

// ConfigTaskResult returns a new instance of TaskResult
func ConfigTaskResult(operation string, seed int) *TaskResult {
	return &TaskResult{
		ResultSeed:   seed,
		Operation:    operation,
		BulkError:    make(map[string][]FailedDocument),
		QueryError:   make(map[string][]FailedQuery),
		SingleResult: make(map[string]SingleOperationResult),
		lock:         sync.Mutex{},
	}
}

// IncrementFailure saves the failure count of doc loading operation.
func (t *TaskResult) IncrementFailure(docId string, doc interface{}, err error) {
	t.lock.Lock()
	t.Failure++
	v, errorString := sdk.CheckSDKException(err)
	t.BulkError[v] = append(t.BulkError[v], FailedDocument{
		DocId:       docId,
		Doc:         doc,
		ErrorString: errorString,
	})
	t.lock.Unlock()
}

// IncrementQueryFailure saves the failure count of query running operation.
func (t *TaskResult) IncrementQueryFailure(query string, err error) {
	t.lock.Lock()
	t.Failure++
	v, errorString := sdk.CheckSDKException(err)
	t.QueryError[v] = append(t.QueryError[v], FailedQuery{
		Query:       query,
		ErrorString: errorString,
	})
	t.lock.Unlock()
}

// ValidationFailures saves the failing validation of documents.
func (t *TaskResult) ValidationFailures(docId string) {
	t.lock.Lock()
	t.ValidationError = append(t.ValidationError, docId)
	t.lock.Unlock()
}

// SaveResultIntoFile stores the task result on a file. It returns an error if saving fails.
func (t *TaskResult) SaveResultIntoFile() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, ResultPath, fmt.Sprintf("%d", t.ResultSeed))
	content, err := json.MarshalIndent(t, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(fileName, content, 0644)
	if err != nil {
		return err
	}
	return nil
}

// ReadResultFromFile reads the task result stored on a file. It returns the task result
// and possible error if task result file is missing, in processing or record file deleted.
func ReadResultFromFile(seed string, deleteRecord bool) (*TaskResult, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	fileName := filepath.Join(cwd, ResultPath, seed)
	result := &TaskResult{}
	content, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("no such result found, reasons:[No such Task, In process, Record Deleted]")
	}
	if err := json.Unmarshal(content, result); err != nil {
		return nil, err
	}
	// deleting the file after reading it to save disk space.
	if deleteRecord {
		if err := os.Remove(fileName); err != nil {
			log.Println("Manually clean " + fileName)
		}
	}
	return result, nil
}

func (t *TaskResult) CreateSingleErrorResult(docId string, errorString string, status bool, cas uint64) {
	t.SingleResult[docId] = SingleOperationResult{
		ErrorString: errorString,
		Status:      status,
		Cas:         cas,
	}
}

func (t *TaskResult) FailWholeBulkOperation(start, end, docSize int, gen *docgenerator.Generator, err error) {
	for i := start; i < end; i++ {
		docId, key := gen.GetDocIdAndKey(i)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		originalDoc, _ := gen.Template.GenerateDocument(&fake, docSize)
		t.IncrementFailure(docId, originalDoc, err)
	}
}

func (t *TaskResult) FailWholeSingleOperation(docIds []string, err error) {
	t.Failure = len(docIds)
	for _, docId := range docIds {
		t.CreateSingleErrorResult(docId, err.Error(), false, 0)
	}
}
