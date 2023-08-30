package task_result

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const ResultPath = "./internal/task_result/task_result_logs"

// TaskResult defines the type of result stored in a response after an operation.
type SDKTiming struct {
	SendTime string `json:"sendTime" doc:"true"`
	AckTime  string `json:"ackTime" doc:"true"`
}

type FailedDocument struct {
	SDKTiming   SDKTiming `json:"sdkTimings" doc:"true"`
	DocId       string    `json:"key" doc:"true"`
	Status      bool      `json:"status"  doc:"true"`
	Cas         uint64    `json:"cas"  doc:"true"`
	ErrorString string    `json:"errorString"  doc:"true"`
	Offset      int64     `json:"-" doc:"false"`
}

type SingleOperationResult struct {
	SDKTiming   SDKTiming `json:"sdkTimings" doc:"true"`
	ErrorString string    `json:"errorString"  doc:"true"`
	Status      bool      `json:"status"  doc:"true"`
	Cas         uint64    `json:"cas"  doc:"true"`
}

type FailedQuery struct {
	Query       string `json:"query" doc:"true"`
	ErrorString string `json:"errorString" doc:"true"`
}

type TaskResult struct {
	ResultSeed   int64                            `json:"resultSeed"`
	Operation    string                           `json:"operation"`
	ErrorOther   string                           `json:"otherErrors"`
	Success      int64                            `json:"success"`
	Failure      int64                            `json:"failure"`
	BulkError    map[string][]FailedDocument      `json:"bulkErrors"`
	RetriedError map[string][]FailedDocument      `json:"retriedError"`
	QueryError   map[string][]FailedQuery         `json:"queryErrors"`
	SingleResult map[string]SingleOperationResult `json:"singleResult"`
	lock         sync.Mutex                       `json:"-"`
}

// ConfigTaskResult returns a new instance of TaskResult
func ConfigTaskResult(operation string, resultSeed int64) *TaskResult {
	return &TaskResult{
		ResultSeed:   resultSeed,
		Operation:    operation,
		BulkError:    make(map[string][]FailedDocument),
		RetriedError: make(map[string][]FailedDocument),
		QueryError:   make(map[string][]FailedQuery),
		SingleResult: make(map[string]SingleOperationResult),
		lock:         sync.Mutex{},
	}
}

// IncrementFailure saves the failure count of doc loading operation.
func (t *TaskResult) IncrementFailure(initTime, docId string, _ interface{}, err error, status bool, cas uint64,
	offset int64) {
	t.lock.Lock()
	t.Failure++
	v, errorString := sdk.CheckSDKException(err)
	t.BulkError[v] = append(t.BulkError[v], FailedDocument{
		SDKTiming: SDKTiming{
			SendTime: initTime,
			AckTime:  time.Now().UTC().Format(time.RFC850),
		},
		DocId:       docId,
		Status:      status,
		Cas:         cas,
		ErrorString: errorString,
		Offset:      offset,
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

func (t *TaskResult) CreateSingleErrorResult(initTime, docId string, errorString string, status bool, cas uint64) {
	defer t.lock.Unlock()
	t.lock.Lock()
	if !status {
		t.Failure++
	}
	t.SingleResult[docId] = SingleOperationResult{
		SDKTiming: SDKTiming{
			SendTime: initTime,
			AckTime:  time.Now().UTC().Format(time.RFC850),
		},
		ErrorString: errorString,
		Status:      status,
		Cas:         cas,
	}
}

func (t *TaskResult) FailWholeBulkOperation(start, end int64, docSize int, gen *docgenerator.Generator, err error,
	state *task_state.TaskState) {

	const routineLimit = 10
	routineLimiter := make(chan struct{}, routineLimit)
	dataChannel := make(chan int64, routineLimit)

	wg := errgroup.Group{}
	initTime := time.Now().UTC().Format(time.RFC850)
	for i := start; i < end; i++ {
		routineLimiter <- struct{}{}
		dataChannel <- i
		wg.Go(func() error {
			offset := <-dataChannel
			docId, key := gen.GetDocIdAndKey(offset)
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			originalDoc, _ := gen.Template.GenerateDocument(&fake, docSize)
			t.IncrementFailure(initTime, docId, originalDoc, err, false, 0, offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			<-routineLimiter
			return nil
		})
	}
	_ = wg.Wait()
}

func (t *TaskResult) FailWholeSingleOperation(docIds []string, err error) {
	t.Failure = int64(len(docIds))

	const routineLimit = 10
	routineLimiter := make(chan struct{}, routineLimit)
	dataChannel := make(chan string, routineLimit)
	initTime := time.Now().UTC().Format(time.RFC850)
	wg := errgroup.Group{}

	for _, docId := range docIds {
		routineLimiter <- struct{}{}
		dataChannel <- docId

		wg.Go(func() error {
			t.CreateSingleErrorResult(initTime, <-dataChannel, err.Error(), false, 0)
			<-routineLimiter
			return nil
		})
	}
	_ = wg.Wait()
}
