package task_result

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const ResultPath = "./internal/task_result/task_result_logs"

// TaskResult defines the type of result stored in a response after an operation.
type TaskResult struct {
	Seed            int64               `json:"seed"`
	Operation       string              `json:"operation"`
	ErrorOther      string              `json:"other-errors,omitempty"`
	Success         int64               `json:"success"`
	Failure         int64               `json:"failure"`
	ValidationError []string            `json:"validation-errors,omitempty"`
	Error           map[string][]string `json:"errors"`
	lock            sync.Mutex          `json:"-"`
}

// ConfigTaskResult returns a new instance of TaskResult
func ConfigTaskResult(operation string, seed int64) *TaskResult {
	return &TaskResult{
		Seed:      seed,
		Operation: operation,
		Error:     make(map[string][]string),
		lock:      sync.Mutex{},
	}
}

// IncrementFailure saves the failure count of doc loading operation.
func (t *TaskResult) IncrementFailure(docId string, errorStr string) {
	t.lock.Lock()
	t.Failure++
	errorIndex := strings.IndexByte(errorStr, '|')
	err := errorStr
	if errorIndex != -1 {
		err = errorStr[:errorIndex]
	}
	t.Error[err] = append(t.Error[err], docId)
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
	fileName := filepath.Join(cwd, ResultPath, fmt.Sprintf("%d", t.Seed))
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
