package tasks

import (
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"strings"
)

// incrementFailure saves the failure count of doc loading operation.
func (t *Task) incrementFailure(key int64, docId string, errorStr string) {
	t.Result.lock.Lock()
	t.Result.Failure++
	switch t.Request.Operation {
	case communication.InsertOperation:
		t.TaskState.InsertTask.Err = append(t.TaskState.InsertTask.Err, key)
	}
	errorIndex := strings.IndexByte(errorStr, '|')
	err := errorStr
	if errorIndex != -1 {
		err = errorStr[:errorIndex]
	}
	t.Result.Error[err] = append(t.Result.Error[err], docId)
	t.Result.lock.Unlock()
}

// validationFailures saves the failing validation of documents.
func (t *Task) validationFailures(docId string) {
	t.Result.lock.Lock()
	t.Result.ValidationError = append(t.Result.ValidationError, docId)
	t.Result.lock.Unlock()
}

// retracePreviousMutations retraces all previous mutation from the saved sequences of upsert operations.
func (t *Task) retracePreviousMutations(key int64, doc interface{}, gen docgenerator.Generator, fake *faker.Faker) (interface{}, error) {
	for _, u := range t.TaskState.UpsertTask {
		if key >= (u.Start+t.TaskState.Seed-1) && (key <= u.End+t.TaskState.Seed-1) {
			flag := true
			for _, e := range u.Err {
				if e == key {
					flag = false
					break
				}
			}
			if flag {
				doc, _ = gen.Template.UpdateDocument(u.FieldToChange, doc, fake)
			}
		}
	}
	return doc, nil
}

// calculateSuccess calculate the success count according to different doc loading operation.
// dock
func (t *Task) calculateSuccess() {
	switch t.Request.Operation {
	case communication.InsertOperation:
		t.Result.Success = t.Request.Iteration*t.Request.BatchSize - t.Result.Failure
	case communication.DeleteOperation, communication.UpsertOperation:
		t.Result.Success = t.Request.End - t.Request.Start - t.Result.Failure + 1
	case communication.ValidateOperation:
		t.Result.Success = t.TaskState.SeedEnd - t.TaskState.Seed - t.Result.Failure - int64(len(t.TaskState.InsertTask.Err)) - int64(len(t.TaskState.DeleteTask.Del))
	}
}
