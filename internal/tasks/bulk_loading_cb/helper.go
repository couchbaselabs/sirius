package bulk_loading_cb

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/jaswdr/faker"
	"golang.org/x/exp/slices"
)

// OperationConfig contains all the configuration for document operation.
type OperationConfig struct {
	Count            int64      `json:"count,omitempty" doc:"true"`
	DocSize          int        `json:"docSize" doc:"true"`
	DocType          string     `json:"docType,omitempty" doc:"true"`
	KeySize          int        `json:"keySize,omitempty" doc:"true"`
	KeyPrefix        string     `json:"keyPrefix" doc:"true"`
	KeySuffix        string     `json:"keySuffix" doc:"true"`
	ReadYourOwnWrite bool       `json:"readYourOwnWrite,omitempty" doc:"true"`
	TemplateName     string     `json:"template" doc:"true"`
	Start            int64      `json:"start" doc:"true"`
	End              int64      `json:"end" doc:"true"`
	FieldsToChange   []string   `json:"fieldsToChange" doc:"true"`
	Exceptions       Exceptions `json:"exceptions,omitempty" doc:"true"`
}

// ConfigureOperationConfig configures and validate the OperationConfig
func ConfigureOperationConfig(o *OperationConfig) error {
	if o == nil {
		return task_errors.ErrParsingOperatingConfig
	}
	if o.DocType == "" {
		o.DocType = docgenerator.JsonDocument
	}

	if o.KeySize > docgenerator.DefaultKeySize {
		o.KeySize = docgenerator.DefaultKeySize
	}
	if o.Count <= 0 {
		o.Count = 1
	}
	if o.DocSize <= 0 {
		o.DocSize = docgenerator.DefaultDocSize
	}
	if o.Start < 0 {
		o.Start = 0
		o.End = 0
	}
	if o.Start > o.End {
		o.End = o.Start
		return task_errors.ErrMalformedOperationRange
	}
	return nil
}

// checkBulkWriteOperation is used to check if the Write operation is on main doc or sub doc
func checkBulkWriteOperation(operation string, subDocFlag bool) bool {
	if subDocFlag {
		switch operation {
		case tasks.SubDocInsertOperation, tasks.SubDocUpsertOperation, tasks.SingleSubDocReplaceOperation:
			return true
		default:
			return false
		}
	} else {
		switch operation {
		case tasks.InsertOperation, tasks.UpsertOperation, tasks.TouchOperation:
			return true
		default:
			return false
		}
	}
}

// retrieveLastConfig retrieves the OperationConfig for the offset for a successful Sirius operation.
func retrieveLastConfig(r *tasks.Request, offset int64, subDocFlag bool) (OperationConfig, error) {
	if r == nil {
		return OperationConfig{}, task_errors.ErrRequestIsNil
	}
	for i := range r.Tasks {
		if checkBulkWriteOperation(r.Tasks[len(r.Tasks)-i-1].Operation, subDocFlag) {
			task, ok := r.Tasks[len(r.Tasks)-i-1].Task.(BulkTask)
			if ok {
				operationConfig, taskState := task.GetOperationConfig()
				if operationConfig == nil {
					continue
				} else {
					if offset >= (operationConfig.Start) && (offset < operationConfig.End) {
						if _, ok := taskState.ReturnCompletedOffset()[offset]; ok {
							return *operationConfig, nil
						}
					}
				}
			}
		}
	}
	return OperationConfig{}, task_errors.ErrNilOperationConfig
}

// retracePreviousFailedInsertions returns a lookup table representing the offsets which are not inserted properly..
func retracePreviousFailedInsertions(r *tasks.Request, collectionIdentifier string,
	resultSeed int64) (map[int64]struct{}, error) {
	if r == nil {
		return map[int64]struct{}{}, task_errors.ErrRequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	result := make(map[int64]struct{})
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.InsertOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*InsertTask)
				if ok1 {
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
	}
	return result, nil
}

// retracePreviousDeletions returns a lookup table representing the offsets which are successfully deleted.
func retracePreviousDeletions(r *tasks.Request, collectionIdentifier string, resultSeed int64) (map[int64]struct{},
	error) {
	if r == nil {
		return map[int64]struct{}{}, task_errors.ErrRequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	result := make(map[int64]struct{})
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.DeleteOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*DeleteTask)
				if ok1 {
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
	}
	return result, nil
}

// retracePreviousSubDocDeletions  returns a lookup table representing the offsets which are successfully deleted.
func retracePreviousSubDocDeletions(r *tasks.Request, collectionIdentifier string,
	resultSeed int64) (map[int64]struct{}, error) {
	if r == nil {
		return map[int64]struct{}{}, task_errors.ErrRequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	result := make(map[int64]struct{})
	if r == nil {
		return result, task_errors.ErrRequestIsNil
	}
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.SubDocDeleteOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*SubDocDelete)
				if ok1 {
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
	}
	return result, nil
}

// retracePreviousMutations returns an updated document after mutating the original documents.
func retracePreviousMutations(r *tasks.Request, collectionIdentifier string, offset int64, doc interface{},
	gen docgenerator.Generator, fake *faker.Faker, resultSeed int64) (interface{}, error) {
	if r == nil {
		return doc, task_errors.ErrRequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.UpsertOperation {

			if tempX, ok := td.Task.(BulkTask); ok {
				u, ok := tempX.(*UpsertTask)
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
							doc, _ = gen.Template.UpdateDocument(u.OperationConfig.FieldsToChange, doc,
								u.OperationConfig.DocSize, fake)
						}
					}

				}
			}

		}
	}
	return doc, nil
}

func retracePreviousSubDocMutations(r *tasks.Request, collectionIdentifier string, offset int64,
	gen docgenerator.Generator,
	fake *faker.Faker, resultSeed int64,
	subDocumentMap map[string]any) (map[string]any, error) {
	if r == nil {
		return map[string]any{}, task_errors.ErrRequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	var result map[string]any = subDocumentMap
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.SubDocUpsertOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*SubDocUpsert)
				if ok1 {
					if collectionIdentifier != u.CollectionIdentifier() {
						continue
					}
					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
						ResultSeed {
						errOffset := u.State.ReturnErrOffset()
						if _, ok := errOffset[offset]; ok {
							continue
						} else {
							result = gen.Template.GenerateSubPathAndValue(fake, u.OperationConfig.DocSize)
						}
					}
				}
			}
		}
	}
	return result, nil
}

// countMutation return the number of mutation happened on an offset
func countMutation(r *tasks.Request, collectionIdentifier string, offset int64, resultSeed int64) (int, error) {
	if r == nil {
		return 0, task_errors.ErrRequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	var result int = 0
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.SubDocUpsertOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*SubDocUpsert)
				if ok1 {
					if collectionIdentifier != u.CollectionIdentifier() {
						continue
					}
					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
						ResultSeed {
						completeOffset := u.State.ReturnCompletedOffset()
						if _, ok := completeOffset[offset]; ok {
							result++
						}
					}
				}
			}
		} else if td.Operation == tasks.SubDocDeleteOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*SubDocDelete)
				if ok1 {
					if collectionIdentifier != u.CollectionIdentifier() {
						continue
					}
					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
						ResultSeed {
						completeOffset := u.State.ReturnCompletedOffset()
						if _, ok := completeOffset[offset]; ok {
							result++
						}
					}
				}
			}
		} else if td.Operation == tasks.SubDocReplaceOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*SubDocReplace)
				if ok1 {
					if collectionIdentifier != u.CollectionIdentifier() {
						continue
					}
					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
						ResultSeed {
						completeOffset := u.State.ReturnCompletedOffset()
						if _, ok := completeOffset[offset]; ok {
							result++
						}
					}
				}
			}
		} else if td.Operation == tasks.SubDocInsertOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*SubDocInsert)
				if ok1 {
					if collectionIdentifier != u.CollectionIdentifier() {
						continue
					}
					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
						ResultSeed {
						completeOffset := u.State.ReturnCompletedOffset()
						if _, ok := completeOffset[offset]; ok {
							result++
						}
					}
				}
			}
		}
	}
	return result, nil

}

// shiftErrToCompletedOnRetrying will bring the offset which successfully completed their respective operation on
// retrying
func shiftErrToCompletedOnRetrying(exception string, result *task_result.TaskResult,
	errorOffsetListMap []map[int64]RetriedResult, errorOffsetMaps, completedOffsetMaps map[int64]struct{}) {
	if _, ok := result.BulkError[exception]; ok {
		for _, x := range errorOffsetListMap {
			for offset, retryResult := range x {
				if retryResult.Status == true {
					delete(errorOffsetMaps, offset)
					completedOffsetMaps[offset] = struct{}{}
					for index := range result.BulkError[exception] {
						if result.BulkError[exception][index].Offset == offset {

							offsetRetriedIndex := slices.IndexFunc(result.RetriedError[exception],
								func(document task_result.FailedDocument) bool {
									return document.Offset == offset
								})

							if offsetRetriedIndex == -1 {
								result.RetriedError[exception] = append(result.RetriedError[exception], result.BulkError[exception][index])

								result.RetriedError[exception][len(result.RetriedError[exception])-1].
									Status = retryResult.Status

								result.RetriedError[exception][len(result.RetriedError[exception])-1].
									Cas = retryResult.CAS

								result.RetriedError[exception][len(result.RetriedError[exception])-1].
									SDKTiming.SendTime = retryResult.InitTime

								result.RetriedError[exception][len(result.RetriedError[exception])-1].
									SDKTiming.AckTime = retryResult.AckTime

							} else {
								result.RetriedError[exception][offsetRetriedIndex].Status = retryResult.Status
								result.RetriedError[exception][offsetRetriedIndex].Cas = retryResult.CAS
								result.RetriedError[exception][offsetRetriedIndex].SDKTiming.SendTime =
									retryResult.InitTime
								result.RetriedError[exception][offsetRetriedIndex].SDKTiming.AckTime =
									retryResult.AckTime
							}

							result.BulkError[exception][index] = result.BulkError[exception][len(
								result.BulkError[exception])-1]

							result.BulkError[exception] = result.BulkError[exception][:len(
								result.BulkError[exception])-1]

							break
						}
					}
				} else {
					for index := range result.BulkError[exception] {
						if result.BulkError[exception][index].Offset == offset {
							result.BulkError[exception][index].SDKTiming.SendTime = retryResult.InitTime
							result.BulkError[exception][index].SDKTiming.AckTime = retryResult.AckTime
							result.RetriedError[exception] = append(result.RetriedError[exception],
								result.BulkError[exception][index])
							break
						}
					}
				}
			}
		}
	}
}

// shiftErrToCompletedOnIgnore will ignore retrying operation for offset lying in ignore exception category
func shiftErrToCompletedOnIgnore(ignoreExceptions []string, result *task_result.TaskResult, errorOffsetMaps,
	completedOffsetMaps map[int64]struct{}) {
	for _, exception := range ignoreExceptions {
		for _, failedDocs := range result.BulkError[exception] {
			if _, ok := errorOffsetMaps[failedDocs.Offset]; ok {
				delete(errorOffsetMaps, failedDocs.Offset)
				completedOffsetMaps[failedDocs.Offset] = struct{}{}
			}
		}
		delete(result.BulkError, exception)
	}
}

type RetriedResult struct {
	Status   bool   `json:"status" doc:"true"`
	CAS      uint64 `json:"cas" doc:"true"`
	InitTime string `json:"initTime" doc:"true"`
	AckTime  string `json:"ackTime" doc:"true"`
}

type Exceptions struct {
	IgnoreExceptions []string `json:"ignoreExceptions,omitempty" doc:"true"`
	RetryExceptions  []string `json:"retryExceptions,omitempty" doc:"true"`
	RetryAttempts    int      `json:"retryAttempts,omitempty" doc:"true"`
}

func GetExceptions(result *task_result.TaskResult, RetryExceptions []string) []string {
	var exceptionList []string
	if len(RetryExceptions) == 0 {
		for exception, _ := range result.BulkError {
			exceptionList = append(exceptionList, exception)
		}
	} else {
		exceptionList = RetryExceptions
	}
	return exceptionList
}
