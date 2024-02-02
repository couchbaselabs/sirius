package bulk_loading_cb

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/jaswdr/faker"
)

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
func retrieveLastConfig(r *tasks.Request, offset int64, subDocFlag bool) (tasks.OperationConfig, error) {
	if r == nil {
		return tasks.OperationConfig{}, task_errors.ErrRequestIsNil
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
	return tasks.OperationConfig{}, task_errors.ErrNilOperationConfig
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
