package bulk_loading

import (
	"encoding/json"
	"log"

	"github.com/barkha06/sirius/internal/cb_sdk"
	"github.com/barkha06/sirius/internal/db"
	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/barkha06/sirius/internal/err_sirius"
	"github.com/barkha06/sirius/internal/task_result"
	"github.com/barkha06/sirius/internal/task_state"
	"github.com/barkha06/sirius/internal/tasks"
	"github.com/jaswdr/faker"
	"golang.org/x/exp/slices"
)

// OperationConfig contains all the configuration for document operation.
type OperationConfig struct {
	DocSize        int        `json:"docSize" doc:"true"`
	DocType        string     `json:"docType,omitempty" doc:"true"`
	KeySize        int        `json:"keySize,omitempty" doc:"true"`
	TemplateName   string     `json:"template" doc:"true"`
	Start          int64      `json:"start" doc:"true"`
	End            int64      `json:"end" doc:"true"`
	FieldsToChange []string   `json:"fieldsToChange" doc:"true"`
	Exceptions     Exceptions `json:"exceptions,omitempty" doc:"true"`
}

func (o *OperationConfig) String() string {
	if o == nil {
		return "nil config"
	}
	b, err := json.Marshal(&o)
	log.Println(string(b))
	if err != nil {
		return ""
	}
	return ""
}

// ConfigureOperationConfig configures and validate the OperationConfig
func ConfigureOperationConfig(o *OperationConfig) error {
	if o == nil {
		return err_sirius.ParsingOperatingConfig
	}
	if o.DocType == "" {
		o.DocType = docgenerator.JsonDocument
	}

	if o.KeySize > docgenerator.DefaultKeySize {
		o.KeySize = docgenerator.DefaultKeySize
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
		return err_sirius.MalformedOperationRange
	}
	return nil
}

// // checkBulkWriteOperation is used to check if the Write operation is on main doc or sub doc
//
//	func checkBulkWriteOperation(operation string, subDocFlag bool) bool {
//		if subDocFlag {
//			switch operation {
//			case tasks.SubDocInsertOperation, tasks.SubDocUpsertOperation, tasks.SingleSubDocReplaceOperation:
//				return true
//			default:
//				return false
//			}
//		} else {
//			switch operation {
//			case tasks.InsertOperation, tasks.UpsertOperation, tasks.TouchOperation:
//				return true
//			default:
//				return false
//			}
//		}
//	}
//
// // retrieveLastConfig retrieves the OperationConfig for the offset for a successful Sirius operation.
//
//	func retrieveLastConfig(r *tasks.Request, offset int64, subDocFlag bool) (OperationConfig, error) {
//		if r == nil {
//			return OperationConfig{}, err_sirius.RequestIsNil
//		}
//		for i := range r.Tasks {
//			if checkBulkWriteOperation(r.Tasks[len(r.Tasks)-i-1].Operation, subDocFlag) {
//				task, ok := r.Tasks[len(r.Tasks)-i-1].Task.(BulkTask)
//				if ok {
//					operationConfig, taskState := task.GetOperationConfig()
//					if operationConfig == nil {
//						continue
//					} else {
//						if offset >= (operationConfig.Start) && (offset < operationConfig.End) {
//							if _, ok := taskState.ReturnCompletedOffset()[offset]; ok {
//								return *operationConfig, nil
//							}
//						}
//					}
//				}
//			}
//		}
//		return OperationConfig{}, err_sirius.NilOperationConfig
//	}
//
// // retracePreviousFailedInsertions returns a lookup table representing the offsets which are not inserted properly..
// func retracePreviousFailedInsertions(r *tasks.Request, collectionIdentifier string,
//
//		resultSeed int64) (map[int64]struct{}, error) {
//		if r == nil {
//			return map[int64]struct{}{}, err_sirius.RequestIsNil
//		}
//		defer r.Unlock()
//		r.Lock()
//		result := make(map[int64]struct{})
//		for i := range r.Tasks {
//			td := r.Tasks[i]
//			if td.Operation == tasks.InsertOperation {
//				if task, ok := td.Task.(BulkTask); ok {
//					u, ok1 := task.(*GenericLoadingTask)
//					if ok1 {
//						if collectionIdentifier != u.MetaDataIdentifier() {
//							continue
//						}
//						if resultSeed != u.ResultSeed {
//							errorOffSet := u.State.ReturnErrOffset()
//							for offSet, _ := range errorOffSet {
//								result[offSet] = struct{}{}
//							}
//						}
//					}
//				}
//			}
//		}
//		return result, nil
//	}
//
// // retracePreviousDeletions returns a lookup table representing the offsets which are successfully deleted.
// func retracePreviousDeletions(r *tasks.Request, collectionIdentifier string, resultSeed int64) (map[int64]struct{},
//
//		error) {
//		if r == nil {
//			return map[int64]struct{}{}, err_sirius.RequestIsNil
//		}
//		defer r.Unlock()
//		r.Lock()
//		result := make(map[int64]struct{})
//		for i := range r.Tasks {
//			td := r.Tasks[i]
//			if td.Operation == tasks.DeleteOperation {
//				if task, ok := td.Task.(BulkTask); ok {
//					u, ok1 := task.(*DeleteTask)
//					if ok1 {
//						if collectionIdentifier != u.MetaDataIdentifier() {
//							continue
//						}
//						if resultSeed != u.ResultSeed {
//							completedOffSet := u.State.ReturnCompletedOffset()
//							for deletedOffset, _ := range completedOffSet {
//								result[deletedOffset] = struct{}{}
//							}
//						}
//					}
//				}
//			}
//		}
//		return result, nil
//	}
//
// // retracePreviousSubDocDeletions  returns a lookup table representing the offsets which are successfully deleted.
// func retracePreviousSubDocDeletions(r *tasks.Request, collectionIdentifier string,
//
//		resultSeed int64) (map[int64]struct{}, error) {
//		if r == nil {
//			return map[int64]struct{}{}, err_sirius.RequestIsNil
//		}
//		defer r.Unlock()
//		r.Lock()
//		result := make(map[int64]struct{})
//		if r == nil {
//			return result, err_sirius.RequestIsNil
//		}
//		for i := range r.Tasks {
//			td := r.Tasks[i]
//			if td.Operation == tasks.SubDocDeleteOperation {
//				if task, ok := td.Task.(BulkTask); ok {
//					u, ok1 := task.(*SubDocDelete)
//					if ok1 {
//						if collectionIdentifier != u.MetaDataIdentifier() {
//							continue
//						}
//						if resultSeed != u.ResultSeed {
//							completedOffSet := u.State.ReturnCompletedOffset()
//							for deletedOffset, _ := range completedOffSet {
//								result[deletedOffset] = struct{}{}
//							}
//						}
//					}
//				}
//			}
//		}
//		return result, nil
//	}
//
// retracePreviousMutations returns an updated document after mutating the original documents.
func retracePreviousMutations(r *tasks.Request, collectionIdentifier string, offset int64, doc interface{},
	gen *docgenerator.Generator, fake *faker.Faker, resultSeed int64) (interface{}, error) {
	if r == nil {
		return doc, err_sirius.RequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.UpsertOperation || td.Operation == tasks.BulkUpsertOperation {
			if tempX, ok := td.Task.(BulkTask); ok {
				u, ok := tempX.(*GenericLoadingTask)
				if ok {
					if collectionIdentifier != u.MetaDataIdentifier() {
						continue
					}
					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
						ResultSeed {

						if u.State == nil {
							u.State = task_state.ConfigTaskState(resultSeed)
						}
						comOffset := u.State.ReturnCompletedOffset()
						if _, ok := comOffset[offset]; ok {
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

// retracePreviousSubDocMutations retraces mutation in sub documents.
func retracePreviousSubDocMutations(r *tasks.Request, collectionIdentifier string, offset int64,
	gen *docgenerator.Generator, fake *faker.Faker, resultSeed int64, subDocumentMap map[string]any) (map[string]any,
	error) {
	if r == nil {
		return map[string]any{}, err_sirius.RequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	var result map[string]any = subDocumentMap
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.SubDocUpsertOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*GenericLoadingTask)
				if ok1 {
					if collectionIdentifier != u.MetaDataIdentifier() {
						continue
					}
					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
						ResultSeed {
						if u.State == nil {
							u.State = task_state.ConfigTaskState(resultSeed)
						}
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

//
//// countMutation return the number of mutation happened on an offset
//func countMutation(r *tasks.Request, collectionIdentifier string, offset int64, resultSeed int64) (int, error) {
//	if r == nil {
//		return 0, err_sirius.RequestIsNil
//	}
//	defer r.Unlock()
//	r.Lock()
//	var result int = 0
//	for i := range r.Tasks {
//		td := r.Tasks[i]
//		if td.Operation == tasks.SubDocUpsertOperation {
//			if task, ok := td.Task.(BulkTask); ok {
//				u, ok1 := task.(*SubDocUpsert)
//				if ok1 {
//					if collectionIdentifier != u.MetaDataIdentifier() {
//						continue
//					}
//					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
//						ResultSeed {
//						completeOffset := u.State.ReturnCompletedOffset()
//						if _, ok := completeOffset[offset]; ok {
//							result++
//						}
//					}
//				}
//			}
//		} else if td.Operation == tasks.SubDocDeleteOperation {
//			if task, ok := td.Task.(BulkTask); ok {
//				u, ok1 := task.(*SubDocDelete)
//				if ok1 {
//					if collectionIdentifier != u.MetaDataIdentifier() {
//						continue
//					}
//					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
//						ResultSeed {
//						completeOffset := u.State.ReturnCompletedOffset()
//						if _, ok := completeOffset[offset]; ok {
//							result++
//						}
//					}
//				}
//			}
//		} else if td.Operation == tasks.SubDocReplaceOperation {
//			if task, ok := td.Task.(BulkTask); ok {
//				u, ok1 := task.(*SubDocReplace)
//				if ok1 {
//					if collectionIdentifier != u.MetaDataIdentifier() {
//						continue
//					}
//					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
//						ResultSeed {
//						completeOffset := u.State.ReturnCompletedOffset()
//						if _, ok := completeOffset[offset]; ok {
//							result++
//						}
//					}
//				}
//			}
//		} else if td.Operation == tasks.SubDocInsertOperation {
//			if task, ok := td.Task.(BulkTask); ok {
//				u, ok1 := task.(*SubDocInsert)
//				if ok1 {
//					if collectionIdentifier != u.MetaDataIdentifier() {
//						continue
//					}
//					if offset >= (u.OperationConfig.Start) && (offset < u.OperationConfig.End) && resultSeed != u.
//						ResultSeed {
//						completeOffset := u.State.ReturnCompletedOffset()
//						if _, ok := completeOffset[offset]; ok {
//							result++
//						}
//					}
//				}
//			}
//		}
//	}
//	return result, nil
//
//}

type RetriedResult struct {
	Status   bool           `json:"status" doc:"true"`
	Extra    map[string]any `json:"extra" doc:"true"`
	InitTime string         `json:"initTime" doc:"true"`
	AckTime  string         `json:"ackTime" doc:"true"`
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
									Extra = retryResult.Extra

								result.RetriedError[exception][len(result.RetriedError[exception])-1].
									SDKTiming.SendTime = retryResult.InitTime

								result.RetriedError[exception][len(result.RetriedError[exception])-1].
									SDKTiming.AckTime = retryResult.AckTime

							} else {
								result.RetriedError[exception][offsetRetriedIndex].Status = retryResult.Status
								result.RetriedError[exception][offsetRetriedIndex].Extra = retryResult.Extra
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

func configExtraParameters(dbType string, d *db.Extras) error {
	if dbType == db.CouchbaseDb {
		if d.Bucket == "" {
			return err_sirius.BucketIsMisssing
		}
		if d.Scope == "" {
			d.Scope = cb_sdk.DefaultScope
		}
		if d.Collection == "" {
			d.Collection = cb_sdk.DefaultCollection
		}
	}
	if dbType == db.MongoDb {
		if d.Collection == "" {
			return err_sirius.CollectionIsMissing
		}
	}
	return nil
}
