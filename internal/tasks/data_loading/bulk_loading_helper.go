package data_loading

import (
	"encoding/json"
	"errors"
	"github.com/bgadrian/fastfaker/faker"
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/shettyh/threadpool"
	"log"
	"reflect"
	"time"
)

// Exceptions will have list of errors to be ignored or to be retried
type Exceptions struct {
	IgnoreExceptions []string `json:"ignoreExceptions,omitempty" doc:"true"`
	RetryExceptions  []string `json:"retryExceptions,omitempty" doc:"true"`
}

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

// checkBulkWriteOperation is used to check if the Write operation is on main doc or sub doc
func checkBulkWriteOperation(operation string, subDocFlag bool) bool {
	if subDocFlag {
		switch operation {
		case tasks.SubDocInsertOperation, tasks.SubDocUpsertOperation, tasks.SubDocReplaceOperation:
			return true
		default:
			return false
		}
	} else {
		switch operation {
		case tasks.InsertOperation, tasks.UpsertOperation, tasks.TouchOperation, tasks.BulkUpsertOperation, tasks.BulkInsertOperation,
			tasks.BulkTouchOperation:
			return true
		default:
			return false
		}
	}
}

// retrieveLastConfig retrieves the OperationConfig for the offset for a successful Sirius operation.
func retrieveLastConfig(r *tasks.Request, offset int64, subDocFlag bool) (OperationConfig, error) {
	if r == nil {
		return OperationConfig{}, err_sirius.RequestIsNil
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
						if ok := taskState.CheckOffsetInComplete(offset); ok {
							return *operationConfig, nil
						}
					}
				}
			}
		}
	}
	return OperationConfig{}, err_sirius.NilOperationConfig
}

// retracePreviousDeletions returns a lookup table representing the offsets which are successfully deleted.
func retracePreviousDeletions(r *tasks.Request, collectionIdentifier string, resultSeed int64) (map[int64]struct{},

	error) {
	if r == nil {
		return map[int64]struct{}{}, err_sirius.RequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	result := make(map[int64]struct{})
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.DeleteOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*GenericLoadingTask)
				if ok1 {
					if collectionIdentifier != u.MetaDataIdentifier() {
						continue
					}
					if resultSeed != u.ResultSeed {
						if u.State == nil {
							u.State = task_state.ConfigTaskState(resultSeed)
						}
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
		return map[int64]struct{}{}, err_sirius.RequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	result := make(map[int64]struct{})
	if r == nil {
		return result, err_sirius.RequestIsNil
	}
	for i := range r.Tasks {
		td := r.Tasks[i]
		if td.Operation == tasks.SubDocDeleteOperation {
			if task, ok := td.Task.(BulkTask); ok {
				u, ok1 := task.(*GenericLoadingTask)
				if ok1 {
					if collectionIdentifier != u.MetaDataIdentifier() {
						continue
					}
					if resultSeed != u.ResultSeed {
						if u.State == nil {
							u.State = task_state.ConfigTaskState(resultSeed)
						}
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
						if u.State.CheckOffsetInComplete(offset) {
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

						if u.State.CheckOffsetInComplete(offset) {
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
		return 0, err_sirius.RequestIsNil
	}
	defer r.Unlock()
	r.Lock()
	var result int = 0
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

						if u.State.CheckOffsetInComplete(offset) {
							result++
						}
					}
				}
			}
		} else if td.Operation == tasks.SubDocDeleteOperation {
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

						if u.State.CheckOffsetInComplete(offset) {
							result++
						}
					}
				}
			}
		} else if td.Operation == tasks.SubDocReplaceOperation {
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

						if u.State.CheckOffsetInComplete(offset) {
							result++
						}
					}
				}
			}
		} else if td.Operation == tasks.SubDocInsertOperation {
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

						if u.State.CheckOffsetInComplete(offset) {
							result++
						}
					}
				}
			}
		} else if checkBulkWriteOperation(td.Operation, false) {
			result = 0
		}
	}

	return result, nil

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

// shiftErrToCompletedOnIgnore will ignore retrying operation for offset lying in ignore exception category
func shiftErrToCompletedOnIgnore(ignoreExceptions []string, result *task_result.TaskResult, state *task_state.TaskState) {
	for _, exception := range ignoreExceptions {
		for _, failedDocs := range result.BulkError[exception] {
			state.AddOffsetToCompleteSet(failedDocs.Offset)
		}
		for _, failedDocs := range result.BulkError[exception] {
			state.RemoveOffsetFromErrSet(failedDocs.Offset)
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

// loadBatch will enqueue the batch to thread pool. if the queue is full,
// it will wait for sometime any thread to pick it up.
func loadBatch(task *GenericLoadingTask, t *loadingTask, batchStart int64, batchEnd int64, _ *threadpool.ThreadPool) {

	retryBatchCounter := 10
	for ; retryBatchCounter > 0; retryBatchCounter-- {
		if err := tasks.Pool.Execute(t); err == nil {
			break
		}
		time.Sleep(1 * time.Minute)
	}
	if retryBatchCounter == 0 {
		task.Result.FailWholeBulkOperation(batchStart, batchEnd, errors.New("internal error, "+
			"sirius is overloaded"), task.State, task.gen, task.MetaData.Seed)
	}
}

func buildKeyAndValues(doc map[string]any, result map[string]any, startString string) {
	for key, value := range doc {
		if subDoc, ok := value.(map[string]any); ok {
			buildKeyAndValues(subDoc, result, key+".")
		} else {
			result[startString+key] = value
		}
	}
}

func CompareDocumentsIsSame(host map[string]any, document1 map[string]any, document2 map[string]any) bool {

	hostMap := make(map[string]any)
	buildKeyAndValues(host, hostMap, "")

	document1Map := make(map[string]any)
	buildKeyAndValues(document1, document1Map, "")

	document2Map := make(map[string]any)
	buildKeyAndValues(document2, document2Map, "")

	for key, value := range hostMap {
		if v1, ok := document1Map[key]; ok {
			if reflect.DeepEqual(value, v1) == false {
				return false
			}
		} else if v2, ok := document2Map[key]; ok {
			if reflect.DeepEqual(v2, value) == false {
				return false
			}
		} else {
			// TODO  fix_the_validation_of_missing_Keys
			continue
		}
	}

	return true
}
