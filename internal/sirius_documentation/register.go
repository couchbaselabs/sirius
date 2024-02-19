package sirius_documentation

import (
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks/bulk_loading"
	"github.com/couchbaselabs/sirius/internal/tasks/db_util"
	"github.com/couchbaselabs/sirius/internal/tasks/util_sirius"
)

type TaskRegister struct {
	HttpMethod string
	Config     interface{}
}

type Register struct {
}

func (r *Register) RegisteredTasks() map[string]TaskRegister {
	return map[string]TaskRegister{
		"/result":          {"POST", &util_sirius.TaskResult{}},
		"/clear_data":      {"POST", &util_sirius.ClearTask{}},
		"/create":          {"POST", &bulk_loading.GenericLoadingTask{}},
		"/delete":          {"POST", &bulk_loading.GenericLoadingTask{}},
		"/upsert":          {"POST", &bulk_loading.GenericLoadingTask{}},
		"/touch":           {"POST", &bulk_loading.GenericLoadingTask{}},
		"/read":            {"POST", &bulk_loading.GenericLoadingTask{}},
		"/bulk-create":     {"POST", &bulk_loading.GenericLoadingTask{}},
		"/bulk-delete":     {"POST", &bulk_loading.GenericLoadingTask{}},
		"/bulk-upsert":     {"POST", &bulk_loading.GenericLoadingTask{}},
		"/bulk-touch":      {"POST", &bulk_loading.GenericLoadingTask{}},
		"/bulk-read":       {"POST", &bulk_loading.GenericLoadingTask{}},
		"/sub-doc-insert":  {"POST", &bulk_loading.GenericLoadingTask{}},
		"/sub-doc-upsert":  {"POST", &bulk_loading.GenericLoadingTask{}},
		"/sub-doc-delete":  {"POST", &bulk_loading.GenericLoadingTask{}},
		"/sub-doc-read":    {"POST", &bulk_loading.GenericLoadingTask{}},
		"/sub-doc-replace": {"POST", &bulk_loading.GenericLoadingTask{}},
		//"/validate":    {"POST", &bulk_loading.ValidateTask{}},
		//"/single-create":          {"POST", &key_based_loading_cb.SingleInsertTask{}},
		//"/single-delete":          {"POST", &key_based_loading_cb.SingleDeleteTask{}},
		//"/single-upsert":          {"POST", &key_based_loading_cb.SingleUpsertTask{}},
		//"/single-read":            {"POST", &key_based_loading_cb.SingleReadTask{}},
		//"/single-touch":           {"POST", &key_based_loading_cb.SingleTouchTask{}},
		//"/single-replace":         {"POST", &key_based_loading_cb.SingleReplaceTask{}},
		//"/run-template-query":     {"POST", &bulk_query_cb.QueryTask{}},
		//"/retry-exceptions":       {"POST", &bulk_loading.RetryExceptions{}},
		//"/single-sub-doc-insert":  {"POST", &key_based_loading_cb.SingleSubDocInsert{}},
		//"/single-sub-doc-upsert":  {"POST", &key_based_loading_cb.SingleSubDocUpsert{}},
		//"/single-sub-doc-replace": {"POST", &key_based_loading_cb.SingleSubDocReplace{}},
		//"/single-sub-doc-delete":  {"POST", &key_based_loading_cb.SingleSubDocDelete{}},
		//"/single-sub-doc-read":    {"POST", &key_based_loading_cb.SingleSubDocRead{}},
		//"/single-doc-validate":    {"POST", &key_based_loading_cb.SingleValidate{}},
		"/warmup-bucket": {"POST", &db_util.BucketWarmUpTask{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"operationConfig": &bulk_loading.OperationConfig{},
		"bulkError":       &task_result.FailedDocument{},
		"retriedError":    &task_result.FailedDocument{},
		"exceptions":      &bulk_loading.Exceptions{},
		"sdkTimings":      &task_result.SDKTiming{},
		"singleResult":    &task_result.SingleOperationResult{},
	}

}
