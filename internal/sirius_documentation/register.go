package sirius_documentation

import (
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks/data_loading"
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
		"/result":            {"POST", &util_sirius.TaskResult{}},
		"/clear_data":        {"POST", &util_sirius.ClearTask{}},
		"/create":            {"POST", &data_loading.GenericLoadingTask{}},
		"/delete":            {"POST", &data_loading.GenericLoadingTask{}},
		"/upsert":            {"POST", &data_loading.GenericLoadingTask{}},
		"/touch":             {"POST", &data_loading.GenericLoadingTask{}},
		"/read":              {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-create":       {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-delete":       {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-upsert":       {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-touch":        {"POST", &data_loading.GenericLoadingTask{}},
		"/bulk-read":         {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-insert":    {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-upsert":    {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-delete":    {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-read":      {"POST", &data_loading.GenericLoadingTask{}},
		"/sub-doc-replace":   {"POST", &data_loading.GenericLoadingTask{}},
		"/retry-exceptions":  {"POST", &data_loading.RetryExceptions{}},
		"/warmup-bucket":     {"POST", &util_sirius.BucketWarmUpTask{}},
		"/validate":          {"POST", &data_loading.GenericLoadingTask{}},
		"/validate-columnar": {"POST", &data_loading.GenericLoadingTask{}},
		"/create-database":   {"POST", &data_loading.GenericLoadingTask{}},
		"/delete-database":   {"POST", &data_loading.GenericLoadingTask{}},
		"/list-database":     {"POST", &data_loading.GenericLoadingTask{}},
		"/count":             {"POST", &data_loading.GenericLoadingTask{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"operationConfig": &data_loading.OperationConfig{},
		"bulkError":       &task_result.FailedDocument{},
		"retriedError":    &task_result.FailedDocument{},
		"exceptions":      &data_loading.Exceptions{},
		"sdkTimings":      &task_result.SDKTiming{},
		"singleResult":    &task_result.SingleOperationResult{},
	}

}
