package sirius_documentation

import (
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
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
		"/result":           {"POST", &util_sirius.TaskResult{}},
		"/clear_data":       {"POST", &util_sirius.ClearTask{}},
		"/create":           {"POST", &tasks.GenericLoadingTask{}},
		"/delete":           {"POST", &tasks.GenericLoadingTask{}},
		"/upsert":           {"POST", &tasks.GenericLoadingTask{}},
		"/touch":            {"POST", &tasks.GenericLoadingTask{}},
		"/read":             {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-create":      {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-delete":      {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-upsert":      {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-touch":       {"POST", &tasks.GenericLoadingTask{}},
		"/bulk-read":        {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-insert":   {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-upsert":   {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-delete":   {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-read":     {"POST", &tasks.GenericLoadingTask{}},
		"/sub-doc-replace":  {"POST", &tasks.GenericLoadingTask{}},
		"/retry-exceptions": {"POST", &tasks.RetryExceptions{}},
		"/warmup-bucket":    {"POST", &tasks.BucketWarmUpTask{}},
		//"/validate":    {"POST", &bulk_loading.ValidateTask{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"operationConfig": &tasks.OperationConfig{},
		"bulkError":       &task_result.FailedDocument{},
		"retriedError":    &task_result.FailedDocument{},
		"exceptions":      &tasks.Exceptions{},
		"sdkTimings":      &task_result.SDKTiming{},
		"singleResult":    &task_result.SingleOperationResult{},
	}

}
