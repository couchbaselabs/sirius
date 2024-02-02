package sirius_documentation

import (
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/tasks/bulk_loading_cb"
	"github.com/couchbaselabs/sirius/internal/tasks/bulk_query_cb"
	"github.com/couchbaselabs/sirius/internal/tasks/key_based_loading_cb"
	"github.com/couchbaselabs/sirius/internal/tasks/util_cb"
	"github.com/couchbaselabs/sirius/internal/tasks/util_sirius"
)

type TaskRegister struct {
	httpMethod string
	config     interface{}
}

type Register struct {
}

func (r *Register) RegisteredTasks() map[string]TaskRegister {
	return map[string]TaskRegister{
		"/bulk-create":            {"POST", &bulk_loading_cb.InsertTask{}},
		"/bulk-delete":            {"POST", &bulk_loading_cb.DeleteTask{}},
		"/bulk-upsert":            {"POST", &bulk_loading_cb.UpsertTask{}},
		"/bulk-touch":             {"POST", &bulk_loading_cb.TouchTask{}},
		"/validate":               {"POST", &bulk_loading_cb.ValidateTask{}},
		"/result":                 {"POST", &util_sirius.TaskResult{}},
		"/clear_data":             {"POST", &util_sirius.ClearTask{}},
		"/bulk-read":              {"POST", &bulk_loading_cb.ReadTask{}},
		"/single-create":          {"POST", &key_based_loading_cb.SingleInsertTask{}},
		"/single-delete":          {"POST", &key_based_loading_cb.SingleDeleteTask{}},
		"/single-upsert":          {"POST", &key_based_loading_cb.SingleUpsertTask{}},
		"/single-read":            {"POST", &key_based_loading_cb.SingleReadTask{}},
		"/single-touch":           {"POST", &key_based_loading_cb.SingleTouchTask{}},
		"/single-replace":         {"POST", &key_based_loading_cb.SingleReplaceTask{}},
		"/run-template-query":     {"POST", &bulk_query_cb.QueryTask{}},
		"/retry-exceptions":       {"POST", &bulk_loading_cb.RetryExceptions{}},
		"/sub-doc-bulk-insert":    {"POST", &bulk_loading_cb.SubDocInsert{}},
		"/sub-doc-bulk-upsert":    {"POST", &bulk_loading_cb.SubDocUpsert{}},
		"/sub-doc-bulk-delete":    {"POST", &bulk_loading_cb.SubDocDelete{}},
		"/sub-doc-bulk-read":      {"POST", &bulk_loading_cb.SubDocRead{}},
		"/sub-doc-bulk-replace":   {"POST", &bulk_loading_cb.SubDocReplace{}},
		"/single-sub-doc-insert":  {"POST", &key_based_loading_cb.SingleSubDocInsert{}},
		"/single-sub-doc-upsert":  {"POST", &key_based_loading_cb.SingleSubDocUpsert{}},
		"/single-sub-doc-replace": {"POST", &key_based_loading_cb.SingleSubDocReplace{}},
		"/single-sub-doc-delete":  {"POST", &key_based_loading_cb.SingleSubDocDelete{}},
		"/single-sub-doc-read":    {"POST", &key_based_loading_cb.SingleSubDocRead{}},
		"/single-doc-validate":    {"POST", &key_based_loading_cb.SingleValidate{}},
		"/warmup-bucket":          {"POST", &util_cb.BucketWarmUpTask{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"clusterConfig":               &cb_sdk.ClusterConfig{},
		"compressionConfig":           &cb_sdk.CompressionConfig{},
		"timeoutsConfig":              &cb_sdk.TimeoutsConfig{},
		"operationConfig":             &tasks.OperationConfig{},
		"insertOptions":               &tasks.InsertOptions{},
		"removeOptions":               &tasks.RemoveOptions{},
		"replaceOption":               &tasks.ReplaceOptions{},
		"touchOptions":                &tasks.TouchOptions{},
		"singleOperationConfig":       &tasks.SingleOperationConfig{},
		"bulkError":                   &task_result.FailedDocument{},
		"retriedError":                &task_result.FailedDocument{},
		"singleResult":                &task_result.SingleOperationResult{},
		"queryOperationConfig":        &tasks.QueryOperationConfig{},
		"exceptions":                  &tasks.Exceptions{},
		"subDocOperationConfig":       &tasks.SubDocOperationConfig{},
		"mutateInOptions":             &tasks.MutateInOptions{},
		"insertSpecOptions":           &tasks.InsertSpecOptions{},
		"removeSpecOptions":           &tasks.RemoveSpecOptions{},
		"getSpecOptions":              &tasks.GetSpecOptions{},
		"lookupInOptions":             &tasks.LookupInOptions{},
		"replaceSpecOptions":          &tasks.ReplaceSpecOptions{},
		"singleSubDocOperationConfig": &tasks.SingleSubDocOperationConfig{},
		"sdkTimings":                  &task_result.SDKTiming{},
	}

}
