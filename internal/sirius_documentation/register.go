package sirius_documentation

import (
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
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
		"operationConfig":             &bulk_loading_cb.OperationConfig{},
		"insertOptions":               &cb_sdk.InsertOptions{},
		"removeOptions":               &cb_sdk.RemoveOptions{},
		"replaceOption":               &cb_sdk.ReplaceOptions{},
		"touchOptions":                &cb_sdk.TouchOptions{},
		"singleOperationConfig":       &key_based_loading_cb.SingleOperationConfig{},
		"bulkError":                   &task_result.FailedDocument{},
		"retriedError":                &task_result.FailedDocument{},
		"singleResult":                &task_result.SingleOperationResult{},
		"queryOperationConfig":        &cb_sdk.QueryOperationConfig{},
		"exceptions":                  &bulk_loading_cb.Exceptions{},
		"mutateInOptions":             &cb_sdk.MutateInOptions{},
		"insertSpecOptions":           &cb_sdk.InsertSpecOptions{},
		"removeSpecOptions":           &cb_sdk.RemoveSpecOptions{},
		"getSpecOptions":              &cb_sdk.GetSpecOptions{},
		"lookupInOptions":             &cb_sdk.LookupInOptions{},
		"replaceSpecOptions":          &cb_sdk.ReplaceSpecOptions{},
		"singleSubDocOperationConfig": &key_based_loading_cb.SingleSubDocOperationConfig{},
		"sdkTimings":                  &task_result.SDKTiming{},
	}

}
