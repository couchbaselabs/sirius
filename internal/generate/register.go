package generate

import (
	"github.com/couchbaselabs/sirius/internal/sdk"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
)

type TaskRegister struct {
	httpMethod string
	config     tasks.Task
}

type Register struct {
}

func (r *Register) RegisteredTasks() map[string]TaskRegister {
	return map[string]TaskRegister{
		"/bulk-create":            {"POST", &tasks.InsertTask{}},
		"/bulk-delete":            {"POST", &tasks.DeleteTask{}},
		"/bulk-upsert":            {"POST", &tasks.UpsertTask{}},
		"/validate":               {"POST", &tasks.ValidateTask{}},
		"/result":                 {"POST", &tasks.TaskResult{}},
		"/clear_data":             {"POST", &tasks.ClearTask{}},
		"/bulk-read":              {"POST", &tasks.ReadTask{}},
		"/single-create":          {"POST", &tasks.SingleInsertTask{}},
		"/single-delete":          {"POST", &tasks.SingleDeleteTask{}},
		"/single-upsert":          {"POST", &tasks.SingleUpsertTask{}},
		"/single-read":            {"POST", &tasks.SingleReadTask{}},
		"/single-touch":           {"POST", &tasks.SingleTouchTask{}},
		"/single-replace":         {"POST", &tasks.SingleReplaceTask{}},
		"/run-template-query":     {"POST", &tasks.QueryTask{}},
		"/retry-exceptions":       {"POST", &tasks.RetryExceptions{}},
		"/sub-doc-bulk-insert":    {"POST", &tasks.SubDocInsert{}},
		"/sub-doc-bulk-upsert":    {"POST", &tasks.SubDocUpsert{}},
		"/sub-doc-bulk-delete":    {"POST", &tasks.SubDocDelete{}},
		"/sub-doc-bulk-read":      {"POST", &tasks.SubDocRead{}},
		"/sub-doc-bulk-replace":   {"POST", &tasks.SubDocReplace{}},
		"/single-sub-doc-insert":  {"POST", &tasks.SingleSubDocInsert{}},
		"/single-sub-doc-upsert":  {"POST", &tasks.SingleSubDocUpsert{}},
		"/single-sub-doc-replace": {"POST", &tasks.SingleSubDocReplace{}},
		"/single-sub-doc-delete":  {"POST", &tasks.SingleSubDocDelete{}},
		"/single-sub-doc-read":    {"POST", &tasks.SingleSubDocRead{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"clusterConfig":               &sdk.ClusterConfig{},
		"compressionConfig":           &sdk.CompressionConfig{},
		"timeoutsConfig":              &sdk.TimeoutsConfig{},
		"operationConfig":             &tasks.OperationConfig{},
		"insertOptions":               &tasks.InsertOptions{},
		"removeOptions":               &tasks.RemoveOptions{},
		"replaceOption":               &tasks.ReplaceOptions{},
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
	}

}
