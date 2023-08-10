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
		"/bulk-create":         {"POST", &tasks.InsertTask{}},
		"/bulk-delete":         {"POST", &tasks.DeleteTask{}},
		"/bulk-upsert":         {"POST", &tasks.UpsertTask{}},
		"/validate":            {"POST", &tasks.ValidateTask{}},
		"/result":              {"POST", &tasks.TaskResult{}},
		"/clear_data":          {"POST", &tasks.ClearTask{}},
		"/bulk-read":           {"POST", &tasks.ReadTask{}},
		"/single-create":       {"POST", &tasks.SingleInsertTask{}},
		"/single-delete":       {"POST", &tasks.SingleDeleteTask{}},
		"/single-upsert":       {"POST", &tasks.SingleUpsertTask{}},
		"/single-read":         {"POST", &tasks.SingleReadTask{}},
		"/single-touch":        {"POST", &tasks.SingleTouchTask{}},
		"/single-replace":      {"POST", &tasks.SingleReplaceTask{}},
		"/run-template-query":  {"POST", &tasks.QueryTask{}},
		"/retry-exceptions":    {"POST", &tasks.RetryExceptions{}},
		"/sub-doc-bulk-insert": {"POST", &tasks.SubDocInsert{}},
		"/sub-doc-bulk-upsert": {"POST", &tasks.SubDocUpsert{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"clusterConfig":         &sdk.ClusterConfig{},
		"compressionConfig":     &sdk.CompressionConfig{},
		"timeoutsConfig":        &sdk.TimeoutsConfig{},
		"operationConfig":       &tasks.OperationConfig{},
		"insertOptions":         &tasks.InsertOptions{},
		"removeOptions":         &tasks.RemoveOptions{},
		"replaceOption":         &tasks.ReplaceOptions{},
		"singleOperationConfig": &tasks.SingleOperationConfig{},
		"keyValue":              &tasks.KeyValue{},
		"bulkError":             &task_result.FailedDocument{},
		"retriedError":          &task_result.FailedDocument{},
		"singleResult":          &task_result.SingleOperationResult{},
		"queryOperationConfig":  &tasks.QueryOperationConfig{},
		"exceptions":            &tasks.Exceptions{},
		"mutateInOptions":       &tasks.MutateInOptions{},
		"insertSpecOptions":     &tasks.InsertSpecOptions{},
		"subDocOperationConfig": &tasks.SubDocOperationConfig{},
	}

}
