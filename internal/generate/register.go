package generate

import (
	"github.com/couchbaselabs/sirius/internal/sdk"
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
		"/insert":      {"POST", &tasks.InsertTask{}},
		"/fast-insert": {"Post", &tasks.FastInsertTask{}},
		"/delete":      {"POST", &tasks.DeleteTask{}},
		"/upsert":      {"POST", &tasks.UpsertTask{}},
		"/validate":    {"POST", &tasks.ValidateTask{}},
		"/result":      {"POST", &tasks.TaskResult{}},
		"/clear_data":  {"POST", &tasks.ClearTask{}},
		"/read":        {"POST", &tasks.ReadTask{}},
	}
}

func (r *Register) HelperStruct() map[string]any {
	return map[string]any{
		"clusterConfig":     &sdk.ClusterConfig{},
		"compressionConfig": &sdk.CompressionConfig{},
		"timeoutsConfig":    &sdk.TimeoutsConfig{},
		"operationConfig":   &tasks.OperationConfig{},
		"insertOptions":     &tasks.InsertOptions{},
		"removeOptions":     &tasks.RemoveOptions{},
	}

}
