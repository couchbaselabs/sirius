package generate

import "github.com/couchbaselabs/sirius/internal/tasks"

type TaskRegister struct {
	httpMethod string
	config     tasks.Task
}

type Register struct {
}

func (r *Register) RegisteredTasks() map[string]TaskRegister {
	return map[string]TaskRegister{
		"/insert":     {"POST", &tasks.InsertTask{}},
		"/delete":     {"POST", &tasks.DeleteTask{}},
		"/upsert":     {"POST", &tasks.UpsertTask{}},
		"/validate":   {"POST", &tasks.ValidateTask{}},
		"/result":     {"POST", &tasks.TaskResult{}},
		"/clear_data": {"POST", &tasks.ClearTask{}},
	}
}
