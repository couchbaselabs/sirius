package util_sirius

import "github.com/barkha06/sirius/internal/tasks"

// TaskResult represents a request structure for retrieving Result of the task.
type TaskResult struct {
	Seed         string `json:"seed" doc:"true"`
	DeleteRecord bool   `json:"deleteRecord" doc:"true"`
	TaskPending  bool   `json:"-" doc:"false"`
}

func (r *TaskResult) Describe() string {
	return " TaskResult is fetch the result using ResultSeed.\n"
}

func (r *TaskResult) tearUp() error {
	return nil
}

func (r *TaskResult) Do() error {
	r.TaskPending = false
	return nil
}

func (r *TaskResult) Config(_ *tasks.Request, _ bool) (int64, error) {
	r.TaskPending = false
	return 0, nil
}

// TaskResponse represents a response structure which is returned to user upon scheduling a task.
type TaskResponse struct {
	Seed string `json:"seed"`
}
