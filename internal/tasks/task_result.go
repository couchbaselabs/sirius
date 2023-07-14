package tasks

// TaskResult represents a request structure for retrieving result of the task.
type TaskResult struct {
	Seed         string `json:"seed" doc:"true"`
	DeleteRecord bool   `json:"deleteRecord" doc:"true"`
	TaskPending  bool   `json:"-" doc:"false"`
}

func (r *TaskResult) Describe() string {
	return " Task result is retrieved via this endpoint.\n"
}

func (r *TaskResult) tearUp() error {
	return nil
}

func (r *TaskResult) Do() error {
	r.TaskPending = false
	return nil
}

func (r *TaskResult) Config(req *Request, rerun bool) (int, error) {
	r.TaskPending = false
	return 0, nil
}

func (r *TaskResult) BuildIdentifier() string {
	return ""
}

func (r *TaskResult) CollectionIdentifier() string {
	return ""
}

func (r *TaskResult) CheckIfPending() bool {
	return r.TaskPending
}

// TaskResponse represents a response structure which is returned to user upon scheduling a task.
type TaskResponse struct {
	Seed string `json:"seed"`
}
