package tasks

const (
	InsertOperation   string = "insert"
	DeleteOperation   string = "delete"
	UpsertOperation   string = "upsert"
	FlushOperation    string = "flush"
	ValidateOperation string = "validate"
)

// TaskResult represents a request structure for retrieving result of the task.
type TaskResult struct {
	Seed         string `json:"seed"`
	DeleteRecord bool   `json:"deleteRecord"`
}

func (r *TaskResult) Describe() string {
	return " Task result is retrieved via this endpoint.\n"
}

func (r *TaskResult) Do() error {
	return nil
}
func (r *TaskResult) Config() (int64, error) {
	return 0, nil
}

// TaskResponse represents a response structure which is returned to user upon scheduling a task.
type TaskResponse struct {
	Seed string `json:"seed"`
}
