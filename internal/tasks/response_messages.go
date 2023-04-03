package tasks

const (
	InsertOperation   string = "insert"
	DeleteOperation   string = "delete"
	UpsertOperation   string = "upsert"
	ValidateOperation string = "validate"
)

// RequestResult represents a request structure for retrieving result of the task.
type RequestResult struct {
	Seed         string `json:"seed"`
	DeleteRecord bool   `json:"deleteRecord"`
}

// TaskResponse represents a response structure which is returned to user upon scheduling a task.
type TaskResponse struct {
	Seed string `json:"seed"`
}
