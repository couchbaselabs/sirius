package tasks

type Task interface {
	Describe() string
	Config(*Request, bool) (int64, error)
	Do() error
	CheckIfPending() bool
	TearUp() error
}
