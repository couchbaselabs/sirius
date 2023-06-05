package tasks

type Task interface {
	Describe() string
	Do() error
	Config(req *Request, seed int64, seedEnd int64, reRun bool) (int64, error)
	BuildIdentifier() string
	CheckIfPending() bool
	tearUp() error
}
