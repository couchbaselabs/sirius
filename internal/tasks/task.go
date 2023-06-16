package tasks

type Task interface {
	Describe() string
	Do() error
	Config(req *Request, seed int, seedEnd int, reRun bool) (int, error)
	BuildIdentifier() string
	CheckIfPending() bool
	tearUp() error
}
