package tasks

type Task interface {
	Describe() string
	Do() error
	Config(req *Request, reRun bool) (int, error)
	BuildIdentifier() string
	CollectionIdentifier() string
	CheckIfPending() bool
	tearUp() error
}
