package tasks

const (
	MaxConcurrentRoutines        = 30
	InsertOperation       string = "insert"
	DeleteOperation       string = "delete"
	UpsertOperation       string = "upsert"
	FlushOperation        string = "flush"
	ValidateOperation     string = "validate"
)

const (
	DurabilityLevelMajority                   string = "MAJORITY"
	DurabilityLevelMajorityAndPersistToActive string = "MAJORITY_AND_PERSIST_TO_ACTIVE"
	DurabilityLevelPersistToMajority          string = "PERSIST_TO_MAJORITY"
	DefaultScope                              string = "_default"
	DefaultCollection                         string = "_default"
)

type Task interface {
	Describe() string
	Do() error
	Config(req *Request, seed int64, seedEnd int64, index int, reRun bool) (int64, error)
	BuildIdentifier() string
	CheckIfPending() bool
	tearUp() error
}
