package tasks

import (
	"sync"

	"github.com/barkha06/sirius/internal/db"
	"github.com/shettyh/threadpool"
)

var MaxConcurrentRoutines = 512
var MaxThreads = 250
var MAXQueueSize int64 = 1000000
var Pool = threadpool.NewThreadPool(MaxThreads, MAXQueueSize)

var lock = sync.Mutex{}

type Task interface {
	Describe() string
	Config(*Request, bool) (int64, error)
	Do()
	CheckIfPending() bool
	TearUp() error
}

type DatabaseInformation struct {
	DBType   string    `json:"dbType" doc:"true"`
	ConnStr  string    `json:"connectionString" doc:"true"`
	Username string    `json:"username" doc:"true"`
	Password string    `json:"password" doc:"true"`
	Extra    db.Extras `json:"extra" doc:"true"`
}
