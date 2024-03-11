package tasks

import (
	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/shettyh/threadpool"
	"runtime"
)

var MaxRetryingRoutines = 250

var MaxThreads = runtime.NumCPU()
var MAXQueueSize int64 = 10000000
var Pool = threadpool.NewThreadPool(MaxThreads, MAXQueueSize)

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
