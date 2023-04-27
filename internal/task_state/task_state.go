package task_state

import (
	"context"
	"log"
	"sync"
	"time"
)

const (
	COMPLETED         = 1
	ERR               = 0
	StateChannelLimit = 100000
)

type StateHelper struct {
	Status int
	Offset int64
}

type KeyStates struct {
	Completed []int64
	Err       []int64
}

type TaskState struct {
	Operation    string
	User         string
	Host         string
	BUCKET       string
	SCOPE        string
	Collection   string
	TemplateName string
	DocumentSize int64
	SeedStart    int64
	SeedEnd      int64
	ResultSeed   int64
	KeyPrefix    string
	KeySuffix    string
	KeyStates    KeyStates
	StateChannel chan StateHelper
	ctx          context.Context
	cancel       context.CancelFunc
	lock         sync.Mutex
}

func ConfigTaskState(templateName, keyPrefix, keySuffix string, docSize, seed, seedEnd, resultSeed int64) *TaskState {
	ctx, cancel := context.WithCancel(context.Background())
	ts := &TaskState{
		TemplateName: templateName,
		DocumentSize: docSize,
		SeedStart:    seed,
		SeedEnd:      seedEnd,
		ResultSeed:   resultSeed,
		KeyPrefix:    keyPrefix,
		KeySuffix:    keySuffix,
		StateChannel: make(chan StateHelper, StateChannelLimit),
		ctx:          ctx,
		cancel:       cancel,
		lock:         sync.Mutex{},
	}
	defer func() {
		ts.StoreState()
	}()
	return ts
}

func (t *TaskState) SetupStoringKeys() {
	ctx, cancel := context.WithCancel(context.Background())
	t.StateChannel = make(chan StateHelper, StateChannelLimit)
	t.ctx = ctx
	t.cancel = cancel
}

func (t *TaskState) AddOffsetToCompleteSet(offset int64) {
	t.KeyStates.Completed = append(t.KeyStates.Completed, offset)
}

func (t *TaskState) AddRangeToCompleteSet(start, end int64) {
	for i := start; i <= end; i++ {
		t.KeyStates.Completed = append(t.KeyStates.Completed, i)
	}
}

func (t *TaskState) AddOffsetToErrSet(offset int64) {
	t.KeyStates.Completed = append(t.KeyStates.Err, offset)
}

func (t *TaskState) AddRangeToErrSet(start, end int64) {
	for i := start; i <= end; i++ {
		t.KeyStates.Err = append(t.KeyStates.Err, i)
	}
}

func (t *TaskState) ReturnCompletedOffset() map[int64]struct{} {
	defer t.lock.Unlock()
	t.lock.Lock()
	completed := make(map[int64]struct{})
	for _, v := range t.KeyStates.Completed {
		completed[v] = struct{}{}
	}
	return completed
}

func (t *TaskState) ReturnErrOffset() map[int64]struct{} {
	defer t.lock.Unlock()
	t.lock.Lock()
	err := make(map[int64]struct{})
	for _, v := range t.KeyStates.Err {
		err[v] = struct{}{}
	}
	return err
}

func (t *TaskState) StoreState() {

	go func() {
		var completed []int64
		var err []int64
		d := time.NewTicker(30 * time.Second)
		if t.ctx.Err() != nil {
			log.Print("Ctx closed for StoreState()")
			return
		}
		for {
			select {
			case <-t.ctx.Done():
				{
					t.storeCompleted(completed)
					t.storeError(err)
					err = err[:0]
					completed = completed[:0]
					close(t.StateChannel)
					return
				}
			case s := <-t.StateChannel:
				{
					if s.Status == COMPLETED {
						completed = append(completed, s.Offset)
					}
					if s.Status == ERR {
						err = append(err, s.Offset)
					}
				}
			case <-d.C:
				{
					t.storeCompleted(completed)
					t.storeError(err)
					err = err[:0]
					completed = completed[:0]
				}
			}
		}
	}()

}

func (t *TaskState) storeCompleted(completed []int64) {
	t.lock.Lock()
	for _, offset := range completed {
		t.AddOffsetToCompleteSet(offset)
	}
	t.lock.Unlock()
}

func (t *TaskState) storeError(err []int64) {
	t.lock.Lock()
	for _, offset := range err {
		t.AddOffsetToCompleteSet(offset)
	}
	t.lock.Unlock()
}

func (t *TaskState) StopStoringState() {
	t.cancel()
}

func (t *TaskState) ClearKeyStates() {
	t.KeyStates.Completed = t.KeyStates.Completed[:0]
	t.KeyStates.Err = t.KeyStates.Err[:0]
}
