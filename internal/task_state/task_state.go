package task_state

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	COMPLETED         = 1
	ERR               = 0
	StateChannelLimit = 10000
	TASKSTATELOGS     = "./internal/task_state/task_state_logs"
)

type StateHelper struct {
	Status int64 `json:"-"`
	Offset int64 `json:"-"`
}

type KeyStates struct {
	Completed []int64 `json:"completed"`
	Err       []int64 `json:"err"`
}

type TaskState struct {
	SeedStart    int64              `json:"seedStart"`
	SeedEnd      int64              `json:"seedEnd"`
	ResultSeed   int64              `json:"resultSeed"`
	KeyStates    KeyStates          `json:"keyStates"`
	StateChannel chan StateHelper   `json:"-"`
	ctx          context.Context    `json:"-"`
	cancel       context.CancelFunc `json:"-"`
	lock         sync.Mutex         `json:"-"`
}

// ConfigTaskState returns an instance of TaskState
func ConfigTaskState(seed, seedEnd int64, resultSeed int64) *TaskState {
	ctx, cancel := context.WithCancel(context.Background())
	ts := &TaskState{
		SeedStart:    seed,
		SeedEnd:      seedEnd,
		ResultSeed:   resultSeed,
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

// SetupStoringKeys will initialize contextWithCancel and calls
// the StoreState to start key states.
func (t *TaskState) SetupStoringKeys() {
	ctx, cancel := context.WithCancel(context.Background())
	t.StateChannel = make(chan StateHelper, StateChannelLimit)
	t.ctx = ctx
	t.cancel = cancel
	t.StoreState()
}

// AddOffsetToCompleteSet will add offset to Complete set
func (t *TaskState) AddOffsetToCompleteSet(offset int64) {
	t.KeyStates.Completed = append(t.KeyStates.Completed, offset)
}

// AddRangeToCompleteSet will add a range of offset to Complete set
func (t *TaskState) AddRangeToCompleteSet(start, end int64) {
	for i := start; i <= end; i++ {
		t.KeyStates.Completed = append(t.KeyStates.Completed, i)
	}
}

// AddOffsetToErrSet will add offset to Error set
func (t *TaskState) AddOffsetToErrSet(offset int64) {
	t.KeyStates.Err = append(t.KeyStates.Err, offset)
}

// AddRangeToErrSet will add a range of offset to Error set
func (t *TaskState) AddRangeToErrSet(start, end int64) {
	for i := start; i <= end; i++ {
		t.KeyStates.Err = append(t.KeyStates.Err, i)
	}
}

// ReturnCompletedOffset returns a lookup table for searching completed offsets
func (t *TaskState) ReturnCompletedOffset() map[int64]struct{} {
	defer t.lock.Unlock()
	t.lock.Lock()
	completed := make(map[int64]struct{})
	for _, v := range t.KeyStates.Completed {
		completed[v] = struct{}{}
	}
	return completed
}

// ReturnErrOffset returns a lookup table for searching  error offsets
func (t *TaskState) ReturnErrOffset() map[int64]struct{} {
	defer t.lock.Unlock()
	t.lock.Lock()
	err := make(map[int64]struct{})
	for _, v := range t.KeyStates.Err {
		err[v] = struct{}{}
	}
	return err
}

// StoreState will receive the offsets on dataChannel after every " d " durations.
// It will append those keys types to Completed or Error Key state .
func (t *TaskState) StoreState() {

	go func() {
		var completed []int64
		var err []int64
		d := time.NewTicker(10 * time.Second)
		defer d.Stop()
		if t.ctx.Err() != nil {
			log.Print("Ctx closed for StoreState()")
			return
		}
		for {
			select {
			case <-t.ctx.Done():
				{
					t.StoreCompleted(completed)
					t.StoreError(err)
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
					t.StoreCompleted(completed)
					t.StoreError(err)
					err = err[:0]
					completed = completed[:0]
				}
			}
		}
	}()

}

// StoreCompleted appends a list of completed offset to Completed Key state
func (t *TaskState) StoreCompleted(completed []int64) {
	t.lock.Lock()
	for _, offset := range completed {
		t.AddOffsetToCompleteSet(offset)
	}
	t.lock.Unlock()
}

// StoreError appends a list of error offset to Error Key state
func (t *TaskState) StoreError(err []int64) {
	t.lock.Lock()
	for _, offset := range err {
		t.AddOffsetToErrSet(offset)
	}
	t.lock.Unlock()
}

// StopStoringState will terminate the thread which is receiving offset
// on dataChannel.
func (t *TaskState) StopStoringState() {
	time.Sleep(1 * time.Second)
	t.cancel()
	time.Sleep(1 * time.Second)
}

// ClearCompletedKeyStates clears the Completed key state
func (t *TaskState) ClearCompletedKeyStates() {
	t.KeyStates.Completed = t.KeyStates.Completed[:0]
}

// ClearErrorKeyStates clears the Error key state
func (t *TaskState) ClearErrorKeyStates() {
	t.KeyStates.Err = t.KeyStates.Err[:0]
}

func (t *TaskState) SaveTaskSateOnDisk() error {
	t.cancel()
	time.Sleep(time.Second * 2)
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, TASKSTATELOGS, fmt.Sprintf("%d", t.ResultSeed))
	content, err := json.MarshalIndent(t, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(fileName, content, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (t *TaskState) MakeCompleteKeyFromMap(maps map[int64]struct{}) {
	defer t.lock.Unlock()
	t.lock.Lock()
	t.ClearCompletedKeyStates()
	for offset, _ := range maps {
		t.KeyStates.Completed = append(t.KeyStates.Completed, offset)
	}
}

func (t *TaskState) MakeErrorKeyFromMap(maps map[int64]struct{}) {
	defer t.lock.Unlock()
	t.lock.Lock()
	t.ClearErrorKeyStates()
	for offset, _ := range maps {
		t.KeyStates.Err = append(t.KeyStates.Err, offset)
	}
}
