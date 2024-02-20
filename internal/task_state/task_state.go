package task_state

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
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
	Completed []int64 `json:"completed_state"`
	Err       []int64 `json:"failed_state"`
}

type TaskState struct {
	ResultSeed   int64              `json:"resultSeed"`
	KeyStates    KeyStates          `json:"keyStates" `
	StateChannel chan StateHelper   `json:"-"`
	ctx          context.Context    `json:"-"`
	cancel       context.CancelFunc `json:"-"`
	lock         sync.Mutex         `json:"-"`
}

// ConfigTaskState returns an instance of TaskState
func ConfigTaskState(resultSeed int64) *TaskState {
	ctx, cancel := context.WithCancel(context.Background())
	ts := &TaskState{}

	if state, err := ReadStateFromFile(fmt.Sprintf("%d", resultSeed)); err == nil {
		ts = state
		ts.ctx = ctx
		ts.cancel = cancel
		ts.StateChannel = make(chan StateHelper, StateChannelLimit)
		ts.lock = sync.Mutex{}
	} else {
		ts = &TaskState{
			ResultSeed:   resultSeed,
			StateChannel: make(chan StateHelper, StateChannelLimit),
			ctx:          ctx,
			cancel:       cancel,
			lock:         sync.Mutex{},
		}
	}

	defer func() {
		ts.StoreState()
	}()

	defer func() {
		go func() {
			time.Sleep(24 * time.Hour)
			ts = nil
		}()
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

// AddOffsetToErrSet will add offset to Error set
func (t *TaskState) AddOffsetToErrSet(offset int64) {
	t.KeyStates.Err = append(t.KeyStates.Err, offset)
}

func (t *TaskState) RemoveOffsetFromErrSet(offset int64) {

	index := sort.Search(len(t.KeyStates.Err), func(i int) bool {
		return t.KeyStates.Err[i] >= offset
	})

	if index < len(t.KeyStates.Err) && t.KeyStates.Err[index] == offset {
		t.KeyStates.Err = append(t.KeyStates.Err[:index], t.KeyStates.Err[index+1:]...)
	}
}

func (t *TaskState) CheckOffsetInErr(offset int64) bool {
	index := sort.Search(len(t.KeyStates.Err), func(i int) bool {
		return t.KeyStates.Err[i] >= offset
	})

	if index < len(t.KeyStates.Err) && t.KeyStates.Err[index] == offset {
		return true
	}
	return false
}

// ReturnCompletedOffset returns a lookup table for searching completed offsets
func (t *TaskState) ReturnCompletedOffset() map[int64]struct{} {
	//defer t.lock.Unlock()
	//t.lock.lock()
	completed := make(map[int64]struct{})
	for _, v := range t.KeyStates.Completed {
		completed[v] = struct{}{}
	}
	return completed
}

func (t *TaskState) RemoveOffsetFromCompleteSet(offset int64) {

	index := sort.Search(len(t.KeyStates.Completed), func(i int) bool {
		return t.KeyStates.Completed[i] >= offset
	})

	if index < len(t.KeyStates.Completed) && t.KeyStates.Completed[index] == offset {
		t.KeyStates.Completed = append(t.KeyStates.Completed[:index], t.KeyStates.Completed[index+1:]...)
	}
}

func (t *TaskState) CheckOffsetInComplete(offset int64) bool {
	index := sort.Search(len(t.KeyStates.Completed), func(i int) bool {
		return t.KeyStates.Completed[i] >= offset
	})

	if index < len(t.KeyStates.Completed) && t.KeyStates.Completed[index] == offset {
		return true
	}
	return false
}

// ReturnErrOffset returns a lookup table for searching  error offsets
func (t *TaskState) ReturnErrOffset() map[int64]struct{} {
	//defer t.lock.Unlock()
	//t.lock.lock()
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
		d := time.NewTicker(30 * time.Second)
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
					t.SaveTaskSateOnDisk()
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
					t.SaveTaskSateOnDisk()
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
	if t.ctx.Err() != nil {
		return
	}
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
	defer t.lock.Unlock()
	t.lock.Lock()
	sort.Slice(t.KeyStates.Completed, func(i, j int) bool {
		return t.KeyStates.Completed[i] < t.KeyStates.Completed[j]
	})

	sort.Slice(t.KeyStates.Err, func(i, j int) bool {
		return t.KeyStates.Err[i] < t.KeyStates.Err[j]
	})
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

// ReadStateFromFile reads the task state stored on a file.
func ReadStateFromFile(seed string) (*TaskState, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	fileName := filepath.Join(cwd, TASKSTATELOGS, seed)
	state := &TaskState{}
	content, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("no such result found, reasons:[No such Task, In process, Record Deleted]")
	}
	if err := json.Unmarshal(content, state); err != nil {
		return nil, err
	}
	return state, nil
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
