package tasks

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"log"
	"sync"
)

// deleteDocuments delete the document stored on a host from start to end.
func (t *Task) deleteDocuments(gen docgenerator.Generator, col *gocb.Collection) {
	var l sync.Mutex
	insertErrorCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.InsertTask.Err {
		insertErrorCheck[k] = struct{}{}
	}
	deleteCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.DeleteTask.Del {
		deleteCheck[k] = struct{}{}
	}

	if t.Request.End < t.Request.Start {
		return
	}

	deleteQueue := make([]int64, 0, MaxConcurrentOps)
	for i := t.Request.Start; i <= t.Request.End; i++ {
		deleteQueue = append(deleteQueue, i)
		if len(deleteQueue) == MaxConcurrentOps || i == t.Request.End {
			var wg sync.WaitGroup
			wg.Add(len(deleteQueue))
			for _, offset := range deleteQueue {
				go func(offset int64) {
					defer wg.Done()
					key := gen.Seed + offset

					if _, ok := insertErrorCheck[key]; ok {
						return
					}
					if _, ok := deleteCheck[key]; ok {
						return
					}
					docId := gen.BuildKey(key)
					if key > t.TaskState.SeedEnd || key < t.TaskState.Seed {
						t.incrementFailure(key, docId, "docId out of bound")
						return
					}
					_, err := col.Remove(docId, nil)
					if err != nil {
						t.incrementFailure(key, docId, err.Error())
						return
					}
					l.Lock()
					t.TaskState.DeleteTask.Del = append(t.TaskState.DeleteTask.Del, key)
					l.Unlock()
				}(offset - 1)
			}
			wg.Wait()
			deleteQueue = deleteQueue[:0]
		}
	}
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection)
}
