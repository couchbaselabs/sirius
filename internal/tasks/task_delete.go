package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"golang.org/x/sync/errgroup"
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

	rateLimiter := make(chan struct{}, MaxConcurrentOps)
	dataChannel := make(chan int64, MaxConcurrentOps)
	group := errgroup.Group{}

	for i := t.Request.Start; i <= t.Request.End; i++ {
		rateLimiter <- struct{}{}
		dataChannel <- i

		group.Go(func() error {
			offset := (<-dataChannel) - 1
			key := gen.Seed + offset
			if _, ok := insertErrorCheck[key]; ok {
				<-rateLimiter
				return fmt.Errorf("key is in InsertErrorCheck")
			}
			if _, ok := deleteCheck[key]; ok {
				<-rateLimiter
				return fmt.Errorf("key is delete from the server")
			}

			docId := gen.BuildKey(key)
			if key > t.TaskState.SeedEnd || key < t.TaskState.Seed {
				t.incrementFailure(key, docId, "docId out of bound")
				<-rateLimiter
				return fmt.Errorf("docId out of bound")
			}
			_, err := col.Remove(docId, nil)
			if err != nil {
				t.incrementFailure(key, docId, err.Error())
				<-rateLimiter
				return err
			}
			l.Lock()
			t.TaskState.DeleteTask.Del = append(t.TaskState.DeleteTask.Del, key)
			l.Unlock()

			<-rateLimiter
			return nil
		})
	}
	_ = group.Wait()
	close(rateLimiter)
	close(dataChannel)
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection)
}
