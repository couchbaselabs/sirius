package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
)

// upsertDocuments updates the fields of a template a described by user request from start to end.
func (t *Task) upsertDocuments(gen docgenerator.Generator, col *gocb.Collection) {
	u := UpsertTask{
		Start:         t.Request.Start,
		End:           t.Request.End,
		FieldToChange: t.Request.FieldsToChange,
	}

	insertErrorCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.InsertTask.Err {
		insertErrorCheck[k] = struct{}{}
	}

	deleteCheck := make(map[int64]struct{})
	for _, k := range t.TaskState.DeleteTask.Del {
		deleteCheck[k] = struct{}{}
	}

	rateLimiter := make(chan struct{}, MaxConcurrentOps)
	dataChannel := make(chan int64, MaxConcurrentOps)
	group := errgroup.Group{}

	for i := t.Request.Start; i <= t.Request.End; i++ {
		rateLimiter <- struct{}{}
		dataChannel <- i

		group.Go(func() error {
			var err error
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
				<-rateLimiter
				t.incrementFailure(key, docId, "docId out of bound")
				return fmt.Errorf("docId out of bound")
			}
			fake := faker.NewWithSeed(rand.NewSource(key))
			originalDoc, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
			if err != nil {
				<-rateLimiter
				t.incrementFailure(key, docId, err.Error())
				return err
			}
			originalDoc, err = t.retracePreviousMutations(key, originalDoc, gen, &fake)
			if err != nil {
				<-rateLimiter
				return err
			}
			docUpdated, err := gen.Template.UpdateDocument(t.Request.FieldsToChange, originalDoc, &fake)
			_, err = col.Upsert(docId, docUpdated, nil)
			if err != nil {
				t.incrementFailure(key, docId, err.Error())
				<-rateLimiter
				return err
			}
			if key > t.TaskState.SeedEnd {
				t.TaskState.SeedEnd = key
			}

			<-rateLimiter
			return nil
		})
	}
	_ = group.Wait()
	close(rateLimiter)
	close(dataChannel)
	t.TaskState.UpsertTask = append(t.TaskState.UpsertTask, u)
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection)
}
