package tasks

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"sync"
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

	upsertQueue := make([]int64, 0, MaxConcurrentOps)
	for i := t.Request.Start; i <= t.Request.End; i++ {
		upsertQueue = append(upsertQueue, i)
		if len(upsertQueue) == MaxConcurrentOps || i == t.Request.End {
			var wg sync.WaitGroup
			wg.Add(len(upsertQueue))
			for _, offset := range upsertQueue {
				go func(offset int64) {
					defer wg.Done()
					var err error
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
					fake := faker.NewWithSeed(rand.NewSource(key))
					originalDoc, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
					if err != nil {
						t.incrementFailure(key, docId, err.Error())
						return
					}
					originalDoc, err = t.retracePreviousMutations(key, originalDoc, gen, &fake)
					if err != nil {
						return
					}
					docUpdated, err := gen.Template.UpdateDocument(t.Request.FieldsToChange, originalDoc, &fake)
					_, err = col.Upsert(docId, docUpdated, nil)
					if err != nil {
						t.incrementFailure(key, docId, err.Error())
						return
					}
					if key > t.TaskState.SeedEnd {
						t.TaskState.SeedEnd = key
					}
				}(offset - 1)
			}
			wg.Wait()
			upsertQueue = upsertQueue[:0]
		}
	}
	t.TaskState.UpsertTask = append(t.TaskState.UpsertTask, u)
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection)
}
