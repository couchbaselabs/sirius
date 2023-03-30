package tasks

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"sync"
)

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func (t *Task) insertDocuments(gen docgenerator.Generator, col *gocb.Collection) {
	for i := int64(0); i < t.Request.Iteration; i++ {
		wg := sync.WaitGroup{}
		wg.Add(int(t.Request.BatchSize))
		for index := int64(0); index < t.Request.BatchSize; index++ {
			go func(iteration, batchSize, index int64) {
				defer wg.Done()
				docId, key := gen.GetDocIdAndKey(iteration, t.Request.BatchSize, index)
				fake := faker.NewWithSeed(rand.NewSource(key))
				doc, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
				if err != nil {
					t.incrementFailure(key, docId, err.Error())
					return
				}
				_, err = col.Insert(docId, doc, &gocb.InsertOptions{
					DurabilityLevel: t.Request.DurabilityLevel,
				})
				if err != nil {
					t.incrementFailure(key, docId, err.Error())
					return
				}
			}(i, t.Request.BatchSize, index)
		}
		wg.Wait()
	}
	t.TaskState.SeedEnd += t.Request.Iteration * t.Request.BatchSize
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection)
}
