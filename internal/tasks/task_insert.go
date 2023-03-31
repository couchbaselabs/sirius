package tasks

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
)

type insertHelper struct {
	iteration int64
	index     int64
}

// insertDocuments uploads new documents in a bucket.scope.collection in a defined batch size at multiple iterations.
func (t *Task) insertDocuments(gen docgenerator.Generator, col *gocb.Collection) {
	rateLimiter := make(chan struct{}, MaxConcurrentOps)
	dataChannel := make(chan insertHelper, MaxConcurrentOps)
	group := errgroup.Group{}
	for iteration := int64(0); iteration < t.Request.Iteration; iteration++ {
		for index := int64(0); index < t.Request.BatchSize; index++ {
			rateLimiter <- struct{}{}
			dataChannel <- insertHelper{
				iteration: iteration,
				index:     index,
			}
			group.Go(func() error {
				h := <-dataChannel
				docId, key := gen.GetDocIdAndKey(h.iteration, t.Request.BatchSize, h.index)
				fake := faker.NewWithSeed(rand.NewSource(key))
				doc, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
				if err != nil {
					t.incrementFailure(key, docId, err.Error())
					<-rateLimiter
					return err
				}
				_, err = col.Insert(docId, doc, nil)
				if err != nil {
					log.Println(err)
					t.incrementFailure(key, docId, err.Error())
					<-rateLimiter
					return err
				}

				<-rateLimiter
				return nil
			})
		}
	}
	_ = group.Wait()
	close(rateLimiter)
	close(dataChannel)
	t.TaskState.SeedEnd += t.Request.Iteration * t.Request.BatchSize
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection)
}
