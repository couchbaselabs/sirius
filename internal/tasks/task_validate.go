package tasks

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
)

// ValidateDocuments return the validity of the collection using TaskState
func (t *Task) ValidateDocuments(gen docgenerator.Generator, col *gocb.Collection) {

	if err := t.checkForTaskValidity(); err != nil {
		log.Println(err)
		return
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

	for key := t.TaskState.Seed; key < t.TaskState.SeedEnd; key++ {
		rateLimiter <- struct{}{}
		dataChannel <- key

		group.Go(func() error {
			key := <-dataChannel
			if _, ok := insertErrorCheck[key]; ok {
				<-rateLimiter
				return fmt.Errorf("key is in InsertErrorCheck")
			}
			if _, ok := deleteCheck[key]; ok {
				<-rateLimiter
				return fmt.Errorf("key is delete from the server")
			}
			docId := gen.BuildKey(key)
			var resultFromHost map[string]interface{}
			documentFromHost, err := template.InitialiseTemplate(t.Request.TemplateToken)
			if err != nil {
				t.validationFailures(docId)
				<-rateLimiter
				return err
			}
			result, err := col.Get(docId, nil)
			if err != nil {
				t.incrementFailure(key, docId, err.Error())
				<-rateLimiter
				return err
			}
			if err := result.Content(&resultFromHost); err != nil {
				t.incrementFailure(key, docId, err.Error())
				<-rateLimiter
				return err
			}
			resultBytes, err := json.Marshal(resultFromHost)
			err = json.Unmarshal(resultBytes, &documentFromHost)
			if err != nil {
				t.validationFailures(docId)
				<-rateLimiter
				return err
			}
			fake := faker.NewWithSeed(rand.NewSource(key))
			originalDocument, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
			if err != nil {
				t.validationFailures(docId)
				<-rateLimiter
				return err
			}
			originalDocument, err = t.retracePreviousMutations(key, originalDocument, gen, &fake)
			if err != nil {
				t.validationFailures(docId)
				<-rateLimiter
				return err
			}
			ok, err := gen.Template.Compare(documentFromHost, originalDocument)
			if err != nil || !ok {
				t.validationFailures(docId)
				<-rateLimiter
				return err
			}

			<-rateLimiter
			return nil
		})
	}
	_ = group.Wait()
	close(rateLimiter)
	close(dataChannel)
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection, t.Result.Failure)
}
