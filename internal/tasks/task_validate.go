package tasks

import (
	"encoding/json"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"sync"
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

	validateQueue := make([]int64, 0, MaxConcurrentOps)
	for key := t.TaskState.Seed; key < t.TaskState.SeedEnd; key++ {
		validateQueue = append(validateQueue, key)
		if len(validateQueue) == MaxConcurrentOps || key == t.Request.End {
			var wg sync.WaitGroup
			wg.Add(len(validateQueue))
			for _, key := range validateQueue {
				go func(key int64) {
					defer wg.Done()

					if _, ok := insertErrorCheck[key]; ok {
						return
					}
					if _, ok := deleteCheck[key]; ok {
						return
					}
					docId := gen.BuildKey(key)
					var resultFromHost map[string]interface{}
					documentFromHost, err := template.InitialiseTemplate(t.Request.TemplateToken)
					if err != nil {
						t.validationFailures(docId)
						return
					}
					result, err := col.Get(docId, nil)
					if err != nil {
						t.incrementFailure(key, docId, err.Error())
						return
					}
					if err := result.Content(&resultFromHost); err != nil {
						t.incrementFailure(key, docId, err.Error())
						return
					}
					resultBytes, err := json.Marshal(resultFromHost)
					err = json.Unmarshal(resultBytes, &documentFromHost)
					if err != nil {
						t.validationFailures(docId)

						return
					}
					fake := faker.NewWithSeed(rand.NewSource(key))
					originalDocument, err := gen.Template.GenerateDocument(&fake, t.TaskState.DocumentSize)
					if err != nil {
						t.validationFailures(docId)

						return
					}
					originalDocument, err = t.retracePreviousMutations(key, originalDocument, gen, &fake)
					if err != nil {
						t.validationFailures(docId)

						return
					}
					ok, err := gen.Template.Compare(documentFromHost, originalDocument)
					if err != nil || !ok {
						t.validationFailures(docId)
						return
					}
				}(key)
			}
			wg.Wait()
			validateQueue = validateQueue[:0]
		}
	}
	log.Println(t.Request.Operation, t.Request.Bucket, t.Request.Scope, t.Request.Collection, t.Result.Failure)
}
