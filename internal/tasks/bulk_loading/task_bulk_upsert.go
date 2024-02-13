package bulk_loading

import (
	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/jaswdr/faker"
	"math/rand"
	"sync"
	"time"
)

func upsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	_ bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, req *tasks.Request, identifier string, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, err := db.ConfigDatabase(databaseInfo.DBType)
	if err != nil {
		result.FailWholeBulkOperation(start, end, err, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		initTime := time.Now().UTC().Format(time.RFC850)

		originalDoc, err := gen.Template.GenerateDocument(&fake, operationConfig.DocSize)
		if err != nil {
			result.IncrementFailure(initTime, docId, err, false, nil, offset)
			continue
		}
		originalDoc, err = retracePreviousMutations(req, identifier, offset, originalDoc, gen, &fake, result.ResultSeed)
		if err != nil {
			result.IncrementFailure(initTime, docId, err, false, nil, offset)
			continue
		}

		docUpdated, err := gen.Template.UpdateDocument(operationConfig.FieldsToChange, originalDoc,
			operationConfig.DocSize, &fake)
		if err != nil {
			result.IncrementFailure(initTime, docId, err, false, nil, offset)
			continue
		}

		operationResult := database.Update(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, docId,
			docUpdated, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, err, false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
		}

		state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
	}
}
