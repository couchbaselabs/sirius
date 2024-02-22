package bulk_loading

import (
	"math/rand"
	"sync"
	"time"

	"github.com/barkha06/sirius/internal/db"
	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/barkha06/sirius/internal/task_result"
	"github.com/barkha06/sirius/internal/task_state"
	"github.com/barkha06/sirius/internal/tasks"
	"github.com/jaswdr/faker"
)

func insertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		doc, err1 := gen.Template.GenerateDocument(docId, &fake, operationConfig.DocSize)
		initTime := time.Now().UTC().Format(time.RFC850)
		if err1 != nil {
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}
		operationResult := database.Create(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, db.KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: offset,
		}, extra)

		if operationResult.GetError() != nil {
			if db.CheckAllowedInsertError(operationResult.GetError()) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
				continue
			} else {
				result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

// func bulkInsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
// 	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
// 	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

// 	defer wg.Done()

// 	skip := make(map[int64]struct{})
// 	for _, offset := range state.KeyStates.Completed {
// 		skip[offset] = struct{}{}
// 	}
// 	for _, offset := range state.KeyStates.Err {
// 		skip[offset] = struct{}{}
// 	}

// 	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
// 	if dbErr != nil {
// 		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
// 		return
// 	}

// 	batchSize := int64(100)
// 	totalNumOfDocs := end - start
// 	numBatches := int64(totalNumOfDocs / batchSize)
// 	remNumOfDocs := totalNumOfDocs - (numBatches * batchSize)
// 	offset := start

// 	for i := int64(0); i < numBatches; i++ {
// 		var keyValues []db.KeyValue
// 		initTime := time.Now().UTC().Format(time.RFC850)
// 		for j := int64(0); j < batchSize; j++ {
// 			if _, ok := skip[offset]; ok {
// 				continue
// 			}

// 			key := offset + seed
// 			docId := gen.BuildKey(key)
// 			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
// 			doc, err1 := gen.Template.GenerateDocument(docId, &fake, operationConfig.DocSize)

// 			if err1 != nil {
// 				result.IncrementFailure(initTime, docId, err1, false, nil, offset)
// 				continue
// 			}
// 			keyVal := db.KeyValue{
// 				Key:    docId,
// 				Doc:    doc,
// 				Offset: offset,
// 			}
// 			keyValues = append(keyValues, keyVal)
// 			offset++
// 		}

// 		bulkOperationResult := database.CreateBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues, extra)

// 		for j := range keyValues {
// 			if bulkOperationResult.GetError(keyValues[j].Key) != nil {
// 				if db.CheckAllowedInsertError(bulkOperationResult.GetError(keyValues[j].Key)) && rerun {
// 					state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
// 					continue
// 				} else {
// 					result.IncrementFailure(initTime, keyValues[j].Key, bulkOperationResult.GetError(keyValues[j].Key), false, bulkOperationResult.GetExtra(keyValues[j].Key), offset)
// 					state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
// 				}
// 			} else {
// 				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
// 			}
// 		}

// 	}

// 	// Inserting the remaining documents
// 	if remNumOfDocs > 0 {
// 		var keyValues []db.KeyValue
// 		initTime := time.Now().UTC().Format(time.RFC850)
// 		for i := int64(0); i < remNumOfDocs; i++ {
// 			if _, ok := skip[offset]; ok {
// 				continue
// 			}

// 			key := offset + seed
// 			docId := gen.BuildKey(key)
// 			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
// 			doc, err1 := gen.Template.GenerateDocument(docId, &fake, operationConfig.DocSize)

// 			if err1 != nil {
// 				result.IncrementFailure(initTime, docId, err1, false, nil, offset)
// 				continue
// 			}
// 			keyVal := db.KeyValue{
// 				Key:    docId,
// 				Doc:    doc,
// 				Offset: offset,
// 			}
// 			keyValues = append(keyValues, keyVal)
// 			offset++
// 		}

// 		bulkOperationResult := database.CreateBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues, extra)

// 		for j := range keyValues {
// 			if bulkOperationResult.GetError(keyValues[j].Key) != nil {
// 				if db.CheckAllowedInsertError(bulkOperationResult.GetError(keyValues[j].Key)) && rerun {
// 					state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
// 					continue
// 				} else {
// 					result.IncrementFailure(initTime, keyValues[j].Key, bulkOperationResult.GetError(keyValues[j].Key), false, bulkOperationResult.GetExtra(keyValues[j].Key), offset)
// 					state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
// 				}
// 			} else {
// 				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
// 			}
// 		}
// 	}
// }

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

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
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

		originalDoc1, err1 := gen.Template.GenerateDocument(docId, &fake, operationConfig.DocSize)
		if err1 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}
		// log.Println("\noriginal doc: ", originalDoc1)
		originalDoc, err1 := retracePreviousMutations(req, identifier, offset, originalDoc1, gen, &fake,
			result.ResultSeed)
		if err1 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}
		// log.Println(" last updated: ", originalDoc)
		docUpdated, err2 := gen.Template.UpdateDocument(operationConfig.FieldsToChange, originalDoc,
			operationConfig.DocSize, &fake)
		if err2 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err2, false, nil, offset)
			continue
		}
		// log.Println("\n new doc ", docUpdated)
		operationResult := database.Update(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, db.KeyValue{
			Key:    docId,
			Doc:    docUpdated,
			Offset: offset,
		}, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func deleteDocuments(start, end, seed int64, rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		initTime := time.Now().UTC().Format(time.RFC850)
		operationResult := database.Delete(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, docId, offset,
			extra)

		if operationResult.GetError() != nil {
			if db.CheckAllowedDeletetError(operationResult.GetError()) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
				continue

			} else {
				result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func readDocuments(start, end, seed int64, _ bool, gen *docgenerator.Generator, state *task_state.TaskState,
	result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		initTime := time.Now().UTC().Format(time.RFC850)
		operationResult := database.Read(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, docId, offset,
			extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func touchDocuments(start, end, seed int64, _ bool, gen *docgenerator.Generator, state *task_state.TaskState,
	result *task_result.TaskResult, databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		initTime := time.Now().UTC().Format(time.RFC850)
		operationResult := database.Touch(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, docId, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocInsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
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

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, value := range gen.Template.GenerateSubPathAndValue(&fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Doc:    value,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.InsertSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocReadDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
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

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, _ := range gen.Template.GenerateSubPathAndValue(&fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.ReadSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocUpsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
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

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
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

		subDocumentMap := gen.Template.GenerateSubPathAndValue(&fake, operationConfig.DocSize)
		if _, err1 := retracePreviousSubDocMutations(req, identifier, offset, gen, &fake, result.ResultSeed,
			subDocumentMap); err1 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, value := range gen.Template.GenerateSubPathAndValue(&fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Doc:    value,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.UpsertSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocDeleteDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
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

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, _ := range gen.Template.GenerateSubPathAndValue(&fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.DeleteSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func subDocReplaceDocuments(start, end, seed int64, operationConfig *OperationConfig,
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

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
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

		subDocumentMap := gen.Template.GenerateSubPathAndValue(&fake, operationConfig.DocSize)
		if _, err1 := retracePreviousSubDocMutations(req, identifier, offset, gen, &fake, result.ResultSeed,
			subDocumentMap); err1 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, value := range gen.Template.GenerateSubPathAndValue(&fake, operationConfig.DocSize) {
			keyValues = append(keyValues, db.KeyValue{
				Key:    subPath,
				Doc:    value,
				Offset: subPathOffset,
			})
			subPathOffset++
		}

		operationResult := database.ReplaceSubDoc(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password,
			docId, keyValues, offset, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func bulkInsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		doc, _ := gen.Template.GenerateDocument(docId, &fake, operationConfig.DocSize)
		keyValues = append(keyValues, db.KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: offset,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.CreateBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedInsertError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}

	}
}

func bulkUpsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, req *tasks.Request, identifier string, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		originalDoc1, _ := gen.Template.GenerateDocument(docId, &fake, operationConfig.DocSize)
		originalDoc, _ := retracePreviousMutations(req, identifier, offset, originalDoc1, gen, &fake, result.ResultSeed)
		docUpdated, _ := gen.Template.UpdateDocument(operationConfig.FieldsToChange, originalDoc, operationConfig.DocSize, &fake)
		keyValues = append(keyValues, db.KeyValue{
			Key:    docId,
			Doc:    docUpdated,
			Offset: offset,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.UpdateBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
				x.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}

	}
}

func bulkDeleteDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, db.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.DeleteBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			if db.CheckAllowedDeletetError(bulkResult.GetError(x.Key)) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
			} else {
				result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
					x.Offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}

func bulkReadDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, db.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.ReadBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
				x.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}

func bulkTouchDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	defer wg.Done()

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	database, dbErr := db.ConfigDatabase(databaseInfo.DBType)
	if dbErr != nil {
		result.FailWholeBulkOperation(start, end, dbErr, state, gen, seed)
		return
	}

	var keyValues []db.KeyValue
	for offset := start; offset < end; offset++ {
		if _, ok := skip[offset]; ok {
			continue
		}

		key := offset + seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, db.KeyValue{
			Key: docId,
		})
	}

	initTime := time.Now().UTC().Format(time.RFC850)
	bulkResult := database.TouchBulk(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, keyValues,
		extra)

	for _, x := range keyValues {
		if bulkResult.GetError(x.Key) != nil {
			result.IncrementFailure(initTime, x.Key, bulkResult.GetError(x.Key), false, bulkResult.GetExtra(x.Key),
				x.Offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: x.Offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: x.Offset}
		}
	}
}
