package data_loading

import (
	"encoding/json"
	"github.com/bgadrian/fastfaker/faker"
	"github.com/couchbaselabs/sirius/internal/db"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"
	"log"
	"sync"
	"time"
)

func validateDocuments(start, end, seed int64, operationConfig *OperationConfig,
	_ bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, req *tasks.Request, identifier string, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	skip := make(map[int64]struct{})
	for _, offset := range state.KeyStates.Completed {
		skip[offset] = struct{}{}
	}
	for _, offset := range state.KeyStates.Err {
		skip[offset] = struct{}{}
	}

	deletedOffset, err1 := retracePreviousDeletions(req, identifier, result.ResultSeed)
	if err1 != nil {
		log.Println(err1)
		return
	}

	deletedOffsetSubDoc, err2 := retracePreviousSubDocDeletions(req, identifier, result.ResultSeed)
	if err2 != nil {
		log.Println(err2)
		return
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

		operationConfigDoc, err3 := retrieveLastConfig(req, offset, false)
		if err3 != nil {
			operationConfigDoc = *operationConfig
		}
		operationConfigSubDoc, err4 := retrieveLastConfig(req, offset, true)
		if err4 != nil {
			operationConfigSubDoc = *operationConfig
		}

		/* Resetting the doc generator for the offset as per
		the last configuration of operation performed on offset.
		*/
		genDoc := docgenerator.Reset(
			operationConfigDoc.KeySize,
			operationConfigDoc.DocSize,
			operationConfigDoc.TemplateName)

		genSubDoc := docgenerator.Reset(
			operationConfigSubDoc.KeySize,
			operationConfigSubDoc.DocSize,
			operationConfigSubDoc.TemplateName)

		fake := faker.NewFastFaker()
		fake.Seed(key)
		fakeSub := faker.NewFastFaker()
		fakeSub.Seed(key)

		originalDoc := genDoc.Template.GenerateDocument(fake, docId, operationConfigDoc.DocSize)

		originalDoc, err5 := retracePreviousMutations(req, identifier, offset, originalDoc, genDoc, fake,
			result.ResultSeed)

		if err5 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err5, false, nil, offset)
			continue
		}

		subDocumentMap := genSubDoc.Template.GenerateSubPathAndValue(fakeSub, operationConfigSubDoc.DocSize)

		if _, err6 := retracePreviousSubDocMutations(req, identifier, offset, genSubDoc, fake, result.ResultSeed,
			subDocumentMap); err6 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err6, false, nil, offset)
			continue
		}

		mutationCount, err7 := countMutation(req, identifier, offset, result.ResultSeed)
		if err7 != nil {
			result.IncrementFailure(initTime, docId, err7, false, nil, offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			continue
		}

		operationResult := database.Read(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, docId, offset,
			extra)

		if operationResult.GetError() != nil {
			if db.CheckAllowedDeletetError(operationResult.GetError()) {
				if _, ok := deletedOffset[offset]; ok {
					state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
					continue
				}
				if _, ok := deletedOffsetSubDoc[offset]; ok {
					state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
					continue
				}
			}
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			continue
		}

		documentByte, _ := json.Marshal(originalDoc)
		documentMap := make(map[string]any)
		if err8 := json.Unmarshal(documentByte, &documentMap); err8 != nil {
			result.IncrementFailure(initTime, docId, err8, false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			continue
		}
		documentMap[template.MutatedPath] = float64(mutationCount)

		serverValue := operationResult.Value()
		resultFromHost := serverValue.(map[string]any)
		if !CompareDocumentsIsSame(resultFromHost, documentMap, subDocumentMap) {
			result.IncrementFailure(initTime, docId, err_sirius.IntegrityLost, false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}

		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}
	}
}

func insertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)
		initTime := time.Now().UTC().Format(time.RFC850)
		operationResult := database.Create(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, db.KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: offset,
		}, extra)

		if operationResult.GetError() != nil {
			if db.CheckAllowedInsertError(operationResult.GetError()) && rerun {
				state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
			} else {
				result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
				state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}

		operationResult = nil
		doc = nil

	}

}

func upsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	_ bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, req *tasks.Request, identifier string, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		var err error
		originalDoc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)

		originalDoc, err = retracePreviousMutations(req, identifier, offset, originalDoc, gen, fake,
			result.ResultSeed)
		if err != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err, false, nil, offset)
			continue
		}

		originalDoc, err = gen.Template.UpdateDocument(operationConfig.FieldsToChange, originalDoc,
			operationConfig.DocSize, fake)
		if err != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err, false, nil, offset)
			continue
		}

		operationResult := database.Update(databaseInfo.ConnStr, databaseInfo.Username, databaseInfo.Password, db.KeyValue{
			Key:    docId,
			Doc:    originalDoc,
			Offset: offset,
		}, extra)

		if operationResult.GetError() != nil {
			result.IncrementFailure(initTime, docId, operationResult.GetError(), false, operationResult.GetExtra(), offset)
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
		} else {
			state.StateChannel <- task_state.StateHelper{Status: task_state.COMPLETED, Offset: offset}
		}

		operationResult = nil
		originalDoc = nil
	}
}

func deleteDocuments(start, end, seed int64, rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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

		operationResult = nil

	}
}

func readDocuments(start, end, seed int64, _ bool, gen *docgenerator.Generator, state *task_state.TaskState,
	result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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

		operationResult = nil
	}
}

func touchDocuments(start, end, seed int64, _ bool, gen *docgenerator.Generator, state *task_state.TaskState,
	result *task_result.TaskResult, databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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

		operationResult = nil
	}
}

func subDocInsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, value := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
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

		operationResult = nil
		keyValues = keyValues[:0]
	}
}

func subDocReadDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, _ := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
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
		operationResult = nil
		keyValues = keyValues[:0]
	}
}

func subDocUpsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	_ bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, req *tasks.Request, identifier string, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		var keyValues []db.KeyValue
		subPathOffset := int64(0)

		subDocumentMap := gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize)

		if _, err1 := retracePreviousSubDocMutations(req, identifier, offset, gen, fake, result.ResultSeed,
			subDocumentMap); err1 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}

		for subPath, value := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
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
		operationResult = nil
		keyValues = keyValues[:0]
	}
}

func subDocDeleteDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, _ := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
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
		operationResult = nil
		keyValues = keyValues[:0]
	}
}

func subDocReplaceDocuments(start, end, seed int64, operationConfig *OperationConfig,
	_ bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, req *tasks.Request, identifier string, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		initTime := time.Now().UTC().Format(time.RFC850)

		subDocumentMap := gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize)
		if _, err1 := retracePreviousSubDocMutations(req, identifier, offset, gen, fake, result.ResultSeed,
			subDocumentMap); err1 != nil {
			state.StateChannel <- task_state.StateHelper{Status: task_state.ERR, Offset: offset}
			result.IncrementFailure(initTime, docId, err1, false, nil, offset)
			continue
		}

		var keyValues []db.KeyValue
		subPathOffset := int64(0)
		for subPath, value := range gen.Template.GenerateSubPathAndValue(fake, operationConfig.DocSize) {
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

		operationResult = nil
		keyValues = keyValues[:0]
	}
}

func bulkInsertDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)
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
	bulkResult = nil
	keyValues = keyValues[:0]
}

func bulkUpsertDocuments(start int64, end int64, seed int64, operationConfig *OperationConfig, rerun bool,
	gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, req *tasks.Request, identifier string,
	wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
		fake := faker.NewFastFaker()
		fake.Seed(key)
		originalDoc := gen.Template.GenerateDocument(fake, docId, operationConfig.DocSize)

		originalDoc, _ = retracePreviousMutations(req, identifier, offset, originalDoc, gen, fake,
			result.ResultSeed)

		originalDoc, _ = gen.Template.UpdateDocument(operationConfig.FieldsToChange, originalDoc,
			operationConfig.DocSize, fake)

		keyValues = append(keyValues, db.KeyValue{
			Key:    docId,
			Doc:    originalDoc,
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
	bulkResult = nil
	keyValues = keyValues[:0]
}

func bulkDeleteDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
	bulkResult = nil
	keyValues = keyValues[:0]
}

func bulkReadDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
	bulkResult = nil
	keyValues = keyValues[:0]
}

func bulkTouchDocuments(start, end, seed int64, operationConfig *OperationConfig,
	rerun bool, gen *docgenerator.Generator, state *task_state.TaskState, result *task_result.TaskResult,
	databaseInfo tasks.DatabaseInformation, extra db.Extras, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

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
	bulkResult = nil
	keyValues = keyValues[:0]
}
