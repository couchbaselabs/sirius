package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/tasks/bulk_loading"
	"github.com/couchbaselabs/sirius/internal/tasks/db_util"
	"github.com/couchbaselabs/sirius/internal/tasks/util_sirius"
	"log"
	"net/http"
)

// testServer supports GET method.
// It returns Document Loader online reflecting availability of doc loading service.
func (app *Config) testServer(w http.ResponseWriter, _ *http.Request) {
	payload := jsonResponse{
		Error:   false,
		Message: "Document Loader Online",
	}

	_ = app.writeJSON(w, http.StatusOK, payload)

}

// clearRequestFromServer clears a test's request from the server.
func (app *Config) clearRequestFromServer(w http.ResponseWriter, r *http.Request) {
	task := &util_sirius.ClearTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, "clear")
	if err := app.serverRequests.ClearIdentifierAndRequest(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, fmt.Errorf("no session for %s", task.IdentifierToken),
			http.StatusUnprocessableEntity)
		return
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully cleared the meta-data",
		Data:    task.IdentifierToken,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// taskResult uses the result token to fetch the desired result and return it to user.
func (app *Config) taskResult(w http.ResponseWriter, r *http.Request) {
	reqPayload := &util_sirius.TaskResult{}
	if err := app.readJSON(w, r, reqPayload); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(reqPayload, "result")
	result, err := task_result.ReadResultFromFile(reqPayload.Seed, reqPayload.DeleteRecord)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusBadRequest)
		return
	}
	respPayload := jsonResponse{
		Error:   false,
		Message: "Successfully retrieved task_result_logs",
		Data:    result,
	}
	_ = app.writeJSON(w, http.StatusOK, respPayload)
}

//// validateTask is validating the cluster's current state.
//func (app *Config) validateTask(w http.ResponseWriter, r *http.Request) {
//	task := &bulk_loading.ValidateTask{}
//	if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	log.Print(task, tasks.ValidateOperation)
//	err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.ValidateOperation, task)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	seedResult, err_sirius := task.Config(req, false)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//	}
//	respPayload := util_sirius.TaskResponse{
//		Seed: fmt.Sprintf("%d", seedResult),
//	}
//	resPayload := jsonResponse{
//		Error:   false,
//		Message: "Successfully started requested doc loading",
//		Data:    respPayload,
//	}
//	_ = app.writeJSON(w, http.StatusOK, resPayload)
//}

// insertTask is used to insert documents.
func (app *Config) insertTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.InsertOperation
	log.Print(task, tasks.InsertOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.InsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// bulkInsertTask is used to insert documents.
func (app *Config) bulkInsertTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.BulkInsertOperation
	log.Print(task, tasks.BulkInsertOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.BulkInsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// deleteTask is used to delete documents.
func (app *Config) deleteTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.DeleteOperation
	log.Print(task, tasks.DeleteOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.DeleteOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// bulkDeleteTask is used to delete documents.
func (app *Config) bulkDeleteTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.BulkDeleteOperation
	log.Print(task, tasks.BulkDeleteOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.BulkDeleteOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// upsertTask is used to update documents.
func (app *Config) upsertTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.UpsertOperation
	log.Print(task, tasks.UpsertOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.UpsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// bulkUpsertTask is used to update documents.
func (app *Config) bulkUpsertTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.BulkUpsertOperation
	log.Print(task, tasks.BulkUpsertOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.UpsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// touchTask is used to update the expiry of documents
func (app *Config) touchTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.TouchOperation
	log.Print(task, tasks.TouchOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.TouchOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// bulkTouchTask is used to update the expiry of documents
func (app *Config) bulkTouchTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.BulkTouchOperation
	log.Print(task, tasks.BulkTouchOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.BulkTouchOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// readTask is to read documents.
func (app *Config) readTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.ReadOperation
	log.Print(task, tasks.ReadOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.ReadOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// bulkReadTask is to read documents.
func (app *Config) bulkReadTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.BulkReadOperation
	log.Print(task, tasks.BulkReadOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.BulkReadOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// SubDocInsertTask is used to load bulk sub documents into buckets
func (app *Config) SubDocInsertTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.SubDocInsertOperation
	log.Print(task, tasks.SubDocInsertOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocInsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// SubDocUpsertTask is used to bulk updating sub documents into buckets
func (app *Config) SubDocUpsertTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.SubDocUpsertOperation
	log.Print(task, tasks.SubDocUpsertOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocUpsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// SubDocDeleteTask is used to bulk updating sub documents into buckets
func (app *Config) SubDocDeleteTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.SubDocDeleteOperation
	log.Print(task, tasks.SubDocDeleteOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocDeleteOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// SubDocReadTask is used to bulk updating sub documents into bucketsSubDocReadOperation
func (app *Config) SubDocReadTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.SubDocReadOperation
	log.Print(task, tasks.SubDocReadOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocReadOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// SubDocReplaceTask is used to bulk updating sub documents into buckets

func (app *Config) SubDocReplaceTask(w http.ResponseWriter, r *http.Request) {
	task := &bulk_loading.GenericLoadingTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := checkIdentifierToken(task.IdentifierToken); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.SubDocReplaceOperation
	log.Print(task, tasks.SubDocReplaceOperation)
	err := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocReplaceOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// // singleInsertTask is used to insert document in a collection
//
//	func (app *Config) singleInsertTask(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleInsertTask{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleInsertOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleInsertOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		seedResult, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", seedResult),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // singleDeleteTask is used to delete documents on a collection
//
//	func (app *Config) singleDeleteTask(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleDeleteTask{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleDeleteOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleDeleteOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		seedResult, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", seedResult),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // singleUpsertTask is used to update the existing document in a collection
//
//	func (app *Config) singleUpsertTask(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleUpsertTask{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleUpsertOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleUpsertOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		seedResult, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", seedResult),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // singleReadTask is used read documents and verify from a collection.
//
//	func (app *Config) singleReadTask(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleReadTask{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleReadOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleReadOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		seedResult, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", seedResult),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // singleTouchTask is used to update expiry of documents in a collection
//
//	func (app *Config) singleTouchTask(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleTouchTask{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleTouchOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleTouchOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		seedResult, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", seedResult),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // singleReplaceTask is used replace content of document on a collection
//
//	func (app *Config) singleReplaceTask(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleReplaceTask{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleReplaceOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleReplaceOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		seedResult, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", seedResult),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // runQueryTask runs the query workload for a duration of time
//
//	func (app *Config) runQueryTask(w http.ResponseWriter, r *http.Request) {
//		task := &bulk_query_cb.QueryTask{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.QueryOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.QueryOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		seedResult, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", seedResult),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested query running",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // RetryExceptionTask runs the query workload for a duration of time
//
//	func (app *Config) RetryExceptionTask(w http.ResponseWriter, r *http.Request) {
//		task := &bulk_loading.RetryExceptions{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.RetryExceptionOperation)
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		seedResult, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", seedResult),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested query running",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // SingleSubDocInsert is used to insert user's input value in sub docs
//
//	func (app *Config) SingleSubDocInsert(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleSubDocInsert{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleSubDocInsertOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleSubDocInsertOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		resultSeed, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", resultSeed),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // SingleSubDocUpsert is used to update user's input value in sub docs
//
//	func (app *Config) SingleSubDocUpsert(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleSubDocUpsert{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleSubDocUpsertOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleSubDocUpsertOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		resultSeed, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", resultSeed),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // SingleSubDocReplace is used to replace user's input value in sub docs
//
//	func (app *Config) SingleSubDocReplace(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleSubDocReplace{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleSubDocReplaceOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleSubDocReplaceOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		resultSeed, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", resultSeed),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // SingleSubDocDelete is used delete user's sub document
//
//	func (app *Config) SingleSubDocDelete(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleSubDocDelete{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleSubDocDeleteOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleSubDocDeleteOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		resultSeed, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", resultSeed),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // SingleSubDocRead is used to read user's sub document
//
//	func (app *Config) SingleSubDocRead(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleSubDocRead{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleSubDocReadOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleSubDocReadOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		resultSeed, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", resultSeed),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//
// // SingleDocValidate is used to read user's sub document
//
//	func (app *Config) SingleDocValidate(w http.ResponseWriter, r *http.Request) {
//		task := &key_based_loading_cb.SingleValidate{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SingleDocValidateOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SingleDocValidateOperation, task)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		resultSeed, err_sirius := task.Config(req, false)
//		if err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		}
//		respPayload := util_sirius.TaskResponse{
//			Seed: fmt.Sprintf("%d", resultSeed),
//		}
//		resPayload := jsonResponse{
//			Error:   false,
//			Message: "Successfully started requested doc loading",
//			Data:    respPayload,
//		}
//		_ = app.writeJSON(w, http.StatusOK, resPayload)
//	}
//

// WarmUpBucket establish a connection to bucket
func (app *Config) WarmUpBucket(w http.ResponseWriter, r *http.Request) {

	task := &db_util.BucketWarmUpTask{}
	if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
		return
	}
	task.Operation = tasks.BucketWarmUpOperation

	if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.BucketWarmUpOperation)
	err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.BucketWarmUpOperation, task)
	if err_sirius != nil {
		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
		return
	}
	req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
	if err_sirius != nil {
		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
		return
	}
	resultSeed, err_sirius := task.Config(req, false)
	if err_sirius != nil {
		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
		return
	}
	if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
	}
	respPayload := util_sirius.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}
