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

// taskResult is responsible for handling user request to get status of the task
// scheduled.
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

// insertTask is used to bulk loading documents into buckets
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

//// deleteTask is used to delete documents in bulk of a bucket.
//func (app *Config) deleteTask(w http.ResponseWriter, r *http.Request) {
//	task := &bulk_loading.DeleteTask{}
//	if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	log.Print(task, tasks.DeleteOperation)
//	err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.DeleteOperation, task)
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
//
//// upsertTask is used to bulk loading updated documents into bucket.
//func (app *Config) upsertTask(w http.ResponseWriter, r *http.Request) {
//	task := &bulk_loading.UpsertTask{}
//	if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	log.Print(task, tasks.UpsertOperation)
//	err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.UpsertOperation, task)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	resultSeed, err_sirius := task.Config(req, false)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//	}
//	respPayload := util_sirius.TaskResponse{
//		Seed: fmt.Sprintf("%d", resultSeed),
//	}
//	resPayload := jsonResponse{
//		Error:   false,
//		Message: "Successfully started requested doc loading",
//		Data:    respPayload,
//	}
//	_ = app.writeJSON(w, http.StatusOK, resPayload)
//}
//
//// touchTask is used to update the ttl of documents in bulk.
//func (app *Config) touchTask(w http.ResponseWriter, r *http.Request) {
//	task := &bulk_loading.TouchTask{}
//	if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	log.Print(task, bulk_loading.TouchTask{})
//	err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.UpsertOperation, task)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	req, err_sirius := app.serverRequests.GetRequestOfIdentifier(task.IdentifierToken)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	resultSeed, err_sirius := task.Config(req, false)
//	if err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//		return
//	}
//	if err_sirius := app.taskManager.AddTask(task); err_sirius != nil {
//		_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//	}
//	respPayload := util_sirius.TaskResponse{
//		Seed: fmt.Sprintf("%d", resultSeed),
//	}
//	resPayload := jsonResponse{
//		Error:   false,
//		Message: "Successfully started requested doc loading",
//		Data:    respPayload,
//	}
//	_ = app.writeJSON(w, http.StatusOK, resPayload)
//}
//
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

// // readTask is validating the cluster's current state.
//
//	func (app *Config) readTask(w http.ResponseWriter, r *http.Request) {
//		task := &bulk_loading.ReadTask{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, "read")
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.ReadOperation, task)
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
// // SubDocInsertTask is used to load bulk sub documents into buckets
//
//	func (app *Config) SubDocInsertTask(w http.ResponseWriter, r *http.Request) {
//		task := &bulk_loading.SubDocInsert{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SubDocInsertOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocInsertOperation, task)
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
// // SubDocUpsertTask is used to bulk updating sub documents into buckets
//
//	func (app *Config) SubDocUpsertTask(w http.ResponseWriter, r *http.Request) {
//		task := &bulk_loading.SubDocUpsert{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SubDocUpsertOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocUpsertOperation, task)
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
// // SubDocDeleteTask is used to bulk updating sub documents into buckets
//
//	func (app *Config) SubDocDeleteTask(w http.ResponseWriter, r *http.Request) {
//		task := &bulk_loading.SubDocDelete{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SubDocDeleteOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocDeleteOperation, task)
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
// // SubDocReadTask is used to bulk updating sub documents into buckets
//
//	func (app *Config) SubDocReadTask(w http.ResponseWriter, r *http.Request) {
//		task := &bulk_loading.SubDocRead{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SubDocReadOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocReadOperation, task)
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
// // SubDocReplaceTask is used to bulk updating sub documents into buckets
//
//	func (app *Config) SubDocReplaceTask(w http.ResponseWriter, r *http.Request) {
//		task := &bulk_loading.SubDocReplace{}
//		if err_sirius := app.readJSON(w, r, task); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		if err_sirius := checkIdentifierToken(task.IdentifierToken); err_sirius != nil {
//			_ = app.errorJSON(w, err_sirius, http.StatusUnprocessableEntity)
//			return
//		}
//		log.Print(task, tasks.SubDocReplaceOperation)
//		err_sirius := app.serverRequests.AddTask(task.IdentifierToken, tasks.SubDocReplaceOperation, task)
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
