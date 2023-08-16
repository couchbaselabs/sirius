package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
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
	reqPayload := &tasks.TaskResult{}
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
	task := &tasks.InsertTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.InsertOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.InsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
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
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// deleteTask is used to delete documents in bulk of a bucket.
func (app *Config) deleteTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.DeleteTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.DeleteOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.DeleteOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// upsertTask is used to bulk loading updated documents into bucket.
func (app *Config) upsertTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.UpsertTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.UpsertOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.UpsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
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
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// validateTask is validating the cluster's current state.
func (app *Config) validateTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.ValidateTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.ValidateOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.ValidateOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// clearRequestFromServer clears a test's request from the server.
func (app *Config) clearRequestFromServer(w http.ResponseWriter, r *http.Request) {
	task := &tasks.ClearTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, "clear")
	if err := app.serverRequests.ClearIdentifierAndRequest(task.BuildIdentifier()); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully cleared the meta-data",
		Data:    task.BuildIdentifier(),
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// readTask is validating the cluster's current state.
func (app *Config) readTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.ReadTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, "read")
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.ReadOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// singleInsertTask is used to insert document in a collection
func (app *Config) singleInsertTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SingleInsertTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SingleInsertOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SingleInsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// singleDeleteTask is used to delete documents on a collection
func (app *Config) singleDeleteTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SingleDeleteTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SingleDeleteOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SingleDeleteOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// singleUpsertTask is used to update the existing document in a collection
func (app *Config) singleUpsertTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SingleUpsertTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SingleUpsertOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SingleUpsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// singleReadTask is used read documents and verify from a collection.
func (app *Config) singleReadTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SingleReadTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SingleReadOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SingleReadOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// singleTouchTask is used to update expiry of documents in a collection
func (app *Config) singleTouchTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SingleTouchTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SingleTouchOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SingleTouchOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// singleReplaceTask is used replace content of document on a collection
func (app *Config) singleReplaceTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SingleReplaceTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SingleReplaceOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SingleReplaceOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// runQueryTask runs the query workload for a duration of time
func (app *Config) runQueryTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.QueryTask{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.QueryOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.QueryOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested query running",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// RetryExceptionTask runs the query workload for a duration of time
func (app *Config) RetryExceptionTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.RetryExceptions{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.RetryExceptionOperation)
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seedResult, err := task.Config(req, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seedResult),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested query running",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// SubDocInsertTask is used to load bulk sub documents into buckets
func (app *Config) SubDocInsertTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SubDocInsert{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SubDocInsertOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SubDocInsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
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
	respPayload := tasks.TaskResponse{
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
	task := &tasks.SubDocUpsert{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SubDocUpsertOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SubDocUpsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
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
	respPayload := tasks.TaskResponse{
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
	task := &tasks.SubDocDelete{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SubDocDeleteOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SubDocDeleteOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
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
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// SubDocReadTask is used to bulk updating sub documents into buckets
func (app *Config) SubDocReadTask(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SubDocRead{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SubDocReadOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SubDocReadOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
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
	respPayload := tasks.TaskResponse{
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
	task := &tasks.SubDocReplace{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SubDocReplaceOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SubDocReplaceOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
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
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// SingleSubDocInsert is used to bulk updating sub documents into buckets
func (app *Config) SingleSubDocInsert(w http.ResponseWriter, r *http.Request) {
	task := &tasks.SingleSubDocInsert{}
	if err := app.readJSON(w, r, task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	log.Print(task, tasks.SingleSubDocInsertOperation)
	err := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.SingleSubDocInsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
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
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", resultSeed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}
