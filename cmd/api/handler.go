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
	log.Print(task, "insert")
	err, index := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.InsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seed, err := task.Config(req, req.Seed, req.SeedEnd, index, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seed),
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
	log.Print(task, "delete")
	err, index := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.DeleteOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seed, err := task.Config(req, req.Seed, req.SeedEnd, index, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seed),
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
	log.Print(task, "upsert")
	err, index := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.UpsertOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seed, err := task.Config(req, req.Seed, req.SeedEnd, index, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seed),
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
	log.Print(task, "validate")
	err, index := app.serverRequests.AddTask(task.BuildIdentifier(), tasks.ValidateOperation, task)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	req, err := app.serverRequests.GetRequestOfIdentifier(task.BuildIdentifier())
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seed, err := task.Config(req, req.Seed, req.SeedEnd, index, false)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	respPayload := tasks.TaskResponse{
		Seed: fmt.Sprintf("%d", seed),
	}
	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}
	_ = app.writeJSON(w, http.StatusOK, resPayload)
}
