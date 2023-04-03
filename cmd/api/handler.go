package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/tasks"
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
	reqPayload := &tasks.RequestResult{}
	if err := app.readJSON(w, r, reqPayload); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	result, err := task_result.ReadResultFromFile(reqPayload.Seed, reqPayload.DeleteRecord)
	if err != nil {
		_ = app.errorJSON(w, err, http.StatusBadRequest)
		return
	}
	respPayload := jsonResponse{
		Error:   false,
		Message: "Successfully retrieved result-logs",
		Data:    result,
	}

	_ = app.writeJSON(w, http.StatusOK, respPayload)
}

// insertTask is used to bulk loading documents into buckets
func (app *Config) insertTask(w http.ResponseWriter, r *http.Request) {
	// decode and validate http request

	request := &tasks.InsertTask{}
	if err := app.readJSON(w, r, request); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seed, err := request.Config()

	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}

	if err := app.taskManager.AddTask(request); err != nil {
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

// deleteTask is used to bulk loading documents into buckets
func (app *Config) deleteTask(w http.ResponseWriter, r *http.Request) {
	// decode and validate http request

	request := &tasks.DeleteTask{}
	if err := app.readJSON(w, r, request); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seed, err := request.Config()

	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}

	if err := app.taskManager.AddTask(request); err != nil {
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

// upsertTask is used to bulk loading documents into buckets
func (app *Config) upsertTask(w http.ResponseWriter, r *http.Request) {
	// decode and validate http request

	request := &tasks.UpsertTask{}
	if err := app.readJSON(w, r, request); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seed, err := request.Config()

	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}

	if err := app.taskManager.AddTask(request); err != nil {
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

// validateTask is used to bulk loading documents into buckets
func (app *Config) validateTask(w http.ResponseWriter, r *http.Request) {
	// decode and validate http request

	request := &tasks.ValidateTask{}
	if err := app.readJSON(w, r, request); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	seed, err := request.Config()

	if err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}

	if err := app.taskManager.AddTask(request); err != nil {
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
