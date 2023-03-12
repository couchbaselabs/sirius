package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/results"
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

func (app *Config) addTask(w http.ResponseWriter, r *http.Request) {

	// decode and validate http request
	reqPayload := &communication.TaskRequest{}
	if err := app.readJSON(w, r, reqPayload); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	if err := reqPayload.Validate(); err != nil {
		_ = app.errorJSON(w, err, http.StatusBadRequest)
		return
	}

	// prepare the user data payload for responding back
	seed := reqPayload.Seed

	// prepare the doc loading task
	task := &tasks.Task{
		UserData: tasks.UserData{
			Seed: seed,
		},
		Request: reqPayload,
	}
	// add the prepared task in the task manager
	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}

	// prepare response for http request
	respPayload := communication.Response{
		Seed: fmt.Sprintf("%d", seed[0]),
	}

	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}

	_ = app.writeJSON(w, http.StatusOK, resPayload)

}

func (app *Config) taskResult(w http.ResponseWriter, r *http.Request) {
	reqPayload := &communication.TaskResult{}
	if err := app.readJSON(w, r, reqPayload); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	result, err := results.ReadResultFromFile(reqPayload.Seed, reqPayload.DeleteRecord)

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
