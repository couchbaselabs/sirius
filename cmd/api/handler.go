package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/communication"
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

// addTask is responsible for decoding user request and building a doc loading task which is scheduled
// by task manager.
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
	log.Println(reqPayload)
	seed := reqPayload.Seed

	task := &tasks.Task{
		UserData: tasks.UserData{
			Seed: seed,
		},
		Result: tasks.TaskResult{
			UserData: tasks.UserData{
				Seed: seed,
			},
			Operation: reqPayload.Operation,
			Success:   0,
			Failure:   0,
			Error:     make(map[string][]string),
		},
		TaskState: tasks.TaskState{
			Host:         reqPayload.Host,
			BUCKET:       reqPayload.Bucket,
			SCOPE:        reqPayload.Scope,
			Collection:   reqPayload.Collection,
			DocumentSize: reqPayload.DocSize,
			Seed:         seed,
			SeedEnd:      seed,
			KeyPrefix:    reqPayload.KeyPrefix,
			KeySuffix:    reqPayload.KeySuffix,
		},
		Request: reqPayload,
	}

	if taskState, err := task.ReadTaskStateFromFile(); err == nil {
		task.TaskState = taskState
		task.Result.UserData.Seed = taskState.Seed
	}

	if err := app.taskManager.AddTask(task); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}

	respPayload := communication.TaskResponse{
		Seed: fmt.Sprintf("%d", task.Result.UserData.Seed),
	}

	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested doc loading",
		Data:    respPayload,
	}

	_ = app.writeJSON(w, http.StatusOK, resPayload)
}

// taskResult is responsible for handling user request to get status of the task
// scheduled.
func (app *Config) taskResult(w http.ResponseWriter, r *http.Request) {
	reqPayload := &communication.TaskResult{}
	if err := app.readJSON(w, r, reqPayload); err != nil {
		_ = app.errorJSON(w, err, http.StatusUnprocessableEntity)
		return
	}
	result, err := tasks.ReadResultFromFile(reqPayload.Seed, reqPayload.DeleteRecord)

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
