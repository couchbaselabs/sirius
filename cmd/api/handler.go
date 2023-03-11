package main

import (
	"crypto/sha256"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"net/http"
)

// testServer supports GET method.
// It returns Document Loader online reflecting availability of doc loading service.
func (app *Config) testServer(w http.ResponseWriter, r *http.Request) {
	payload := jsonResponse{
		Error:   false,
		Message: "Document Loader Online",
	}

	_ = app.writeJSON(w, http.StatusOK, payload)

}

func (app *Config) addTask(w http.ResponseWriter, r *http.Request) {

	// Decode and validate request
	reqPayload := &communication.Request{}
	if err := app.readJSON(w, r, reqPayload); err != nil {
		app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}
	if err := reqPayload.Validate(); err != nil {
		app.errorJSON(w, err, http.StatusBadRequest)
		return
	}

	// prepare the UserData Payload
	token := sha256.Sum256([]byte(reqPayload.Username + reqPayload.Password))
	seed := reqPayload.Seed

	// prepare and start tasks
	e := &tasks.Task{
		UserData: tasks.UserData{
			Token: token,
			Seed:  seed,
		},
		Req: reqPayload,
	}
	e.Handler()

	// prepare response for http request
	respPayload := communication.Response{
		Token: token,
		Seed:  seed[0],
	}

	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested operation",
		Data:    respPayload,
	}

	app.writeJSON(w, http.StatusOK, resPayload)

}
