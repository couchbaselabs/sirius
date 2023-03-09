package main

import (
	"crypto/sha256"
	"github.com/couchbaselabs/sirius/internal/communication"
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

func (app *Config) startExperiment(w http.ResponseWriter, r *http.Request) {

	reqPayload := &communication.Request{}

	if err := app.readJSON(w, r, reqPayload); err != nil {
		app.errorJSON(w, err, http.StatusUnprocessableEntity)
	}

	if err := reqPayload.Validate(); err != nil {
		app.errorJSON(w, err, http.StatusBadRequest)
		return
	}

	// start the experiment
	token := sha256.Sum256([]byte(reqPayload.Username + reqPayload.Password))
	seed := reqPayload.Seed

	// return a response with token and seed
	resData := communication.Response{
		Token: token,
		Seed:  seed,
	}

	resPayload := jsonResponse{
		Error:   false,
		Message: "Successfully started requested operation",
		Data:    resData,
	}

	app.writeJSON(w, http.StatusOK, resPayload)

}
