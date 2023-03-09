package main

import (
	"encoding/json"
	"github.com/couchbaselabs/sirius/internal/requests"
	"io"
	"log"
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

func (app *Config) createDocument(w http.ResponseWriter, r *http.Request) {
	payload := jsonResponse{
		Error:   false,
		Message: "Document Creation Accepted",
	}

	req := &requests.CreateRequest{}

	// Decode request
	b, err := io.ReadAll(r.Body)
	if err != nil {
		app.errorJSON(w, err, http.StatusInternalServerError)
	}

	err = json.Unmarshal(b, req)
	if err != nil {
		app.errorJSON(w, err, http.StatusBadRequest)
	}

	// validate request

	log.Println(req)
	/* initiate a couchbase client */

	/* Service request by uploading document in the cluster using couchbase */

	/* return token for polling result */
	app.writeJSON(w, http.StatusOK, payload)
}
