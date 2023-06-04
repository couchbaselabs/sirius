package main

import (
	"encoding/gob"
	"encoding/json"
	"github.com/couchbaselabs/sirius/internal/generate"
	"github.com/couchbaselabs/sirius/internal/server_requests"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/template"
	"net/http"
)

type jsonResponse struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (app *Config) readJSON(w http.ResponseWriter, r *http.Request, data interface{}) error {
	maxBytes := 10485760
	r.Body = http.MaxBytesReader(w, r.Body, int64(maxBytes))

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(data); err != nil {
		return err
	}

	return nil
}

func (app *Config) writeJSON(w http.ResponseWriter, status int, data any, headers ...http.Header) error {
	out, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if len(headers) > 0 {
		for key, value := range headers[0] {
			w.Header()[key] = value
		}
	}
	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(status)

	_, err = w.Write(out)
	if err != nil {
		return err
	}

	return nil
}

func (app *Config) errorJSON(w http.ResponseWriter, err error, status ...int) error {
	statusCode := http.StatusBadRequest
	if len(status) > 0 {
		statusCode = status[0]
	}

	var payload jsonResponse
	payload.Error = true
	payload.Message = err.Error()
	return app.writeJSON(w, statusCode, payload)
}

func registerInterfaces() {
	gob.Register(&tasks.Request{})
	gob.Register(&template.Person{})
	gob.Register(&template.SmallTemplate{})
	gob.Register(&server_requests.ServerRequests{})
	gob.Register(&tasks.InsertTask{})
	gob.Register(&tasks.FastInsertTask{})
	gob.Register(&tasks.UpsertTask{})
	gob.Register(&tasks.TaskResult{})
	gob.Register(&tasks.DeleteTask{})
	gob.Register(&tasks.ValidateTask{})
	gob.Register(&task_result.TaskResult{})
	gob.Register(&task_state.TaskState{})
	gob.Register(&tasks.ReadTask{})

	r := generate.Register{}
	for _, i := range r.HelperStruct() {
		gob.Register(i)
	}

}
