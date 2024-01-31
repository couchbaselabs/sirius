package main

import (
	"encoding/gob"
	"encoding/json"
	"github.com/couchbaselabs/sirius/internal/server_requests"
	"github.com/couchbaselabs/sirius/internal/sirius_documentation"
	"github.com/couchbaselabs/sirius/internal/task_meta_data"
	"github.com/couchbaselabs/sirius/internal/task_result"
	"github.com/couchbaselabs/sirius/internal/task_state"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/couchbaselabs/sirius/internal/tasks/bulk_loading_cb"
	"github.com/couchbaselabs/sirius/internal/tasks/bulk_query_cb"
	"github.com/couchbaselabs/sirius/internal/tasks/key_based_loading_cb"
	"github.com/couchbaselabs/sirius/internal/tasks/util_cb"
	"github.com/couchbaselabs/sirius/internal/tasks/util_sirius"
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
	decoder.DisallowUnknownFields()
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
	gob.Register(&[]interface{}{})
	gob.Register(&map[string]interface{}{})
	gob.Register(&map[string]any{})
	gob.Register(&tasks.Request{})
	gob.Register(&task_meta_data.MetaData{})
	gob.Register(&task_meta_data.DocumentsMetaData{})
	gob.Register(&template.Person{})
	gob.Register(&template.Hotel{})
	gob.Register(&template.SmallTemplate{})
	gob.Register(&server_requests.ServerRequests{})
	gob.Register(&bulk_loading_cb.InsertTask{})
	gob.Register(&bulk_loading_cb.UpsertTask{})
	gob.Register(&util_sirius.TaskResult{})
	gob.Register(&bulk_loading_cb.DeleteTask{})
	gob.Register(&bulk_loading_cb.TouchTask{})
	gob.Register(&bulk_loading_cb.ValidateTask{})
	gob.Register(&task_result.TaskResult{})
	gob.Register(&task_state.TaskState{})
	gob.Register(&bulk_loading_cb.ReadTask{})
	gob.Register(&key_based_loading_cb.SingleInsertTask{})
	gob.Register(&key_based_loading_cb.SingleDeleteTask{})
	gob.Register(&key_based_loading_cb.SingleUpsertTask{})
	gob.Register(&key_based_loading_cb.SingleReadTask{})
	gob.Register(&key_based_loading_cb.SingleTouchTask{})
	gob.Register(&key_based_loading_cb.SingleReplaceTask{})
	gob.Register(&bulk_query_cb.QueryTask{})
	gob.Register(&task_meta_data.MetaData{})
	gob.Register(&task_meta_data.CollectionMetaData{})
	gob.Register(&bulk_loading_cb.RetryExceptions{})
	gob.Register(&bulk_loading_cb.SubDocInsert{})
	gob.Register(&bulk_loading_cb.SubDocUpsert{})
	gob.Register(&bulk_loading_cb.SubDocDelete{})
	gob.Register(&bulk_loading_cb.SubDocRead{})
	gob.Register(&bulk_loading_cb.SubDocReplace{})
	gob.Register(&key_based_loading_cb.SingleSubDocInsert{})
	gob.Register(&key_based_loading_cb.SingleSubDocUpsert{})
	gob.Register(&key_based_loading_cb.SingleSubDocReplace{})
	gob.Register(&key_based_loading_cb.SingleSubDocDelete{})
	gob.Register(&key_based_loading_cb.SingleSubDocRead{})
	gob.Register(&key_based_loading_cb.SingleValidate{})
	gob.Register(&util_cb.BucketWarmUpTask{})

	r := sirius_documentation.Register{}
	for _, i := range r.HelperStruct() {
		gob.Register(i)
	}

}
