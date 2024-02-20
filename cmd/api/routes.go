package main

import (
	"github.com/couchbaselabs/sirius/internal/tasks"
	"github.com/go-chi/chi/v5"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

// routes returns a http Handler which supports multiple http request.
func (app *Config) routes() http.Handler {

	mux := chi.NewRouter()

	// who is allowed to use
	mux.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"https://*", "http://*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	mux.Use(middleware.Heartbeat("/ping"))

	mux.Get("/check-online", app.testServer)
	mux.Post("/result", app.taskResult)
	mux.Post("/clear_data", app.clearRequestFromServer)
	//mux.Post("/validate", app.validateTask)
	mux.Post("/retry-exceptions", app.RetryExceptionTask)
	mux.Post("/create", app.insertTask)
	mux.Post("/bulk-create", app.bulkInsertTask)
	mux.Post("/delete", app.deleteTask)
	mux.Post("/bulk-delete", app.bulkDeleteTask)
	mux.Post("/upsert", app.upsertTask)
	mux.Post("/bulk-upsert", app.bulkUpsertTask)
	mux.Post("/touch", app.touchTask)
	mux.Post("/bulk-touch", app.bulkTouchTask)
	mux.Post("/read", app.readTask)
	mux.Post("/bulk-read", app.bulkReadTask)
	mux.Post("/sub-doc-insert", app.SubDocInsertTask)
	mux.Post("/sub-doc-upsert", app.SubDocUpsertTask)
	mux.Post("/sub-doc-delete", app.SubDocDeleteTask)
	mux.Post("/sub-doc-read", app.SubDocReadTask)
	mux.Post("/sub-doc-replace", app.SubDocReplaceTask)
	//mux.Post("/single-create", app.singleInsertTask)
	//mux.Post("/single-delete", app.singleDeleteTask)
	//mux.Post("/single-upsert", app.singleUpsertTask)
	//mux.Post("/single-read", app.singleReadTask)
	//mux.Post("/single-touch", app.singleTouchTask)
	//mux.Post("/single-replace", app.singleReplaceTask)
	//mux.Post("/run-template-query", app.runQueryTask)
	//mux.Post("/single-sub-doc-insert", app.SingleSubDocInsert)
	//mux.Post("/single-sub-doc-upsert", app.SingleSubDocUpsert)
	//mux.Post("/single-sub-doc-replace", app.SingleSubDocReplace)
	//mux.Post("/single-sub-doc-delete", app.SingleSubDocDelete)
	//mux.Post("/single-sub-doc-read", app.SingleSubDocRead)
	//mux.Post("/single-doc-validate", app.SingleDocValidate)
	mux.Post("/warmup-bucket", app.WarmUpBucket)

	return mux
}

func getFileName() string {
	cw, err := os.Getwd()
	if err != nil {
		log.Fatalf(err.Error())
	}
	return filepath.Join(cw, tasks.RequestPath, "sirius_logs")
}
