package main

import (
	"github.com/go-chi/chi/v5"
	"net/http"

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
	mux.Post("/bulk-create", app.insertTask)
	mux.Post("/bulk-delete", app.deleteTask)
	mux.Post("/bulk-upsert", app.upsertTask)
	mux.Post("/validate", app.validateTask)
	mux.Post("/clear_data", app.clearRequestFromServer)
	mux.Post("/bulk-read", app.readTask)
	mux.Post("/single-create", app.singleInsertTask)
	mux.Post("/single-delete", app.singleDeleteTask)
	mux.Post("/single-upsert", app.singleUpsertTask)
	mux.Post("/single-read", app.singleReadTask)
	mux.Post("/single-touch", app.singleTouchTask)
	mux.Post("/single-replace", app.singleReplaceTask)
	mux.Post("/run-template-query", app.runQueryTask)
	mux.Post("/retry-exceptions", app.RetryExceptionTask)
	mux.Post("/sub-doc-bulk-insert", app.SubDocInsertTask)

	return mux
}
