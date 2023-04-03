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

	mux.Get("/", app.testServer)
	mux.Post("/task-result", app.taskResult)
	mux.Post("/insert", app.insertTask)
	mux.Post("/delete", app.deleteTask)
	mux.Post("/upsert", app.upsertTask)
	mux.Post("/validate", app.validateTask)

	return mux
}
