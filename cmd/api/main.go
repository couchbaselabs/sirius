package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"log"
	"net/http"
)

const webPort = "80"

type Config struct {
	experiments map[*communication.Response]*tasks.Task
}

func main() {
	app := Config{}

	log.Printf("Starting Document Loading Service at port %s\n", webPort)

	//define the server
	srv := http.Server{
		Addr:    fmt.Sprintf(":%s", webPort),
		Handler: app.routes(),
	}
	// start the server
	err := srv.ListenAndServe()
	if err != nil {
		log.Panic(err)
	}
}
