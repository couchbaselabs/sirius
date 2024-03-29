package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/server_requests"
	"github.com/couchbaselabs/sirius/internal/sirius_documentation"
	"github.com/couchbaselabs/sirius/internal/tasks_manager"
	"io"
	"log"
	"net/http"
	"os"
)

var webPort = ""

const DefaultWebPort = "4000"
const TaskQueueSize = 100

type Config struct {
	taskManager    *tasks_manager.TaskManager
	serverRequests *server_requests.ServerRequests
}

func main() {
	configureAppPort()
	registerInterfaces()
	logFile, err := os.OpenFile(getFileName(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	app := Config{
		taskManager:    tasks_manager.NewTasKManager(TaskQueueSize),
		serverRequests: server_requests.NewServerRequests(),
	}
	go sirius_documentation.Generate()

	//define the server
	log.Printf("Starting Document Loading Service at port %s\n", webPort)
	srv := http.Server{
		Addr:    fmt.Sprintf(":%s", webPort),
		Handler: app.routes(),
	}
	// start the server
	err = srv.ListenAndServe()
	if err != nil {
		app.taskManager.StopTaskManager()
		log.Println(err)
		os.Exit(-1)
	}
}
