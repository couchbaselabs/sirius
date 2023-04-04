package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/generate"
	"github.com/couchbaselabs/sirius/internal/tasks-manager"
	"log"
	"net/http"
	"os"
)

const webPort = "80"
const TaskQueueSize = 100

type Config struct {
	taskManager *tasks_manager.TaskManager
}

func main() {
	go generate.Generate()

	app := Config{
		taskManager: tasks_manager.NewTasKManager(TaskQueueSize),
	}

	//start the Task Manager
	app.taskManager.StartTaskManager()

	//define the server
	log.Printf("Starting Document Loading Service at port %s\n", webPort)
	srv := http.Server{
		Addr:    fmt.Sprintf(":%s", webPort),
		Handler: app.routes(),
	}
	// start the server
	err := srv.ListenAndServe()
	if err != nil {
		app.taskManager.StopTaskManager()
		log.Println(err)
		os.Exit(-1)
	}
}
