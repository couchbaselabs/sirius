package main

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"log"
	"net/http"
)

const webPort = "80"
const TaskQueueSize = 30

type Config struct {
	taskManager *tasks.TaskManager
}

func main() {

	app := Config{
		taskManager: tasks.NewTasKManager(TaskQueueSize),
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
		log.Panic(err)
	}

	log.Println("Yes Yes Yes")
}
