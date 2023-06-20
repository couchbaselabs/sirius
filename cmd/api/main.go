package main

import (
	_ "expvar" // Register the expvar handlers
	"fmt"
	"github.com/couchbaselabs/sirius/internal/server_requests"
	"github.com/couchbaselabs/sirius/internal/sirius_documentation"
	"github.com/couchbaselabs/sirius/internal/tasks_manager"
	"github.com/pkg/profile"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof" // Register the pprof handlers
	"os"
)

const webPort = "4000"
const TaskQueueSize = 100

type Config struct {
	taskManager    *tasks_manager.TaskManager
	serverRequests *server_requests.ServerRequests
}

type DebugServer struct {
	*http.Server
}

// NewDebugServer provides new debug http server
func NewDebugServer(address string) *DebugServer {
	return &DebugServer{
		&http.Server{
			Addr:    address,
			Handler: http.DefaultServeMux,
		},
	}
}

func main() {
	registerInterfaces()
	defer profile.Start(profile.MemProfile).Stop()
	//gocb.SetLogger(gocb.DefaultStdioLogger())

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
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			app.taskManager.StopTaskManager()
			log.Println(err)
			os.Exit(-1)
		}
	}()

	debugServer := NewDebugServer(fmt.Sprintf("%s:%d", "0.0.0.0", 6060))
	log.Println("Starting Sirius profiling service at 6060")

	log.Fatal(debugServer.ListenAndServe())

}
