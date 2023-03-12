package results

import (
	"encoding/gob"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"log"
	"os"
	"path/filepath"
)

// ReadResultFromFile reads the task result stored on a file. It returns the task result
// and possible error if task result file is missing, in processing or record file deleted.
func ReadResultFromFile(seed string, deleteRecord bool) (interface{}, error) {

	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	fileName := filepath.Join(cwd, tasks.ResultPath, seed)

	// preparing the result-logs to be added into the type TaskResult
	var result *tasks.TaskResult
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("no such result found, reasons:[No such Task, In process, Record Deleted]")
	}

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("unable to decode result : internal server error")
	}
	file.Close()

	// deleting the file after reading it to save disk space.
	if deleteRecord {
		if err := os.Remove(fileName); err != nil {
			log.Println("Manually clean " + fileName)
		}
	}

	return result, nil
}
