package tasks

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// SaveResultIntoFile stores the task result on a file. It returns an error if saving fails.
func SaveResultIntoFile(result TaskResult) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, ResultPath, fmt.Sprintf("%d", result.UserData.Seed))
	// save the value to a file
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(&result); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return nil
}

// ReadResultFromFile reads the task result stored on a file. It returns the task result
// and possible error if task result file is missing, in processing or record file deleted.
func ReadResultFromFile(seed string, deleteRecord bool) (TaskResult, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return TaskResult{}, err
	}
	fileName := filepath.Join(cwd, ResultPath, seed)
	// preparing the result-logs to be added into the type TaskResult
	var result TaskResult
	file, err := os.Open(fileName)
	if err != nil {
		return TaskResult{}, fmt.Errorf("no such result found, reasons:[No such Task, In process, Record Deleted]")
	}
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&result); err != nil {
		return TaskResult{}, err
	}
	if err := file.Close(); err != nil {
		return TaskResult{}, err
	}
	// deleting the file after reading it to save disk space.
	if deleteRecord {
		if err := os.Remove(fileName); err != nil {
			log.Println("Manually clean " + fileName)
		}
	}
	return result, nil
}
