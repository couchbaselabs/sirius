package server_requests

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/sirius/internal/tasks"
	"log"
	os "os"
	"path/filepath"
	"sync"
	"time"
)

const ServerRequestsPath = "./internal/server_requests/server_requests_logs"
const ServerRequestFileName = "server_requests"
const SnapShortTime = 10

type ServerRequests struct {
	RequestLookup sync.Map            `json:"-"`
	Lock          sync.Mutex          `json:"-"`
	Identifiers   map[string]struct{} `json:"identifiers"`
}

func NewServerRequests() *ServerRequests {
	sr, err := ReadServerRequestsFromFile()
	if err == nil {
		for identifier, _ := range sr.Identifiers {
			r, err := tasks.ReadRequestFromFile(identifier)
			if r != nil && err == nil {
				_ = sr.add(identifier, r, false)
				for index, t := range r.Tasks {
					if t.Task.CheckIfPending() {
						if _, err := t.Task.Config(r, r.Seed, r.SeedEnd, index, true); err == nil {
							go t.Task.Do()
						}
					}
				}
			} else {
				_ = sr.remove(identifier, true)
				if r != nil {
					if err := r.RemoveRequestFromFile(identifier); err != nil {
						log.Print(err.Error())
					}
				}
			}
		}
	} else {
		sr.Identifiers = make(map[string]struct{})
	}
	go sr.saveRequestsIntoFilePeriodically()
	return sr
}

func (sr *ServerRequests) saveRequestsIntoFilePeriodically() {
	d := time.NewTicker(SnapShortTime * time.Second)
	for {
		select {
		case _ = <-d.C:
			sr.RequestLookup.Range(func(key, value any) bool {
				r, ok := value.(*tasks.Request)
				if ok {
					if err := r.SaveRequestIntoFile(); err != nil {
						log.Println(err.Error())
					}
				}
				return true
			})
		}
	}
}

func ReadServerRequestsFromFile() (*ServerRequests, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return &ServerRequests{}, err
	}
	fileName := filepath.Join(cwd, ServerRequestsPath, ServerRequestFileName)
	r := &ServerRequests{}
	content, err := os.ReadFile(fileName)
	if err != nil {
		return &ServerRequests{}, err
	}
	if err := json.Unmarshal(content, r); err != nil {
		return &ServerRequests{}, err
	}
	return r, nil
}

func (sr *ServerRequests) deleteIdentifiersToFile(identifier string) error {
	defer sr.Lock.Unlock()
	sr.Lock.Lock()
	delete(sr.Identifiers, identifier)
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, ServerRequestsPath, ServerRequestFileName)
	content, err := json.Marshal(sr)
	if err != nil {
		return err
	}
	err = os.WriteFile(fileName, content, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (sr *ServerRequests) saveIdentifiersToFile(identifier string) error {
	defer sr.Lock.Unlock()
	sr.Lock.Lock()
	sr.Identifiers[identifier] = struct{}{}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(cwd, ServerRequestsPath, ServerRequestFileName)
	content, err := json.Marshal(sr)
	if err != nil {
		return err
	}
	err = os.WriteFile(fileName, content, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (sr *ServerRequests) remove(identifier string, saveToFile bool) error {
	sr.RequestLookup.Delete(identifier)
	if saveToFile {
		return sr.deleteIdentifiersToFile(identifier)
	}
	return nil
}

func (sr *ServerRequests) add(identifier string, request *tasks.Request, saveToFile bool) error {
	_, ok := sr.RequestLookup.Load(identifier)
	if !ok {
		sr.RequestLookup.Store(identifier, request)
		if saveToFile {
			return sr.saveIdentifiersToFile(identifier)
		}
	}
	return nil
}

func (sr *ServerRequests) checkIfExists(identifier string) bool {
	_, ok := sr.RequestLookup.Load(identifier)
	return ok
}

func (sr *ServerRequests) GetRequestOfIdentifier(identifier string) (*tasks.Request, error) {

	if sr.checkIfExists(identifier) {
		r, _ := sr.RequestLookup.Load(identifier)
		req, ok := r.(*tasks.Request)
		if ok && req != nil {
			return req, nil
		}
	} else {
		var fileSaveCheck error
		newRequest := tasks.NewRequest(identifier)

		sr.Lock.Lock()
		requestFromFile, err := tasks.ReadRequestFromFile(identifier)
		sr.Lock.Unlock()

		if sr.checkIfExists(identifier) == false {
			if requestFromFile != nil && err == nil {
				fileSaveCheck = sr.add(identifier, requestFromFile, true)
			} else {
				if requestFromFile != nil {
					if err := requestFromFile.RemoveRequestFromFile(identifier); err != nil {
						log.Println(err.Error())
					}
				}
				fileSaveCheck = sr.add(identifier, newRequest, true)
			}
		}

		if requestFromFile != nil && err == nil {
			return requestFromFile, fileSaveCheck
		} else {
			return newRequest, fileSaveCheck
		}
	}
	return nil, fmt.Errorf("unknown identifer or request")
}

func (sr *ServerRequests) AddTask(identifier string, o string, t tasks.Task) (error, int) {
	r, err := sr.GetRequestOfIdentifier(identifier)
	if r == nil {
		return fmt.Errorf("unable to create request"), -1
	}
	if err != nil {
		return err, -1
	}
	return r.AddTask(o, t)
}
