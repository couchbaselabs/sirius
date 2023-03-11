package tasks

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/communication"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"log"
	"sync"
	"time"
)

type UserData struct {
	Token [32]byte `json:"token"`
	Seed  []int64  `json:"seed"`
}

type Task struct {
	UserData    UserData
	Req         *communication.Request
	TaskResults []interface{}
	TaskErrors  []error
	ClientError error
}

func (e *Task) Handler() {

	go e.initiateLoading()
}

func (e *Task) initiateLoading() {

	var connectionString string
	switch e.Req.Service {
	case communication.OnPremService:
		connectionString = "couchbase://" + e.Req.Host
	case communication.CapellaService:
		connectionString = "couchbases://" + e.Req.Host
	}

	cluster, err := gocb.Connect(connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: e.Req.Username,
			Password: e.Req.Password,
		},
	})
	if err != nil {
		e.ClientError = err
		log.Println(err)
		return
	} else {
		log.Println("Connected")
	}

	// open bucket
	bucket := cluster.Bucket(e.Req.Bucket)
	log.Println(bucket.Name(), connectionString, e.Req.Username, e.Req.Password, e.Req.Scope, e.Req.Collection)

	err = bucket.WaitUntilReady(5*time.Second, nil)

	if err != nil {
		e.ClientError = err
		log.Println(err)
		return
	}

	log.Println(bucket.Name())
	col := bucket.Scope(e.Req.Scope).Collection(e.Req.Collection)
	log.Println(col.Name())
	switch e.Req.Operation {
	case communication.InsertOperation:
		e.insert(col)
	case communication.UpsertOperation:
		log.Println("upsert")
	case communication.DeleteOperation:
		log.Println("delete")
	case communication.GetOperation:
		log.Println("get")
	}

	// Close connection and cluster.
	err = cluster.Close(nil)
	e.ClientError = err

}

func (e *Task) insert(col *gocb.Collection) {
	gen := docgenerator.Generator{
		Itr:           0,
		End:           e.Req.Iteration,
		BatchSize:     e.Req.BatchSize,
		DocType:       e.Req.DocType,
		KeySize:       e.Req.KeySize,
		DocSize:       0,
		RandomDocSize: false,
		RandomKeySize: false,
		Seed:          e.Req.Seed,
		Template:      nil,
	}

	for i := gen.Itr; i < gen.End; i++ {
		keys, personsTemplate := gen.Next(gen.Seed[i])
		counter := 0
		wg := sync.WaitGroup{}
		wg.Add(len(keys))
		for index, key := range keys {
			go func(key string, doc interface{}) {
				defer wg.Done()
				result, err := col.Insert(key, doc, nil)
				counter += 1
				if err != nil {
					e.TaskErrors = append(e.TaskErrors, err)
				}
				e.TaskResults = append(e.TaskResults, result)
			}(key, *personsTemplate[index])
		}
		wg.Wait()

		log.Println(i, e.Req.Bucket, e.Req.Scope, e.Req.Collection, counter)
	}
}
