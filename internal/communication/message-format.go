package communication

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"time"
)

type DocumentOperation string

const (
	InsertOperation DocumentOperation = "insert"
	UpsertOperation DocumentOperation = "upsert"
	DeleteOperation DocumentOperation = "delete"
	GetOperation    DocumentOperation = "get"
)

type ServiceType string

const (
	OnPremService  ServiceType = "onPrem"
	CapellaService ServiceType = "capella"
)

const DefaultScope string = "_default"
const DefaultCollection string = "_default"

// TaskRequest represents a structure of doc loading task.
type TaskRequest struct {
	Service         ServiceType               `json:"service"`
	Username        string                    `json:"username"`
	Password        string                    `json:"password"`
	Host            string                    `json:"host"`
	Bucket          string                    `json:"bucket"`
	Scope           string                    `json:"scope,omitempty"`
	Collection      string                    `json:"collection,omitempty"`
	Iteration       int64                     `json:"iteration,omitempty"`
	BatchSize       int64                     `json:"batchSize,omitempty"`
	DocType         docgenerator.DocumentType `json:"docType,omitempty"`
	KeySize         int                       `json:"keySize,omitempty"`
	DocSize         int                       `json:"docSize,omitempty"`
	RandomDocSize   bool                      `json:"randomDocSize,omitempty"`
	RandomKeySize   bool                      `json:"randomKeySize,omitempty"`
	Expiry          time.Duration             `json:"expiry,omitempty"`
	PersistTo       uint                      `json:"PersistTo,omitempty"`
	ReplicateTo     uint                      `json:"replicateTo,omitempty"`
	DurabilityLevel gocb.DurabilityLevel      `json:"durabilityLevel,omitempty"`
	Transcoder      gocb.Transcoder           `json:"transcoder,omitempty"`
	Timeout         time.Duration             `json:"timeout,omitempty"`
	RetryStrategy   gocb.RetryStrategy        `json:"retryStrategy,omitempty"`
	Operation       DocumentOperation         `json:"operation"`
	Seed            int64                     `json:"seed,omitempty"`
}

// Validate cross checks the validity of an incoming request to schedule a task.
func (r *TaskRequest) Validate() error {

	if r.Service == "" {
		r.Service = OnPremService
	}
	if r.Username == "" || r.Password == "" {
		return fmt.Errorf("cluster's credentials are missing ")
	}
	if r.Host == "" {
		return fmt.Errorf("hostname of the cluster is missing")
	}
	if r.Bucket == "" {
		return fmt.Errorf("bucker is missing")
	}
	if r.Scope == "" {
		r.Scope = DefaultScope
	}
	if r.Collection == "" {
		r.Collection = DefaultCollection
	}
	if r.Iteration == 0 {
		r.Iteration = 1
	}
	if r.BatchSize == 0 {
		r.BatchSize = 1
	}
	if r.DocType == "" {
		r.DocType = docgenerator.JsonDocument
	}
	if r.KeySize == 0 || r.KeySize > docgenerator.DefaultKeySize {
		r.KeySize = docgenerator.DefaultKeySize
	}
	if r.DocSize == 0 {
		r.DocSize = docgenerator.DefaultDocSize
	}
	switch r.Operation {
	case InsertOperation, DeleteOperation, UpsertOperation, GetOperation:
	default:
		return fmt.Errorf("incorrect operation type")
	}
	// this sleep ensures that seed generated is always different.
	time.Sleep(1 * time.Microsecond)
	r.Seed = time.Now().UnixNano()
	return nil
}

// TaskResult represents a request structure for retrieving result of the task.
type TaskResult struct {
	Seed         string `json:"seed"`
	DeleteRecord bool   `json:"deleteRecord"`
}

// Response represents a response structure which is returned to user upon scheduling a task.
type Response struct {
	Seed string `json:"seed"`
}
