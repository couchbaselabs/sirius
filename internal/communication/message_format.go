package communication

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/template"
	"time"
)

type DocumentOperation string

const (
	InsertOperation   DocumentOperation = "insert"
	UpsertOperation   DocumentOperation = "upsert"
	DeleteOperation   DocumentOperation = "delete"
	GetOperation      DocumentOperation = "get"
	ValidateOperation DocumentOperation = "validate"
)

type ServiceType string

const (
	OnPremService  ServiceType = "onPrem"
	CapellaService ServiceType = "capella"
)

const (
	DurabilityLevelMajority                   string = "MAJORITY"
	DurabilityLevelMajorityAndPersistToActive string = "MAJORITY_AND_PERSIST_TO_ACTIVE"
	DurabilityLevelPersistToMajority          string = "PERSIST_TO_MAJORITY"
	DefaultScope                              string = "_default"
	DefaultCollection                         string = "_default"
)

// TaskRequest represents a structure of doc loading task.
type TaskRequest struct {
	Service                  ServiceType               `json:"service"`
	Username                 string                    `json:"username"`
	Password                 string                    `json:"password"`
	Host                     string                    `json:"host"`
	Bucket                   string                    `json:"bucket"`
	Scope                    string                    `json:"scope,omitempty"`
	Collection               string                    `json:"collection,omitempty"`
	Iteration                int64                     `json:"iteration,omitempty"`
	BatchSize                int64                     `json:"batchSize,omitempty"`
	DocType                  docgenerator.DocumentType `json:"docType,omitempty"`
	KeySize                  int                       `json:"keySize,omitempty"`
	DocSize                  int64                     `json:"docSize,omitempty"`
	RandomDocSize            bool                      `json:"randomDocSize,omitempty"`
	RandomKeySize            bool                      `json:"randomKeySize,omitempty"`
	Expiry                   time.Duration             `json:"expiry,omitempty"`
	PersistTo                uint                      `json:"PersistTo,omitempty"`
	ReplicateTo              uint                      `json:"replicateTo,omitempty"`
	Durability               string                    `json:"durability,omitempty"`
	Transcoder               gocb.Transcoder           `json:"transcoder,omitempty"`
	Timeout                  time.Duration             `json:"timeout,omitempty"`
	RetryStrategy            gocb.RetryStrategy        `json:"retryStrategy,omitempty"`
	Operation                DocumentOperation         `json:"operation"`
	SeedToken                string                    `json:"seed"`
	Start                    int64                     `json:"start,omitempty"`
	End                      int64                     `json:"end,omitempty"`
	TemplateToken            string                    `json:"template"`
	KeyPrefix                string                    `json:"keyPrefix,omitempty"`
	KeySuffix                string                    `json:"keySuffix,omitempty"`
	FieldsToChange           []string                  `json:"fieldsToChange,omitempty"`
	ReadYourOwnWrite         bool                      `json:"readYourOwnWrite,omitempty"`
	ReadYourOwnWriteAttempts int                       `json:"readYourOwnWriteAttempts,omitempty"`
	Template                 template.Template
	Seed                     int64
	DurabilityLevel          gocb.DurabilityLevel
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
	case InsertOperation:
		time.Sleep(1 * time.Microsecond) // this sleep ensures that seed generated is always different.
		r.Seed = time.Now().UnixNano()
	case DeleteOperation, UpsertOperation, GetOperation, ValidateOperation:
	default:
		return fmt.Errorf("invalid Operation")
	}

	r.Template, _ = template.InitialiseTemplate(r.TemplateToken)

	switch r.Durability {
	case DurabilityLevelMajority:
		r.DurabilityLevel = gocb.DurabilityLevelMajority
	case DurabilityLevelMajorityAndPersistToActive:
		r.DurabilityLevel = gocb.DurabilityLevelMajorityAndPersistOnMaster
	case DurabilityLevelPersistToMajority:
		r.DurabilityLevel = gocb.DurabilityLevelPersistToMajority
	default:
		r.DurabilityLevel = gocb.DurabilityLevelNone
	}
	return nil
}

// TaskResult represents a request structure for retrieving result of the task.
type TaskResult struct {
	Seed         string `json:"seed"`
	DeleteRecord bool   `json:"deleteRecord"`
}

// TaskResponse represents a response structure which is returned to user upon scheduling a task.
type TaskResponse struct {
	Seed string `json:"seed"`
}
