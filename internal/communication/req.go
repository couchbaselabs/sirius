package communication

import (
	"fmt"
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

type Request struct {
	Service    ServiceType       `json:"service,omitempty"`
	Username   string            `json:"username,omitempty"`
	Password   string            `json:"password,omitempty"`
	Host       string            `json:"host,omitempty"`
	Bucket     string            `json:"bucket,omitempty"`
	Scope      string            `json:"scope,omitempty"`
	Collection string            `json:"collection,omitempty"`
	Count      int               `json:"count,omitempty"`
	BatchSize  int               `json:"batchSize,omitempty"`
	Operation  DocumentOperation `json:"operation,omitempty"`
	Seed       int64             `json:"seed,omitempty"`
}

func (r *Request) Validate() error {

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
		r.Scope = "_default"
	}
	if r.Collection == "" {
		return fmt.Errorf("collection is missing")
	}
	if r.Count == 0 {
		r.Count = 1
	}

	if r.BatchSize == 0 {
		r.BatchSize = 1
	}
	switch r.Operation {
	case InsertOperation, DeleteOperation, UpsertOperation, GetOperation:
	default:
		return fmt.Errorf("incorrect operation type")
	}
	if r.Seed == 0 {
		r.Seed = time.Now().UnixNano()
	}
	return nil
}
