package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
)

const (
	MaxConcurrentRoutines        = 30
	InsertOperation       string = "insert"
	DeleteOperation       string = "delete"
	UpsertOperation       string = "upsert"
	ValidateOperation     string = "validate"
	ReadOperation         string = "read"
)

const (
	DurabilityLevelMajority                   string = "MAJORITY"
	DurabilityLevelMajorityAndPersistToActive string = "MAJORITY_AND_PERSIST_TO_ACTIVE"
	DurabilityLevelPersistToMajority          string = "PERSIST_TO_MAJORITY"
	DefaultScope                              string = "_default"
	DefaultCollection                         string = "_default"
	DefaultBucket                             string = "default"
	DefaultUser                               string = "Administrator"
	DefaultPassword                           string = "password"
)

// getDurability returns gocb.DurabilityLevel required for doc loading operation
func getDurability(durability string) gocb.DurabilityLevel {
	switch durability {
	case DurabilityLevelMajority:
		return gocb.DurabilityLevelMajority
	case DurabilityLevelMajorityAndPersistToActive:
		return gocb.DurabilityLevelMajorityAndPersistOnMaster
	case DurabilityLevelPersistToMajority:
		return gocb.DurabilityLevelPersistToMajority
	default:
		return gocb.DurabilityLevelNone
	}
}

// OperationConfig contains all the configuration for document operation.
type OperationConfig struct {
	Count            int64    `json:"count,omitempty" doc:"true"`
	DocSize          int64    `json:"docSize" doc:"true"`
	DocType          string   `json:"docType,omitempty" doc:"true"`
	KeySize          int      `json:"keySize,omitempty" doc:"true"`
	KeyPrefix        string   `json:"keyPrefix" doc:"true"`
	KeySuffix        string   `json:"keySuffix" doc:"true"`
	RandomDocSize    bool     `json:"randomDocSize,omitempty" doc:"true"`
	RandomKeySize    bool     `json:"randomKeySize,omitempty" doc:"true"`
	ReadYourOwnWrite bool     `json:"readYourOwnWrite,omitempty" doc:"true"`
	TemplateName     string   `json:"template" doc:"true"`
	Start            int64    `json:"start" doc:"true"`
	End              int64    `json:"end" doc:"true"`
	FieldsToChange   []string `json:"fieldsToChange" doc:"true"`
}

// configureOperationConfig configures and validate the OperationConfig
func configureOperationConfig(o *OperationConfig) error {
	if o == nil {
		return fmt.Errorf("unable to parse OperationConfig")
	}
	if o.DocType == "" {
		o.DocType = docgenerator.JsonDocument
	}

	if o.KeySize == 0 || o.KeySize > docgenerator.DefaultKeySize {
		o.KeySize = docgenerator.DefaultKeySize
	}
	if o.Count == 0 {
		o.Count = 1
	}
	if o.DocSize == 0 {
		o.DocSize = docgenerator.DefaultDocSize
	}
	if o.Start < 0 {
		o.Start = 0
		o.End = 0
	}
	if o.Start > o.End {
		o.End = o.Start
		return fmt.Errorf("operation start to end range is malformed")
	}
	return nil
}

// InsertOptions are used when performing insert operation on CB server.
type InsertOptions struct {
	Expiry      int64  `json:"expiry,omitempty" doc:"true"`
	PersistTo   uint   `json:"persistTo,omitempty" doc:"true"`
	ReplicateTo uint   `json:"replicateTo,omitempty" doc:"true"`
	Durability  string `json:"durability,omitempty" doc:"true"`
	Timeout     int    `json:"timeout,omitempty" doc:"true"`
}

// configInsertOptions configures and validate the InsertOptions
func configInsertOptions(i *InsertOptions) error {
	if i == nil {
		return fmt.Errorf("unable to parse InsertOptions")
	}
	if i.Timeout == 0 {
		i.Timeout = 10
	}
	return nil
}

// RemoveOptions are used when performing delete operation on CB server.
type RemoveOptions struct {
	Cas         uint64 `json:"cas,omitempty" doc:"true"`
	PersistTo   uint   `json:"persistTo,omitempty" doc:"true"`
	ReplicateTo uint   `json:"replicateTo,omitempty" doc:"true"`
	Durability  string `json:"durability,omitempty" doc:"true"`
	Timeout     int    `json:"timeout,omitempty" doc:"true"`
}

func configRemoveOptions(r *RemoveOptions) error {
	if r == nil {
		return fmt.Errorf("unable to parse RemoveOptions")
	}
	if r.Timeout == 0 {
		r.Timeout = 10
	}
	return nil
}
