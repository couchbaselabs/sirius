package tasks

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
)

const (
	MaxConcurrentRoutines          = 45
	DefaultIdentifierToken         = "default"
	MaxQueryRuntime         int    = 86400
	DefaultQueryRunTime     int    = 100
	WatchIndexDuration      int    = 120
	InsertOperation         string = "insert"
	QueryOperation          string = "query"
	DeleteOperation         string = "delete"
	UpsertOperation         string = "upsert"
	ReadOperation           string = "read"
	ValidateOperation       string = "validate"
	SingleInsertOperation   string = "singleInsert"
	SingleDeleteOperation   string = "singleDelete"
	SingleUpsertOperation   string = "singleUpsert"
	SingleReadOperation     string = "singleRead"
	SingleTouchOperation    string = "singleTouch"
	SingleReplaceOperation  string = "singleReplace"
	CreatePrimaryIndex      string = "createPrimaryIndex"
	CreateIndex             string = "createIndex"
	BuildIndex              string = "buildIndex"
	RetryExceptionOperation string = "retryException"
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

// getDurability returns gocb.DurabilityLevel required for Doc loading operation
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
	Count            int64      `json:"count,omitempty" doc:"true"`
	DocSize          int        `json:"docSize" doc:"true"`
	DocType          string     `json:"docType,omitempty" doc:"true"`
	KeySize          int        `json:"keySize,omitempty" doc:"true"`
	KeyPrefix        string     `json:"keyPrefix" doc:"true"`
	KeySuffix        string     `json:"keySuffix" doc:"true"`
	ReadYourOwnWrite bool       `json:"readYourOwnWrite,omitempty" doc:"true"`
	TemplateName     string     `json:"template" doc:"true"`
	Start            int64      `json:"start" doc:"true"`
	End              int64      `json:"end" doc:"true"`
	FieldsToChange   []string   `json:"fieldsToChange" doc:"true"`
	Exceptions       Exceptions `json:"exceptions,omitempty" doc:"true"`
}

type KeyValue struct {
	Key string      `json:"key" doc:"true"`
	Doc interface{} `json:"value,omitempty" doc:"true"`
}

type SingleOperationConfig struct {
	KeyValue         []KeyValue `json:"keyValue" doc:"true"`
	ReadYourOwnWrite bool       `json:"readYourOwnWrite,omitempty" doc:"true"`
}

func configSingleOperationConfig(s *SingleOperationConfig) error {
	if s == nil {
		return fmt.Errorf("unable to parse SingleOperationConfig")
	}

	var finalKeyValue []KeyValue
	for _, kv := range s.KeyValue {
		if kv.Key != "" {
			finalKeyValue = append(finalKeyValue, kv)
		}
	}
	s.KeyValue = finalKeyValue
	return nil
}

type QueryOperationConfig struct {
	Template         string `json:"template,omitempty" doc:"true"`
	Duration         int    `json:"duration,omitempty" doc:"true"`
	BuildIndex       bool   `json:"buildIndex" doc:"true"`
	BuildIndexViaSDK bool   `json:"buildIndexViaSDK" doc:"true"`
}

func configQueryOperationConfig(s *QueryOperationConfig) error {
	if s == nil {
		return fmt.Errorf("unable to parse QueryOperationConfig")
	}

	if s.Duration == 0 || s.Duration > MaxQueryRuntime {
		s.Duration = DefaultQueryRunTime
	}
	return nil
}

// configureOperationConfig configures and validate the OperationConfig
func configureOperationConfig(o *OperationConfig) error {
	if o == nil {
		return fmt.Errorf("unable to parse OperationConfig")
	}
	if o.DocType == "" {
		o.DocType = docgenerator.JsonDocument
	}

	if o.KeySize <= 0 || o.KeySize > docgenerator.DefaultKeySize {
		o.KeySize = docgenerator.DefaultKeySize
	}
	if o.Count <= 0 {
		o.Count = 1
	}
	if o.DocSize <= 0 {
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

type ReplaceOptions struct {
	Expiry      int64  `json:"expiry,omitempty" doc:"true"`
	Cas         uint64 `json:"cas,omitempty" doc:"true"`
	PersistTo   uint   `json:"persistTo,omitempty" doc:"true"`
	ReplicateTo uint   `json:"replicateTo,omitempty" doc:"true"`
	Durability  string `json:"durability,omitempty" doc:"true"`
	Timeout     int    `json:"timeout,omitempty" doc:"true"`
}

func configReplaceOptions(r *ReplaceOptions) error {
	if r == nil {
		return fmt.Errorf("unable to parse RemoveOptions")
	}
	if r.Timeout == 0 {
		r.Timeout = 10
	}
	return nil
}

type Exceptions struct {
	IgnoreExceptions []string `json:"ignoreExceptions,omitempty" doc:"true"`
	RetryExceptions  []string `json:"retryExceptions,omitempty" doc:"true"`
	RetryAttempts    int      `json:"retryAttempts,omitempty" doc:"true"`
}

type RetriedResult struct {
	Status bool   `json:"status,omitempty" doc:"true"`
	CAS    uint64 `json:"cas,omitempty" doc:"true"`
}
