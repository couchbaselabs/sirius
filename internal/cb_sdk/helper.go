package cb_sdk

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/task_errors"
)

const (
	DefaultCollection                         string = "_default"
	DurabilityLevelMajority                   string = "MAJORITY"
	DurabilityLevelMajorityAndPersistToActive string = "MAJORITY_AND_PERSIST_TO_ACTIVE"
	DurabilityLevelPersistToMajority          string = "PERSIST_TO_MAJORITY"
	DefaultScope                              string = "_default"
	DefaultBucket                             string = "default"
	MaxQueryRuntime                           int    = 86400
	DefaultQueryRunTime                       int    = 100
)

// GetDurability returns gocb.DurabilityLevel required for Doc loading operation
func GetDurability(durability string) gocb.DurabilityLevel {
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

func GetTranscoder(docType string) gocb.Transcoder {
	switch docType {
	case "json":
		return gocb.NewJSONTranscoder()
	case "string":
		return gocb.NewRawStringTranscoder()
	case "binary":
		return gocb.NewRawBinaryTranscoder()
	default:
		return gocb.NewJSONTranscoder()
	}

}

// InsertOptions are used when performing insert operation on CB server.
type InsertOptions struct {
	Expiry      int64  `json:"expiry,omitempty" doc:"true"`
	PersistTo   uint   `json:"persistTo,omitempty" doc:"true"`
	ReplicateTo uint   `json:"replicateTo,omitempty" doc:"true"`
	Durability  string `json:"durability,omitempty" doc:"true"`
	Timeout     int    `json:"timeout,omitempty" doc:"true"`
}

// ConfigInsertOptions configures and validate the InsertOptions
func ConfigInsertOptions(i *InsertOptions) error {
	if i == nil {
		return task_errors.ErrParsingInsertOptions
	}
	if i.Timeout == 0 {
		i.Timeout = 10
	}
	return nil
}

type TouchOptions struct {
	Timeout int `json:"timeout,omitempty" doc:"true"`
}

func ConfigTouchOptions(i *TouchOptions) error {
	if i == nil {
		return task_errors.ErrParsingTouchOptions
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

func ConfigRemoveOptions(r *RemoveOptions) error {
	if r == nil {
		return task_errors.ErrParsingRemoveOptions
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

func ConfigReplaceOptions(r *ReplaceOptions) error {
	if r == nil {
		return task_errors.ErrParsingReplaceOptions
	}
	if r.Timeout == 0 {
		r.Timeout = 10
	}
	return nil
}

type QueryOperationConfig struct {
	Template         string `json:"template,omitempty" doc:"true"`
	Duration         int    `json:"duration,omitempty" doc:"true"`
	BuildIndex       bool   `json:"buildIndex" doc:"true"`
	BuildIndexViaSDK bool   `json:"buildIndexViaSDK" doc:"true"`
}

func ConfigQueryOperationConfig(s *QueryOperationConfig) error {
	if s == nil {
		return task_errors.ErrParsingQueryConfig
	}

	if s.Duration == 0 || s.Duration > MaxQueryRuntime {
		s.Duration = DefaultQueryRunTime
	}
	return nil
}

type GetSpecOptions struct {
	IsXattr bool `json:"isXattr,omitempty" doc:"true"`
}

func ConfigGetSpecOptions(g *GetSpecOptions) error {
	if g == nil {
		return task_errors.ErrParsingGetSpecOptions
	}
	return nil
}

type LookupInOptions struct {
	Timeout int `json:"timeout,omitempty" doc:"true"`
}

func ConfigLookupInOptions(l *LookupInOptions) error {
	if l == nil {
		return task_errors.ErrParsingLookupInOptions
	}
	return nil
}

type InsertSpecOptions struct {
	CreatePath bool `json:"createPath,omitempty" doc:"true"`
	IsXattr    bool `json:"isXattr,omitempty" doc:"true"`
}

func ConfigInsertSpecOptions(i *InsertSpecOptions) error {
	if i == nil {
		return task_errors.ErrParsingInsertSpecOptions
	}
	return nil
}

type RemoveSpecOptions struct {
	IsXattr bool `json:"isXattr,omitempty" doc:"true"`
}

func ConfigRemoveSpecOptions(r *RemoveSpecOptions) error {
	if r == nil {
		return task_errors.ErrParsingRemoveSpecOptions
	}
	return nil
}

type ReplaceSpecOptions struct {
	IsXattr bool `json:"isXattr,omitempty" doc:"true"`
}

func ConfigReplaceSpecOptions(r *ReplaceSpecOptions) error {
	if r == nil {
		return task_errors.ErrParsingReplaceSpecOptions
	}
	return nil
}

type MutateInOptions struct {
	Expiry         int    `json:"expiry,omitempty" doc:"true"`
	Cas            uint64 `json:"cas,omitempty" doc:"true"`
	PersistTo      uint   `json:"persistTo,omitempty" doc:"true"`
	ReplicateTo    uint   `json:"replicateTo,omitempty" doc:"true"`
	Durability     string `json:"durability,omitempty" doc:"true"`
	StoreSemantic  int    `json:"storeSemantic,omitempty" doc:"true"`
	Timeout        int    `json:"timeout,omitempty" doc:"true"`
	PreserveExpiry bool   `json:"preserveExpiry,omitempty" doc:"true"`
}

func ConfigMutateInOptions(m *MutateInOptions) error {
	if m == nil {
		return task_errors.ErrParsingMutateInOptions
	}
	return nil
}

func GetStoreSemantic(storeSemantic int) gocb.StoreSemantics {
	if storeSemantic >= 3 {
		return gocb.StoreSemanticsUpsert
	}
	return gocb.StoreSemantics(storeSemantic)
}
