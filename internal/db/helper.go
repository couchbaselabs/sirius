package db

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
)

type KeyValue struct {
	Key    string
	Doc    interface{}
	Offset int64
}

type Extras struct {
	CompressionDisabled bool    `json:"compressionDisabled,omitempty" doc:"true"`
	CompressionMinSize  uint32  `json:"compressionMinSize,omitempty" doc:"true"`
	CompressionMinRatio float64 `json:"compressionMinRatio,omitempty" doc:"true"`
	ConnectionTimeout   int     `json:"connectionTimeout,omitempty" doc:"true"`
	KVTimeout           int     `json:"KVTimeout,omitempty" doc:"true"`
	KVDurableTimeout    int     `json:"KVDurableTimeout,omitempty" doc:"true"`
	Bucket              string  `json:"bucket,omitempty" doc:"true"`
	Scope               string  `json:"scope,omitempty" doc:"true"`
	Collection          string  `json:"collection,omitempty" doc:"true"`
	Expiry              int     `json:"expiry,omitempty" doc:"true"`
	PersistTo           uint    `json:"persistTo,omitempty" doc:"true"`
	ReplicateTo         uint    `json:"replicateTo,omitempty" doc:"true"`
	Durability          string  `json:"durability,omitempty" doc:"true"`
	OperationTimeout    int     `json:"operationTimeout,omitempty" doc:"true"`
	Cas                 uint64  `json:"cas,omitempty" doc:"true"`
	IsXattr             bool    `json:"isXattr,omitempty" doc:"true"`
	StoreSemantic       int     `json:"storeSemantic,omitempty" doc:"true"`
	PreserveExpiry      bool    `json:"preserveExpiry,omitempty" doc:"true"`
	CreatePath          bool    `json:"createPath,omitempty" doc:"true"`
}

func validateStrings(values ...string) error {
	for _, v := range values {
		if v == "" {
			return fmt.Errorf("%s %w", v, err_sirius.InvalidInfo)
		}
	}
	return nil
}
