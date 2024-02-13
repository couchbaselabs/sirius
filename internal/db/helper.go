package db

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
)

//const (
//	couchbaseClusterConfig = "couchbaseClusterConfig"
//	couchbaseInsertOptions = "couchbaseInsertOptions"
//)
//
//type RegisterDatabaseHelper struct {
//}
//
//func (r RegisterDatabaseHelper) Helper() map[string]any {
//	return map[string]any{
//		couchbaseClusterConfig:           &cb_sdk.ClusterConfig{},
//		"couchbaseCompressionConfig":     &cb_sdk.CompressionConfig{},
//		"couchbaseClusterTimeoutsConfig": &cb_sdk.TimeoutsConfig{},
//		"operationConfig":                &bulk_loading.OperationConfig{},
//		couchbaseInsertOptions:           &cb_sdk.InsertOptions{},
//		"couchbaseRemoveOptions":         &cb_sdk.RemoveOptions{},
//		"couchbaseReplaceOption":         &cb_sdk.ReplaceOptions{},
//		"couchbaseTouchOptions":          &cb_sdk.TouchOptions{},
//		"couchbaseSingleOperationConfig": &key_based_loading_cb.SingleOperationConfig{},
//		"bulkError":                      &task_result.FailedDocument{},
//		"retriedError":                   &task_result.FailedDocument{},
//		"singleResult":                   &task_result.SingleOperationResult{},
//		"couchbaseQueryOperationConfig":  &cb_sdk.QueryOperationConfig{},
//		"exceptions":                     &bulk_loading.Exceptions{},
//		"couchbaseMutateInOptions":       &cb_sdk.MutateInOptions{},
//		"couchbaseInsertSpecOptions":     &cb_sdk.InsertSpecOptions{},
//		"couchbaseRemoveSpecOptions":     &cb_sdk.RemoveSpecOptions{},
//		"couchbaseGetSpecOptions":        &cb_sdk.GetSpecOptions{},
//		"couchbaseLookupInOptions":       &cb_sdk.LookupInOptions{},
//		"couchbaseReplaceSpecOptions":    &cb_sdk.ReplaceSpecOptions{},
//		"singleSubDocOperationConfig":    &key_based_loading_cb.SingleSubDocOperationConfig{},
//		"sdkTimings":                     &task_result.SDKTiming{},
//	}
//
//}

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
