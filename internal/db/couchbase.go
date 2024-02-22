package db

import (
	"errors"
	"fmt"

	// "log"
	"time"

	"github.com/barkha06/sirius/internal/cb_sdk"
	"github.com/barkha06/sirius/internal/template"
	"github.com/couchbase/gocb/v2"
)

type perDocResult struct {
	value  interface{}
	error  error
	status bool
	cas    uint64
	offset int64
}

type perSubDocResult struct {
	keyValue []KeyValue
	error    error
	status   bool
	cas      uint64
	offset   int64
}

type couchbaseBulkOperationResult struct {
	keyValues map[string]perDocResult
}

func newCouchbaseBulkOperation() *couchbaseBulkOperationResult {
	return &couchbaseBulkOperationResult{
		keyValues: make(map[string]perDocResult),
	}
}

func (c *couchbaseBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, cas uint64,
	offset int64) {
	c.keyValues[key] = perDocResult{
		value:  value,
		error:  err,
		status: status,
		cas:    cas,
		offset: offset,
	}
}

func (c *couchbaseBulkOperationResult) Value(key string) interface{} {
	if x, ok := c.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (c *couchbaseBulkOperationResult) GetStatus(key string) bool {
	if x, ok := c.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (c *couchbaseBulkOperationResult) GetError(key string) error {
	if x, ok := c.keyValues[key]; ok {
		return x.error
	}
	return errors.New("key not found in bulk operation")
}

func (c *couchbaseBulkOperationResult) GetExtra(key string) map[string]any {
	if x, ok := c.keyValues[key]; ok {
		return map[string]any{
			"cas": x.cas,
		}
	}
	return nil
}

func (c *couchbaseBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := c.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (c *couchbaseBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		c.keyValues[x.Key] = perDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
			cas:    0,
		}
	}
}

func (c *couchbaseBulkOperationResult) GetSize() int {
	return len(c.keyValues)
}

type couchbaseOperationResult struct {
	key    string
	result perDocResult
}

func newCouchbaseOperationResult(key string, value interface{}, err error, status bool, cas uint64, offset int64) *couchbaseOperationResult {
	return &couchbaseOperationResult{
		key: key,
		result: perDocResult{
			value:  value,
			error:  err,
			status: status,
			cas:    cas,
			offset: offset,
		},
	}
}

func (c *couchbaseOperationResult) Key() string {
	return c.key
}

func (c *couchbaseOperationResult) Value() interface{} {
	return c.result.value
}

func (c *couchbaseOperationResult) GetStatus() bool {
	return c.result.status
}

func (c *couchbaseOperationResult) GetError() error {
	return c.result.error
}

func (c *couchbaseOperationResult) GetExtra() map[string]any {
	return map[string]any{
		"cas": c.result.cas,
	}
}

func (c *couchbaseOperationResult) GetOffset() int64 {
	return c.result.offset
}

type couchbaseSubDocOperationResult struct {
	key    string
	result perSubDocResult
}

func newCouchbaseSubDocOperationResult(key string, keyValue []KeyValue, err error, status bool, cas uint64, offset int64) *couchbaseSubDocOperationResult {
	return &couchbaseSubDocOperationResult{
		key: key,
		result: perSubDocResult{
			keyValue: keyValue,
			error:    err,
			status:   status,
			cas:      cas,
			offset:   offset,
		},
	}
}

func (c *couchbaseSubDocOperationResult) Key() string {
	return c.key
}

func (c *couchbaseSubDocOperationResult) Value(subPath string) (interface{}, int64) {
	for _, x := range c.result.keyValue {
		if x.Key == subPath {
			return x.Doc, x.Offset
		}
	}
	return nil, -1
}

func (c *couchbaseSubDocOperationResult) Values() []KeyValue {
	return c.result.keyValue
}

func (c *couchbaseSubDocOperationResult) GetError() error {
	return c.result.error
}

func (c *couchbaseSubDocOperationResult) GetExtra() map[string]any {
	return map[string]any{
		"cas": c.result.cas,
	}
}

func (c *couchbaseSubDocOperationResult) GetOffset() int64 {
	return c.result.offset
}

type Couchbase struct {
	connectionManager *cb_sdk.ConnectionManager
}

func NewCouchbaseConnectionManager() *Couchbase {
	return &Couchbase{
		connectionManager: cb_sdk.ConfigConnectionManager(),
	}
}

func (c *Couchbase) Connect(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	clusterConfig := &cb_sdk.ClusterConfig{
		CompressionConfig: cb_sdk.CompressionConfig{
			Disabled: extra.CompressionDisabled,
			MinSize:  extra.CompressionMinSize,
			MinRatio: extra.CompressionMinRatio,
		},
		TimeoutsConfig: cb_sdk.TimeoutsConfig{
			ConnectTimeout:   extra.ConnectionTimeout,
			KVTimeout:        extra.KVTimeout,
			KVDurableTimeout: extra.KVDurableTimeout,
		},
	}

	if _, err := c.connectionManager.GetCluster(connStr, username, password, clusterConfig); err != nil {
		return err
	}

	return nil
}

func (c *Couchbase) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, err, false, 0, keyValue.Offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, errors.New("bucket is missing"), false, 0,
			keyValue.Offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, err1, false, 0, keyValue.Offset)
	}
	result, err2 := collectionObj.Collection.Insert(keyValue.Key, keyValue.Doc, &gocb.InsertOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
	})

	if err2 != nil {
		return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, err2, false, 0, keyValue.Offset)
	}
	if result == nil {
		return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful CREATE operation %s ", connStr), false, 0,
			keyValue.Offset)
	}
	return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, nil, true, uint64(result.Cas()), keyValue.Offset)
}

func (c *Couchbase) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, err, false, 0, keyValue.Offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, errors.New("bucket is missing"), false, 0,
			keyValue.Offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, err1, false, 0, keyValue.Offset)
	}
	result, err2 := collectionObj.Collection.Upsert(keyValue.Key, keyValue.Doc, &gocb.UpsertOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, err2, false, 0, keyValue.Offset)
	}
	if result == nil {
		return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false, 0,
			keyValue.Offset)
	}
	return newCouchbaseOperationResult(keyValue.Key, keyValue.Doc, nil, true, uint64(result.Cas()), keyValue.Offset)
}

func (c *Couchbase) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		return newCouchbaseOperationResult(key, nil, err1, false, 0, offset)
	}
	result, err2 := collectionObj.Collection.Get(key, &gocb.GetOptions{
		Timeout: time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(key, nil, err2, false, 0, offset)
	}
	resultFromHost := make(map[string]any)
	if err := result.Content(&resultFromHost); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful READ operation %s ", connStr), false, 0,
			offset)
	}
	return newCouchbaseOperationResult(key, resultFromHost, nil, true, uint64(result.Cas()), offset)
}

func (c *Couchbase) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		return newCouchbaseOperationResult(key, nil, err1, false, 0, offset)
	}
	result, err2 := collectionObj.Collection.Remove(key, &gocb.RemoveOptions{
		Cas:             gocb.Cas(extra.Cas),
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(key, nil, err2, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful DELETE operation %s ", connStr), false, 0,
			offset)
	}
	return newCouchbaseOperationResult(key, nil, nil, true, uint64(result.Cas()), offset)
}

func (c *Couchbase) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		return newCouchbaseOperationResult(key, nil, err1, false, 0, offset)
	}
	result, err2 := collectionObj.Collection.Touch(key, time.Duration(extra.Expiry)*time.Second, &gocb.TouchOptions{
		Timeout: time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(key, nil, err2, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful TOUCH operation %s ", connStr), false, 0,
			offset)
	}
	return newCouchbaseOperationResult(key, nil, nil, true, uint64(result.Cas()), offset)
}

func (c *Couchbase) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("bucket is missing"), false, 0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObject, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err1, false, 0, offset)
	}

	var iOps []gocb.MutateInSpec
	for _, x := range keyValues {
		iOps = append(iOps, gocb.InsertSpec(x.Key, x.Doc, &gocb.InsertSpecOptions{
			CreatePath: extra.CreatePath,
			IsXattr:    extra.IsXattr,
		}))
	}

	if !extra.IsXattr {
		iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
			int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
				CreatePath: true,
				IsXattr:    false,
			}))
	}

	result, err2 := collectionObject.Collection.MutateIn(key, iOps, &gocb.MutateInOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		StoreSemantic:   cb_sdk.GetStoreSemantic(extra.StoreSemantic),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
		PreserveExpiry:  extra.PreserveExpiry,
	})
	if err2 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err2, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseSubDocOperationResult(key, keyValues,
			fmt.Errorf("result is nil even after successful SUB DOC INSERT operation %s ", connStr), false, 0,
			offset)
	}
	return newCouchbaseSubDocOperationResult(key, keyValues, nil, false, uint64(result.Cas()), offset)
}

func (c *Couchbase) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("bucket is missing"), false, 0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObject, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err1, false, 0, offset)
	}

	var iOps []gocb.MutateInSpec
	for _, x := range keyValues {
		iOps = append(iOps, gocb.UpsertSpec(x.Key, x.Doc, &gocb.UpsertSpecOptions{
			CreatePath: extra.CreatePath,
			IsXattr:    extra.IsXattr,
		}))
	}

	if !extra.IsXattr {
		iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
			int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
				CreatePath: true,
				IsXattr:    false,
			}))
	}

	result, err2 := collectionObject.Collection.MutateIn(key, iOps, &gocb.MutateInOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		StoreSemantic:   cb_sdk.GetStoreSemantic(extra.StoreSemantic),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
		PreserveExpiry:  extra.PreserveExpiry,
	})
	if err2 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err2, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseSubDocOperationResult(key, keyValues,
			fmt.Errorf("result is nil even after successful SUB DOC UPSERT operation %s ", connStr), false, 0,
			offset)
	}
	return newCouchbaseSubDocOperationResult(key, keyValues, nil, false, uint64(result.Cas()), offset)
}

func (c *Couchbase) Increment(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("bucket is missing"), false, 0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObject, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err1, false, 0, offset)
	}

	var iOps []gocb.MutateInSpec

	for _, x := range keyValues {
		delta, ok := x.Doc.(int)
		if !ok {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("delta value is not int"), false, 0,
				offset)
		}
		iOps = append(iOps, gocb.IncrementSpec(x.Key, int64(delta), &gocb.CounterSpecOptions{
			CreatePath: extra.CreatePath,
			IsXattr:    extra.IsXattr,
		}))
	}

	if !extra.IsXattr {
		iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
			int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
				CreatePath: true,
				IsXattr:    false,
			}))
	}

	result, err2 := collectionObject.Collection.MutateIn(key, iOps, &gocb.MutateInOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		StoreSemantic:   cb_sdk.GetStoreSemantic(extra.StoreSemantic),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
		PreserveExpiry:  extra.PreserveExpiry,
	})

	if err2 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err2, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseSubDocOperationResult(key, keyValues,
			fmt.Errorf("result is nil even after successful SUB DOC INCREMENT operation %s ", connStr), false, 0,
			offset)
	}
	return newCouchbaseSubDocOperationResult(key, keyValues, nil, true, uint64(result.Cas()), offset)
}

func (c *Couchbase) ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		newCouchbaseSubDocOperationResult(key, keyValues, errors.New("bucket is missing"), false, 0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObject, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		newCouchbaseSubDocOperationResult(key, keyValues, err1, false, 0, offset)
	}

	var iOps []gocb.MutateInSpec
	for _, x := range keyValues {
		iOps = append(iOps, gocb.ReplaceSpec(x.Key, x.Doc, &gocb.ReplaceSpecOptions{
			IsXattr: extra.IsXattr,
		}))
	}

	if !extra.IsXattr {
		iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
			int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
				CreatePath: true,
				IsXattr:    false,
			}))
	}

	result, err2 := collectionObject.Collection.MutateIn(key, iOps, &gocb.MutateInOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		StoreSemantic:   cb_sdk.GetStoreSemantic(extra.StoreSemantic),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
		PreserveExpiry:  extra.PreserveExpiry,
	})
	if err2 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err2, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseSubDocOperationResult(key, keyValues,
			fmt.Errorf("result is nil even after successful SUB DOC REPLACE operation %s ", connStr), false, 0,
			offset)
	}
	return newCouchbaseSubDocOperationResult(key, keyValues, nil, false, uint64(result.Cas()), offset)
}

func (c *Couchbase) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		newCouchbaseSubDocOperationResult(key, keyValues, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("bucket is missing"), false,
			0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObject, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err1, false, 0, offset)
	}

	var iOps []gocb.LookupInSpec

	for _, x := range keyValues {
		iOps = append(iOps, gocb.GetSpec(x.Key, &gocb.GetSpecOptions{
			IsXattr: extra.IsXattr,
		}))
	}

	result, err2 := collectionObject.Collection.LookupIn(key, iOps, &gocb.LookupInOptions{
		Timeout: time.Duration(extra.OperationTimeout) * time.Second,
	})

	if err2 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err2, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseSubDocOperationResult(key, keyValues,
			fmt.Errorf("result is nil even after successful SUB DOC READ operation %s ", connStr), false, 0,
			offset)
	}

	var resultKeyValue []KeyValue

	for i, x := range keyValues {
		var resultFromHost interface{}
		if err := result.ContentAt(uint(i), &resultFromHost); err == nil {
			resultKeyValue = append(resultKeyValue, KeyValue{
				Key:    x.Key,
				Doc:    resultFromHost,
				Offset: x.Offset,
			})
		} else {
			resultKeyValue = append(resultKeyValue, KeyValue{
				Key:    x.Key,
				Doc:    nil,
				Offset: x.Offset,
			})
		}
	}

	return newCouchbaseSubDocOperationResult(key, resultKeyValue, nil, false, uint64(result.Cas()), offset)
}

func (c *Couchbase) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, 0, offset)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		newCouchbaseSubDocOperationResult(key, keyValues, errors.New("bucket is missing"), false, 0, offset)
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObject, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		newCouchbaseSubDocOperationResult(key, keyValues, err1, false, 0, offset)
	}

	var iOps []gocb.MutateInSpec
	for _, x := range keyValues {
		iOps = append(iOps, gocb.RemoveSpec(x.Key, &gocb.RemoveSpecOptions{
			IsXattr: extra.IsXattr,
		}))
	}

	if !extra.IsXattr {
		iOps = append(iOps, gocb.IncrementSpec(template.MutatedPath,
			int64(template.MutateFieldIncrement), &gocb.CounterSpecOptions{
				CreatePath: true,
				IsXattr:    false,
			}))
	}

	result, err2 := collectionObject.Collection.MutateIn(key, iOps, &gocb.MutateInOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		StoreSemantic:   cb_sdk.GetStoreSemantic(extra.StoreSemantic),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
		PreserveExpiry:  extra.PreserveExpiry,
	})
	if err2 != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err2, false, 0, offset)
	}
	if result == nil {
		return newCouchbaseSubDocOperationResult(key, keyValues,
			fmt.Errorf("result is nil even after successful SUB DOC DELETE operation %s ", connStr), false, 0,
			offset)
	}
	return newCouchbaseSubDocOperationResult(key, keyValues, nil, true, uint64(result.Cas()), offset)
}

func (c *Couchbase) Warmup(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return errors.New("bucket is missing")
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	_, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	return err1
}

func (c *Couchbase) Close(connStr string) error {
	return c.connectionManager.Disconnect(connStr)
}

func (c *Couchbase) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newCouchbaseBulkOperation()

	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	var bulkOp []gocb.BulkOp

	keyToOffset := make(map[string]int64)

	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset

		bulkOp = append(bulkOp, &gocb.InsertOp{
			ID:     x.Key,
			Value:  x.Doc,
			Expiry: time.Duration(extra.Expiry) * time.Minute,
		})
	}

	err2 := collectionObj.Collection.Do(bulkOp,
		&gocb.BulkOpOptions{
			Timeout: time.Duration(extra.OperationTimeout) * time.Second,
		})

	if err2 != nil {
		result.failBulk(keyValues, err2)
		return result
	}

	for _, x := range bulkOp {
		insertOp, ok := x.(*gocb.InsertOp)
		if !ok {
			result.AddResult(insertOp.ID, insertOp.Value, errors.New("decoding error InsertOp"), false,
				0, -1)
		} else if insertOp.Err != nil {
			result.AddResult(insertOp.ID, insertOp.Value, insertOp.Err, false, 0, keyToOffset[insertOp.ID])

		} else {
			result.AddResult(insertOp.ID, insertOp.Value, nil, true, uint64(insertOp.Result.Cas()),
				keyToOffset[insertOp.ID])
		}
		//if mutationResults[x.Key].err != nil {
		//	result.AddResult(x.Key, x.Doc, mutationResults[x.Key].err, false,
		//		uint64(mutationResults[x.Key].result.Cas()))
		//} else {
		//	result.AddResult(x.Key, x.Doc, nil, true, uint64(mutationResults[x.Key].result.Cas()))
		//}

	}
	return result
}

func (c *Couchbase) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newCouchbaseBulkOperation()

	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	var bulkOp []gocb.BulkOp
	keyToOffset := make(map[string]int64)

	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		bulkOp = append(bulkOp, &gocb.UpsertOp{
			ID:     x.Key,
			Value:  x.Doc,
			Expiry: time.Duration(extra.Expiry) * time.Minute,
		})
	}

	err2 := collectionObj.Collection.Do(bulkOp,
		&gocb.BulkOpOptions{
			Timeout: time.Duration(extra.OperationTimeout) * time.Second,
		})

	if err2 != nil {
		result.failBulk(keyValues, err2)
		return result
	}

	for _, x := range bulkOp {
		upsertOp, ok := x.(*gocb.UpsertOp)
		if !ok {
			result.AddResult(upsertOp.ID, nil, errors.New("decoding error UpsertOp"), false,
				0, -1)
		} else if upsertOp.Err != nil {
			result.AddResult(upsertOp.ID, upsertOp.Value, upsertOp.Err, false, 0, keyToOffset[upsertOp.ID])

		} else {
			result.AddResult(upsertOp.ID, upsertOp.Value, nil, true, uint64(upsertOp.Result.Cas()), keyToOffset[upsertOp.ID])
		}
	}
	return result
}

func (c *Couchbase) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newCouchbaseBulkOperation()

	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	var bulkOp []gocb.BulkOp
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		bulkOp = append(bulkOp, &gocb.GetOp{
			ID: x.Key,
		})
	}

	err2 := collectionObj.Collection.Do(bulkOp,
		&gocb.BulkOpOptions{
			Timeout: time.Duration(extra.OperationTimeout) * time.Second,
		})

	if err2 != nil {
		result.failBulk(keyValues, err2)
		return result
	}

	for _, x := range bulkOp {
		getOp, ok := x.(*gocb.GetOp)
		if !ok {
			result.AddResult(getOp.ID, nil, errors.New("decoding error GetOp"), false, 0, -1)
		} else if getOp.Err != nil {
			result.AddResult(getOp.ID, nil, getOp.Err, false, 0, keyToOffset[getOp.ID])
		} else {
			var resultFromHost interface{}
			if err := getOp.Result.Content(&resultFromHost); err != nil {
				result.AddResult(getOp.ID, nil, err, false, uint64(getOp.Result.Cas()), keyToOffset[getOp.ID])
			} else {
				result.AddResult(getOp.ID, resultFromHost, err, true, uint64(getOp.Result.Cas()), keyToOffset[getOp.ID])
			}
		}
	}
	return result
}

func (c *Couchbase) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newCouchbaseBulkOperation()

	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	var bulkOp []gocb.BulkOp
	keyToOffset := make(map[string]int64)

	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		bulkOp = append(bulkOp, &gocb.RemoveOp{
			ID: x.Key,
		})
	}

	err2 := collectionObj.Collection.Do(bulkOp,
		&gocb.BulkOpOptions{
			Timeout: time.Duration(extra.OperationTimeout) * time.Second,
		})

	if err2 != nil {
		result.failBulk(keyValues, err2)
		return result
	}

	for _, x := range bulkOp {
		removeOp, ok := x.(*gocb.RemoveOp)
		if !ok {
			result.AddResult(removeOp.ID, nil, errors.New("decoding error RemoveOp"), false, 0, -1)
		} else if removeOp.Err != nil {
			result.AddResult(removeOp.ID, nil, removeOp.Err, false, 0, keyToOffset[removeOp.ID])
		} else {
			result.AddResult(removeOp.ID, nil, nil, true, uint64(removeOp.Result.Cas()), keyToOffset[removeOp.ID])
		}
	}
	return result
}

func (c *Couchbase) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newCouchbaseBulkOperation()

	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	if scope == "" {
		scope = cb_sdk.DefaultScope
	}

	if collection == "" {
		collection = cb_sdk.DefaultCollection
	}

	collectionObj, err1 := c.connectionManager.GetCollection(connStr, username, password, nil, bucketName,
		scope, collection)
	if err1 != nil {
		result.failBulk(keyValues, errors.New("bucket is missing"))
		return result
	}

	var bulkOp []gocb.BulkOp
	keyToOffset := make(map[string]int64)

	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		bulkOp = append(bulkOp, &gocb.TouchOp{
			ID:     x.Key,
			Expiry: time.Duration(extra.Expiry) * time.Minute,
		})
	}

	err2 := collectionObj.Collection.Do(bulkOp,
		&gocb.BulkOpOptions{
			Timeout: time.Duration(extra.OperationTimeout) * time.Second,
		})

	if err2 != nil {
		result.failBulk(keyValues, err2)
		return result
	}

	for _, x := range bulkOp {
		touchOp, ok := x.(*gocb.TouchOp)
		if !ok {
			result.AddResult(touchOp.ID, nil, errors.New("decoding error UpsertOp"), false,
				0, -1)
		} else if touchOp.Err != nil {
			result.AddResult(touchOp.ID, nil, touchOp.Err, false, 0, keyToOffset[touchOp.ID])

		} else {
			result.AddResult(touchOp.ID, nil, nil, true, uint64(touchOp.Result.Cas()), keyToOffset[touchOp.ID])
		}
	}
	return result
}
