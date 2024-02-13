package db

import (
	"errors"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/cb_sdk"
	"github.com/couchbaselabs/sirius/internal/template"
	"time"
)

type CouchbaseOperationResult struct {
	key    string
	value  interface{}
	error  error
	status bool
	cas    uint64
}

func newCouchbaseOperationResult(key string, value interface{}, err error, status bool, cas uint64) *CouchbaseOperationResult {
	return &CouchbaseOperationResult{
		key:    key,
		value:  value,
		error:  err,
		status: status,
		cas:    cas,
	}
}

func (c *CouchbaseOperationResult) Key() string {
	return c.key
}

func (c *CouchbaseOperationResult) Value() interface{} {
	return c.value
}

func (c *CouchbaseOperationResult) GetStatus() bool {
	return c.status
}

func (c *CouchbaseOperationResult) GetError() error {
	return c.error
}

func (c *CouchbaseOperationResult) GetExtra() map[string]any {
	return map[string]any{
		"cas": c.cas,
	}
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

func (c *Couchbase) Create(connStr, username, password, key string, doc interface{}, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, doc, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, doc, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, doc, err1, false, 0)
	}
	result, err2 := collectionObj.Collection.Insert(key, doc, &gocb.InsertOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(key, doc, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, doc, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) Read(connStr, username, password, key string, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}
	result, err2 := collectionObj.Collection.Get(key, &gocb.GetOptions{
		Timeout: time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}
	resultFromHost := make(map[string]any)
	if err := result.Content(&resultFromHost); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}
	return newCouchbaseOperationResult(key, resultFromHost, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) Update(connStr, username, password, key string, doc interface{}, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, doc, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, doc, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, doc, err1, false, 0)
	}
	result, err2 := collectionObj.Collection.Upsert(key, doc, &gocb.UpsertOptions{
		Expiry:          time.Duration(extra.Expiry) * time.Second,
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(key, doc, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, doc, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) Delete(connStr, username, password, key string, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}
	result, err2 := collectionObj.Collection.Remove(key, &gocb.RemoveOptions{
		Cas:             gocb.Cas(extra.Cas),
		PersistTo:       extra.PersistTo,
		ReplicateTo:     extra.ReplicateTo,
		DurabilityLevel: cb_sdk.GetDurability(extra.Durability),
		Timeout:         time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, nil, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) Touch(connStr, username, password, key string, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}
	result, err2 := collectionObj.Collection.Touch(key, time.Duration(extra.Expiry)*time.Second, &gocb.TouchOptions{
		Timeout: time.Duration(extra.OperationTimeout) * time.Second,
	})
	if err2 != nil {
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, nil, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) InsertSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}

	var iOps []gocb.MutateInSpec
	for path, value := range subPathValues {
		iOps = append(iOps, gocb.InsertSpec(path, value, &gocb.InsertSpecOptions{
			CreatePath: extra.CreatePath,
			IsXattr:    extra.IsXattr,
		}))
	}

	if extra.IsXattr {
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
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, subPathValues, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) UpsertSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}

	var iOps []gocb.MutateInSpec
	for path, value := range subPathValues {
		iOps = append(iOps, gocb.UpsertSpec(path, value, &gocb.UpsertSpecOptions{
			CreatePath: extra.CreatePath,
			IsXattr:    extra.IsXattr,
		}))
	}

	if extra.IsXattr {
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
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, subPathValues, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) ReplaceSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}

	var iOps []gocb.MutateInSpec
	for path, value := range subPathValues {
		iOps = append(iOps, gocb.ReplaceSpec(path, value, &gocb.ReplaceSpecOptions{
			IsXattr: extra.IsXattr,
		}))
	}

	if extra.IsXattr {
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
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, subPathValues, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) ReadSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}

	var iOps []gocb.LookupInSpec

	for path, _ := range subPathValues {
		iOps = append(iOps, gocb.GetSpec(path, &gocb.GetSpecOptions{
			IsXattr: extra.IsXattr,
		}))
	}

	result, err2 := collectionObject.Collection.LookupIn(key, iOps, &gocb.LookupInOptions{
		Timeout: time.Duration(extra.OperationTimeout) * time.Second,
	})

	if err2 != nil {
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}

	return newCouchbaseOperationResult(key, subPathValues, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) DeleteSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}

	var iOps []gocb.MutateInSpec
	for path, _ := range subPathValues {
		iOps = append(iOps, gocb.RemoveSpec(path, &gocb.RemoveSpecOptions{
			IsXattr: extra.IsXattr,
		}))
	}

	if extra.IsXattr {
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
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, subPathValues, nil, true, uint64(result.Cas()))
}

func (c *Couchbase) IncrementMutationCount(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseOperationResult(key, nil, err, false, 0)
	}

	bucketName := extra.Bucket
	scope := extra.Scope
	collection := extra.Collection

	if err := validateStrings(bucketName); err != nil {
		return newCouchbaseOperationResult(key, nil, errors.New("bucket is missing"), false, 0)
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
		return newCouchbaseOperationResult(key, nil, err1, false, 0)
	}

	var iOps []gocb.MutateInSpec

	for path, _ := range subPathValues {
		iOps = append(iOps, gocb.IncrementSpec(path, 1, &gocb.CounterSpecOptions{
			CreatePath: extra.CreatePath,
			IsXattr:    extra.IsXattr,
		}))
	}

	if extra.IsXattr {
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
		return newCouchbaseOperationResult(key, nil, err2, false, 0)
	}
	return newCouchbaseOperationResult(key, subPathValues, nil, true, uint64(result.Cas()))
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
