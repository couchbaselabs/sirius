package db

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/couchbaselabs/sirius/internal/sdk_mongo"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	connectionManager *sdk_mongo.MongoConnectionManager
}

type perMongoDocResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

type mongoOperationResult struct {
	key    string
	result perMongoDocResult
}

func NewMongoConnectionManager() *Mongo {
	return &Mongo{
		connectionManager: sdk_mongo.ConfigMongoConnectionManager(),
	}
}

// Operation Results for Single Operations like Create, Update, Touch and Delete

func newMongoOperationResult(key string, value interface{}, err error, status bool, offset int64) *mongoOperationResult {
	return &mongoOperationResult{
		key: key,
		result: perMongoDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (m *mongoOperationResult) Key() string {
	return m.key
}

func (m *mongoOperationResult) Value() interface{} {
	return m.result.value
}

func (m *mongoOperationResult) GetStatus() bool {
	return m.result.status
}

func (m *mongoOperationResult) GetError() error {
	return m.result.error
}

func (m *mongoOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *mongoOperationResult) GetOffset() int64 {
	return m.result.offset
}

// Operation Results for Bulk Operations like Bulk-Create, Bulk-Update, Bulk-Touch and Bulk-Delete

type mongoBulkOperationResult struct {
	keyValues map[string]perMongoDocResult
}

func newMongoBulkOperation() *mongoBulkOperationResult {
	return &mongoBulkOperationResult{
		keyValues: make(map[string]perMongoDocResult),
	}
}

func (m *mongoBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, cas uint64) {
	m.keyValues[key] = perMongoDocResult{
		value:  value,
		error:  err,
		status: status,
	}
}

func (m *mongoBulkOperationResult) Value(key string) interface{} {
	if x, ok := m.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (m *mongoBulkOperationResult) GetStatus(key string) bool {
	if x, ok := m.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (m *mongoBulkOperationResult) GetError(key string) error {
	if x, ok := m.keyValues[key]; ok {
		return x.error
	}
	return errors.New("Key not found in bulk operation")
}

func (m *mongoBulkOperationResult) GetExtra(key string) map[string]any {
	if _, ok := m.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (m *mongoBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := m.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (m *mongoBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		m.keyValues[x.Key] = perMongoDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (m *mongoBulkOperationResult) GetSize() int {
	return len(m.keyValues)
}

func (m Mongo) Connect(connStr, username, password string, extra Extras) error {
	clusterConfig := &sdk_mongo.MongoClusterConfig{
		ConnectionString: connStr,
		Username:         username,
		Password:         password,
	}

	if _, err := m.connectionManager.GetMongoCluster(connStr, username, password, clusterConfig); err != nil {
		return err
	}

	return nil
}

func (m Mongo) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	fmt.Println("MongoDB Client:", mongoClient)

	if err := validateStrings(extra.Bucket); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("database is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Collection); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("collection is missing"), false,
			keyValue.Offset)
	}
	mongoDatabase := mongoClient.Database(extra.Bucket)
	mongoCollection := mongoDatabase.Collection(extra.Collection)

	result, err2 := mongoCollection.InsertOne(context.TODO(), keyValue.Doc, nil)

	if err2 != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err2, false, keyValue.Offset)
	}
	if result == nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful CREATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newMongoOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (m Mongo) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) InsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) UpsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Increment(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) ReplaceSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) ReadSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) DeleteSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newMongoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	//fmt.Println("In CreateBulk(), Mongo Client:", mongoClient)

	if err := validateStrings(extra.Bucket); err != nil {
		result.failBulk(keyValues, errors.New("database name is missing"))
		return result
	}
	if err := validateStrings(extra.Collection); err != nil {
		result.failBulk(keyValues, errors.New("collection name is missing"))
		return result
	}
	mongoDatabase := mongoClient.Database(extra.Bucket)
	mongoCollection := mongoDatabase.Collection(extra.Collection)

	var models []mongo.WriteModel
	for _, x := range keyValues {
		model := mongo.NewInsertOneModel().SetDocument(x.Doc)
		models = append(models, model)
	}
	opts := options.BulkWrite().SetOrdered(false)
	_, err := mongoCollection.BulkWrite(context.TODO(), models, opts)
	if err != nil {
		log.Println("Bulk Insert Error:", err)
	}
	return result
}

func (m Mongo) Warmup(connStr, username, password string, extra Extras) error {
	//TODO implement me
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	return nil
}

func (m Mongo) Close(connStr string) error {
	if err := m.connectionManager.Clusters[connStr].MongoClusterClient.Disconnect(context.TODO()); err != nil {
		fmt.Println("Disconnect failed!")
		log.Fatal(err)
		return err
	}
	return nil
}

func (m Mongo) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}
