package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/couchbaselabs/sirius/internal/sdk_mongo"

	"go.mongodb.org/mongo-driver/bson"
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

func (m *mongoBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	m.keyValues[key] = perMongoDocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
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

// Operation Result for SubDoc Operations
type perMongoSubDocResult struct {
	keyValue []KeyValue
	error    error
	status   bool
	offset   int64
}

type mongoSubDocOperationResult struct {
	key    string
	result perMongoSubDocResult
}

func newMongoSubDocOperationResult(key string, keyValue []KeyValue, err error, status bool, offset int64) *mongoSubDocOperationResult {
	return &mongoSubDocOperationResult{
		key: key,
		result: perMongoSubDocResult{
			keyValue: keyValue,
			error:    err,
			status:   status,
			offset:   offset,
		},
	}
}

func (m *mongoSubDocOperationResult) Key() string {
	return m.key
}

func (m *mongoSubDocOperationResult) Value(subPath string) (interface{}, int64) {
	for _, x := range m.result.keyValue {
		if x.Key == subPath {
			return x.Doc, x.Offset
		}
	}
	return nil, -1
}

func (m *mongoSubDocOperationResult) Values() []KeyValue {
	return m.result.keyValue
}

func (m *mongoSubDocOperationResult) GetError() error {
	return m.result.error
}

func (m *mongoSubDocOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *mongoSubDocOperationResult) GetOffset() int64 {
	return m.result.offset
}

func (m *Mongo) Connect(connStr, username, password string, extra Extras) error {
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

func (m *Mongo) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	//fmt.Println("MongoDB Create(): Client:", mongoClient)

	if err := validateStrings(extra.Database); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("database is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Collection); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("collection is missing"), false,
			keyValue.Offset)
	}
	mongoDatabase := mongoClient.Database(extra.Database)
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

func (m *Mongo) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient

	if err := validateStrings(extra.Database); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("database is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Collection); err != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, errors.New("collection is missing"), false,
			keyValue.Offset)
	}

	mongoDatabase := mongoClient.Database(extra.Database)
	mongoCollection := mongoDatabase.Collection(extra.Collection)

	filter := bson.M{"_id": keyValue.Key}
	update := bson.M{"$set": keyValue.Doc}
	//log.Println(filter, update)

	result, err2 := mongoCollection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
	if err2 != nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc, err2, false, keyValue.Offset)
	}
	if result == nil {
		return newMongoOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false,
			keyValue.Offset)
	}

	return newMongoOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (m *Mongo) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoOperationResult(key, nil, err, false, offset)
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		return newMongoOperationResult(key, nil, errors.New("MongoDB database name is missing"), false, offset)
	}
	if err := validateStrings(collectionName); err != nil {
		return newMongoOperationResult(key, nil, errors.New("MongoDB collection name is missing"), false, offset)
	}

	mongoCollObj, err1 := m.connectionManager.GetMongoCollection(connStr, username, password, nil, databaseName, collectionName)
	if err1 != nil {
		return newMongoOperationResult(key, nil, err1, false, offset)
	}

	mongoCollection := mongoCollObj.MongoCollection
	filter := bson.M{"_id": key}
	var result map[string]interface{}
	err2 := mongoCollection.FindOne(context.TODO(), filter, nil).Decode(&result)
	if err2 != nil {
		return newMongoOperationResult(key, nil, err2, false, offset)
	}
	if result == nil {
		return newMongoOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful READ operation %s ", connStr), false,
			offset)
	}
	//log.Println(result)
	return newMongoOperationResult(key, result, nil, true, offset)

}

func (m *Mongo) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoOperationResult(key, nil, err, false, offset)
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		return newMongoOperationResult(key, nil, errors.New("MongoDB database name is missing"), false, offset)
	}
	if err := validateStrings(collectionName); err != nil {
		// TODO default collection implementation for MongoDB
		return newMongoOperationResult(key, nil, errors.New("MongoDB database name is missing"), false, offset)
	}

	mongoCollObj, err1 := m.connectionManager.GetMongoCollection(connStr, username, password, nil, databaseName, collectionName)
	if err1 != nil {
		return newMongoOperationResult(key, nil, err1, false, offset)
	}

	mongoCollection := mongoCollObj.MongoCollection

	// filter is used to define on what basis shall we delete the documents
	filter := bson.M{"_id": key}

	result, err2 := mongoCollection.DeleteOne(context.TODO(), filter)
	if err2 != nil {
		return newMongoOperationResult(key, nil, err2, false, offset)
	}
	if result == nil {
		return newMongoOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful DELETE operation %s ", connStr), false, offset)
	}

	return newMongoOperationResult(key, nil, nil, true, offset)
}

func (m *Mongo) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	// panic("implement me")
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoOperationResult(key, nil, err, false, offset)
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		return newMongoOperationResult(key, nil, errors.New("MongoDB database name is missing"), false, offset)
	}
	if err := validateStrings(collectionName); err != nil {
		// TODO default collection implementation for MongoDB
		return newMongoOperationResult(key, nil, errors.New("MongoDB collection name is missing"), false, offset)
	}

	mongoCollObj, err1 := m.connectionManager.GetMongoCollection(connStr, username, password, nil, databaseName, collectionName)
	if err1 != nil {
		return newMongoOperationResult(key, nil, err1, false, offset)
	}

	mongoCollection := mongoCollObj.MongoCollection
	newExpirationTime := time.Now().Add((time.Minute) * time.Duration(extra.Expiry)) // Add 1 hour to current time

	// Update the document's expiration time
	filter := bson.M{"_id": key}
	update := bson.M{"$set": bson.M{"expireAt": newExpirationTime}}

	// Perform the update operation
	_, err := mongoCollection.UpdateOne(context.Background(), filter, update)
	if err != nil {
		return newMongoOperationResult(key, nil, err, false, offset)
	}
	return newMongoOperationResult(key, nil, nil, true, offset)
}

func (m *Mongo) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult {

	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoSubDocOperationResult(key, keyValues, err, false, offset)
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		return newMongoSubDocOperationResult(key, keyValues, errors.New("MongoDB Database name is missing"), false, offset)
	}

	if err := validateStrings(collectionName); err != nil {
		return newMongoSubDocOperationResult(key, keyValues, errors.New("MongoDB Collection name is missing"), false, offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	mongoCollection := mongoClient.Database(databaseName).Collection(collectionName)

	for _, x := range keyValues {
		// filter defines on what basis we find the doc to insert the sub documents
		filter := bson.M{"_id": key}
		// Defines the update to add a sub-document to the existing document
		update := bson.M{
			"$set": bson.M{
				x.Key: x.Doc,
			},
			"$inc": bson.M{
				"mutated": 1,
			},
		}

		result, err := mongoCollection.UpdateOne(context.TODO(), filter, update)
		if err != nil {
			log.Println("In MongoDB InsertSubDoc(), error:", err)
			return newMongoSubDocOperationResult(key, keyValues, err, false, offset)
		}

		// Checking if the update operation was successful
		if result.UpsertedCount == 0 && result.ModifiedCount == 0 {
			log.Println("No documents matched the filter or no modifications were made")
			return newMongoSubDocOperationResult(key, keyValues,
				fmt.Errorf("no documents matched the filter or no modifications were made"), false, offset)
		}
	}

	return newMongoSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (m *Mongo) UpsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	// panic("implement me")
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, err, false, offset)
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, errors.New("MongoDB Database name is missing"), false, offset)
	}

	if err := validateStrings(collectionName); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, errors.New("MongoDB Collection name is missing"), false, offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	mongoCollection := mongoClient.Database(databaseName).Collection(collectionName)
	for _, x := range keyValue {
		// filter defines on what basis we find the doc to insert the sub documents
		filter := bson.M{"_id": key}
		// Defines the update to add a sub-document to the existing document
		update := bson.M{
			"$set": bson.M{
				x.Key: x.Doc,
			},
			"$inc": bson.M{
				"mutated": 1,
			},
		}

		result, err := mongoCollection.UpdateOne(context.TODO(), filter, update)
		if err != nil {
			log.Println("In MongoDB InsertSubDoc(), error:", err)
			return newMongoSubDocOperationResult(key, keyValue, err, false, offset)
		}

		// Checking if the update operation was successful
		if result.UpsertedCount == 0 && result.ModifiedCount == 0 {
			log.Println("No documents matched the filter or no modifications were made")
			return newMongoSubDocOperationResult(key, keyValue,
				fmt.Errorf("no documents matched the filter or no modifications were made"), false, offset)
		}
	}

	return newMongoSubDocOperationResult(key, keyValue, nil, true, offset)

}

func (m *Mongo) Increment(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) ReplaceSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	// panic("implement me")
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, err, false, offset)
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, errors.New("MongoDB Database name is missing"), false, offset)
	}

	if err := validateStrings(collectionName); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, errors.New("MongoDB Collection name is missing"), false, offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	mongoCollection := mongoClient.Database(databaseName).Collection(collectionName)
	for _, x := range keyValue {
		// filter defines on what basis we find the doc to insert the sub documents
		filter := bson.M{"_id": key}
		// Defines the update to add a sub-document to the existing document
		update := bson.M{
			"$set": bson.M{
				x.Key: x.Doc,
			},
			"$inc": bson.M{
				"mutated": 1,
			},
		}

		result, err := mongoCollection.UpdateOne(context.TODO(), filter, update)
		if err != nil {
			log.Println("In MongoDB InsertSubDoc(), error:", err)
			return newMongoSubDocOperationResult(key, keyValue, err, false, offset)
		}

		// Checking if the update operation was successful
		if result.UpsertedCount == 0 && result.ModifiedCount == 0 {
			log.Println("No documents matched the filter or no modifications were made")
			return newMongoSubDocOperationResult(key, keyValue,
				fmt.Errorf("no documents matched the filter or no modifications were made"), false, offset)
		}

	}

	return newMongoSubDocOperationResult(key, keyValue, nil, true, offset)

}

func (m *Mongo) ReadSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, err, false, offset)
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, errors.New("MongoDB Database name is missing"), false, offset)
	}

	if err := validateStrings(collectionName); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, errors.New("MongoDB Collection name is missing"), false, offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	mongoCollection := mongoClient.Database(databaseName).Collection(collectionName)
	filter := bson.M{"_id": key}
	projection := bson.M{}
	for _, x := range keyValue {
		projection[x.Key] = 1
	}

	var result interface{}
	err := mongoCollection.FindOne(context.TODO(), filter, options.FindOne().SetProjection(projection)).Decode(&result)
	if err != nil {
		log.Println("In MongoDB ReadSubDoc(), error:", err)
		return newMongoSubDocOperationResult(key, keyValue, err, false, offset)
	}

	if result == nil {
		log.Println("No documents were read despite successful subdocread")
		return newMongoSubDocOperationResult(key, keyValue,
			fmt.Errorf("No documents were read despite successful subdocread"), false, offset)
	}

	return newMongoSubDocOperationResult(key, keyValue, nil, true, offset)

	// panic("implement me")
}

func (m *Mongo) DeleteSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, err, false, offset)
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, errors.New("MongoDB Database name is missing"), false, offset)
	}

	if err := validateStrings(collectionName); err != nil {
		return newMongoSubDocOperationResult(key, keyValue, errors.New("MongoDB Collection name is missing"), false, offset)
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	mongoCollection := mongoClient.Database(databaseName).Collection(collectionName)

	for _, x := range keyValue {

		filter := bson.M{"_id": key}

		// Defines the update to remove the sub-document from the existing document
		update := bson.M{
			"$unset": bson.M{
				x.Key: "",
			},
		}

		result, err := mongoCollection.UpdateOne(context.TODO(), filter, update)
		if err != nil {
			log.Println("In MongoDB DeleteSubDoc, error:", err)
			return newMongoSubDocOperationResult(key, keyValue, err, false, offset)
		}

		// Checking if the update operation was successful
		if result.ModifiedCount == 0 && result.UpsertedCount == 0 {
			log.Println("No documents matched the filter or no modifications were made")
			return newMongoSubDocOperationResult(key, keyValue,
				fmt.Errorf("no documents matched the filter or no modifications were made"), false, offset)
		}
	}

	return newMongoSubDocOperationResult(key, keyValue, nil, true, offset)
}

func (m *Mongo) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newMongoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	//fmt.Println("In CreateBulk(), Mongo Client:", mongoClient)

	if err := validateStrings(extra.Database); err != nil {
		result.failBulk(keyValues, errors.New("database name is missing"))
		return result
	}
	if err := validateStrings(extra.Collection); err != nil {
		result.failBulk(keyValues, errors.New("collection name is missing"))
		return result
	}
	mongoDatabase := mongoClient.Database(extra.Database)
	mongoCollection := mongoDatabase.Collection(extra.Collection)

	var models []mongo.WriteModel
	for _, x := range keyValues {
		model := mongo.NewInsertOneModel().SetDocument(x.Doc)
		models = append(models, model)
	}
	opts := options.BulkWrite().SetOrdered(false)

	mongoBulkWriteResult, err := mongoCollection.BulkWrite(context.TODO(), models, opts)
	if err != nil {
		// log.Println("In MongoDB CreateBulk(), BulkWrite() Error:", err)
		result.failBulk(keyValues, err)
		return result
	} else if int64(len(keyValues)) != mongoBulkWriteResult.InsertedCount {
		log.Println("In MongoDB CreateBulk(), Error: Inserted Count does not match batch size, err:", err)
		result.failBulk(keyValues, errors.New("MongoDB CreateBulk(): Inserted Count does not match batch size"))
		return result
	}

	for _, x := range keyValues {
		//log.Println("Successfully inserted document with id:", x.Key)
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}

	return result
}

// Warmup
/*
 * Validates all the string fields
 * TODO Checks if the MongoDB Database or Collection exists
 * TODO If Database or Collection name is not specified then we create a Default
 */
func (m *Mongo) Warmup(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}

	databaseName := extra.Database
	if err := validateStrings(databaseName); err != nil {
		return errors.New("MongoDB Database name is missing")
	}

	collectionName := extra.Collection
	if err := validateStrings(collectionName); err != nil {
		return errors.New("MongoDB Collection name is missing")
	}

	// Checking if the Collection exists or not. Will not work if User does not have readWriteAnyDatabase or if not using Auth

	//mongoDatabase := m.connectionManager.Clusters[connStr].MongoClusterClient.Database(databaseName)
	//
	//collectionNames, err := mongoDatabase.ListCollectionNames(context.TODO(), bson.D{{"options.NameOnly", true}})
	//if err != nil {
	//	log.Println("In MongoDB Warmup(), ListCollectionNames() Error: unable to list Collection Names for the given MongoDB Database")
	//	log.Println(err)
	//	//return errors.New("unable to list Collections for the given MongoDB Database")
	//	return err
	//}
	////log.Println("Collection Names:", collectionNames)
	//for _, collName := range collectionNames {
	//	//log.Println("Collection Name:", collName)
	//	if collectionName == collName {
	//		log.Println("In MongoDB Warmup(),", collectionName, "Collection exists")
	//	}
	//}

	return nil
}

func (m *Mongo) Close(connStr string, extra Extras) error {
	if err := m.connectionManager.Clusters[connStr].MongoClusterClient.Disconnect(context.TODO()); err != nil {
		log.Println("MongoDB Close(): Disconnect failed!")
		return err
	}
	return nil
}

func (m *Mongo) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newMongoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient

	if err := validateStrings(extra.Database); err != nil {
		result.failBulk(keyValues, errors.New("database name is missing"))
		return result
	}
	if err := validateStrings(extra.Collection); err != nil {
		result.failBulk(keyValues, errors.New("collection name is missing"))
		return result
	}
	mongoDatabase := mongoClient.Database(extra.Database)
	mongoCollection := mongoDatabase.Collection(extra.Collection)

	var models []mongo.WriteModel
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		filter := bson.M{"_id": x.Key}
		update := bson.M{"$set": x.Doc}
		// result, err2 := mongoCollection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
		model := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
		models = append(models, model)
	}
	opts := options.BulkWrite().SetOrdered(false)
	// log.Println("Lengths: ", len(models), len(keyValues))
	mongoBulkWriteResult, err := mongoCollection.BulkWrite(context.TODO(), models, opts)
	// log.Println(mongoBulkWriteResult)
	if err != nil {
		log.Println("MongoDB UpsertBulk(): Bulk Insert Error:", err)
		result.failBulk(keyValues, errors.New("MongoDB UpdateBulk(): Bulk Upsert Error"))
		return result
	} else if int64(len(keyValues)) != mongoBulkWriteResult.ModifiedCount && int64(len(keyValues)) != mongoBulkWriteResult.UpsertedCount {
		// log.Println("count: ", int64(len(keyValues)), mongoBulkWriteResult)
		result.failBulk(keyValues, errors.New("MongoDB UpdateBulk(): Upserted Count does not match batch size"))
		return result
	}

	for _, x := range models {
		upsertOp, ok := x.(*mongo.UpdateOneModel)
		docid := upsertOp.Filter.(bson.M)["_id"]
		value := upsertOp.Update.(bson.M)["$set"]
		// log.Println("docid: value:   ", docid, value)
		if !ok {
			result.AddResult(docid.(string), nil, errors.New("decoding error GetOp"), false, -1)
		} else {
			result.AddResult(docid.(string), value, nil, true, keyToOffset[docid.(string)])
			// log.Println("docid: value:   ", docid, value)
		}
	}
	return result
}

func (m *Mongo) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newMongoBulkOperation()

	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		result.failBulk(keyValues, errors.New("MongoDB database name is missing"))
		return result
	}

	if err := validateStrings(collectionName); err != nil {
		// TODO implement default collection
		result.failBulk(keyValues, errors.New("MongoDB collection name is missing"))
		return result
	}

	mongoCollObj, err1 := m.connectionManager.GetMongoCollection(connStr, username, password, nil, databaseName, collectionName)
	if err1 != nil {
		result.failBulk(keyValues, err1)
		return result
	}

	mongoCollection := mongoCollObj.MongoCollection

	var documentIDs []string // Add your document IDs here
	keyToOffset := make(map[string]int64)

	for _, x := range keyValues {
		documentIDs = append(documentIDs, x.Key)
		keyToOffset[x.Key] = x.Offset
	}

	// filter defines on what basis do we delete the documents
	filter := bson.M{"_id": bson.M{"$in": documentIDs}}

	resultOfDelete, err2 := mongoCollection.DeleteMany(context.TODO(), filter)
	if err2 != nil {
		result.failBulk(keyValues, err2)
		return result
	}
	if resultOfDelete == nil {
		for _, x := range keyValues {
			result.AddResult(x.Key, nil, errors.New("delete successful but result is nil"), true, keyToOffset[x.Key])
		}
		return result
	}
	if resultOfDelete.DeletedCount == 0 {
		for _, x := range keyValues {
			result.AddResult(x.Key, nil, errors.New("zero documents were deleted"), true, keyToOffset[x.Key])
		}
		return result
	}
	//log.Printf("Deleted %d document(s)\n", resultOfDelete.DeletedCount)
	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (m *Mongo) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	// panic("implement me")
	result := newMongoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	mongoClient := m.connectionManager.Clusters[connStr].MongoClusterClient
	// opts1 := options.Client().ApplyURI(connStr)
	// mongoClient, err := mongo.Connect(context.TODO(), opts1)
	if err := validateStrings(extra.Database); err != nil {
		result.failBulk(keyValues, errors.New("database name is missing"))
		return result
	}
	if err := validateStrings(extra.Collection); err != nil {
		result.failBulk(keyValues, errors.New("collection name is missing"))
		return result
	}
	mongoDatabase := mongoClient.Database(extra.Database)
	mongoCollection := mongoDatabase.Collection(extra.Collection)
	newExpirationTime := time.Now().Add((time.Minute) * time.Duration(extra.Expiry)) // Add 1 hour to current time

	var models []mongo.WriteModel
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		filter := bson.M{"_id": x.Key}
		update := bson.M{"$set": bson.M{"expireAt": newExpirationTime}}
		// result, err2 := mongoCollection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
		model := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
		models = append(models, model)
	}
	opts := options.BulkWrite().SetOrdered(false)
	// log.Println("Lengths: ", len(models), len(keyValues))
	mongoBulkWriteResult, err := mongoCollection.BulkWrite(context.TODO(), models, opts)
	// log.Println(mongoBulkWriteResult)
	if err != nil {
		log.Println("MongoDB UpsertBulk(): Bulk Insert Error:", err)
		result.failBulk(keyValues, errors.New("MongoDB TouchBulk(): Bulk Touch Error"))
		return result
	} else if int64(len(keyValues)) != mongoBulkWriteResult.ModifiedCount && int64(len(keyValues)) != mongoBulkWriteResult.UpsertedCount {
		// log.Println("count: ", int64(len(keyValues)), mongoBulkWriteResult)
		result.failBulk(keyValues, errors.New("MongoDB TouchBulk(): Touch Count does not match batch size"))

		return result
	}

	for _, x := range models {
		upsertOp, ok := x.(*mongo.UpdateOneModel)
		docid := upsertOp.Filter.(bson.M)["_id"]
		value := upsertOp.Update.(bson.M)["$set"]
		// log.Println("docid: value:   ", docid, value)
		if !ok {
			result.AddResult(docid.(string), nil, errors.New("decoding error GetOp"), false, -1)
		} else {
			result.AddResult(docid.(string), value, nil, true, keyToOffset[docid.(string)])
			// log.Println("docid: value:   ", docid, value)
		}
	}
	// mongoClient.Disconnect(context.TODO())
	return result
}

func (m *Mongo) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newMongoBulkOperation()

	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	databaseName := extra.Database
	collectionName := extra.Collection

	if err := validateStrings(databaseName); err != nil {
		result.failBulk(keyValues, errors.New("MongoDB database name is missing"))
		return result
	}

	if err := validateStrings(collectionName); err != nil {
		result.failBulk(keyValues, errors.New("MongoDB collection name is missing"))
		return result
	}

	mongoCollObj, err1 := m.connectionManager.GetMongoCollection(connStr, username, password, nil, databaseName, collectionName)
	if err1 != nil {
		result.failBulk(keyValues, err1)
		return result
	}

	mongoCollection := mongoCollObj.MongoCollection

	var docIDs []string
	keyToOffset := make(map[string]int64)

	for _, x := range keyValues {
		docIDs = append(docIDs, x.Key)
		keyToOffset[x.Key] = x.Offset
	}
	filter := bson.M{"_id": bson.M{"$in": docIDs}}

	cursor, err2 := mongoCollection.Find(context.TODO(), filter)
	if err2 != nil {
		log.Println("MongoDB ReadBulk(): Bulk Read Error:", err2)
		result.failBulk(keyValues, err2)
		return result
	}

	var results []map[string]interface{}
	if err := cursor.All(context.TODO(), &results); err != nil {
		result.failBulk(keyValues, err)
	}
	for _, resultdoc := range results {
		// log.Println(resultdoc, resultdoc["_id"].(string))
		result.AddResult(resultdoc["_id"].(string), resultdoc, nil, true, keyToOffset[resultdoc["_id"].(string)])
	}
	return result
}
func (m *Mongo) CreateDatabase(connStr, username, password string, extra Extras, templateName string, docSize int) (string, error) {

	if err := validateStrings(connStr, username, password); err != nil {
		return "", err
	}

	mongoClient, err := m.connectionManager.GetMongoCluster(connStr, username, password, nil)
	if err != nil {
		return "", errors.New("creating mongo database or collection: unable to get mongo cluster client: " + err.Error())
	}

	if extra.Database == "" {
		return "", errors.New("creating mongo database: database name not provided")
	}

	database := mongoClient.Database(extra.Database)
	if database == nil {
		return "", errors.New("creating mongo database or collection: unable to get mongo database client")
	}
	if extra.Collection == "" {
		return "Database '" + extra.Database + "' created successfully", nil
	}

	err = database.CreateCollection(context.TODO(), extra.Collection, nil)
	if err != nil {
		return "", err
	} else {
		return "Collection '" + extra.Collection + "' in Database '" + extra.Database + "' created successfully in MongoDB", nil
	}
}

func (m *Mongo) DeleteDatabase(connStr, username, password string, extra Extras) (string, error) {

	if err := validateStrings(connStr, username, password); err != nil {
		return "", err
	}

	mongoClient, err := m.connectionManager.GetMongoCluster(connStr, username, password, nil)
	if err != nil {
		return "", errors.New("deleting mongo database or collection: unable to get mongo cluster client: " + err.Error())
	}

	if extra.Database == "" {
		return "", errors.New("deleting mongo database or collection: database name not provided")
	}

	mongoDatabase := mongoClient.Database(extra.Database)
	if mongoDatabase == nil {
		return "", errors.New("deleting mongo database or collection: database '" + extra.Database + "' not found in cluster")
	}

	if extra.Collection == "" {
		err = mongoDatabase.Drop(context.TODO())
		if err != nil {
			return "", errors.New("deleting mongo database or collection: database '" + extra.Database + "' not found in cluster")
		}
		return "Database " + extra.Database + " Deletion Successful", nil
	} else {
		err := mongoDatabase.Collection(extra.Collection).Drop(context.TODO())
		if err != nil {
			return "", err
		} else {
			return "Collection Deletion Successful  : " + extra.Collection, nil
		}
	}
}

func (m *Mongo) Count(connStr, username, password string, extra Extras) (int64, error) {

	if err := validateStrings(connStr, username, password); err != nil {
		return -1, errors.New("listing count of documents in mongo collection: connection string or auth parameters not provided")
	}

	mongoClient, err := m.connectionManager.GetMongoCluster(connStr, username, password, nil)
	if err != nil {
		return -1, errors.New("listing count of documents in mongo collection: unable to get mongo cluster client: " + err.Error())
	}

	if extra.Database == "" {
		return -1, errors.New("listing count of documents in mongo collection: database name not provided")
	}
	if extra.Collection == "" {
		return -1, errors.New("listing count of documents in mongo collection: collection name not provided")
	}

	mongoDatabase := mongoClient.Database(extra.Database)
	if mongoDatabase == nil {
		return -1, errors.New("listing count of documents in mongo collection: database '" + extra.Database + "' not found in cluster")
	}

	mongoCollection := mongoDatabase.Collection(extra.Collection)
	if mongoCollection == nil {
		return -1, errors.New("listing count of documents in mongo collection: collection '" + extra.Collection + "' not found in database '" + extra.Database + "'")
	}

	count, err := mongoCollection.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		return -1, errors.New("listing count of documents in mongo collection: unable to count documents: " + err.Error())
	}

	return count, nil
}

func (m *Mongo) ListDatabase(connStr, username, password string, extra Extras) (any, error) {

	dbList := make(map[string][]string)
	if err := validateStrings(connStr, username, password); err != nil {
		return nil, errors.New("listing mongo databases or collections: connection string or auth parameters not provided")
	}

	mongoClient, err := m.connectionManager.GetMongoCluster(connStr, username, password, nil)
	if err != nil {
		return nil, errors.New("listing mongo databases or collections: unable to get mongo cluster client: " + err.Error())
	}

	// Getting all the databases for MongoDB cluster
	databases, err := mongoClient.ListDatabaseNames(context.TODO(), bson.D{})
	if err != nil {
		return nil, errors.New("listing mongo databases: " + err.Error())
	}

	// Getting all the collections for all the databases in MongoDB cluster
	for _, mongoDatabase := range databases {
		collections, err := mongoClient.Database(mongoDatabase).ListCollectionNames(context.TODO(), bson.D{})
		if err != nil {
			return nil, errors.New("listing mongo collections for database " + mongoDatabase + ":" + err.Error())
		}

		dbList[mongoDatabase] = collections
	}
	return dbList, nil
}
