package sdk_mongo

import (
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDatabaseObject struct {
	MongoDatabase    *mongo.Database                   `json:"-"`
	MongoCollections map[string]*MongoCollectionObject `json:"-"`
}

func (mongoDbObj *MongoDatabaseObject) setCollectionObject(collectionName string, mongoCollObj *MongoCollectionObject) {
	mongoDbObj.MongoCollections[collectionName] = mongoCollObj
}

func (mongoDbObj *MongoDatabaseObject) getCollectionObject(collectionName string) (*MongoCollectionObject, error) {
	_, ok := mongoDbObj.MongoCollections[collectionName]
	if ok {
		return mongoDbObj.MongoCollections[collectionName], nil
	} else {
		mongoCollection := mongoDbObj.MongoDatabase.Collection(collectionName)
		mongoCollectionObj := &MongoCollectionObject{
			MongoCollection: mongoCollection,
		}
		mongoDbObj.setCollectionObject(collectionName, mongoCollectionObj)
		return mongoCollectionObj, nil
	}

}
