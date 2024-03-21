package sdk_mongo

import "go.mongodb.org/mongo-driver/mongo"

type MongoCollectionObject struct {
	MongoCollection *mongo.Collection `json:"-"`
}
