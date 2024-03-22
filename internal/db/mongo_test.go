package db

import (
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/meta_data"
	"github.com/couchbaselabs/sirius/internal/template"
	"os"

	// "github.com/jaswdr/faker"
	"log"

	"github.com/bgadrian/fastfaker/faker"

	"testing"
)

func TestMongoDB(t *testing.T) {
	/*
		This test does the following
		1. Insert in the range of 0-10
		2. Bulk Insert documents in the range of 10-50
		3. Update Documents from range 0-10
		4. Bulk Update documents in the range 10-50
		5. Read Docs from 0-10 and check if they are updated
		6. Bulk Read Docs in the range 10-50 and check if they are updated
		7. Delete in the range of 40-50
		8. Bulk Delete documents in the range of 0-40
	*/

	db, err := ConfigDatabase("mongodb")
	if err != nil {
		t.Fatal(err)
	}
	connStr, ok := os.LookupEnv("sirius_mongo_connStr")
	if !ok {
		t.Error("connStr not found")
	}
	username, ok := os.LookupEnv("sirius_mongo_username")
	if !ok {
		t.Error("username not found")

	}
	password, ok := os.LookupEnv("sirius_mongo_password")
	if !ok {
		t.Error("password not found")
	}
	if err := db.Connect(connStr, username, password, Extras{}); err != nil {
		t.Error(err)
		t.FailNow()
	}

	// Creating the Database and Collection
	resultString, err := db.CreateDatabase(connStr, username, password, Extras{
		Database:   "TestMongoDatabase",
		Collection: "TestingMongoSirius",
	}, "", 0)
	if err != nil {
		log.Println("creating database and collection:", err)
		t.Error("creating database and collection:", err)
	} else {
		log.Println(resultString)
	}

	m := meta_data.NewMetaData()
	cm1 := m.GetCollectionMetadata("x")

	temp := template.InitialiseTemplate("hotel")
	g := docgenerator.Generator{
		Template: temp,
	}
	gen := &docgenerator.Generator{
		KeySize:  0,
		DocType:  "json",
		Template: template.InitialiseTemplate("hotel"),
	}

	// Inserting Documents into MongoDB
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		//log.Println(docId, Doc)
		createResult := db.Create(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, Extras{
			Database:   "TestMongoDatabase",
			Collection: "TestingMongoSirius",
		})
		if createResult.GetError() != nil {
			t.Error(createResult.GetError())
		} else {
			log.Println("Inserting", createResult.Key(), " ", createResult.Value())
		}
	}

	// Bulk Inserting Documents into MongoDB
	var keyValues []KeyValue
	for i := int64(10); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		//log.Println(docId, Doc)
		keyVal := KeyValue{docId, doc, i}
		keyValues = append(keyValues, keyVal)
	}
	createBulkResult := db.CreateBulk(connStr, username, password, keyValues, Extras{
		Database:   "TestMongoDatabase",
		Collection: "TestingMongoSirius",
	})
	for _, i := range keyValues {
		if createBulkResult.GetError(i.Key) != nil {
			t.Error(createBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Insert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}

	//Upserting Documents into MongoDB
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)

		doc := g.Template.GenerateDocument(fake, docId, 1024) // Original Doc
		doc = g.Template.GenerateDocument(fake, docId, 1024)  // 1 Time Mutated Doc
		//log.Println(docId, doc)
		updateResult := db.Update(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, Extras{
			Database:   "TestMongoDatabase",
			Collection: "TestingMongoSirius",
		})
		if updateResult.GetError() != nil {
			t.Error(updateResult.GetError())
		} else {
			log.Println("Upserting", updateResult.Key(), " ", updateResult.Value())
		}
	}

	// Bulk Updating Documents into MongoDB
	keyValues = nil
	for i := int64(10); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		doc = g.Template.GenerateDocument(fake, docId, 1024) // 1 Time Mutated Doc
		//log.Println(docId, Doc)
		keyVal := KeyValue{docId, doc, i}
		keyValues = append(keyValues, keyVal)
	}
	updateBulkResult := db.UpdateBulk(connStr, username, password, keyValues, Extras{
		Database:   "TestMongoDatabase",
		Collection: "TestingMongoSirius",
	})
	for _, i := range keyValues {
		if updateBulkResult.GetError(i.Key) != nil {
			t.Error(updateBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Upsert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}

	//  Reading Documents into MongoDB
	for i := int64(0); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		createResult := db.Read(connStr, username, password, docId, i, Extras{
			Database:   "TestMongoDatabase",
			Collection: "TestingMongoSirius",
		})
		if createResult.GetError() != nil {
			t.Error(createResult.GetError())
		} else {
			log.Println("Inserting", createResult.Key(), " ", createResult.Value())
		}
	}
	//  Bulk Reading Documents into MongoDB
	keyValues = nil
	for i := int64(0); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, KeyValue{
			Key: docId,
		})
	}
	readBulkResult := db.ReadBulk(connStr, username, password, keyValues, Extras{
		Database:   "TestMongoDatabase",
		Collection: "TestingMongoSirius",
	})
	for _, i := range keyValues {
		if readBulkResult.GetError(i.Key) != nil {
			t.Error(updateBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Read, Inserted Key:", i.Key, "| Value:", readBulkResult.Value(i.Key))
		}
	}

	// Deleting Documents from MongoDB
	for i := int64(40); i < int64(50); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		deleteResult := db.Delete(connStr, username, password, docId, i, Extras{
			Database:   "TestMongoDatabase",
			Collection: "TestingMongoSirius",
		})
		if deleteResult.GetError() != nil {
			t.Error(deleteResult.GetError())
		} else {
			log.Println("Deleting", deleteResult.Key())
		}
	}

	// Bulk Deleting Documents from MongoDB
	keyValues = nil
	for i := int64(0); i < int64(40); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		keyVal := KeyValue{docId, nil, i}
		keyValues = append(keyValues, keyVal)
	}
	deleteBulkResult := db.DeleteBulk(connStr, username, password, keyValues, Extras{
		Database:   "TestMongoDatabase",
		Collection: "TestingMongoSirius",
	})
	for _, i := range keyValues {
		if deleteBulkResult.GetError(i.Key) != nil {
			t.Error(deleteBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Deleting, Deleted Key:", i.Key)
		}
	}

	// Deleting the Collection
	resultString, err = db.DeleteDatabase(connStr, username, password, Extras{
		Database:   "TestMongoDatabase",
		Collection: "TestingMongoSirius",
	})
	if err != nil {
		log.Println("deleting database and collection:", err)
		t.Error("deleting database and collection:", err)
	} else {
		log.Println(resultString)
	}

	// Closing the Connection to MongoDB
	if err = db.Close(connStr, Extras{}); err != nil {
		t.Error(err)
		t.Fail()
	}
}
