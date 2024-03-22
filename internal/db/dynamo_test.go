package db

import (
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/meta_data"
	"github.com/couchbaselabs/sirius/internal/template"
	"os"

	"log"

	"github.com/bgadrian/fastfaker/faker"

	"testing"
)

func TestDynamoDB(t *testing.T) {
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

	db, err := ConfigDatabase("dynamodb")
	if err != nil {
		t.Fatal(err)
	}
	connStr, ok := os.LookupEnv("sirius_dynamo_connStr")
	if !ok {
		t.Error("connStr not found")
	}
	username, ok := os.LookupEnv("sirius_dynamo_username")
	if !ok {
		t.Error("username not found")

	}
	password, ok := os.LookupEnv("sirius_dynamo_password")
	if !ok {
		t.Error("password not found")
	}
	if err := db.Connect(connStr, username, password, Extras{}); err != nil {
		t.Error(err)
		t.FailNow()
	}
	extra := Extras{
		Table: "testing_sirius",
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
	//Creating Table
	_, err = db.CreateDatabase(connStr, username, password, extra, "", -1)
	if err != nil {
		t.Error(err)
	}
	// listing tables:
	tables, err := db.ListDatabase(connStr, username, password, extra)
	if err != nil {
		t.Error(err)
	} else {
		log.Println("Listing Tables:\n", tables)
	}
	// Inserting Documents into DynamoDB
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
		}, extra)
		if createResult.GetError() != nil {
			t.Error(createResult.GetError())
		} else {
			log.Println("Inserting", createResult.Key(), " ", createResult.Value())
		}
	}
	count, err := db.Count(connStr, username, password, extra)
	if err != nil {
		t.Error(err)
	} else {
		log.Println("Table Document Count:\n", count)
	}

	// Bulk Inserting Documents into DynamoDB
	var keyValues []KeyValue
	for i := int64(10); i < int64(35); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		//log.Println(docId, Doc)
		keyVal := KeyValue{docId, doc, i}
		keyValues = append(keyValues, keyVal)
	}
	createBulkResult := db.CreateBulk(connStr, username, password, keyValues, extra)
	for _, i := range keyValues {
		if createBulkResult.GetError(i.Key) != nil {
			t.Error(createBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Insert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}
	count, err = db.Count(connStr, username, password, extra)
	if err != nil {
		t.Error(err)
	} else {
		log.Println("Table Document Count:\n", count)
	}

	//Upserting Documents into DynamoDB
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
		}, extra)
		if updateResult.GetError() != nil {
			t.Error(updateResult.GetError())
		} else {
			log.Println("Upserting", updateResult.Key(), " ", updateResult.Value())
		}
	}
	count, err = db.Count(connStr, username, password, extra)
	if err != nil {
		t.Error(err)
	} else {
		log.Println("Table Document Count:\n", count)
	}
	// Bulk Updating Documents into DynamoDB
	keyValues = nil
	for i := int64(10); i < int64(35); i++ {
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
	updateBulkResult := db.UpdateBulk(connStr, username, password, keyValues, extra)
	for _, i := range keyValues {
		if updateBulkResult.GetError(i.Key) != nil {
			t.Error(updateBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Upsert, Inserted Key:", i.Key, "| Value:", i.Doc)
		}
	}

	//  Reading Documents into DynamoDB
	for i := int64(0); i < int64(35); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		createResult := db.Read(connStr, username, password, docId, i, extra)
		if createResult.GetError() != nil {
			t.Error(createResult.GetError())
		} else {
			log.Println("Inserting", createResult.Key(), " ", createResult.Value())
		}
	}
	//  Bulk Reading Documents into DynamoDB
	keyValues = nil
	for i := int64(0); i < int64(35); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		keyValues = append(keyValues, KeyValue{
			Key: docId,
		})
	}
	readBulkResult := db.ReadBulk(connStr, username, password, keyValues, extra)
	for _, i := range keyValues {
		if readBulkResult.GetError(i.Key) != nil {
			t.Error(updateBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Read, Inserted Key:", i.Key, "| Value:", readBulkResult.Value(i.Key))
		}
	}

	// Deleting Documents from DynamoDB
	for i := int64(25); i < int64(35); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		deleteResult := db.Delete(connStr, username, password, docId, i, extra)
		if deleteResult.GetError() != nil {
			t.Error(deleteResult.GetError())
		} else {
			log.Println("Deleting", deleteResult.Key())
		}
	}
	count, err = db.Count(connStr, username, password, extra)
	if err != nil {
		t.Error(err)
	} else {
		log.Println("Table Document Count:\n", count)
	}
	//Sub doc ops:
	// db.InsertSubDoc(connStr, username, password,KeyValue(),extra)

	// Bulk Deleting Documents from DynamoDB
	keyValues = nil
	for i := int64(0); i < int64(25); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)

		keyVal := KeyValue{docId, nil, i}
		keyValues = append(keyValues, keyVal)
	}
	deleteBulkResult := db.DeleteBulk(connStr, username, password, keyValues, extra)
	for _, i := range keyValues {
		if deleteBulkResult.GetError(i.Key) != nil {
			t.Error(deleteBulkResult.GetError(i.Key))
		} else {
			log.Println("Bulk Deleting, Deleted Key:", i.Key)
		}
	}
	count, err = db.Count(connStr, username, password, extra)
	if err != nil {
		t.Error(err)
	} else {
		log.Println("Table Document Count:\n", count)
	}
	//deleting table
	_, err = db.DeleteDatabase(connStr, username, password, extra)
	if err != nil {
		t.Error(err)
	} else {
		log.Println("Table deleted")
	}
	// Closing the Connection to DynamoDB
	if err = db.Close(connStr, extra); err != nil {
		t.Error(err)
		t.Fail()
	}
}
