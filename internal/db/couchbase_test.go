package db

import (
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/barkha06/sirius/internal/meta_data"
	"github.com/barkha06/sirius/internal/template"
	"github.com/jaswdr/faker"
)

func TestCouchbase(t *testing.T) {
	/*
		This test does the following
		1. Upsert in the range of 0-10
		2. Upsert in the range of 0-10
		3. Read from 0-10
		4. SubDoc Insert from 0-5
		5. SubDoc Read from 0-5
		6. Delete from 5-10
		7. Touch 0-5 with expiry of 30 second
		8. Read 0-5 after 30 second, Fail if document found.

	*/
	db, err := ConfigDatabase("couchbase")
	if err != nil {
		t.Fatal(err)
	}
	connStr := "couchbase://172.23.100.12"
	username := "Administrator"
	password := "password"
	if err := db.Connect(connStr, username, password, Extras{}); err != nil {
		t.Error(err)
	}

	m := meta_data.NewMetaData()
	cm1 := m.GetCollectionMetadata("x")

	temp := template.InitialiseTemplate("person")
	g := docgenerator.Generator{
		Template: temp,
	}
	gen := &docgenerator.Generator{
		KeySize:  0,
		DocType:  "json",
		Template: template.InitialiseTemplate("person"),
	}
	// update
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		doc, _ := g.Template.GenerateDocument(docID, &fake, 10)
		//log.Println(docId, Doc)
		x := db.Update(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, Extras{
			Bucket: "default",
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println("Update", x.Key(), " ", x.Value())
		}

	}
	// update
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		doc, _ := g.Template.GenerateDocument(docID, &fake, 10)
		//log.Println(docId, Doc)
		x := db.Update(connStr, username, password, KeyValue{
			Key:    docId,
			Doc:    doc,
			Offset: i,
		}, Extras{
			Bucket: "default",
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println("Update", x.Key(), " ", x.Value())
		}

	}
	// Read
	for i := int64(0); i < int64(10); i++ {
		docId := gen.BuildKey(i + cm1.Seed)
		x := db.Read(connStr, username, password, docId, i, Extras{
			Bucket: "default",
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println("Read", x.Value(), " key ", x.Key())
		}
	}

	// SubDoc Insert
	for i := int64(0); i < int64(5); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		var keyValues []KeyValue
		offsetCount := int64(0)
		for key, value := range gen.Template.GenerateSubPathAndValue(&fake, 10) {
			keyValues = append(keyValues, KeyValue{
				Key:    key,
				Doc:    value,
				Offset: offsetCount,
			})
			offsetCount++
		}

		x := db.InsertSubDoc(connStr, username, password, docId, keyValues, i, Extras{
			Bucket: "default",
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println("InsertSubDoc", x.Values(), " key ", x.Key())
		}
	}

	// SubDoc Read
	for i := int64(0); i < int64(5); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		var keyValues []KeyValue
		offsetCount := int64(0)
		for key, _ := range gen.Template.GenerateSubPathAndValue(&fake, 10) {
			keyValues = append(keyValues, KeyValue{
				Key:    key,
				Offset: offsetCount,
			})
			offsetCount++
		}

		x := db.ReadSubDoc(connStr, username, password, docId, keyValues, i, Extras{
			Bucket: "default",
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println("ReadSubDoc", x.Values(), " key ", x.Key())
		}
	}

	// Delete
	for i := int64(5); i < int64(10); i++ {
		docId := gen.BuildKey(i + cm1.Seed)
		x := db.Delete(connStr, username, password, docId, i, Extras{
			Bucket: "default",
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println("Delete", x.Key(), " ", x.GetExtra())
		}
	}

	// Touch
	for i := int64(0); i < int64(5); i++ {
		docId := gen.BuildKey(i + cm1.Seed)
		x := db.Touch(connStr, username, password, docId, i, Extras{
			Bucket: "default",
			Expiry: 30,
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println("Touch", x.Key(), " ", x.GetExtra())
		}
	}

	time.Sleep(35 * time.Second)

	for i := int64(0); i < int64(5); i++ {
		docId := gen.BuildKey(i + cm1.Seed)
		x := db.Read(connStr, username, password, docId, i, Extras{
			Bucket: "default",
		})
		if x.GetError() == nil {
			t.Error("expected document should not be on server", docId)
			t.Fail()
		} else {
			log.Println(x)
		}
	}

	if err = db.Close(connStr); err != nil {
		t.Error(err)
		t.Fail()
	}
}

func TestCouchbase_CreateBulk(t *testing.T) {
	db, err := ConfigDatabase("couchbase")
	//gocb.SetLogger(gocb.VerboseStdioLogger())
	if err != nil {
		t.Fatal(err)
	}
	connStr := "couchbase://172.23.100.12"
	username := "Administrator"
	password := "password"
	if err := db.Connect(connStr, username, password, Extras{}); err != nil {
		t.Error(err)
	}

	m := meta_data.NewMetaData()
	cm1 := m.GetCollectionMetadata("x")

	batchSize := 100
	totalBatches := 20000

	wg := sync.WaitGroup{}

	wg.Add(totalBatches)

	for i := 0; i < totalBatches; i++ {
		go func(x int) {
			defer wg.Done()
			log.Println(x)
			gen := &docgenerator.Generator{
				DocType:  "json",
				Template: template.InitialiseTemplate("small"),
			}
			var keyValue []KeyValue
			for k := x * batchSize; k < (x+1)*batchSize; k++ {
				key := int64(k) + cm1.Seed
				docId := gen.BuildKey(key)
				fake := faker.NewWithSeed(rand.NewSource(int64(key)))
				doc, _ := gen.Template.GenerateDocument(docID, &fake, 10)
				keyValue = append(keyValue, KeyValue{
					Key:    docId,
					Doc:    doc,
					Offset: int64(k),
				})
			}
			//log.Println(keyValues)
			result := db.CreateBulk(connStr, username, password, keyValue, Extras{
				Bucket: "default",
			})
			bulkResult, ok := result.(*couchbaseBulkOperationResult)
			if !ok {
				t.Fatal("error decoding bulkResult")
			}
			log.Println(len(bulkResult.keyValues))

		}(i)
	}
	wg.Wait()

}
