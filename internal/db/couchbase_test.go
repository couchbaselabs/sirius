package db

import (
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/meta_data"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"testing"
)

func TestNewCouchbaseConnectionManager(t *testing.T) {
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
		KeySize:  25,
		DocType:  "json",
		Template: template.InitialiseTemplate("person"),
	}
	for i := int64(0); i < int64(10); i++ {
		key := i + cm1.Seed
		docId := gen.BuildKey(key)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		doc, _ := g.Template.GenerateDocument(&fake, 100)
		//log.Println(docId, doc)
		x := db.Update(connStr, username, password, docId, doc, Extras{
			Bucket: "default",
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println(x)
		}

	}

	for i := int64(0); i < int64(10); i++ {
		docId := gen.BuildKey(i + cm1.Seed)
		x := db.Read(connStr, username, password, docId, Extras{
			Bucket: "default",
		})
		if x.GetError() != nil {
			t.Error(x.GetError())
		} else {
			log.Println(x)
		}
	}
	log.Println(db.Close(connStr))
}
