package sdk

import (
	"errors"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/task_meta_data"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"testing"
)

func TestConfigConnectionManager(t *testing.T) {
	cConfig := &ClusterConfig{
		Username:          "Administrator",
		Password:          "password",
		ConnectionString:  "couchbase://172.23.138.142",
		CompressionConfig: CompressionConfig{},
		TimeoutsConfig:    TimeoutsConfig{},
	}

	cmObj := ConfigConnectionManager()
	if _, err := cmObj.GetBucket(cConfig, "lol"); err != nil {
		log.Println(err)
	}
	if _, err := cmObj.GetBucket(cConfig, "lol"); err != nil {
		log.Println(err)
	}
	if _, err := cmObj.GetBucket(cConfig, "lol"); err != nil {
		log.Println(err)
	}
	c, err := cmObj.GetCollection(cConfig, "lol", "_default", "_default")
	if err != nil {
		log.Println(err.Error())
		KVError := &gocb.KeyValueError{}
		timeOutError := &gocb.TimeoutError{}
		if errors.As(err, KVError) {
			log.Println(KVError)
		} else if errors.As(err, timeOutError) {
			log.Println(timeOutError)
		} else {
			log.Println("Failed to parse Error")
		}
		t.Error(err)
	} else {

		m := task_meta_data.NewMetaData()
		cm1 := m.GetCollectionMetadata("x", 255, 1024, "json", "", "", "person")

		cm2 := m.GetCollectionMetadata("x", 255, 1024, "json", "", "", "person")

		if cm1.Seed != cm2.Seed {
			t.Fail()
		}

		temp := template.InitialiseTemplate(cm1.TemplateName)
		g := docgenerator.Generator{
			Seed:     cm1.Seed,
			SeedEnd:  cm1.Seed,
			Template: temp,
		}
		for i := 0; i < 10; i++ {
			docId, key := g.GetDocIdAndKey(i)
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			doc, _ := g.Template.GenerateDocument(&fake, 1024)
			log.Println(docId, doc)
			_, e := c.Collection.Upsert(docId, doc, nil)
			if e != nil {
				log.Println(e.Error())
				t.Error(e)
			}
		}
		for i := 0; i < 10; i++ {
			docId, _ := g.GetDocIdAndKey(i)
			r, e := c.Collection.Get(docId, nil)
			if e != nil {
				t.Error(e)
			} else {
				var resultFromHost map[string]any
				r.Content(&resultFromHost)
				log.Println(resultFromHost)
			}
		}

	}

}
