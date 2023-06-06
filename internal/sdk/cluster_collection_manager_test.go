package sdk

import (
	"errors"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
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
		ConnectionString:  "couchbase://172.23.120.59",
		CompressionConfig: CompressionConfig{},
		TimeoutsConfig:    TimeoutsConfig{},
	}
	cmObj := ConfigConnectionManager()
	c, err := cmObj.GetCollection(cConfig, "default", "_default", "_default")
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
		temp := template.InitialiseTemplate("person")
		g := docgenerator.Generator{
			Seed:     1678383842563225000,
			SeedEnd:  1678383842563225000,
			Template: temp,
		}
		for i := int64(0); i < int64(10); i++ {
			docId, key := g.GetDocIdAndKey(i)
			fake := faker.NewWithSeed(rand.NewSource(key))
			doc, _ := g.Template.GenerateDocument(&fake, 1024)
			log.Println(docId, doc)
			_, e := c.Upsert(docId, doc, nil)
			if e != nil {
				log.Println(e.Error())
				t.Error(e)
			}
		}
		for i := int64(0); i < int64(10); i++ {
			docId, _ := g.GetDocIdAndKey(i)
			r, e := c.Get(docId, nil)
			if e != nil {
				t.Error(e)
			} else {
				var resultFromHost map[string]interface{}
				r.Content(&resultFromHost)
				log.Println(resultFromHost)
			}
		}

	}

}
