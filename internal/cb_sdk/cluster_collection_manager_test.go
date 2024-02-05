package cb_sdk

import (
	"errors"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/docgenerator"
	"github.com/couchbaselabs/sirius/internal/meta_data"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"strings"
	"testing"
)

func TestConfigConnectionManager(t *testing.T) {
	cConfig := &ClusterConfig{
		Username:          "Administrator",
		Password:          "password",
		ConnectionString:  "couchbases://172.23.100.12",
		CompressionConfig: CompressionConfig{},
		TimeoutsConfig:    TimeoutsConfig{},
	}

	cmObj := ConfigConnectionManager()

	if _, err := cmObj.GetCluster(cConfig); err != nil {
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

		m := meta_data.NewMetaData()
		cm1 := m.GetCollectionMetadata("x")

		cm2 := m.GetCollectionMetadata("x")

		if cm1.Seed != cm2.Seed {
			t.Fail()
		}

		temp := template.InitialiseTemplate("person")
		g := docgenerator.Generator{
			Template: temp,
		}
		gen := &docgenerator.Generator{
			KeySize:   25,
			DocType:   "json",
			KeyPrefix: "",
			KeySuffix: "",
			Template:  template.InitialiseTemplate("person"),
		}
		for i := int64(0); i < int64(10000); i++ {
			key := i + cm1.Seed
			docId := gen.BuildKey(key)
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			doc, _ := g.Template.GenerateDocument(&fake, 100)
			//log.Println(docId, doc)
			_, e := c.Collection.Upsert(docId, doc, nil)
			if e != nil {
				log.Println(e.Error())
				t.Error(e)
			}
		}
		for i := int64(0); i < int64(10000); i++ {
			docId := gen.BuildKey(i + cm1.Seed)
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

func TestGetClusterIdentifier(t *testing.T) {
	clusterIdentifier, err := GetClusterIdentifier("couchbases://172.23.100.17,172.23.100.18,172.23.100.19," +
		"172.23.100.20")
	log.Println(clusterIdentifier)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	if strings.Compare(clusterIdentifier, "172.23.100.17,172.23.100.18,172.23.100.19,172.23.100.20") != 0 {
		t.Fail()
	}

	clusterIdentifier, err = GetClusterIdentifier("couchbases://172.23.100.17,172.23.100.18,172.23.100.19," +
		"172.23.100.20?kv_pool_size=32")
	log.Println(clusterIdentifier)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	if strings.Compare(clusterIdentifier, "172.23.100.17,172.23.100.18,172.23.100.19,172.23.100.20") != 0 {
		t.Fail()
	}

}
