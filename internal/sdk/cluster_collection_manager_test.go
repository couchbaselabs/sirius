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

		m := task_meta_data.NewMetaData()
		cm1 := m.GetCollectionMetadata("x", 255, 1024, "json", "", "", "person")

		cm2 := m.GetCollectionMetadata("x", 255, 1024, "json", "", "", "person")

		if cm1.Seed != cm2.Seed {
			t.Fail()
		}

		temp := template.InitialiseTemplate(cm1.TemplateName)
		g := docgenerator.Generator{
			Seed:     int64(cm1.Seed),
			SeedEnd:  int64(cm1.Seed),
			Template: temp,
		}
		for i := int64(0); i < int64(10); i++ {
			docId, key := g.GetDocIdAndKey(i)
			fake := faker.NewWithSeed(rand.NewSource(int64(key)))
			doc, _ := g.Template.GenerateDocument(&fake, 100)
			//log.Println(docId, doc)
			_, e := c[int(i)%len(c)].Collection.Upsert(docId, doc, nil)
			if e != nil {
				log.Println(e.Error())
				t.Error(e)
			}
		}
		for i := int64(0); i < int64(10); i++ {
			docId, _ := g.GetDocIdAndKey(i)
			r, e := c[int(i)%len(c)].Collection.Get(docId, nil)
			if e != nil {
				t.Error(e)
			} else {
				var resultFromHost map[string]any
				r.Content(&resultFromHost)
				log.Println(resultFromHost)
			}
		}

	}

	cmObj.DisconnectAll()

}

func TestGetClusterIdentfier(t *testing.T) {
	clusterIdentifier, err := getClusterIdentifier("couchbases://172.23.100.17,172.23.100.18,172.23.100.19," +
		"172.23.100.20")
	log.Println(clusterIdentifier)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	if strings.Compare(clusterIdentifier, "172.23.100.17,172.23.100.18,172.23.100.19,172.23.100.20") != 0 {
		t.Fail()
	}

	clusterIdentifier, err = getClusterIdentifier("couchbases://172.23.100.17,172.23.100.18,172.23.100.19," +
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
