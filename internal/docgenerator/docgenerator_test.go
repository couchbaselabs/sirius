package docgenerator

import (
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"testing"
)

func TestGenerator_GetNextKey(t *testing.T) {

	temp, err := template.InitialiseTemplate("person")
	if err != nil {
		t.Fail()
	}
	g := Generator{
		Itr:       0,
		ItrEnd:    2,
		BatchSize: 10,
		DocType:   "",
		KeySize:   120,
		Seed:      1678383842563225000,
		template:  temp,
	}
	for i := g.Itr; i < g.ItrEnd; i++ {
		for index := int64(0); index < g.BatchSize; index++ {
			docId, key := g.GetDocIdAndKey(i, g.BatchSize, index, g.Seed)
			log.Println(key, docId)
			fake := faker.NewWithSeed(rand.NewSource(key))
			doc, ok := g.template.GenerateDocument(&fake).(*template.Person)
			if !ok {
				t.Fail()
			}
			log.Println(doc)
		}
	}
}
