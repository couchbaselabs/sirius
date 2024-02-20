package docgenerator

import (
	"github.com/bgadrian/fastfaker/faker"
	"github.com/couchbaselabs/sirius/internal/template"
	"log"
	"testing"
)

func TestGenerator_GetNextKey(t *testing.T) {

	temp := template.InitialiseTemplate("person")
	seed := int64(1678383842563225000)

	g := &Generator{
		Template: temp,
		KeySize:  128,
	}
	for i := int64(0); i < int64(10); i++ {
		key := seed + i
		docId := g.BuildKey(key)
		log.Println(docId)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		doc := g.Template.GenerateDocument(fake, docId, 1024)
		if doc == nil {
			t.Fail()
		}
	}

}
