package docgenerator

import (
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
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
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		_, err := g.Template.GenerateDocument(&fake, 1024)
		if err != nil {
			t.Fail()
		}
	}

}
