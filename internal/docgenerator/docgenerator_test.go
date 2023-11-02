package docgenerator

import (
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"math/rand"
	"testing"
)

func TestGenerator_GetNextKey(t *testing.T) {

	temp := template.InitialiseTemplate("person")

	g := Generator{
		Seed:     1678383842563225000,
		SeedEnd:  1678383842563225000,
		Template: temp,
	}
	for i := int64(0); i < int64(10000000); i++ {
		_, key := g.GetDocIdAndKey(i)
		fake := faker.NewWithSeed(rand.NewSource(int64(key)))
		_, err := g.Template.GenerateDocument(&fake, 1024)
		if err != nil {
			t.Fail()
		}
	}

}
