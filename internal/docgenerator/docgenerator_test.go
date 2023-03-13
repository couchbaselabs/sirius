package docgenerator

import (
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"testing"
)

func TestNext(t *testing.T) {
	g := Generator{
		Itr:       0,
		End:       2,
		BatchSize: 10,
		DocType:   "",
		KeySize:   120,
		Seed:      1678383842563225000,
		Fake:      faker.NewWithSeed(rand.NewSource(1678383842563225000)),
	}
	for i := g.Itr; i < g.End; i++ {
		personsTemplate := g.Next(g.BatchSize)
		for _, person := range personsTemplate {
			log.Println(person)
		}
	}

	check := make(map[string]struct{})
	for i := g.Itr; i < g.End; i++ {
		for index := int64(0); index < g.BatchSize; index++ {
			if _, ok := check[g.GetKey(i, g.BatchSize, index, g.Seed)]; ok {
				t.Fail()
			}
		}
	}

}
