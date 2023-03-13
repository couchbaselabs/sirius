package template

import (
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"testing"
)

func TestGeneratePerson(t *testing.T) {
	persons := GeneratePersons(10, faker.NewWithSeed(rand.NewSource(1678693916037126000)))
	if len(persons) == 0 {
		t.Fail()
	}
	for _, p := range persons {
		if p == nil {
			t.Fail()
		}
		log.Println(*p)
	}
}
