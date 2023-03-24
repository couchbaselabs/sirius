package template

import (
	"fmt"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"testing"
)

func TestGeneratePerson(t *testing.T) {
	// Test to compare two same document generated from same seed
	fake1 := faker.NewWithSeed(rand.NewSource(1678693916037126000))
	fake2 := faker.NewWithSeed(rand.NewSource(1678693916037126000))
	template, _ := InitialiseTemplate("person")
	document1 := template.GenerateDocument(&fake1)
	document2 := template.GenerateDocument(&fake2)
	log.Println(document1)
	log.Println(document2)
	ok, err := template.Compare(document1, document2)

	if err != nil {
		log.Println(err)
		t.Fail()
	}

	if !ok {
		t.Fail()
	}

	// test to update the document1 and comparing it with original document
	document3, err := template.UpdateDocument([]string{"firstName", "lastName", "age"}, document1, &fake1)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	document1Updated, ok := document3.(*Person)
	if !ok {
		t.Fail()
	}
	log.Println(document1Updated, document1)

	ok, err = template.Compare(document1Updated, document1)

	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	if !ok {
		t.Fail()
	}

}
