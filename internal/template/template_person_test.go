package template

import (
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"
)

func TestGeneratePerson(t *testing.T) {
	// Test to compare two same document generated from same seed
	var seed int64 = 1678693916037126000
	fake1 := faker.NewWithSeed(rand.NewSource(seed))
	fake2 := faker.NewWithSeed(rand.NewSource(seed))
	template := InitialiseTemplate("person")
	gen := &docgenerator.Generator{
		KeySize:  25,
		DocType:  "json",
		Template: template,
	}
	docID := gen.BuildKey(seed)
	document1, err := template.GenerateDocument(docID, &fake1, 0)
	if err != nil {
		t.Fail()
	}
	document2, err := template.GenerateDocument(docID, &fake2, 0)
	if err != nil {
		t.Fail()
	}
	log.Println(document1)
	log.Println(document2)
	ok, err := template.Compare(document1, document2)

	if err != nil {
		log.Println(err)
		t.Fail()
	}

	if !ok {
		log.Println("test failed while comparing the document1 and document2")
		t.Fail()
	}

	// test to update the document1 and comparing it with original document
	document3, err := template.UpdateDocument([]string{}, document1, 2100, &fake1)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	log.Println()
	log.Println(document3)
	log.Println()

	document1Updated, ok := document3.(*Person)
	if !ok {
		log.Println("test failed while updating the document3")
		t.Fail()
	}

	ok, err = template.Compare(document1Updated, document1)
	if !ok {
		log.Println("test failed while comparing the document1 and document1updated")
		t.Fatal("test failed while comparing the document1 and document1updated")
	}

	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	queries, err := template.GenerateQueries("bucket-1", "saurabh-1", "mishra-1")
	if err != nil {
		fmt.Println(err)
		t.Fail()
	} else {
		log.Println(queries)
	}
	indexes, err := template.GenerateIndexes("bucket-1", "saurabh-1", "mishra-1")
	if err != nil {
		fmt.Println(err)
		t.Fail()
	} else {
		log.Println(indexes)
	}

}
