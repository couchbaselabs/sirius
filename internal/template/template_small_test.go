package template

import (
	"fmt"
	"log"
	"math/rand"

	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/jaswdr/faker"

	"testing"
)

func TestGenerateSmall(t *testing.T) {
	// Test to compare two same document generated from same seed
	var seed int64 = 1678693916037126000
	fake1 := faker.NewWithSeed(rand.NewSource(seed))
	fake2 := faker.NewWithSeed(rand.NewSource(seed))
	template := InitialiseTemplate("small")
	gen := &docgenerator.Generator{
		KeySize:  4,
		DocType:  "json",
		Template: template,
	}
	docID := gen.BuildKey(seed)
	document1, err := template.GenerateDocument(docID, &fake1, 12)
	if err != nil {
		t.Fail()
	}
	document2, err := template.GenerateDocument(docID, &fake2, 12)
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
		t.Fail()
	}

	// test to update the document1 and comparing it with original document
	document3, err := template.UpdateDocument([]string{}, document1, 20, &fake1)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	document1Updated, ok := document3.(*SmallTemplate)
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
