package template

import (
	"fmt"
	"github.com/bgadrian/fastfaker/faker"
	"log"
	"testing"
)

func TestGeneratePerson(t *testing.T) {
	// Test to compare two same document generated from same seed
	//fake1 := faker.NewWithSeed(rand.NewSource(1678693916037126000))
	//fake2 := faker.NewWithSeed(rand.NewSource(1678693916037126000))
	fake1 := faker.NewFastFaker()
	fake1.Seed(1678693916037126000)
	fake2 := faker.NewFastFaker()
	fake2.Seed(1678693916037126000)
	personTemplate := InitialiseTemplate("person")
	document1 := personTemplate.GenerateDocument(fake1, "1678693916037126000", 0)
	document2 := personTemplate.GenerateDocument(fake2, "1678693916037126000", 0)

	log.Println(document1)
	log.Println(document2)
	ok, err := personTemplate.Compare(document1, document2)

	if err != nil {
		log.Println(err)
		t.Fail()
	}

	if !ok {
		log.Println("test failed while comparing the document1 and document2")
		t.Fail()
	}

	// test to update the document1 and comparing it with original document
	document3, err := personTemplate.UpdateDocument([]string{}, document1, 0, fake1)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	log.Println()
	log.Println(document3)
	log.Println()

	document4, err := personTemplate.UpdateDocument([]string{}, document2, 0, fake2)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	log.Println()
	log.Println(document4)
	log.Println()

	ok, err = personTemplate.Compare(document4, document3)
	if !ok {
		log.Println("test failed while comparing the document4 and document3")
		t.Fatal("test failed while comparing the document4 and document3")
	}

	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	//queries, err := personTemplate.GenerateQueries("bucket-1", "saurabh-1", "mishra-1")
	//if err != nil {
	//	fmt.Println(err)
	//	t.Fail()
	//} else {
	//	log.Println(queries)
	//}
	//indexes, err := personTemplate.GenerateIndexes("bucket-1", "saurabh-1", "mishra-1")
	//if err != nil {
	//	fmt.Println(err)
	//	t.Fail()
	//} else {
	//	log.Println(indexes)
	//}

}
