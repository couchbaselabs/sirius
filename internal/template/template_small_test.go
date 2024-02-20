package template

import (
	"fmt"
	"github.com/bgadrian/fastfaker/faker"

	"log"
	"testing"
)

func TestGenerateSmall(t *testing.T) {
	//Test to compare two same document generated from same seed
	fake1 := faker.NewFastFaker()
	fake1.Seed(1678693916037126002)
	fake2 := faker.NewFastFaker()
	fake2.Seed(1678693916037126001)
	smallTemplate := InitialiseTemplate("small")
	document1 := smallTemplate.GenerateDocument(fake1, "1678693916037126002", 10)
	document2 := smallTemplate.GenerateDocument(fake2, "1678693916037126001", 10)

	log.Println(document1)
	log.Println(document2)
	ok, err := smallTemplate.Compare(document1, document2)

	if err != nil {
		log.Println(err)
		t.Fail()
	}

	if ok {
		log.Println("document1 is same as document 2 but expected different")
		t.Fail()
	}

	// test to update the document1 and comparing it with original document
	document3, err := smallTemplate.UpdateDocument([]string{}, document1, 10, fake1)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	log.Println()
	log.Println(document3)
	log.Println()

	document4, err := smallTemplate.UpdateDocument([]string{}, document2, 10, fake2)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	log.Println()
	log.Println(document4)
	log.Println()

	ok, err = smallTemplate.Compare(document4, document3)
	if ok {
		log.Println("document3 is same as document4  but expected different")
		t.Fail()
	}

	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	//queries, err := smallTemplate.GenerateQueries("bucket-1", "saurabh-1", "mishra-1")
	//if err != nil {
	//	fmt.Println(err)
	//	t.Fail()
	//} else {
	//	log.Println(queries)
	//}
	//indexes, err := smallTemplate.GenerateIndexes("bucket-1", "saurabh-1", "mishra-1")
	//if err != nil {
	//	fmt.Println(err)
	//	t.Fail()
	//} else {
	//	log.Println(indexes)
	//}

}
