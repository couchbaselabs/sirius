package template

import (
	"log"
	"reflect"
	"testing"

	"github.com/bgadrian/fastfaker/faker"
)

func TestGenerateProduct(t *testing.T) {
	// Test: Comparing two documents generated from the same seed
	fake1 := faker.NewFastFaker()
	fake1.Seed(1678693916037126000)
	fake2 := faker.NewFastFaker()
	fake2.Seed(1678693916037126000)
	productTemplate := InitialiseTemplate("product")
	document1 := productTemplate.GenerateDocument(fake1, "1678693916037126000", 0)
	document2 := productTemplate.GenerateDocument(fake2, "1678693916037126000", 0)

	log.Println("Document 1:", document1)
	log.Println("Document 2:", document2)

	ok, errDocCompare := productTemplate.Compare(document1, document2)
	if errDocCompare != nil {
		log.Println(errDocCompare)
		t.Fail()
	}
	if !ok {
		log.Println("test failed while comparing the document1 and document2")
		t.Fail()
	}

	// Test: Updating doc1 (becomes doc3) and then checking if it was updated or not by comparing doc1 and doc3
	document3, errUpdateDoc1 := productTemplate.UpdateDocument([]string{}, document1, 0, fake1)
	if errUpdateDoc1 != nil {
		log.Println(errUpdateDoc1)
		t.Fail()
	}

	log.Println("Document 3:", document3)

	// Test: Updating doc2 (becomes doc4) and then comparing doc3 and doc4
	document4, errUpdateDoc2 := productTemplate.UpdateDocument([]string{}, document2, 0, fake2)
	if errUpdateDoc2 != nil {
		log.Println(errUpdateDoc2)
		t.Fail()
	}
	log.Println("Document 4:", document4)

	ok, errDocCompare = productTemplate.Compare(document3, document4)
	if errDocCompare != nil {
		log.Println(errDocCompare)
		t.Fail()
	}
	if !ok {
		log.Println("test failed while comparing the document3 and document4")
		t.Fatal("test failed while comparing the document3 and document4")
	}

	// Test: Trying to decode doc3 into template.Product type
	product, ok := document3.(*Product)
	if !ok {
		log.Println("unable to decode document3 to product template")
		t.Fail()
	}
	log.Println("Type of document3:", reflect.TypeOf(document3), reflect.TypeOf(product))
	log.Println("Document 3 decoded as Product type:", product)

	// Test: Size of Documents
	log.Println(calculateSizeOfStruct(document3))
}
