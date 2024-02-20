package test

import (
	"log"
	"math/rand"
	"testing"

	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/barkha06/sirius/internal/template"
	"github.com/jaswdr/faker"
)

func TestGenerateHotel(t *testing.T) {
	// Test to compare two same document generated from same seed
	var seed int64 = 1678693916037126000
	fake1 := faker.NewWithSeed(rand.NewSource(seed))
	fake11 := faker.NewWithSeed(rand.NewSource(seed))
	fake2 := faker.NewWithSeed(rand.NewSource(seed))
	temp := template.InitialiseTemplate("hotel")
	gen := &docgenerator.Generator{
		KeySize:  25,
		DocType:  "json",
		Template: temp,
	}
	docID := gen.BuildKey(seed)
	document1, err := temp.GenerateDocument(docID, &fake1, 100)
	if err != nil {
		t.Fail()
	}
	document2, err := temp.GenerateDocument(docID, &fake2, 100)
	if err != nil {
		t.Fail()
	}
	log.Println("Hello")
	log.Println(document1)
	log.Println(document2)
	ok, err := temp.Compare(document1, document2)

	if err != nil {
		log.Println(err)
		t.Fail()
	}

	if !ok {
		log.Println("test failed while comparing the document1 and document2")
		t.Fail()
	}

	temp.UpdateDocument([]string{}, document1, 0, &fake1)

	//// test to update the document1 and comparing it with original document
	//document3, err_sirius := template.UpdateDocument([]string{}, document1, &fake1)
	//log.Println(document3)
	//if err_sirius != nil {
	//	log.Println(err_sirius)
	//	t.Fail()
	//}
	//
	//document1Updated, ok := document3.(*Hotel)
	//if !ok {
	//	log.Println("test failed while updating the document3")
	//	t.Fail()
	//}
	//log.Println(document1Updated, document1)
	//
	//ok, err_sirius = template.Compare(document1Updated, document1)
	//
	//if err_sirius != nil {
	//	fmt.Println(err_sirius)
	//	t.Fail()
	//}
	//
	//if !ok {
	//	log.Println("test failed while comparing the document1 and document1updated")
	//	t.Fail()
	//}

	document1Copy, _ := temp.GenerateDocument(docID, &fake11, 0)
	temp.UpdateDocument([]string{}, document1Copy, 0, &fake11)
	ok, err = temp.Compare(document1Copy, document1)

	if err != nil {
		t.Fail()
	}
	if !ok {
		t.Fail()
	}

}
