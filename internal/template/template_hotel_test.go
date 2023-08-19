package template

import (
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"testing"
)

func TestGenerateHotel(t *testing.T) {
	// Test to compare two same document generated from same seed
	fake1 := faker.NewWithSeed(rand.NewSource(1678693916037126000))
	fake11 := faker.NewWithSeed(rand.NewSource(1678693916037126000))
	fake2 := faker.NewWithSeed(rand.NewSource(1678693916037126000))
	template := InitialiseTemplate("hotel")
	document1, err := template.GenerateDocument(&fake1, 0)
	if err != nil {
		t.Fail()
	}
	document2, err := template.GenerateDocument(&fake2, 0)
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

	template.UpdateDocument([]string{}, document1, &fake1)

	//// test to update the document1 and comparing it with original document
	//document3, err := template.UpdateDocument([]string{}, document1, &fake1)
	//log.Println(document3)
	//if err != nil {
	//	log.Println(err)
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
	//ok, err = template.Compare(document1Updated, document1)
	//
	//if err != nil {
	//	fmt.Println(err)
	//	t.Fail()
	//}
	//
	//if !ok {
	//	log.Println("test failed while comparing the document1 and document1updated")
	//	t.Fail()
	//}

	document1Copy, _ := template.GenerateDocument(&fake11, 0)
	template.UpdateDocument([]string{}, document1Copy, &fake11)
	ok, err = template.Compare(document1Copy, document1)

	if err != nil {
		t.Fail()
	}
	if !ok {
		t.Fail()
	}
	//ok, err = template.Compare(document1Copy, document2)
	//if err != nil {
	//	t.Fail()
	//}
	//if !ok {
	//	t.Fail()
	//}

}
