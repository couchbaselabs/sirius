package template

import (
	"log"
	"testing"
)

func TestGeneratePerson(t *testing.T) {
	persons := GeneratePersons(10, 1678361550549466000)
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

func TestGenerateKeys(t *testing.T) {
	keys := GenerateKeys(10, 256, 1678361550549466000)
	if len(keys) == 0 {
		t.Fail()
	}
	for _, key := range keys {
		if key == "" {
			t.Fail()
		}
		log.Println(key)
	}
}
