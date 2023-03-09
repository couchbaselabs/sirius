package template

import (
	"log"
	"testing"
)

func TestGeneratePerson(t *testing.T) {
	persons := GeneratePerson(10, 1678361550549466000)
	if len(persons) == 0 {
		t.Fail()
	}
	for _, p := range persons {
		log.Println(p)
	}
}
