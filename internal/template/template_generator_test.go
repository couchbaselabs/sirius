package template

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"
)

func TestGenerateRandomJSON(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	pData := generateRandomJSON(personTemplate)
	if pStr, err := json.Marshal(pData); err != nil {
		t.Fail()
	} else {
		p := &Person{}
		if err = json.Unmarshal(pStr, p); err != nil {
			t.Fail()
		}
	}
}

func TestGeneratePerson(t *testing.T) {
	p, e := GeneratePerson()
	if e != nil {
		t.Fail()
	}
	if p == (Person{}) {
		t.Fail()
	}
}
