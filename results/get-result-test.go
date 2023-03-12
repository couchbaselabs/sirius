package results

import (
	"log"
	"testing"
)

func TestReadResultFromFile(t *testing.T) {
	seed := "1678619466166244000"
	if val, err := ReadResultFromFile(seed, false); err != nil {
		log.Println(err.Error())
		t.Fail()
	} else {
		log.Println(val)
	}
}
