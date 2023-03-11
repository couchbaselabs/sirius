package docgenerator

import (
	"log"
	"testing"
)

func TestNext(t *testing.T) {
	g := Generator{
		Itr:       0,
		End:       1,
		BatchSize: 1,
		DocType:   "",
		KeySize:   120,
		Seed:      []int64{1678383842563225000},
	}

	for _, seed := range g.Seed {
		keys, personsTemplate := g.Next(seed)

		for index, key := range keys {
			log.Println(key, *personsTemplate[index])
			log.Println()
		}
	}
}
