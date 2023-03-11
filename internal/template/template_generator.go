package template

import (
	"fmt"
	"github.com/jaswdr/faker"
	"math/rand"
	"time"
)

type Address struct {
	Street  string `json:"street,omitempty"`
	City    string `json:"city,omitempty"`
	State   string `json:"state,omitempty"`
	Zipcode string `json:"zipcode,omitempty"`
	Country string `json:"country,omitempty"`
}

type Person struct {
	FirstName string  `json:"firstName,omitempty"`
	Lastname  string  `json:"lastName,omitempty"`
	Age       int     `json:"age,omitempty"`
	Email     string  `json:"email,omitempty"`
	Address   Address `json:"address,omitempty"`
	Summary   string  `json:"summary,omitempty"`
	//Summary2  string  `json:"summary2,omitempty"`
}

// GeneratePersons return a Person with random details
func GeneratePersons(count int, seed int64) []*Person {

	p := make([]*Person, count)

	fake := faker.NewWithSeed(rand.NewSource(seed))
	for i := 0; i < count; i++ {
		p[i] = &Person{
			FirstName: fake.Person().FirstName(),
			Lastname:  fake.Person().LastName(),
			Age:       fake.IntBetween(0, 100),
			Email:     fake.Internet().CompanyEmail(),
			Address: Address{
				State:   fake.Address().State(),
				City:    fake.Address().City(),
				Street:  fake.Address().StreetName(),
				Zipcode: fake.Address().PostCode(),
				Country: fake.Address().Country(),
			},
			Summary: fake.Lorem().Sentence(1000),
			//Summary2: fake.Lorem().Sentence(1000),
		}
	}

	return p
}

func GenerateKeys(count int, size int, seed int64) []string {
	var keys []string
	fake := faker.NewWithSeed(rand.NewSource(seed))
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%d", time.Now().UnixNano())
		time.Sleep(1 * time.Microsecond)
		key += fmt.Sprintf("%d%s", time.Now().UnixNano(), fake.BinaryString().BinaryString(size))
		key = key[:size]
		keys = append(keys, key)
	}
	return keys
}
