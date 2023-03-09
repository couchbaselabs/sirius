package template

import (
	"github.com/jaswdr/faker"
	"math/rand"
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
}

// GeneratePersons return a Person with random details
func GeneratePersons(count int, seed int64) []Person {

	p := make([]Person, count)

	fake := faker.NewWithSeed(rand.NewSource(seed))
	for i := 0; i < count; i++ {
		p[i].FirstName = fake.Person().FirstName()
		p[i].Lastname = fake.Person().LastName()
		p[i].Age = fake.IntBetween(0, 100)
		p[i].Email = fake.Internet().CompanyEmail()
		p[i].Address.State = fake.Address().State()
		p[i].Address.City = fake.Address().City()
		p[i].Address.Street = fake.Address().StreetName()
		p[i].Address.Zipcode = fake.Address().PostCode()
		p[i].Address.Country = fake.Address().Country()
	}
	return p
}
