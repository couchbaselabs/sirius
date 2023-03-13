package template

import "github.com/jaswdr/faker"

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

// GeneratePersons return a Person with random details.
func GeneratePersons(count int64, fake faker.Faker) []*Person {

	p := make([]*Person, count)
	for i := int64(0); i < count; i++ {
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
		}
	}
	return p
}
