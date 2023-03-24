package template

import (
	"fmt"
	"github.com/jaswdr/faker"
	"reflect"
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
	Payload   string  `json:"payload"`
}

func (p *Person) GenerateDocument(fake *faker.Faker) interface{} {
	return &Person{
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

func (p *Person) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, fake *faker.Faker) (interface{}, error) {
	person, ok := lastUpdatedDocument.(*Person)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
	}
	// Generating the original document
	checkFields := make(map[string]struct{})
	for _, s := range fieldsToChange {
		checkFields[s] = struct{}{}
	}
	if _, ok := checkFields["firstName"]; ok {
		person.FirstName = fake.Person().FirstName()
	}
	if _, ok := checkFields["lastName"]; ok {
		person.Lastname = fake.Person().LastName()
	}
	if _, ok := checkFields["age"]; ok {
		person.Age = fake.IntBetween(0, 100)
	}
	if _, ok := checkFields["email"]; ok {
		person.Email = fake.Internet().CompanyEmail()
	}
	if _, ok := checkFields["address.State"]; ok {
		person.Address.State = fake.Address().State()
	}
	if _, ok := checkFields["address.city"]; ok {
		person.Address.City = fake.Address().City()
	}
	if _, ok := checkFields["address.street"]; ok {
		person.Address.Street = fake.Address().StreetName()
	}
	if _, ok := checkFields["address.zipcode"]; ok {
		person.Address.Zipcode = fake.Address().PostCode()
	}
	if _, ok := checkFields["address.country"]; ok {
		person.Address.Country = fake.Address().Country()
	}
	return person, nil
}

func (p *Person) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	p1, ok := document1.(*Person)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to person template")
	}
	p2, ok := document2.(*Person)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to person template")
	}
	return reflect.DeepEqual(p1, p2), nil
}
