package template

import (
	"encoding/json"
	"fmt"
	"github.com/jaswdr/faker"
	"reflect"
)

var maritalChoices = []string{"Single", "Married", "Divorcee"}
var bodyColor = []string{"Dark", "Fair", "Brown", "Grey"}
var hobbyChoices = []string{"Video Gaming", "Football", "Basketball", "Cricket",
	"Hockey", "Running", "Walking", "Guitar", "Flute", "Piano", "Chess", "Puzzle", "Skating", "Travelling"}
var HairType = []string{"straight", "wavy", "curly", "Coily"}
var HairColor = []string{"Red", "Green", "Yellow", "Grey", "Brown", "Black"}
var HairLength = []string{"Long", "Short", "Medium"}
var HairThickness = []string{"Thick", "Thin", "Medium"}
var BodyType = []string{"Ectomorph", "endomorph", "Mesomorph", "triangle", "Inverted triangle",
	"Rectangle", "Hourglass", "apple"}

type Address struct {
	Street  string `json:"street,omitempty"`
	City    string `json:"city,omitempty"`
	State   string `json:"state,omitempty"`
	Zipcode string `json:"zipcode,omitempty"`
	Country string `json:"country,omitempty"`
}

type Hair struct {
	Type      string `json:"type,omitempty"`
	Colour    string `json:"colour,omitempty"`
	Length    string `json:"length,omitempty"`
	Thickness string `json:"thickness,omitempty"`
}

type Attribute struct {
	Weight   int    `json:"weight,omitempty"`
	Height   int    `json:"height,omitempty"`
	Colour   string `json:"colour,omitempty"`
	Hair     Hair   `json:"hair,omitempty"`
	BodyType string `json:"bodyType,omitempty"`
}

type Person struct {
	FirstName     string    `json:"firstName,omitempty"`
	Lastname      string    `json:"lastName,omitempty"`
	Age           int       `json:"age,omitempty"`
	Email         string    `json:"email,omitempty"`
	Address       Address   `json:"address,omitempty"`
	Gender        string    `json:"gender,omitempty"`
	MaritalStatus string    `json:"maritalStatus,omitempty"`
	Hobbies       []string  `json:"hobbies,omitempty"`
	Attributes    Attribute `json:"attributes,omitempty"`
	Padding       string    `json:"payload"`
}

func (p *Person) GenerateDocument(fake *faker.Faker, documentSize int) (interface{}, error) {
	person := &Person{
		FirstName:     fake.Person().FirstName(),
		Lastname:      fake.Person().LastName(),
		Age:           fake.IntBetween(0, 100),
		Email:         fake.Internet().CompanyEmail(),
		Gender:        fake.Gender().Name(),
		MaritalStatus: maritalChoices[fake.IntBetween(1, len(maritalChoices)-1)],
		Hobbies:       hobbyChoices[:fake.IntBetween(1, len(hobbyChoices)-1)],
		Address: Address{
			State:   fake.Address().State(),
			City:    fake.Address().City(),
			Street:  fake.Address().StreetName(),
			Zipcode: fake.Address().PostCode(),
			Country: fake.Address().Country(),
		},
		Attributes: Attribute{
			Weight: fake.IntBetween(55, 200),
			Height: fake.IntBetween(100, 300),
			Colour: bodyColor[fake.IntBetween(1, len(bodyColor)-1)],
			Hair: Hair{
				Type:      HairType[fake.IntBetween(1, len(HairType)-1)],
				Colour:    HairColor[fake.IntBetween(1, len(HairType)-1)],
				Length:    HairLength[fake.IntBetween(1, len(HairLength)-1)],
				Thickness: HairThickness[fake.IntBetween(1, len(HairThickness)-1)],
			},
			BodyType: BodyType[fake.IntBetween(1, len(BodyType)-1)],
		},
	}
	personDocument, err := json.Marshal(*person)
	if err != nil {
		return nil, err
	}

	if (len(personDocument)) < int(documentSize) {
		person.Padding = fake.RandomStringWithLength(int(documentSize) - (len(personDocument)))
	}
	return person, nil
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

	if _, ok := checkFields["firstName"]; ok || (len(checkFields) == 0) {
		person.FirstName = fake.Person().FirstName()
	}
	if _, ok := checkFields["lastName"]; ok || (len(checkFields) == 0) {
		person.Lastname = fake.Person().LastName()
	}
	if _, ok := checkFields["age"]; ok || (len(checkFields) == 0) {
		person.Age = fake.IntBetween(0, 100)
	}
	if _, ok := checkFields["email"]; ok || (len(checkFields) == 0) {
		person.Email = fake.Internet().CompanyEmail()
	}
	if _, ok := checkFields["address.state"]; ok || (len(checkFields) == 0) {
		person.Address.State = fake.Address().State()
	}
	if _, ok := checkFields["address.city"]; ok || (len(checkFields) == 0) {
		person.Address.City = fake.Address().City()
	}
	if _, ok := checkFields["address.street"]; ok || (len(checkFields) == 0) {
		person.Address.Street = fake.Address().StreetName()
	}
	if _, ok := checkFields["address.zipcode"]; ok || (len(checkFields) == 0) {
		person.Address.Zipcode = fake.Address().PostCode()
	}
	if _, ok := checkFields["address.country"]; ok || (len(checkFields) == 0) {
		person.Address.Country = fake.Address().Country()
	}
	if _, ok := checkFields["gender"]; ok || (len(checkFields) == 0) {
		person.Gender = fake.Gender().Name()
	}
	if _, ok := checkFields["maritalStatus"]; ok || (len(checkFields) == 0) {
		person.MaritalStatus = maritalChoices[fake.IntBetween(1, len(maritalChoices)-1)]
	}
	if _, ok := checkFields["hobbies"]; ok || (len(checkFields) == 0) {
		person.Hobbies = hobbyChoices[:fake.IntBetween(1, len(hobbyChoices)-1)]
	}
	if _, ok := checkFields["attributes.weight"]; ok || (len(checkFields) == 0) {
		person.Attributes.Weight = fake.IntBetween(55, 200)
	}
	if _, ok := checkFields["attributes.height"]; ok || (len(checkFields) == 0) {
		person.Attributes.Height = fake.IntBetween(100, 300)
	}
	if _, ok := checkFields["attributes.colour"]; ok || (len(checkFields) == 0) {
		person.Attributes.Colour = bodyColor[fake.IntBetween(1, len(bodyColor)-1)]
	}
	if _, ok := checkFields["attributes.hair.type"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Type = HairType[fake.IntBetween(1, len(HairType)-1)]
	}
	if _, ok := checkFields["attributes.hair.colour"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Colour = HairColor[fake.IntBetween(1, len(HairType)-1)]
	}
	if _, ok := checkFields["attributes.hair.length"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Length = HairLength[fake.IntBetween(1, len(HairLength)-1)]
	}
	if _, ok := checkFields["attributes.hair.thickness"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Thickness = HairThickness[fake.IntBetween(1, len(HairThickness)-1)]
	}
	if _, ok := checkFields["attributes.bodyType"]; ok || (len(checkFields) == 0) {
		person.Attributes.BodyType = BodyType[fake.IntBetween(1, len(BodyType)-1)]
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
