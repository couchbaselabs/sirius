package template

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/jaswdr/faker"
)

var maritalChoices = []string{"Single", "Married", "Divorcee"}
var bodyColor = []string{"Dark", "Fair", "Brown", "Grey"}
var hobbyChoices = []string{"Video Gaming", "Football", "Basketball", "Cricket",
	"Hockey", "Running", "Walking", "Guitar", "Flute", "Piano", "Chess", "Puzzle", "Skating", "Travelling"}
var hairType = []string{"straight", "wavy", "curly", "Coily"}
var HairColor = []string{"Red", "Green", "Yellow", "Grey", "Brown", "Black"}
var hairLength = []string{"Long", "Short", "Medium"}
var hairThickness = []string{"Thick", "Thin", "Medium"}
var bodyType = []string{"Ectomorph", "endomorph", "Mesomorph", "triangle", "Inverted triangle",
	"Rectangle", "Hourglass", "apple"}

var state = []string{"Andhra Pradesh",
	"Arunachal Pradesh",
	"Assam",
	"Bihar",
	"Chhattisgarh",
	"Goa",
	"Gujarat",
	"Haryana",
	"Himachal Pradesh",
	"Jammu and Kashmir",
	"Jharkhand",
	"Karnataka",
	"Kerala",
	"Madhya Pradesh",
	"Maharashtra",
	"Manipur",
	"Meghalaya",
	"Mizoram",
	"Nagaland",
	"Odisha",
	"Punjab",
	"Rajasthan",
	"Sikkim",
	"Tamil Nadu",
	"Telangana",
	"Tripura",
	"Uttarakhand",
	"Uttar Pradesh",
	"West Bengal",
	"Andaman and Nicobar Islands",
	"Chandigarh",
	"Dadra and Nagar Haveli",
	"Daman and Diu",
	"Delhi",
	"Lakshadweep",
	"Puducherry",
}

var city = []string{"Lake Penelop", "New Charlene", "Prosaccobury", "West Jasenmouth", "East Taya", "Wardborough",
	"Baumbachfort", "New Elzaport", "Theresiaton"}

var gender = []string{"male", "female"}

type Address struct {
	City  string `json:"city,omitempty"`
	State string `json:"state,omitempty"`
}

type Hair struct {
	Type      string `json:"type,omitempty"`
	Colour    string `json:"colour,omitempty"`
	Length    string `json:"length,omitempty"`
	Thickness string `json:"thickness,omitempty"`
}

type Attribute struct {
	Weight   float64 `json:"weight,omitempty"`
	Height   float64 `json:"height,omitempty"`
	Colour   string  `json:"colour,omitempty"`
	Hair     Hair    `json:"hair,omitempty"`
	BodyType string  `json:"bodyType,omitempty"`
}

type Person struct {
	ID            string    `json:" _id"`
	FirstName     string    `json:"firstName,omitempty"`
	Age           float64   `json:"age,omitempty"`
	Email         string    `json:"email,omitempty"`
	Address       Address   `json:"address,omitempty"`
	Gender        string    `json:"gender,omitempty"`
	MaritalStatus string    `json:"maritalStatus,omitempty"`
	Hobbies       []string  `json:"hobbies,omitempty"`
	Attributes    Attribute `json:"attributes,omitempty"`
	Mutated       float64   `json:"mutated"`
	Padding       string    `json:"payload"`
}

func (p *Person) GenerateDocument(key string, fake *faker.Faker, documentSize int) (interface{}, error) {
	person := &Person{
		ID:            key,
		FirstName:     fake.Person().FirstName(),
		Age:           fake.Float64(2, 0, 100),
		Email:         fake.Internet().CompanyEmail(),
		Gender:        gender[fake.IntBetween(1, len(gender)-1)],
		MaritalStatus: maritalChoices[fake.IntBetween(1, len(maritalChoices)-1)],
		Hobbies:       hobbyChoices[:fake.IntBetween(1, len(hobbyChoices)-1)],
		Address: Address{
			State: state[fake.IntBetween(1, len(state)-1)],
			City:  city[fake.IntBetween(1, len(city)-1)],
		},
		Attributes: Attribute{
			Weight: fake.Float64(2, 0, 100),
			Height: fake.Float64(2, 0, 100),
			Colour: bodyColor[fake.IntBetween(1, len(bodyColor)-1)],
			Hair: Hair{
				Type:      hairType[fake.IntBetween(1, len(hairType)-1)],
				Colour:    HairColor[fake.IntBetween(1, len(hairType)-1)],
				Length:    hairLength[fake.IntBetween(1, len(hairLength)-1)],
				Thickness: hairThickness[fake.IntBetween(1, len(hairThickness)-1)],
			},
			BodyType: bodyType[fake.IntBetween(1, len(bodyType)-1)],
		},
		Mutated: MutatedPathDefaultValue,
	}
	personDocument, err := json.Marshal(*person)
	if err != nil {
		return nil, err
	}

	if (len(personDocument)) < int(documentSize) {
		person.Padding = strings.Repeat("a", int(documentSize)-(len(personDocument)))
	}
	return person, nil
}

func (p *Person) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	person, ok := lastUpdatedDocument.(*Person)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
	}

	checkFields := make(map[string]struct{})
	for _, s := range fieldsToChange {
		checkFields[s] = struct{}{}
	}

	if _, ok := checkFields["firstName"]; ok || (len(checkFields) == 0) {
		person.FirstName = fake.Person().FirstName()
	}
	if _, ok := checkFields["age"]; ok || (len(checkFields) == 0) {
		person.Age = fake.Float64(2, 0, 100)
	}
	if _, ok := checkFields["email"]; ok || (len(checkFields) == 0) {
		person.Email = fake.Internet().CompanyEmail()
	}
	if _, ok := checkFields["address.state"]; ok || (len(checkFields) == 0) {
		person.Address.State = state[fake.IntBetween(1, len(state)-1)]
	}
	if _, ok := checkFields["address.city"]; ok || (len(checkFields) == 0) {
		person.Address.City = city[fake.IntBetween(1, len(city)-1)]
	}
	if _, ok := checkFields["gender"]; ok || (len(checkFields) == 0) {
		person.Gender = gender[fake.IntBetween(1, len(gender)-1)]
	}
	if _, ok := checkFields["maritalStatus"]; ok || (len(checkFields) == 0) {
		person.MaritalStatus = maritalChoices[fake.IntBetween(1, len(maritalChoices)-1)]
	}
	if _, ok := checkFields["hobbies"]; ok || (len(checkFields) == 0) {
		person.Hobbies = hobbyChoices[:fake.IntBetween(1, len(hobbyChoices)-1)]
	}
	if _, ok := checkFields["attributes.weight"]; ok || (len(checkFields) == 0) {
		person.Attributes.Weight = fake.Float64(2, 0, 100)
	}
	if _, ok := checkFields["attributes.height"]; ok || (len(checkFields) == 0) {
		person.Attributes.Height = fake.Float64(2, 0, 100)
	}
	if _, ok := checkFields["attributes.colour"]; ok || (len(checkFields) == 0) {
		person.Attributes.Colour = bodyColor[fake.IntBetween(1, len(bodyColor)-1)]
	}
	if _, ok := checkFields["attributes.hair.type"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Type = hairType[fake.IntBetween(1, len(hairType)-1)]
	}
	if _, ok := checkFields["attributes.hair.colour"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Colour = HairColor[fake.IntBetween(1, len(hairType)-1)]
	}
	if _, ok := checkFields["attributes.hair.length"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Length = hairLength[fake.IntBetween(1, len(hairLength)-1)]
	}
	if _, ok := checkFields["attributes.hair.thickness"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Thickness = hairThickness[fake.IntBetween(1, len(hairThickness)-1)]
	}
	if _, ok := checkFields["attributes.bodyType"]; ok || (len(checkFields) == 0) {
		person.Attributes.BodyType = bodyType[fake.IntBetween(1, len(bodyType)-1)]
	}

	person.Padding = ""
	personDocument, err := json.Marshal(*person)
	if err != nil {
		return nil, err
	}

	if (len(personDocument)) < int(documentSize) {
		person.Padding = strings.Repeat("a", int(documentSize)-(len(personDocument)))
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

func (p *Person) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {
	return map[string]interface{}{
		"subDocData": fake.RandomStringWithLength(subDocSize),
	}
}
