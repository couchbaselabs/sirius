package template

import (
	"fmt"
	"github.com/bgadrian/fastfaker/faker"
	"reflect"
	"strings"
)

var maritalChoices = []string{"Single", "Married", "Divorcee"}
var bodyColor = []string{"Dark", "Fair", "Brown", "Grey"}
var hobbyChoices = []string{"Video Gaming", "Football", "Basketball", "Cricket",
	"Hockey", "Running", "Walking", "Guitar", "Flute", "Piano", "Chess", "Puzzle", "Skating", "Travelling"}
var hairType = []string{"straight", "wavy", "curly", "Coily"}
var hairColor = []string{"Red", "Green", "Yellow", "Grey", "Brown", "Black"}
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
	ID            string    `json:"_id" bson:"_id"`
	FirstName     string    `json:"firstName,omitempty"`
	Age           float64   `json:"age,omitempty"`
	Email         string    `json:"email,omitempty"`
	Address       Address   `json:"address,omitempty"`
	Gender        string    `json:"gender,omitempty"`
	MaritalStatus string    `json:"maritalStatus,omitempty"`
	Hobbies       string    `json:"hobbies,omitempty"`
	Attributes    Attribute `json:"attributes,omitempty"`
	Mutated       float64   `json:"mutated"`
	Padding       string    `json:"payload"`
}

func (p *Person) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	person := &Person{
		ID:            key,
		FirstName:     fake.Name(),
		Age:           fake.Float64Range(1, 100),
		Email:         fake.Email(),
		Gender:        fake.Gender(),
		MaritalStatus: fake.RandString(maritalChoices),
		Hobbies:       fake.RandString(hobbyChoices),
		Address: Address{
			State: fake.State(),
			City:  fake.City(),
		},
		Attributes: Attribute{
			Weight: fake.Float64Range(1, 100),
			Height: fake.Float64Range(1, 250),
			Colour: fake.Color(),
			Hair: Hair{
				Type:      fake.RandString(hairType),
				Colour:    fake.RandString(hairColor),
				Length:    fake.RandString(hairLength),
				Thickness: fake.RandString(hairThickness),
			},
			BodyType: fake.RandString(bodyType),
		},
		Mutated: MutatedPathDefaultValue,
	}

	currentDocSize := calculateSizeOfStruct(person)

	if (currentDocSize) < int(documentSize) {
		person.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
	}
	return person
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
		person.FirstName = fake.Name()
	}
	if _, ok := checkFields["age"]; ok || (len(checkFields) == 0) {
		person.Age = fake.Float64Range(1, 100)
	}
	if _, ok := checkFields["email"]; ok || (len(checkFields) == 0) {
		person.Email = fake.Email()
	}
	if _, ok := checkFields["address.state"]; ok || (len(checkFields) == 0) {
		person.Address.State = fake.State()
	}
	if _, ok := checkFields["address.city"]; ok || (len(checkFields) == 0) {
		person.Address.City = fake.City()
	}
	if _, ok := checkFields["gender"]; ok || (len(checkFields) == 0) {
		person.Gender = fake.Gender()
	}
	if _, ok := checkFields["maritalStatus"]; ok || (len(checkFields) == 0) {
		person.MaritalStatus = fake.RandString(maritalChoices)
	}
	if _, ok := checkFields["hobbies"]; ok || (len(checkFields) == 0) {
		person.Hobbies = fake.RandString(hobbyChoices)
	}
	if _, ok := checkFields["attributes.weight"]; ok || (len(checkFields) == 0) {
		person.Attributes.Weight = fake.Float64Range(1, 100)
	}
	if _, ok := checkFields["attributes.height"]; ok || (len(checkFields) == 0) {
		person.Attributes.Height = fake.Float64Range(1, 250)
	}
	if _, ok := checkFields["attributes.colour"]; ok || (len(checkFields) == 0) {
		person.Attributes.Colour = fake.Color()
	}
	if _, ok := checkFields["attributes.hair.type"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Type = fake.RandString(hairType)
	}
	if _, ok := checkFields["attributes.hair.colour"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Colour = fake.RandString(hairColor)
	}
	if _, ok := checkFields["attributes.hair.length"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Length = fake.RandString(hairLength)
	}
	if _, ok := checkFields["attributes.hair.thickness"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Thickness = fake.RandString(hairThickness)
	}
	if _, ok := checkFields["attributes.bodyType"]; ok || (len(checkFields) == 0) {
		person.Attributes.BodyType = fake.RandString(bodyType)
	}

	person.Padding = ""
	currentDocSize := calculateSizeOfStruct(person)

	if (currentDocSize) < int(documentSize) {
		person.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
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
		"_1": strings.Repeat(fake.Letter(), subDocSize),
	}
}
