package template

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

var maritalChoices = []string{"Single", "Married", "Divorcee"}
var bodyColor = []string{"Dark", "Fair", "Brown", "Grey"}
var hobbyChoices = []string{"Video Gaming", "Football", "Basketball", "Cricket",
	"Hockey", "Running", "Walking", "Guitar", "Flute", "Piano", "Chess", "Puzzle", "Skating", "Travelling"}
var hairType = []string{"Straight", "Wavy", "Curly", "Coily"}
var hairColor = []string{"Red", "Green", "Yellow", "Grey", "Brown", "Black"}
var hairLength = []string{"Long", "Short", "Medium"}
var hairThickness = []string{"Thick", "Thin", "Medium"}
var bodyType = []string{"Ectomorph", "Endomorph", "Mesomorph", "Triangle", "Inverted Triangle",
	"Rectangle", "Hourglass", "Apple"}

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
	City  string `json:"city,omitempty" dynamodbav:"city" parquet:"name=city, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	State string `json:"state,omitempty" dynamodbav:"state" parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type Hair struct {
	HairType   string `json:"hair_type,omitempty" dynamodbav:"hair_type" parquet:"name=hair_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	HairColour string `json:"hair_colour,omitempty" dynamodbav:"hair_colour" parquet:"name=hair_colour, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Length     string `json:"length,omitempty" dynamodbav:"length" parquet:"name=length, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Thickness  string `json:"thickness,omitempty" dynamodbav:"thickness" parquet:"name=thickness, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type Attribute struct {
	Weight   float64 `json:"weight,omitempty" dynamodbav:"weight" parquet:"name=weight, type=DOUBLE"`
	Height   float64 `json:"height,omitempty" dynamodbav:"height" parquet:"name=height, type=DOUBLE"`
	Colour   string  `json:"colour,omitempty" dynamodbav:"colour" parquet:"name=colour, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Hair     Hair    `json:"hair,omitempty" dynamodbav:"hair" parquet:"name=hair"`
	BodyType string  `json:"body_type,omitempty" dynamodbav:"body_type" parquet:"name=body_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type Person struct {
	ID            string    `json:"id" bson:"_id" dynamodbav:"id" parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TemplateName  string    `json:"template_name" dynamodbav:"template_name" parquet:"name=template_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	FirstName     string    `json:"first_name,omitempty" dynamodbav:"first_name" parquet:"name=first_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age           float64   `json:"age,omitempty" dynamodbav:"age" parquet:"name=age, type=DOUBLE"`
	Email         string    `json:"email,omitempty" dynamodbav:"email" parquet:"name=email, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Address       Address   `json:"address,omitempty" dynamodbav:"address" parquet:"name=address"`
	Gender        string    `json:"gender,omitempty" dynamodbav:"gender" parquet:"name=gender, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MaritalStatus string    `json:"marital_status,omitempty" dynamodbav:"marital_status" parquet:"name=marital_status, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Hobbies       string    `json:"hobbies,omitempty" dynamodbav:"hobbies" parquet:"name=hobbies, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Attributes    Attribute `json:"attributes,omitempty" dynamodbav:"attributes" parquet:"name=attributes"`
	Mutated       float64   `json:"mutated" dynamodbav:"mutated" parquet:"name=mutated, type=DOUBLE"`
	Padding       string    `json:"padding" dynamodbav:"padding" parquet:"name=padding, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func (p *Person) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	person := &Person{
		ID:            key,
		TemplateName:  "Person",
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
				HairType:   fake.RandString(hairType),
				HairColour: fake.RandString(hairColor),
				Length:     fake.RandString(hairLength),
				Thickness:  fake.RandString(hairThickness),
			},
			BodyType: fake.RandString(bodyType),
		},
		Mutated: MutatedPathDefaultValue,
	}

	currentDocSize := calculateSizeOfStruct(person)
	if currentDocSize < documentSize {
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

	if _, ok := checkFields["first_name"]; ok || (len(checkFields) == 0) {
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
	if _, ok := checkFields["marital_status"]; ok || (len(checkFields) == 0) {
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
	if _, ok := checkFields["attributes.hair.hair_type"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.HairType = fake.RandString(hairType)
	}
	if _, ok := checkFields["attributes.hair.hair_colour"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.HairColour = fake.RandString(hairColor)
	}
	if _, ok := checkFields["attributes.hair.length"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Length = fake.RandString(hairLength)
	}
	if _, ok := checkFields["attributes.hair.thickness"]; ok || (len(checkFields) == 0) {
		person.Attributes.Hair.Thickness = fake.RandString(hairThickness)
	}
	if _, ok := checkFields["attributes.body_type"]; ok || (len(checkFields) == 0) {
		person.Attributes.BodyType = fake.RandString(bodyType)
	}
	person.Padding = ""

	currentDocSize := calculateSizeOfStruct(person)
	if currentDocSize < documentSize {
		person.Padding = strings.Repeat("a", documentSize-currentDocSize)
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
		"SubDoc": strings.Repeat(fake.Letter(), subDocSize),
	}
}
func (p *Person) GetValues(document interface{}) (interface{}, error) {
	return document, nil
}
