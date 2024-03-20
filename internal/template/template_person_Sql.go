package template

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type PersonSql struct {
	ID            string  `json:"_id" bson:"_id"`
	FirstName     string  `json:"firstName,omitempty"`
	Age           float64 `json:"age,omitempty"`
	Email         string  `json:"email,omitempty"`
	Gender        string  `json:"gender,omitempty"`
	MaritalStatus string  `json:"maritalStatus,omitempty"`
	Hobbies       string  `json:"hobbies,omitempty"`
	Value         []interface{}
	Mutated       float64 `json:"mutated"`
	Padding       string  `json:"payload"`
}

func (p *PersonSql) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	person := &PersonSql{
		ID:            key,
		FirstName:     fake.Name(),
		Age:           fake.Float64Range(1, 100),
		Email:         fake.Email(),
		Gender:        fake.Gender(),
		MaritalStatus: fake.RandString(maritalChoices),
		Hobbies:       fake.RandString(hobbyChoices),
		Mutated:       MutatedPathDefaultValue,
	}
	currentDocSize := calculateSizeOfStruct(person)

	if (currentDocSize) < int(documentSize) {
		person.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
	}
	values := []interface{}{&person.ID, &person.FirstName, &person.Age, &person.Email, &person.Gender, &person.MaritalStatus, &person.Hobbies, &person.Padding, &person.Mutated}
	person.Value = values
	return person
}

func (p *PersonSql) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	person, ok := lastUpdatedDocument.(*PersonSql)
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
	if _, ok := checkFields["gender"]; ok || (len(checkFields) == 0) {
		person.Gender = fake.Gender()
	}
	if _, ok := checkFields["maritalStatus"]; ok || (len(checkFields) == 0) {
		person.MaritalStatus = fake.RandString(maritalChoices)
	}
	if _, ok := checkFields["hobbies"]; ok || (len(checkFields) == 0) {
		person.Hobbies = fake.RandString(hobbyChoices)
	}
	person.Padding = ""
	currentDocSize := calculateSizeOfStruct(person)
	if (currentDocSize) < int(documentSize) {
		person.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
	}
	values := []interface{}{&person.ID, &person.FirstName, &person.Age, &person.Email, &person.Gender, &person.MaritalStatus, &person.Hobbies, &person.Padding, &person.Mutated}
	person.Value = values
	return person, nil
}

func (p *PersonSql) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	p1, ok := document1.(*PersonSql)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to person template")
	}
	p2, ok := document2.(*PersonSql)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to person template")
	}
	return reflect.DeepEqual(p1, p2), nil
}

func (p *PersonSql) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {
	return map[string]interface{}{
		"_1": strings.Repeat(fake.Letter(), subDocSize),
	}
}
func (p *PersonSql) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, errors.New("not implemented")
}

func (p *PersonSql) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, errors.New("not implemented")
}

func (p *PersonSql) GenerateIndexesForSdk() (map[string][]string, error) {
	return map[string][]string{}, errors.New("not implemented")
}
func (p *PersonSql) GetValues(document interface{}) (interface{}, error) {
	person, ok := document.(*PersonSql)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
	}
	return person.Value, nil
}
