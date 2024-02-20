package template

import (
	"fmt"
	"reflect"

	"github.com/jaswdr/faker"
)

type SmallTemplate struct {
	ID         string  `json:"_id"`
	RandomData string  `json:"d,omitempty"`
	Mutated    float64 `json:"mutated,omitempty"`
}

func (s *SmallTemplate) GenerateDocument(key string, fake *faker.Faker, documentSize int) (interface{}, error) {
	return &SmallTemplate{
		ID:         key,
		RandomData: fake.RandomStringWithLength(int(documentSize) - 1 - len(key)),
		Mutated:    MutatedPathDefaultValue,
	}, nil
}

func (s *SmallTemplate) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	t, ok := lastUpdatedDocument.(*SmallTemplate)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
	}
	t.RandomData = fake.RandomStringWithLength(documentSize - 1)
	return t, nil
}

func (s *SmallTemplate) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	t1, ok := document1.(*SmallTemplate)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to person template")
	}
	t2, ok := document2.(*SmallTemplate)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to person template")
	}
	return reflect.DeepEqual(t1, t2), nil
}

func (s *SmallTemplate) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, nil
}

func (s *SmallTemplate) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, nil
}

func (s *SmallTemplate) GenerateIndexesForSdk() (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (s *SmallTemplate) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {

	return map[string]interface{}{
		"subDocData": fake.RandomStringWithLength(subDocSize),
	}
}
