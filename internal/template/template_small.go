package template

import (
	"fmt"
	"github.com/jaswdr/faker"
	"reflect"
)

type SmallTemplate struct {
	RandomData string `json:"data,omitempty"`
}

func (s *SmallTemplate) GenerateDocument(fake *faker.Faker, documentSize int) (interface{}, error) {
	return &SmallTemplate{RandomData: fake.RandomStringWithLength(int(documentSize))}, nil
}

func (s *SmallTemplate) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, fake *faker.Faker) (interface{}, error) {
	t, ok := lastUpdatedDocument.(*SmallTemplate)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
	}
	size := len(t.RandomData)
	t.RandomData = fake.RandomStringWithLength(size)
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
