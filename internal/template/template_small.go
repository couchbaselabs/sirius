package template

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type Small struct {
	ID           string  `json:"id" bson:"_id" dynamodbav:"id" parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TemplateName string  `json:"template_name" dynamodbav:"template_name" parquet:"name=template_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RandomData   string  `json:"random_data,omitempty" dynamodbav:"random_data" parquet:"name=random_data, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Mutated      float64 `json:"mutated,omitempty" dynamodbav:"mutated" parquet:"name=mutated, type=DOUBLE"`
}

func (s *Small) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	return &Small{
		ID:           key,
		TemplateName: "small",
		RandomData:   strings.Repeat(fake.Letter(), documentSize),
		Mutated:      MutatedPathDefaultValue,
	}
}

func (s *Small) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {
	t, ok := lastUpdatedDocument.(*Small)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
	}
	t.RandomData = strings.Repeat(fake.Letter(), documentSize)
	return t, nil
}

func (s *Small) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	t1, ok := document1.(*Small)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to person template")
	}
	t2, ok := document2.(*Small)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to person template")
	}

	return reflect.DeepEqual(t1, t2), nil
}

func (s *Small) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, nil
}

func (s *Small) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, nil
}

func (s *Small) GenerateIndexesForSdk() (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (s *Small) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {

	return map[string]interface{}{
		"subDocData": fake.Sentence(subDocSize),
	}
}
func (s *Small) GetValues(document interface{}) (interface{}, error) {
	return document, nil
}
