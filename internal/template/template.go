package template

import (
	"github.com/jaswdr/faker"
)

type Template interface {
	GenerateDocument(fake *faker.Faker) interface{}
	UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, fake *faker.Faker) (interface{}, error)
	Compare(document1 interface{}, document2 interface{}) (bool, error)
}

func InitialiseTemplate(template string) (Template, error) {
	switch template {
	case "person":
		return &Person{}, nil
	default:
		return &Person{}, nil
	}
}
