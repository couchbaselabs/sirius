package template

import (
	"fmt"
	"github.com/jaswdr/faker"
)

type Template interface {
	GenerateDocument(fake *faker.Faker) interface{}
	UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, fake *faker.Faker) (error, interface{})
	Compare(document1 interface{}, document2 interface{}) (error, bool)
}

func InitialiseTemplate(template string) (Template, error) {
	switch template {
	case "person":
		return &Person{}, nil
	default:
		return &Person{}, nil
	}
	return nil, fmt.Errorf("no such Template found")
}
