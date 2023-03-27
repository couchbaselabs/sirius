package template

import (
	"github.com/jaswdr/faker"
)

type Template interface {
	GenerateDocument(fake *faker.Faker, documentSize int64) (interface{}, error)
	UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, fake *faker.Faker) (interface{}, error)
	Compare(document1 interface{}, document2 interface{}) (bool, error)
}

// InitialiseTemplate returns a template as an interface defined by user request.
func InitialiseTemplate(template string) (Template, error) {
	switch template {
	case "person":
		return &Person{}, nil
	default:
		return &Person{}, nil
	}
}
