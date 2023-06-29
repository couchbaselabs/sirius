package template

import (
	"github.com/jaswdr/faker"
	"strings"
)

type Template interface {
	GenerateDocument(fake *faker.Faker, documentSize int) (interface{}, error)
	UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, fake *faker.Faker) (interface{}, error)
	Compare(document1 interface{}, document2 interface{}) (bool, error)
	GenerateIndexes() ([]string, error)
	GenerateQueries() ([]string, error)
}

// InitialiseTemplate returns a template as an interface defined by user request.
func InitialiseTemplate(template string) Template {
	switch strings.ToLower(template) {
	case "person":
		return &Person{}
	case "small":
		return &SmallTemplate{}
	default:
		return &Person{}
	}
}
