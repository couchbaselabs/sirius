package docgenerator

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/template"
)

type DocumentType string

const (
	JsonDocument   string = "json"
	BinaryDocument string = "binary"
)

const DefaultKeySize int = 250
const DefaultDocSize int = 1024

// Generator helps to generate random document for inserting and updating random
// as per the doc loading task requirement.
type Generator struct {
	DocType   string
	KeyPrefix string
	KeySuffix string
	Seed      int
	SeedEnd   int
	Template  template.Template
}

func ConfigGenerator(doctype, keyPrefix, keySuffix string, seed, seedEnd int, template template.Template) *Generator {
	return &Generator{
		DocType:   doctype,
		KeyPrefix: keyPrefix,
		KeySuffix: keySuffix,
		Seed:      seed,
		SeedEnd:   seedEnd,
		Template:  template,
	}
}

type QueryGenerator struct {
	Template template.Template
}

func ConfigQueryGenerator(template template.Template) *QueryGenerator {
	return &QueryGenerator{
		Template: template,
	}
}

// GetDocIdAndKey will return key for the next document
func (g *Generator) GetDocIdAndKey(iteration int) (string, int) {
	newKey := iteration + g.SeedEnd
	return fmt.Sprintf("%s%d%s", g.KeyPrefix, newKey, g.KeySuffix), newKey
}

// BuildKey returns the formatted key with unique identifier.
func (g *Generator) BuildKey(key int) string {
	return fmt.Sprintf("%s%d%s", g.KeyPrefix, key, g.KeySuffix)
}
