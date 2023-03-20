package docgenerator

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/template"
)

type DocumentType string

const (
	JsonDocument   DocumentType = "json"
	BinaryDocument DocumentType = "binary"
)

const DefaultKeySize int = 250
const DefaultDocSize int = 1000000

// Generator helps to generate random document for inserting and updating random
// as per the doc loading task requirement.
type Generator struct {
	DocType   DocumentType
	KeyPrefix string
	KeySuffix string
	Seed      int64
	SeedEnd   int64
	Template  template.Template
}

// GetDocIdAndKey will return key for the next document
func (g *Generator) GetDocIdAndKey(iteration, batchSize, offset int64) (string, int64) {
	newKey := offset + (iteration * batchSize) + g.SeedEnd
	return fmt.Sprintf("%s%d%s", g.KeyPrefix, newKey, g.KeySuffix), newKey
}

func (g *Generator) BuildKey(key int64) string {
	return fmt.Sprintf("%s%d%s", g.KeyPrefix, key, g.KeySuffix)
}
