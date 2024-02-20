package docgenerator

import (
	"fmt"
	"strings"

	"github.com/barkha06/sirius/internal/template"
)

type DocumentType string

const (
	JsonDocument   string = "json"
	BinaryDocument string = "binary"
	DefaultDocSize int    = 128
	DefaultKeySize int    = 250
)

// Generator helps to sirius_documentation random document for inserting and updating random
// as per the doc loading task requirement.
type Generator struct {
	KeySize  int               `json:"keySize"`
	DocSize  int               `json:"docSize"`
	DocType  string            `json:"docType"`
	Template template.Template `json:"template"`
}

func ConfigGenerator(keySize, docSize int, template template.Template) *Generator {

	return &Generator{
		KeySize:  keySize,
		DocSize:  docSize,
		Template: template,
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

// BuildKey returns the formatted key with unique identifier.
func (g *Generator) BuildKey(key int64) string {
	tempKey := fmt.Sprintf("%d", key)
	if g.KeySize >= 0 && len(tempKey) < g.KeySize {
		tempKey += strings.Repeat("a", g.KeySize-len(tempKey))
	}
	return tempKey
}

func Reset(keySize, docSize int, templateName string) *Generator {
	return ConfigGenerator(keySize, docSize, template.InitialiseTemplate(templateName))
}
