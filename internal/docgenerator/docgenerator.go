package docgenerator

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/template"
	"strings"
)

type DocumentType string

const (
	JsonDocument     string = "json"
	BinaryDocument   string = "binary"
	DefaultKeyPrefix        = ""
	DefaultKeySuffix        = ""
	DefaultTemplate         = "person"
	DefaultDocSize   int    = 128
	DefaultKeySize   int    = 250
)

// Generator helps to sirius_documentation random document for inserting and updating random
// as per the doc loading task requirement.
type Generator struct {
	KeySize   int               `json:"keySize"`
	DocSize   int               `json:"docSize"`
	DocType   string            `json:"docType"`
	KeyPrefix string            `json:"keyPrefix"`
	KeySuffix string            `json:"keySuffix"`
	Template  template.Template `json:"template"`
}

func ConfigGenerator(keySize, docSize int, doctype, keyPrefix, keySuffix string,
	template template.Template) *Generator {

	return &Generator{
		KeySize:   keySize,
		DocSize:   docSize,
		DocType:   doctype,
		KeyPrefix: keyPrefix,
		KeySuffix: keySuffix,
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

// BuildKey returns the formatted key with unique identifier.
func (g *Generator) BuildKey(key int64) string {
	tempKey := fmt.Sprintf("%s%d%s", g.KeyPrefix, key, g.KeySuffix)
	if g.KeySize >= 0 && len(tempKey) < g.KeySize {
		tempKey += strings.Repeat("a", g.KeySize-len(tempKey))
	}
	return tempKey
}

func Reset(keySize, docSize int, docType, keyPrefix, keySuffix, templateName string) *Generator {
	return ConfigGenerator(keySize, docSize, docType, keyPrefix, keySuffix, template.InitialiseTemplate(templateName))
}
