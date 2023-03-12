package docgenerator

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/template"
	"time"
)

type DocumentType string

const (
	JsonDocument   DocumentType = "json"
	BinaryDocument DocumentType = "binary"
)

const DefaultKeySize int = 250
const DefaultDocSize int = 1000000

type Generator struct {
	Itr             int
	End             int
	BatchSize       int
	DocType         DocumentType
	KeySize         int
	DocSize         int
	RandomDocSize   bool
	RandomKeySize   bool
	Expiry          time.Duration
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel gocb.DurabilityLevel
	Transcoder      gocb.Transcoder
	Timeout         time.Duration
	RetryStrategy   gocb.RetryStrategy
	Seed            []int64
	Template        interface{}
}

// Next will return list of keys and person templates
func (g *Generator) Next(seed int64) ([]string, []*template.Person) {
	keys := template.GenerateKeys(g.BatchSize, g.KeySize, seed)
	personTemplate := template.GeneratePersons(g.BatchSize, seed)
	return keys, personTemplate
}
