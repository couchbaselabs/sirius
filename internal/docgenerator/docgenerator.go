package docgenerator

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"time"
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
	Itr             int64
	End             int64
	BatchSize       int64
	DocType         DocumentType
	KeyPrefix       string
	KeySuffix       string
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
	Seed            int64
	Fake            faker.Faker
	Template        interface{}
}

// Next will return list of keys and person templates
func (g *Generator) Next(count int64) []*template.Person {
	personTemplate := template.GeneratePersons(count, g.Fake)
	return personTemplate
}

// GetKey will return key for the next document
func (g *Generator) GetKey(iteration, batchSize, index, initialKey int64) string {
	newKey := initialKey + index + (iteration * batchSize)
	return fmt.Sprintf("%s%d%s", g.KeyPrefix, newKey, g.KeySuffix)
}
