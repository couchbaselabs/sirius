package meta_data

import (
	"sync"
	"time"

	"github.com/barkha06/sirius/internal/docgenerator"
	"github.com/barkha06/sirius/internal/template"
	"github.com/jaswdr/faker"
)

type SubDocMutations struct {
	Seed            int64      `json:"seed"`
	xattr           bool       `json:"xattr"`
	countOfMutation int        `json:"countOfMutation"`
	DocSize         int        `json:"docSize"`
	lock            sync.Mutex `json:"-"`
}

func (m *SubDocMutations) IsXattr() bool {
	defer m.lock.Unlock()
	m.lock.Lock()
	return m.xattr
}

func (m *SubDocMutations) GenerateValue(f *faker.Faker) interface{} {
	defer m.lock.Unlock()
	m.lock.Lock()
	return f.RandomStringWithLength(m.DocSize)
}

func (m *SubDocMutations) RetracePreviousMutations(value interface{}, f *faker.Faker) interface{} {
	defer m.lock.Unlock()
	m.lock.Lock()
	for i := 0; i < m.countOfMutation; i++ {
		value = f.RandomStringWithLength(m.DocSize)
	}
	return value
}

func (m *SubDocMutations) UpdateValue(value interface{}, f *faker.Faker) interface{} {
	defer m.lock.Unlock()
	m.lock.Lock()
	value = f.RandomStringWithLength(m.DocSize)
	m.IncrementCount()
	return value
}

func (m *SubDocMutations) DecrementCount() {
	defer m.lock.Unlock()
	m.lock.Lock()
	m.countOfMutation--
}

func (m *SubDocMutations) IncrementCount() {
	m.countOfMutation++
}

type DocumentMetaData struct {
	Seed                 int64                       `json:"seed"`
	DocId                string                      `json:"docId"`
	DocSize              int                         `json:"docSize"`
	Template             string                      `json:"template"`
	countOfMutation      int                         `json:"countOfMutation"`
	SubDocMutations      map[string]*SubDocMutations `json:"subDocMutations"`
	subDocMutationsCount int                         `json:"subDocMutationsCount"`
	lock                 sync.Mutex                  `json:"-"`
}

func (d *DocumentMetaData) IncrementCount() {
	d.countOfMutation++
}

func (d *DocumentMetaData) DecrementCount() {
	defer d.lock.Unlock()
	d.lock.Lock()
	d.countOfMutation++
}

func (d *DocumentMetaData) RetracePreviousMutations(template template.Template, doc interface{}, docSize int,
	fake *faker.Faker) interface{} {
	defer d.lock.Unlock()
	defer d.lock.Lock()
	for i := 0; i < d.countOfMutation; i++ {
		template.UpdateDocument([]string{}, doc, docSize, fake)
	}
	return doc
}

func (d *DocumentMetaData) UpdateDocument(t template.Template, doc interface{}, docSize int,
	fake *faker.Faker) interface{} {
	defer d.lock.Unlock()
	defer d.lock.Lock()
	updatedDoc, _ := t.UpdateDocument([]string{}, doc, docSize, fake)
	d.SubDocMutations = make(map[string]*SubDocMutations)
	d.subDocMutationsCount = 0
	d.IncrementCount()
	return updatedDoc
}

func (d *DocumentMetaData) SubDocument(subPath string, xattr bool, docSize int, reset bool) *SubDocMutations {
	defer d.lock.Unlock()
	d.lock.Lock()
	seed := int64(time.Now().UnixNano())
	if docSize == 0 {
		docSize = 50
	}
	if _, ok := d.SubDocMutations[subPath]; !ok {
		d.SubDocMutations[subPath] = &SubDocMutations{
			Seed:            seed,
			xattr:           xattr,
			DocSize:         docSize,
			countOfMutation: 0,
			lock:            sync.Mutex{},
		}
	}

	if reset {
		d.SubDocMutations[subPath].Seed = seed
		d.SubDocMutations[subPath].countOfMutation = 0
		d.SubDocMutations[subPath].DocSize = docSize
	}
	return d.SubDocMutations[subPath]
}

func (d *DocumentMetaData) RemovePath(path string) {
	delete(d.SubDocMutations, path)
}

func (d *DocumentMetaData) IncrementMutationCount() {
	defer d.lock.Unlock()
	d.lock.Lock()
	d.subDocMutationsCount++
}

func (d *DocumentMetaData) SubDocMutationCount() float64 {
	defer d.lock.Unlock()
	d.lock.Lock()
	return float64(d.subDocMutationsCount)
}

type DocumentsMetaData struct {
	MetaData map[string]*DocumentMetaData `json:"metaData"`
	lock     sync.Mutex                   `json:"-"`
}

func NewDocumentsMetaData() *DocumentsMetaData {
	return &DocumentsMetaData{
		MetaData: make(map[string]*DocumentMetaData),
		lock:     sync.Mutex{},
	}
}

func (m *DocumentsMetaData) GetDocumentsMetadata(collectionIdentifier, docId, template string, docSize int,
	resetValue bool) *DocumentMetaData {
	defer m.lock.Unlock()
	m.lock.Lock()
	seed := int64(time.Now().UnixNano())
	docId = docId + " " + collectionIdentifier
	_, ok := m.MetaData[docId]
	if docSize == 0 {
		docSize = docgenerator.DefaultDocSize
	}
	if !ok {
		dObj := &DocumentMetaData{
			Seed:                 seed,
			DocId:                docId,
			DocSize:              docSize,
			Template:             template,
			countOfMutation:      0,
			SubDocMutations:      make(map[string]*SubDocMutations),
			subDocMutationsCount: 0,
			lock:                 sync.Mutex{},
		}
		m.MetaData[docId] = dObj
	}
	if resetValue {
		m.MetaData[docId].Seed = seed
		m.MetaData[docId].countOfMutation = 0
		m.MetaData[docId].DocSize = docSize
		m.MetaData[docId].Template = template
		m.MetaData[docId].SubDocMutations = make(map[string]*SubDocMutations)
		m.MetaData[docId].subDocMutationsCount = 0
	}
	return m.MetaData[docId]
}

func (m *DocumentsMetaData) RemoveDocument(collectionIdentifier, docId string) {
	docId = docId + " " + collectionIdentifier
	delete(m.MetaData, docId)
}
