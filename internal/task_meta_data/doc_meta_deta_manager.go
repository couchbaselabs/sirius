package task_meta_data

import (
	"github.com/couchbaselabs/sirius/internal/template"
	"github.com/jaswdr/faker"
	"sync"
	"time"
)

type SubDocMutations struct {
	Seed            int64      `json:"seed"`
	SubPath         string     `json:"subPath"`
	countOfMutation int        `json:"countOfMutation"`
	DocSize         int        `json:"docSize"`
	lock            sync.Mutex `json:"-"`
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
	Seed            int64                       `json:"seed"`
	DocId           string                      `json:"docId"`
	DocSize         int                         `json:"docSize"`
	Template        string                      `json:"template"`
	countOfMutation int                         `json:"countOfMutation"`
	SubDocMutations map[string]*SubDocMutations `json:"subDocMutations"`
	lock            sync.Mutex                  `json:"-"`
}

func (d *DocumentMetaData) IncrementCount() {
	d.countOfMutation++
}

func (d *DocumentMetaData) DecrementCount() {
	defer d.lock.Unlock()
	d.lock.Lock()
	d.countOfMutation++
}

func (d *DocumentMetaData) RetracePreviousMutations(template template.Template, doc interface{},
	fake *faker.Faker) interface{} {
	defer d.lock.Unlock()
	defer d.lock.Lock()
	for i := 0; i < d.countOfMutation; i++ {
		template.UpdateDocument([]string{}, doc, fake)
	}
	return doc
}

func (d *DocumentMetaData) UpdateDocument(t template.Template, doc interface{}, fake *faker.Faker) interface{} {
	defer d.lock.Unlock()
	defer d.lock.Lock()
	updatedDoc, _ := t.UpdateDocument([]string{}, doc, fake)
	d.SubDocMutations = make(map[string]*SubDocMutations)
	d.IncrementCount()
	return updatedDoc
}

func (d *DocumentMetaData) SubDocument(subPath, template string, docSize int, reset bool) *SubDocMutations {
	defer d.lock.Unlock()
	d.lock.Lock()
	seed := int64(time.Now().UnixNano())
	if _, ok := d.SubDocMutations[subPath]; !ok {
		d.SubDocMutations[subPath] = &SubDocMutations{
			Seed:            seed,
			SubPath:         subPath,
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

func (m *DocumentsMetaData) GetDocumentsMetadata(docId, template string, docSize int,
	resetValue bool) *DocumentMetaData {
	defer m.lock.Unlock()
	m.lock.Lock()
	seed := int64(time.Now().UnixNano())
	_, ok := m.MetaData[docId]
	if !ok {
		dObj := &DocumentMetaData{
			Seed:            seed,
			DocId:           docId,
			DocSize:         docSize,
			Template:        template,
			countOfMutation: 0,
			SubDocMutations: make(map[string]*SubDocMutations),
			lock:            sync.Mutex{},
		}
		m.MetaData[docId] = dObj
	}
	if resetValue {
		m.MetaData[docId].Seed = seed
		m.MetaData[docId].countOfMutation = 0
		m.MetaData[docId].DocSize = docSize
		m.MetaData[docId].Template = template
		m.MetaData[docId].SubDocMutations = make(map[string]*SubDocMutations)
	}
	return m.MetaData[docId]
}

func (m *DocumentsMetaData) RemoveDocument(key string) {
	delete(m.MetaData, key)
}
