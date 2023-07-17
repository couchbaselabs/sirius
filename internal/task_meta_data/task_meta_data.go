package task_meta_data

import (
	"sync"
	"time"
)

type CollectionMetaData struct {
	Seed         int64      `json:"_"`
	SeedEnd      int64      `json:"-"`
	DocType      string     `json:"-"`
	DocSize      int        `json:"-"`
	KeySize      int        `json:"-"`
	TemplateName string     `json:"-"`
	KeyPrefix    string     `json:"-"`
	KeySuffix    string     `json:"-"`
	lock         sync.Mutex `json:"-"`
}

func (cmd *CollectionMetaData) Lock() {
	cmd.lock.Lock()
}

func (cmd *CollectionMetaData) UnLock() {
	cmd.lock.Unlock()
}

type MetaData struct {
	MetaData map[string]*CollectionMetaData
	lock     sync.Mutex
}

func NewMetaData() *MetaData {
	return &MetaData{
		MetaData: make(map[string]*CollectionMetaData),
		lock:     sync.Mutex{},
	}
}

func (m *MetaData) GetCollectionMetadata(identifier string, keySize, docSize int, docType, keyPrefix, keySuffix,
	templateName string) *CollectionMetaData {
	defer m.lock.Unlock()
	m.lock.Lock()
	seed := int64(time.Now().UnixNano())
	_, ok := m.MetaData[identifier]
	if !ok {
		cObj := &CollectionMetaData{
			Seed:         seed,
			SeedEnd:      seed,
			DocType:      docType,
			DocSize:      docSize,
			KeySize:      keySize,
			TemplateName: templateName,
			KeyPrefix:    keyPrefix,
			KeySuffix:    keySuffix,
			lock:         sync.Mutex{},
		}
		m.MetaData[identifier] = cObj
	}
	return m.MetaData[identifier]
}
