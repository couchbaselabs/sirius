package sdk

import (
	"github.com/couchbase/gocb/v2"
)

type ScopeObject struct {
	scope       *gocb.Scope                  `json:"-"`
	collections map[string]*CollectionObject `json:"-"`
}

func (s *ScopeObject) setCollection(collectionName string, c *CollectionObject) {
	s.collections[collectionName] = c
}

func (s *ScopeObject) getCollection(collectionName string) (*CollectionObject,
	error) {
	_, ok := s.collections[collectionName]
	if ok {
		return s.collections[collectionName], nil
	} else {
		c := s.scope.Collection(collectionName)
		cObj := &CollectionObject{
			Collection: c,
		}
		s.setCollection(collectionName, cObj)
	}
	return s.collections[collectionName], nil
}
