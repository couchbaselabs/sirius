package sdk

import "github.com/couchbase/gocb/v2"

type ScopeObject struct {
	scope       *gocb.Scope                 `json:"-"`
	collections map[string]*gocb.Collection `json:"-"`
}

func (s *ScopeObject) setCollection(collectionName string, c *gocb.Collection) {
	s.collections[collectionName] = c
}

func (s *ScopeObject) getCollection(collectionName string) (*gocb.Collection, error) {
	_, ok := s.collections[collectionName]
	if ok {
		return s.collections[collectionName], nil
	} else {
		c := s.scope.Collection(collectionName)
		s.setCollection(collectionName, c)
		return c, nil
	}
}
