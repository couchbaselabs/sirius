package sdk

import "github.com/couchbase/gocb/v2"

type BucketObject struct {
	bucket *gocb.Bucket            `json:"-"`
	scopes map[string]*ScopeObject `json:"-"`
}

func (b *BucketObject) setScopeObject(scopeName string, s *ScopeObject) {
	b.scopes[scopeName] = s
}

func (b *BucketObject) getScopeObject(scopeName string) (*ScopeObject, error) {
	_, ok := b.scopes[scopeName]
	if ok {
		return b.scopes[scopeName], nil
	} else {
		s := b.bucket.Scope(scopeName)
		sObj := &ScopeObject{
			scope:       s,
			collections: make(map[string]*gocb.Collection),
		}
		b.setScopeObject(scopeName, sObj)
		return sObj, nil
	}

}
