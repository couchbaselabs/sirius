package cb_sdk

import (
	"github.com/couchbase/gocb/v2"
)

type CollectionObject struct {
	Collection *gocb.Collection `json:"-"`
}
