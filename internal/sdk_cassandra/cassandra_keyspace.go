package sdk_cassandra

import (
	"github.com/gocql/gocql"
)

type CassandraKeyspaceObject struct {
	CassandraKeyspace *gocql.Session `json:"-"`
}
