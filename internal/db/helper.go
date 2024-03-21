package db

import (
	"fmt"
	"log"
	"strings"

	"github.com/gocql/gocql"

	"github.com/couchbaselabs/sirius/internal/err_sirius"
)

type KeyValue struct {
	Key    string
	Doc    interface{}
	Offset int64
}

type Extras struct {
	CompressionDisabled bool    `json:"compressionDisabled,omitempty" doc:"true"`
	CompressionMinSize  uint32  `json:"compressionMinSize,omitempty" doc:"true"`
	CompressionMinRatio float64 `json:"compressionMinRatio,omitempty" doc:"true"`
	ConnectionTimeout   int     `json:"connectionTimeout,omitempty" doc:"true"`
	KVTimeout           int     `json:"KVTimeout,omitempty" doc:"true"`
	KVDurableTimeout    int     `json:"KVDurableTimeout,omitempty" doc:"true"`
	Bucket              string  `json:"bucket,omitempty" doc:"true"`
	Scope               string  `json:"scope,omitempty" doc:"true"`
	Collection          string  `json:"collection,omitempty" doc:"true"`
	Expiry              int     `json:"expiry,omitempty" doc:"true"`
	PersistTo           uint    `json:"persistTo,omitempty" doc:"true"`
	ReplicateTo         uint    `json:"replicateTo,omitempty" doc:"true"`
	Durability          string  `json:"durability,omitempty" doc:"true"`
	OperationTimeout    int     `json:"operationTimeout,omitempty" doc:"true"`
	Cas                 uint64  `json:"cas,omitempty" doc:"true"`
	IsXattr             bool    `json:"isXattr,omitempty" doc:"true"`
	StoreSemantic       int     `json:"storeSemantic,omitempty" doc:"true"`
	PreserveExpiry      bool    `json:"preserveExpiry,omitempty" doc:"true"`
	CreatePath          bool    `json:"createPath,omitempty" doc:"true"`
	SDKBatchSize        int     `json:"SDKBatchSize,omitempty" doc:"true"`
	Database            string  `json:"database,omitempty" doc:"true"`
	Query               string  `json:"query,omitempty" doc:"true"`
	ConnStr             string  `json:"connstr,omitempty" doc:"true"`
	Username            string  `json:"username,omitempty" doc:"true"`
	Password            string  `json:"password,omitempty" doc:"true"`
	ColumnarBucket      string  `json:"columnarBucket,omitempty" doc:"true"`
	ColumnarScope       string  `json:"columnarScope,omitempty" doc:"true"`
	ColumnarCollection  string  `json:"columnarCollection,omitempty" doc:"true"`
	Provisioned         bool    `json:"provisioned,omitempty" doc:"true"`
	ReadCapacity        int     `json:"readCapacity,omitempty" doc:"true"`
	WriteCapacity       int     `json:"writeCapacity,omitempty" doc:"true"`
	Keyspace            string  `json:"keyspace,omitempty" doc:"true"`
	Table               string  `json:"table,omitempty" doc:"true"`
	NumOfConns          int     `json:"numOfConns,omitempty" doc:"true"`
	SubDocPath          string  `json:"subDocPath,omitempty" doc:"true"`
	ReplicationFactor   int     `json:"replicationFactor,omitempty" doc:"true"`
	CassandraClass      string  `json:"cassandraClass,omitempty" doc:"true"`
	Port                string  `json:"port,omitempty" doc:"true"`
	MaxIdleConnections  int     `json:"maxIdleConnections,omitempty" doc:"true"`
	MaxOpenConnections  int     `json:"maxOpenConnections,omitempty" doc:"true"`
	MaxIdleTime         int     `json:"maxIdleTime,omitempty" doc:"true"`
	MaxLifeTime         int     `json:"maxLifeTime,omitempty" doc:"true"`
}

func validateStrings(values ...string) error {
	for _, v := range values {
		if v == "" {
			return fmt.Errorf("%s %w", v, err_sirius.InvalidInfo)
		}
	}
	return nil
}

func cassandraColumnExists(session *gocql.Session, keyspace, tableName, columnName string) bool {
	keyspaceMetadata, err := session.KeyspaceMetadata(keyspace)
	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}

	tableMetadata, found := keyspaceMetadata.Tables[tableName]
	if !found {
		return false
	}
	for _, column := range tableMetadata.Columns {
		if strings.EqualFold(column.Name, columnName) {
			return true
		}
	}
	return false
}
