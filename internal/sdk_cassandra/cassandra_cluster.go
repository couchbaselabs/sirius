package sdk_cassandra

import (
	"fmt"
	"log"
	"time"

	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"github.com/gocql/gocql"
)

// ClusterConfigOptions defines the various options which can be used to configure the Cassandra cluster
type ClusterConfigOptions struct {
	KeyspaceName   string
	CQLVersion     string
	ProtoVersion   int
	ConnectTimeout time.Duration
	NumConns       int
}

// CassandraClusterConfig maintains the Config Settings for Connection to the Cassandra Cluster
type CassandraClusterConfig struct {
	ConnectionString     string               `json:"connectionString,omitempty"`
	Username             string               `json:"username,omitempty"`
	Password             string               `json:"password,omitempty"`
	ClusterConfigOptions ClusterConfigOptions `json:"clusterConfigOptions,omitempty"`
}

func ValidateClusterConfig(connStr, username, password string, c *CassandraClusterConfig) error {
	if c == nil {
		c = &CassandraClusterConfig{}
	}
	if connStr == "" {
		return err_sirius.InvalidConnectionString
	}
	if username == "" || password == "" {
		return fmt.Errorf("connection string : %s | %w", connStr, err_sirius.CredentialMissing)
	}
	return nil
}

// CassandraClusterObject has the CassandraClusterClient of type *gocql.Session.
// Here, *gocql.Session does not have Keyspace parameter configured, hence it is cluster level.
type CassandraClusterObject struct {
	CassandraClusterConfig *gocql.ClusterConfig                `json:"-"`
	CassandraClusterClient *gocql.Session                      `json:"-"`
	CassandraKeyspaces     map[string]*CassandraKeyspaceObject `json:"-"`
}

// setCassandraKeyspaceObject adds the Keyspace Object to the CassandraClusterObject
func (cassandraClusterObj *CassandraClusterObject) setCassandraKeyspaceObject(cassKeyspaceName string,
	cassKeyspaceObj *CassandraKeyspaceObject) {

	cassandraClusterObj.CassandraKeyspaces[cassKeyspaceName] = cassKeyspaceObj
}

func (cassandraClusterObj *CassandraClusterObject) getCassandraKeyspaceObject(cassKeyspaceName string) (*CassandraKeyspaceObject, error) {
	_, ok := cassandraClusterObj.CassandraKeyspaces[cassKeyspaceName]

	if !ok {
		cassClusterConfig := cassandraClusterObj.CassandraClusterConfig
		cassClusterConfig.Keyspace = cassKeyspaceName

		// Trying some cluster configurations here
		//log.Println("In getCassandraKeyspaceObject()")
		//cassClusterConfig.NumConns = 100
		//cassClusterConfig.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
		//cassClusterConfig.Timeout = 10 * time.Second
		//cassClusterConfig.WriteTimeout = 10 * time.Second
		//cassClusterConfig.ReconnectionPolicy = &gocql.ConstantReconnectionPolicy{MaxRetries: 3}

		cassandraSession, err := cassClusterConfig.CreateSession()
		if err != nil {
			log.Println("Unable to connect to Cassandra!")
			log.Println(err)
			return nil, err
		}

		cassKeyspaceObj := &CassandraKeyspaceObject{
			CassandraKeyspace: cassandraSession,
		}
		cassandraClusterObj.setCassandraKeyspaceObject(cassKeyspaceName, cassKeyspaceObj)
	}

	return cassandraClusterObj.CassandraKeyspaces[cassKeyspaceName], nil
}

// Close closes the Cassandra Client connection.
func Close(cassandraClusterObj *CassandraClusterObject) error {
	cassandraClusterObj.CassandraClusterClient.Close()
	return nil
}
