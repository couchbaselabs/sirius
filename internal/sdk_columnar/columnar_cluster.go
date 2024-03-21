package sdk_columnar

import (
	"fmt"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
)

type ClusterConfig struct {
	ConnectionString string `json:"connectionString,omitempty"`
	Username         string `json:"username,omitempty"`
	Password         string `json:"password,omitempty"`
}

func ValidateClusterConfig(connStr, username, password string, c *ClusterConfig) error {
	if c == nil {
		c = &ClusterConfig{}
	}
	if connStr == "" {
		return err_sirius.InvalidConnectionString
	}
	if username == "" || password == "" {
		return fmt.Errorf("connection string : %s | %w", connStr, err_sirius.CredentialMissing)
	}
	return nil
}

type ClusterObject struct {
	Cluster *gocb.Cluster `json:"-"`
}

func Close(c *ClusterObject) {
	_ = c.Cluster.Close(nil)
}
