package sdk_mysql

import (
	"fmt"

	"github.com/couchbaselabs/sirius/internal/err_sirius"
)

// MongoClusterConfig maintains the Config Settings for Connection to the MongoDB Cluster
type SqlClusterConfig struct {
	ConnectionString   string `json:"connectionString,omitempty"`
	Username           string `json:"username,omitempty"`
	Password           string `json:"password,omitempty"`
	Database           string `json:"database,omitempty"`
	MaxIdleConnections int    `json:"maxIdleConnections,omitempty" `
	MaxOpenConnections int    `json:"maxOpenConnections,omitempty"`
	MaxIdleTime        int    `json:"maxIdleTime,omitempty"`
	MaxLifeTime        int    `json:"maxLifeTime,omitempty" `
	Port               string `json:"port,omitempty" doc:"true"`
}

func ValidateClusterConfig(connStr, username, password string, c *SqlClusterConfig) error {
	if c == nil {
		c = &SqlClusterConfig{}
	}
	if connStr == "" {
		return err_sirius.InvalidConnectionString
	}
	if username == "" || password == "" {
		return fmt.Errorf("connection string : %s | %w", connStr, err_sirius.CredentialMissing)
	}
	return nil
}
