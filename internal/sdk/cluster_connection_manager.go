package sdk

import (
	"github.com/couchbase/gocb/v2"
	"time"
)

type ConnectionManager struct {
	connectionString string
	username         string
	password         string
	bucket           string
	scope            string
	collection       string
	Cluster          *gocb.Cluster
	Bucket           *gocb.Bucket
	Collection       *gocb.Collection
}

// ConfigConnectionManager returns an instance of ConnectionManager.
func ConfigConnectionManager(connectionString, username, password, bucket, scope, collection string) *ConnectionManager {
	return &ConnectionManager{
		connectionString: connectionString,
		username:         username,
		password:         password,
		bucket:           bucket,
		scope:            scope,
		collection:       collection,
	}
}

// Connect will authenticate a user to connect with cluster. After successful
// connection it setups scope and collection.
func (c *ConnectionManager) Connect() error {
	var err error
	c.Cluster, err = gocb.Connect(c.connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: c.username,
			Password: c.password,
		},
		CircuitBreakerConfig: gocb.CircuitBreakerConfig{
			Disabled: true,
		},
	})

	if err != nil {
		return err
	}

	c.Bucket = c.Cluster.Bucket(c.bucket)
	if err = c.Bucket.WaitUntilReady(5*time.Second, nil); err != nil {
		return err
	}

	c.Collection = c.Bucket.Scope(c.scope).Collection(c.collection)
	return nil
}

// Close will close the connection to cluster.
func (c *ConnectionManager) Close() error {
	if err := c.Cluster.Close(nil); err != nil {
		return err
	}
	return nil
}
