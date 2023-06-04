package sdk

import (
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"time"
)

const WaitUnityReadyTime = 120

type TimeoutsConfig struct {
	ConnectTimeout   int64 `json:"connectTimeout,omitempty" doc:"true"`
	KVTimeout        int64 `json:"KVTimeout,omitempty" doc:"true"`
	KVDurableTimeout int64 `json:"KVDurableTimeout,omitempty" doc:"true"`
}

type CompressionConfig struct {
	Disabled bool `json:"disabled,omitempty" doc:"true"`
	// MinSize specifies the minimum size of the document to consider compression.
	MinSize uint32 `json:"minSize,omitempty" doc:"true"`
	// MinRatio specifies the minimal compress ratio (compressed / original) for the document to be sent compressed.
	MinRatio float64 `json:"minRatio,omitempty" doc:"true"`
}

type ClusterConfig struct {
	Username          string            `json:"username" doc:"true"`
	Password          string            `json:"password" doc:"true"`
	ConnectionString  string            `json:"connectionString" doc:"true"`
	CompressionConfig CompressionConfig `json:"compressionConfig,omitempty" doc:"true"`
	TimeoutsConfig    TimeoutsConfig    `json:"timeoutsConfig,omitempty" doc:"true"`
}

func ValidateClusterConfig(c *ClusterConfig) error {
	if c == nil {
		return fmt.Errorf("clusterConfig is nil")
	}
	if c.ConnectionString == "" {
		return fmt.Errorf("empty connection string")
	}
	if c.Username == "" || c.Password == "" {
		return fmt.Errorf("connection string : %s | %w", c.ConnectionString, errors.New("credentials are missing"))
	}
	return nil
}

type ClusterObject struct {
	Cluster *gocb.Cluster            `json:"-"`
	Buckets map[string]*BucketObject `json:"-"`
}

func (c *ClusterObject) setBucketObject(bucketName string, b *BucketObject) {
	c.Buckets[bucketName] = b
}

func (c *ClusterObject) getBucketObject(bucketName string) (*BucketObject, error) {
	_, ok := c.Buckets[bucketName]
	if ok {
		return c.Buckets[bucketName], nil
	} else {
		bucket := c.Cluster.Bucket(bucketName)
		if err := bucket.WaitUntilReady(WaitUnityReadyTime*time.Second, nil); err != nil {
			return nil, err
		}

		b := &BucketObject{
			bucket: bucket,
			scopes: make(map[string]*ScopeObject),
		}

		c.setBucketObject(bucketName, b)
		return b, nil
	}
}

func Close(c *ClusterObject) {
	_ = c.Cluster.Close(nil)
}
