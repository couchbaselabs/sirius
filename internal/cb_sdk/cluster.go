package cb_sdk

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/task_errors"
	"log"
	"time"
)

const WaitUnityReadyTime = 10
const WaitUntilReadyTimeRetries = 5

type TimeoutsConfig struct {
	ConnectTimeout   int `json:"connectTimeout,omitempty" doc:"true"`
	KVTimeout        int `json:"KVTimeout,omitempty" doc:"true"`
	KVDurableTimeout int `json:"KVDurableTimeout,omitempty" doc:"true"`
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
		return task_errors.ErrParsingClusterConfig
	}
	if c.ConnectionString == "" {
		return task_errors.ErrInvalidConnectionString
	}
	if c.Username == "" || c.Password == "" {
		return fmt.Errorf("connection string : %s | %w", c.ConnectionString, task_errors.ErrCredentialMissing)
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

	if !ok {
		bucket := c.Cluster.Bucket(bucketName)
		var waitUntilReadyError error
		for i := 0; i < WaitUntilReadyTimeRetries; i++ {
			if waitUntilReadyError = bucket.WaitUntilReady(WaitUnityReadyTime*time.Second,
				nil); waitUntilReadyError == nil {
				break
			}
			log.Println("retrying bucket WaitUntilReady")
		}

		if waitUntilReadyError != nil {
			return nil, waitUntilReadyError
		}

		b := &BucketObject{
			bucket: bucket,
			scopes: make(map[string]*ScopeObject),
		}
		c.setBucketObject(bucketName, b)
	}

	return c.Buckets[bucketName], nil
}

func Close(c *ClusterObject) {
	_ = c.Cluster.Close(nil)
}
