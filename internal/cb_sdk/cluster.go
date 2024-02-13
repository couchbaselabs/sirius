package cb_sdk

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
	"log"
	"time"
)

const WaitUnityReadyTime = 10
const WaitUntilReadyTimeRetries = 5
const (
	ConnectTimeout      = "connectTimeout"
	KVTimeout           = "kvTimeout"
	KVDurableTimeout    = "kvDurableTimeout"
	CompressionDisabled = "compressionDisabled"
	CompressionMinSize  = "compressionMinSize"
	CompressionMaxSize  = "compressionMinSize"
)

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
	CompressionConfig CompressionConfig `json:"compressionConfig,omitempty" doc:"true"`
	TimeoutsConfig    TimeoutsConfig    `json:"timeoutsConfig,omitempty" doc:"true"`
	ConnectionString  string            `json:"connectionString,omitempty"`
	Username          string            `json:"username,omitempty"`
	Password          string            `json:"password,omitempty"`
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
