package sdk_dynamodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DynamoConnectionManager struct {
	Clusters map[string]*DynamoClusterObject
	lock     sync.Mutex
}

func ConfigConnectionManager() *DynamoConnectionManager {
	return &DynamoConnectionManager{
		Clusters: make(map[string]*DynamoClusterObject),
		lock:     sync.Mutex{},
	}
}

func (cm *DynamoConnectionManager) DisconnectAll() {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	for cS := range cm.Clusters {
		delete(cm.Clusters, cS)
	}
}

func (cm *DynamoConnectionManager) setClientObject(clusterIdentifier string, c *DynamoClusterObject) {
	cm.Clusters[clusterIdentifier] = c
}

func (cm *DynamoConnectionManager) getDynamoDBObject(clusterConfig *DynamoClusterConfig) (*DynamoClusterObject, error) {
	if clusterConfig == nil {
		return nil, fmt.Errorf("unable to parse clusterConfig | %w", errors.New("clusterConfig is nil"))
	}

	clusterIdentifier := clusterConfig.Region
	_, ok := cm.Clusters[clusterIdentifier]
	if !ok {
		if err := ValidateClusterConfig(clusterConfig.AccessKey, clusterConfig.SecretKeyId, clusterConfig.Region, clusterConfig); err != nil {
			return nil, err
		}
		httpClient := http.NewBuildableClient().WithDialerOptions(func(d *net.Dialer) {
			d.KeepAlive = 0
		})
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(clusterConfig.AccessKey, clusterConfig.SecretKeyId, "")),
			config.WithRegion(clusterConfig.Region),
			config.WithRetryMaxAttempts(0),
			config.WithHTTPClient(httpClient),
		)
		// cfg.APIOptions = append(cfg.APIOptions)

		if err != nil {
			fmt.Println("Unable to connect to DynamoDB!")
			log.Fatal(err)
			return nil, err
		}

		client := dynamodb.NewFromConfig(cfg)
		clusterObject := &DynamoClusterObject{DynamoClusterClient: client, Table: ""}
		cm.setClientObject(clusterIdentifier, clusterObject)
	}
	return cm.Clusters[clusterIdentifier], nil
}

func (cm *DynamoConnectionManager) GetCluster(clusterConfig *DynamoClusterConfig) (*DynamoClusterObject, error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getDynamoDBObject(clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	return cObj, nil
}
