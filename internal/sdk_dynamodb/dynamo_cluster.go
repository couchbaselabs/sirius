package sdk_dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/couchbaselabs/sirius/internal/err_sirius"
)

type DynamoClusterConfig struct {
	AccessKey   string `json:"accessKey" doc:"true"`
	SecretKeyId string `json:"secretKeyId" doc:"true"`
	Region      string `json:"region" doc:"true"`
}

func ValidateClusterConfig(accessKey, secretKeyId, region string, c *DynamoClusterConfig) error {
	if c == nil {
		c = &DynamoClusterConfig{}
	}
	if accessKey == "" {
		return err_sirius.InvalidConnectionString
	}
	if secretKeyId == "" || region == "" {
		return fmt.Errorf("AccessKey : %s | %w", accessKey, err_sirius.CredentialMissing)
	}
	return nil
}

type DynamoClusterObject struct {
	DynamoClusterClient *dynamodb.Client `json:"-"`
	Table               string           `json:"-"`
}
