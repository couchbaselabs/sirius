package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/couchbaselabs/sirius/internal/sdk_dynamodb"
)

type Dynamo struct {
	connectionManager *sdk_dynamodb.DynamoConnectionManager
}

type perDynamoDocResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

type dynamoOperationResult struct {
	key    string
	result perDynamoDocResult
}

func NewDynamoConnectionManager() *Dynamo {
	return &Dynamo{
		connectionManager: sdk_dynamodb.ConfigConnectionManager(),
	}
}

// Operation Results for Single Operations like Create, Update, Touch and Delete

func newDynamoOperationResult(key string, value interface{}, err error, status bool, offset int64) *dynamoOperationResult {
	return &dynamoOperationResult{
		key: key,
		result: perDynamoDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (d *dynamoOperationResult) Key() string {
	return d.key
}

func (d *dynamoOperationResult) Value() interface{} {
	return d.result.value
}

func (d *dynamoOperationResult) GetStatus() bool {
	return d.result.status
}

func (d *dynamoOperationResult) GetError() error {
	return d.result.error
}

func (d *dynamoOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (d *dynamoOperationResult) GetOffset() int64 {
	return d.result.offset
}

// Operation Results for Bulk Operations like Bulk-Create, Bulk-Update, Bulk-Touch and Bulk-Delete

type dynamoBulkOperationResult struct {
	keyValues map[string]perDynamoDocResult
}

func newDynamoBulkOperation() *dynamoBulkOperationResult {
	return &dynamoBulkOperationResult{
		keyValues: make(map[string]perDynamoDocResult),
	}
}

func (d *dynamoBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	d.keyValues[key] = perDynamoDocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (d *dynamoBulkOperationResult) Value(key string) interface{} {
	if x, ok := d.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (d *dynamoBulkOperationResult) GetStatus(key string) bool {
	if x, ok := d.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (d *dynamoBulkOperationResult) GetError(key string) error {
	if x, ok := d.keyValues[key]; ok {
		return x.error
	}
	return errors.New("Key not found in bulk operation")
}

func (d *dynamoBulkOperationResult) GetExtra(key string) map[string]any {
	if _, ok := d.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (d *dynamoBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := d.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (d *dynamoBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		d.keyValues[x.Key] = perDynamoDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (d *dynamoBulkOperationResult) GetSize() int {
	return len(d.keyValues)
}

// Operation Result for SubDoc Operations
type perDynamoSubDocResult struct {
	keyValue []KeyValue
	error    error
	status   bool
	offset   int64
}

type dynamoSubDocOperationResult struct {
	key    string
	result perDynamoSubDocResult
}

func newDynamoSubDocOperationResult(key string, keyValue []KeyValue, err error, status bool, offset int64) *dynamoSubDocOperationResult {
	return &dynamoSubDocOperationResult{
		key: key,
		result: perDynamoSubDocResult{
			keyValue: keyValue,
			error:    err,
			status:   status,
			offset:   offset,
		},
	}
}

func (d *dynamoSubDocOperationResult) Key() string {
	return d.key
}

func (d *dynamoSubDocOperationResult) Value(subPath string) (interface{}, int64) {
	for _, x := range d.result.keyValue {
		if x.Key == subPath {
			return x.Doc, x.Offset
		}
	}
	return nil, -1
}

func (d *dynamoSubDocOperationResult) Values() []KeyValue {
	return d.result.keyValue
}

func (d *dynamoSubDocOperationResult) GetError() error {
	return d.result.error
}

func (d *dynamoSubDocOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (d *dynamoSubDocOperationResult) GetOffset() int64 {
	return d.result.offset
}

func (d Dynamo) Connect(connStr, username, password string, extra Extras) error {
	clusterConfig := &sdk_dynamodb.DynamoClusterConfig{
		Region:      connStr,
		AccessKey:   username,
		SecretKeyId: password,
	}

	if _, err := d.connectionManager.GetCluster(clusterConfig); err != nil {
		return err
	}

	return nil
}
func (d Dynamo) TableExists(connStr, username, password string, extra Extras) (bool, string) {
	exists := true
	errString := ""
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	if err := validateStrings(extra.Table); err != nil {
		return false, "Table name is missing"
	}
	_, err := DynamoDbClient.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: aws.String(extra.Table)},
	)

	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			errString = "Table %v does not exist. " + extra.Table
			err = nil
		} else {
			errString = "Couldn't determine existence of table  " + extra.Table + ". Here's why:   " + err.Error()
		}
		exists = false
	}
	return exists, errString
}

func (d Dynamo) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc, errors.New(errString), false, keyValue.Offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient

	item, err := attributevalue.MarshalMap(keyValue.Doc)
	if err != nil {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}

	out, err := DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(extra.Table), Item: item, ConditionExpression: aws.String("attribute_not_exists(id)")})
	if err != nil {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	if out == nil {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful CREATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newDynamoOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (d Dynamo) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {

	if err := validateStrings(connStr, username, password); err != nil {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc, errors.New(errString), false, keyValue.Offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient

	item, err := attributevalue.MarshalMap(keyValue.Doc)
	if err != nil {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	out, err := DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(extra.Table), Item: item})
	if err != nil {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	if out == nil {
		return newDynamoOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newDynamoOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)

}

func (d Dynamo) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoOperationResult(key, nil, errors.New(errString), false, offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	filter, err := attributevalue.Marshal(key)
	if err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	}
	response, err := DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{"id": filter}, TableName: aws.String(extra.Table),
	})
	if err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	} else {
		var result map[string]interface{}
		err = attributevalue.UnmarshalMap(response.Item, &result)
		if err != nil {
			return newDynamoOperationResult(key, nil, err, false, offset)
		}

		if result == nil {
			return newDynamoOperationResult(key, nil,
				fmt.Errorf("result is nil even after successful READ operation %s ", connStr), false,
				offset)
		}
		return newDynamoOperationResult(key, result, nil, true, offset)

	}
}

func (d Dynamo) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoOperationResult(key, nil, errors.New(errString), false, offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	filter, _ := attributevalue.Marshal(key)
	out, err := DynamoDbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: aws.String(extra.Table), Key: map[string]types.AttributeValue{"id": filter}})
	if err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	}
	if out == nil {
		return newDynamoOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful DELETE operation %s ", connStr), false, offset)
	}
	return newDynamoOperationResult(key, nil, nil, true, offset)
}
func (d Dynamo) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	// panic("Implement the function")
	if err := validateStrings(connStr, username, password); err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoOperationResult(key, nil, errors.New(errString), false, offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	filter, err := attributevalue.Marshal(key)
	if err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	}
	newExpirationTime := time.Now().Add((time.Minute) * time.Duration(extra.Expiry))
	ttl, err := attributevalue.Marshal(newExpirationTime)
	if err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	}
	out, err := DynamoDbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName:                 aws.String(extra.Table),
		Key:                       map[string]types.AttributeValue{"id": filter},
		ExpressionAttributeValues: map[string]types.AttributeValue{":ttl": ttl},
		UpdateExpression:          aws.String("SET #ttlAttr = :ttl"),
		ExpressionAttributeNames: map[string]string{
			"#ttlAttr": "ttl",
		},
	})
	if err != nil {
		return newDynamoOperationResult(key, nil, err, false, offset)
	}
	if out == nil {
		return newDynamoOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false,
			offset)
	}
	return newDynamoOperationResult(key, nil, nil, true, offset)
}

func (d Dynamo) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (d Dynamo) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newDynamoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		result.failBulk(keyValues, errors.New(errString))
		return result
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient

	keyToOffset := make(map[string]int64)
	retries := 0.0
	var writeReqs []types.WriteRequest
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		item, err := attributevalue.MarshalMap(x.Doc)
		if err != nil {
			result.failBulk(keyValues, errors.New("Couldn't process data for batch . Here's why:  \n"+strconv.Itoa(len(keyValues))+err.Error()))
			return result
		} else {
			writeReqs = append(
				writeReqs,
				types.WriteRequest{PutRequest: &types.PutRequest{Item: item}},
			)
		}
	}
	dynamoBulkWriteResult := &dynamodb.BatchWriteItemOutput{}
	var err error
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{extra.Table: writeReqs}}
	for {
		dynamoBulkWriteResult, err = DynamoDbClient.BatchWriteItem(context.TODO(), input)
		timeout := int(math.Pow(2, retries)-1) * 100
		if err != nil {
			time.Sleep(time.Duration(timeout) * time.Millisecond)
			continue
		}
		if dynamoBulkWriteResult != nil && dynamoBulkWriteResult.UnprocessedItems != nil && len(dynamoBulkWriteResult.UnprocessedItems) > 0 {
			time.Sleep(time.Duration(timeout) * time.Millisecond)
			input.RequestItems = dynamoBulkWriteResult.UnprocessedItems
		} else {
			break
		}
		retries += 1
	}
	var failed map[string]bool
	var fail map[string]interface{}
	failed = make(map[string]bool)
	if dynamoBulkWriteResult != nil && dynamoBulkWriteResult.UnprocessedItems != nil && len(dynamoBulkWriteResult.UnprocessedItems) > 0 {
		req := dynamoBulkWriteResult.UnprocessedItems[extra.Table]
		for _, r := range req {
			_ = attributevalue.UnmarshalMap(r.PutRequest.Item, &fail)
			failed[fail["id"].(string)] = false

		}
	}
	for _, x := range keyValues {
		_, ok := failed[x.Key]
		if ok {
			result.AddResult(x.Key, x.Doc, errors.New("document unprocessed"), false, keyToOffset[x.Key])
		} else {
			result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
		}
	}

	return result
}

// Warmup
/*
 * Validates all the string fields
 * TODO Checks if the DynamoDB Database or Collection exists
 * TODO If Database or Collection name is not specified then we create a Default
 */
func (d Dynamo) Warmup(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return (errors.New(errString))
	}
	return nil
}

func (d Dynamo) Close(connStr string, extra Extras) error {
	d.connectionManager.DisconnectAll()
	return nil
}

func (d Dynamo) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newDynamoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		result.failBulk(keyValues, errors.New(errString))
		return result
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	var writeReqs []types.WriteRequest
	for _, x := range keyValues {
		item, err := attributevalue.MarshalMap(x.Doc)
		if err != nil {
			result.failBulk(keyValues, errors.New("Couldn't marshal data for batch writing. Here's why:  \n"+err.Error()))
			return result
		} else {
			writeReqs = append(
				writeReqs,
				types.WriteRequest{PutRequest: &types.PutRequest{Item: item}},
			)
		}
	}
	dynamoBulkWriteResult := &dynamodb.BatchWriteItemOutput{}
	var err error
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{extra.Table: writeReqs}}
	for {
		dynamoBulkWriteResult, err = DynamoDbClient.BatchWriteItem(context.TODO(), input)
		if err != nil {
			continue
		}
		if dynamoBulkWriteResult != nil && dynamoBulkWriteResult.UnprocessedItems != nil && len(dynamoBulkWriteResult.UnprocessedItems) > 0 {
			input.RequestItems = dynamoBulkWriteResult.UnprocessedItems
		} else {
			break
		}

	}
	var failed map[string]bool
	var fail map[string]interface{}
	failed = make(map[string]bool)
	if dynamoBulkWriteResult != nil && dynamoBulkWriteResult.UnprocessedItems != nil && len(dynamoBulkWriteResult.UnprocessedItems) > 0 {
		req := dynamoBulkWriteResult.UnprocessedItems[extra.Table]
		for _, r := range req {
			_ = attributevalue.UnmarshalMap(r.PutRequest.Item, &fail)
			failed[fail["id"].(string)] = false

		}
	}
	for _, x := range keyValues {
		_, ok := failed[x.Key]
		if ok {
			result.AddResult(x.Key, x.Doc, errors.New("document unprocessed"), false, keyToOffset[x.Key])
		} else {
			result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
		}
	}

	return result
}

func (d Dynamo) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newDynamoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		result.failBulk(keyValues, errors.New(errString))
		return result
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient

	keyToOffset := make(map[string]int64)
	var writeReqs []types.WriteRequest
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		item, err := attributevalue.Marshal(x.Key)
		if err != nil {
			result.failBulk(keyValues, errors.New("Couldn't marshal data for batch writing. Here's why:  \n"+err.Error()))
			return result
		} else {
			writeReqs = append(
				writeReqs,
				types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{"id": item}}},
			)
		}
	}
	dynamoBulkWriteResult := &dynamodb.BatchWriteItemOutput{}
	var err error
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{extra.Table: writeReqs}}

	for {
		dynamoBulkWriteResult, err = DynamoDbClient.BatchWriteItem(context.TODO(), input)
		if err != nil {
			continue
		}
		if dynamoBulkWriteResult != nil && dynamoBulkWriteResult.UnprocessedItems != nil && len(dynamoBulkWriteResult.UnprocessedItems) > 0 {
			input.RequestItems = dynamoBulkWriteResult.UnprocessedItems
		} else {
			break
		}
	}
	var failed map[string]bool
	var fail map[string]interface{}
	failed = make(map[string]bool)
	if dynamoBulkWriteResult != nil && dynamoBulkWriteResult.UnprocessedItems != nil && len(dynamoBulkWriteResult.UnprocessedItems) > 0 {
		req := dynamoBulkWriteResult.UnprocessedItems[extra.Table]
		for _, r := range req {
			_ = attributevalue.UnmarshalMap(r.PutRequest.Item, &fail)
			failed[fail["id"].(string)] = false

		}
	}
	for _, x := range keyValues {
		_, ok := failed[x.Key]
		if ok {
			result.AddResult(x.Key, nil, errors.New("document unprocessed"), false, x.Offset)
		} else {
			result.AddResult(x.Key, nil, nil, true, x.Offset)
		}
	}

	return result
}
func (d Dynamo) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newDynamoBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		result.failBulk(keyValues, errors.New(errString))
		return result
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	var docs []map[string]types.AttributeValue
	keyToOffset := make(map[string]int64)

	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		item, err := attributevalue.Marshal(x.Key)
		if err != nil {
			result.failBulk(keyValues, errors.New("Couldn't marshal data for batch reading. Here's why:  \n"+err.Error()))
			return result
		} else {

			docs = append(docs, map[string]types.AttributeValue{"id": item})
		}
	}
	dynamoBulkWriteResult := &dynamodb.BatchGetItemOutput{}
	var err error
	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{extra.Table: {Keys: docs}}}

	for {
		dynamoBulkWriteResult, err = DynamoDbClient.BatchGetItem(context.TODO(), input)
		if err != nil {
			continue
		}
		if dynamoBulkWriteResult != nil && dynamoBulkWriteResult.UnprocessedKeys != nil && len(dynamoBulkWriteResult.UnprocessedKeys) > 0 {
			input.RequestItems = dynamoBulkWriteResult.UnprocessedKeys
		} else {
			break
		}
	}

	results := make(map[string]map[string]interface{})
	var resultDoc map[string]interface{}
	if dynamoBulkWriteResult.UnprocessedKeys != nil && len(dynamoBulkWriteResult.UnprocessedKeys) > 0 {
		req := dynamoBulkWriteResult.UnprocessedKeys[extra.Table]
		for _, r := range req.Keys {
			_ = attributevalue.UnmarshalMap(r, &resultDoc)
			results[resultDoc["id"].(string)] = resultDoc
			result.AddResult(resultDoc["id"].(string), nil, errors.New("document unprocessed"), false, keyToOffset[resultDoc["id"].(string)])
		}
	}

	for _, doc := range dynamoBulkWriteResult.Responses[extra.Table] {
		_ = attributevalue.UnmarshalMap(doc, &resultDoc)
		results[resultDoc["id"].(string)] = resultDoc
		if resultDoc == nil {
			result.AddResult(resultDoc["id"].(string), nil, errors.New("result is nil even after successful READ operation"), false, keyToOffset[resultDoc["id"].(string)])
		} else {
			result.AddResult(resultDoc["id"].(string), resultDoc, nil, true, keyToOffset[resultDoc["id"].(string)])
		}
	}
	if len(keyValues) > len(results) {
		for _, x := range keyValues {
			if results[x.Key] == nil {
				result.AddResult(resultDoc["id"].(string), nil, errors.New("document unprocessed"), false, keyToOffset[resultDoc["id"].(string)])
			}
		}
	}
	return result
}

func (d Dynamo) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	var err error
	if err = validateStrings(connStr, username, password); err != nil {
		return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoSubDocOperationResult(key, keyValues, errors.New(errString), false, offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	var response *dynamodb.UpdateItemOutput
	var update expression.UpdateBuilder
	var attributeMap map[string]map[string]interface{}
	for _, keyVal := range keyValues {
		update = update.Set(expression.Name(*aws.String(keyVal.Key)), expression.IfNotExists(expression.Name(keyVal.Key), expression.Value(keyVal.Doc)))
	}
	update = update.Add(expression.Name("mutated"), expression.Value(1))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return newDynamoSubDocOperationResult(key, keyValues, errors.New("Couldn't build expression for update. Here's why: %v\n"+err.Error()), false, offset)
	} else {
		filter, _ := attributevalue.Marshal(key)
		response, err = DynamoDbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			TableName:                 aws.String(extra.Table),
			Key:                       map[string]types.AttributeValue{"id": filter},
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			UpdateExpression:          expr.Update(),
			ReturnValues:              types.ReturnValueUpdatedNew,
		})
		if err != nil {
			return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)

		} else {
			err = attributevalue.UnmarshalMap(response.Attributes, &attributeMap)
			if err != nil {
				return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)
			}

		}

		return newDynamoSubDocOperationResult(key, keyValues, nil, true, offset)
	}
}

func (d Dynamo) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	var err error
	if err = validateStrings(connStr, username, password); err != nil {
		return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoSubDocOperationResult(key, keyValues, errors.New(errString), false, offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	var response *dynamodb.UpdateItemOutput
	var update expression.UpdateBuilder
	var attributeMap map[string]map[string]interface{}
	for _, keyVal := range keyValues {
		update = update.Set(expression.Name(keyVal.Key), expression.Value(keyVal.Doc))
	}
	update = update.Add(expression.Name("mutated"), expression.Value(1))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return newDynamoSubDocOperationResult(key, keyValues, errors.New("Couldn't build expression for update. Here's why: %v\n"+err.Error()), false, offset)
	} else {
		filter, _ := attributevalue.Marshal(key)
		response, err = DynamoDbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			TableName:                 aws.String(extra.Table),
			Key:                       map[string]types.AttributeValue{"id": filter},
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			UpdateExpression:          expr.Update(),
			ReturnValues:              types.ReturnValueUpdatedNew,
		})
		if err != nil {
			return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)

		} else {
			err = attributevalue.UnmarshalMap(response.Attributes, &attributeMap)
			if err != nil {
				return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)
			}
		}
		return newDynamoSubDocOperationResult(key, keyValues, nil, true, offset)
	}
}

func (d Dynamo) Increment(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (d Dynamo) ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	var err error
	if err = validateStrings(connStr, username, password); err != nil {
		return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoSubDocOperationResult(key, keyValues, errors.New(errString), false, offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	var response *dynamodb.UpdateItemOutput
	var update expression.UpdateBuilder
	var attributeMap map[string]map[string]interface{}
	for _, keyVal := range keyValues {
		update = update.Set(expression.Name(keyVal.Key), expression.Value(keyVal.Doc))
	}
	update = update.Add(expression.Name("mutated"), expression.Value(1))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return newDynamoSubDocOperationResult(key, keyValues, errors.New("Couldn't build expression for update. Here's why: %v\n"+err.Error()), false, offset)
	} else {
		filter, _ := attributevalue.Marshal(key)
		response, err = DynamoDbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			TableName:                 aws.String(extra.Table),
			Key:                       map[string]types.AttributeValue{"id": filter},
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			UpdateExpression:          expr.Update(),
			ReturnValues:              types.ReturnValueUpdatedNew,
		})
		if err != nil {
			return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)

		} else {
			err = attributevalue.UnmarshalMap(response.Attributes, &attributeMap)
			if err != nil {
				return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)
			}
		}
		return newDynamoSubDocOperationResult(key, keyValues, nil, true, offset)
	}
}

func (d Dynamo) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	var err error
	if err = validateStrings(connStr, username, password); err != nil {
		return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoSubDocOperationResult(key, keyValues, errors.New(errString), false, offset)
	}
	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	var response *dynamodb.UpdateItemOutput
	var update expression.UpdateBuilder
	var attributeMap map[string]map[string]interface{}
	for _, keyVal := range keyValues {
		update = update.Remove(expression.Name(keyVal.Key))
	}
	update = update.Add(expression.Name("mutated"), expression.Value(1))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return newDynamoSubDocOperationResult(key, keyValues, errors.New("Couldn't build expression for update. Here's why: %v\n"+err.Error()), false, offset)
	} else {
		filter, _ := attributevalue.Marshal(key)
		response, err = DynamoDbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
			TableName:                 aws.String(extra.Table),
			Key:                       map[string]types.AttributeValue{"id": filter},
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			UpdateExpression:          expr.Update(),
			ReturnValues:              types.ReturnValueUpdatedNew,
		})
		if err != nil {
			return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)

		} else {
			err = attributevalue.UnmarshalMap(response.Attributes, &attributeMap)
			if err != nil {
				return newDynamoSubDocOperationResult(key, keyValues, err, false, offset)
			}
		}
		return newDynamoSubDocOperationResult(key, keyValues, nil, true, offset)
	}
}

func (d Dynamo) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	var err error
	if err = validateStrings(connStr, username, password); err != nil {
		return newDynamoSubDocOperationResult(key, nil, err, false, offset)
	}
	ok, errString := d.TableExists(connStr, username, password, extra)
	if !ok {
		return newDynamoSubDocOperationResult(key, nil, errors.New(errString), false, offset)
	}

	DynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	filter, err := attributevalue.Marshal(key)
	if err != nil {
		return newDynamoSubDocOperationResult(key, nil, err, false, offset)
	}
	projection := ""
	for _, key := range keyValues {
		projection = projection + key.Key + ","
	}
	response, err := DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key:                  map[string]types.AttributeValue{"id": filter},
		TableName:            aws.String(extra.Table),
		ProjectionExpression: aws.String(projection[:len(projection)-1]),
	})
	if err != nil {
		return newDynamoSubDocOperationResult(key, nil, err, false, offset)
	} else {
		var result map[string]interface{}
		err = attributevalue.UnmarshalMap(response.Item, &result)
		if err != nil {
			return newDynamoSubDocOperationResult(key, nil, err, false, offset)
		}
		if result == nil {
			return newDynamoSubDocOperationResult(key, nil,
				fmt.Errorf("result is nil even after successful READ operation %s ", connStr), false,
				offset)
		}
		return newDynamoSubDocOperationResult(key, keyValues, nil, true, offset)

	}
}

func (d Dynamo) CreateDatabase(connStr, username, password string, extra Extras, templateName string, docSize int) (string, error) {
	if err := validateStrings(connStr, username, password); err != nil {
		return "", err
	}
	err := d.Connect(connStr, username, password, extra)
	if err != nil {
		return "", err
	}
	if extra.Table == "" {
		return "", errors.New("Empty Table name")
	}
	dynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	var dynamoInput dynamodb.CreateTableInput
	dynamoInput.AttributeDefinitions = []types.AttributeDefinition{{
		AttributeName: aws.String("id"),
		AttributeType: types.ScalarAttributeTypeS,
	}}
	dynamoInput.KeySchema = []types.KeySchemaElement{{
		AttributeName: aws.String("id"),
		KeyType:       types.KeyTypeHash,
	}}
	dynamoInput.TableName = aws.String(extra.Table)
	if extra.Provisioned {
		dynamoInput.BillingMode = types.BillingModeProvisioned
		dynamoInput.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(extra.ReadCapacity)),
			WriteCapacityUnits: aws.Int64(int64(extra.WriteCapacity)),
		}
	} else {
		dynamoInput.BillingMode = types.BillingModePayPerRequest
	}
	table, err := dynamoDbClient.CreateTable(context.TODO(), &dynamoInput)
	if err != nil {
		return "", err
	}
	waiter := dynamodb.NewTableExistsWaiter(dynamoDbClient)
	err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(extra.Table)}, 5*time.Minute)
	if err != nil {
		return "", err
	}
	return "Table successfully created at " + table.TableDescription.CreationDateTime.GoString(), nil
}

func (d Dynamo) DeleteDatabase(connStr, username, password string, extra Extras) (string, error) {
	if err := validateStrings(connStr, username, password); err != nil {
		return "", err
	}
	err := d.Connect(connStr, username, password, extra)
	if err != nil {
		return "", err
	}
	if extra.Table == "" {
		return "", errors.New("Empty Table name")
	}
	dynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	del, err := dynamoDbClient.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String(extra.Table)})
	if err != nil {
		return "", err
	}
	return "Successful deletion : " + *del.TableDescription.TableName, nil
}
func (d Dynamo) Count(connStr, username, password string, extra Extras) (int64, error) {
	var count int64
	if err := validateStrings(connStr, username, password); err != nil {
		return -1, err
	}
	err := d.Connect(connStr, username, password, extra)
	if err != nil {
		return -1, err
	}
	if extra.Table == "" {
		return -1, errors.New("Empty Table name")
	}
	dynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	input := &dynamodb.ScanInput{
		TableName: aws.String(extra.Table),
		Select:    types.SelectCount,
	}
	result, err := dynamoDbClient.Scan(context.TODO(), input)
	if err != nil {
		return -1, err
	}
	count += int64(result.Count)
	for result.LastEvaluatedKey != nil && len(result.LastEvaluatedKey) != 0 {
		input.ExclusiveStartKey = result.LastEvaluatedKey
		result, err = dynamoDbClient.Scan(context.TODO(), input)
		if err != nil {
			return -1, err
		}
		count += int64(result.Count)
	}
	return count, nil
}

func (d Dynamo) ListDatabase(connStr, username, password string, extra Extras) (any, error) {
	if err := validateStrings(connStr, username, password); err != nil {
		return -1, err
	}
	err := d.Connect(connStr, username, password, extra)
	if err != nil {
		return -1, err
	}
	dblist := make(map[string][]string)
	dynamoDbClient := d.connectionManager.Clusters[connStr].DynamoClusterClient
	tablePaginator := dynamodb.NewListTablesPaginator(dynamoDbClient, &dynamodb.ListTablesInput{})
	var dbArr []string
	for tablePaginator.HasMorePages() {
		output, err := tablePaginator.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		dbArr = append(dbArr, output.TableNames...)

	}
	dblist[connStr] = dbArr
	return dblist, nil
}
