package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"

	"github.com/couchbaselabs/sirius/internal/sdk_cassandra"
	"github.com/couchbaselabs/sirius/internal/template"

	"github.com/gocql/gocql"
)

type Cassandra struct {
	CassandraConnectionManager *sdk_cassandra.CassandraConnectionManager
}

type perCassandraDocResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

// cassandraOperationResult stores the result information for Insert, Upsert, Delete and Read.
type cassandraOperationResult struct {
	key    string
	result perCassandraDocResult
}

func NewCassandraConnectionManager() *Cassandra {
	return &Cassandra{
		CassandraConnectionManager: sdk_cassandra.ConfigCassandraConnectionManager(),
	}
}

func newCassandraOperationResult(key string, value interface{}, err error, status bool, offset int64) *cassandraOperationResult {
	return &cassandraOperationResult{
		key: key,
		result: perCassandraDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (c *cassandraOperationResult) Key() string {
	return c.key
}

func (c *cassandraOperationResult) Value() interface{} {
	return c.result.value
}

func (c *cassandraOperationResult) GetStatus() bool {
	return c.result.status
}

func (c *cassandraOperationResult) GetError() error {
	return c.result.error
}

func (c *cassandraOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (c *cassandraOperationResult) GetOffset() int64 {
	return c.result.offset
}

// Operation Results for Bulk Operations like Bulk-Create, Bulk-Update, Bulk-Touch and Bulk-Delete
type cassandraBulkOperationResult struct {
	keyValues map[string]perCassandraDocResult
}

func newCassandraBulkOperation() *cassandraBulkOperationResult {
	return &cassandraBulkOperationResult{
		keyValues: make(map[string]perCassandraDocResult),
	}
}

func (m *cassandraBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	m.keyValues[key] = perCassandraDocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (m *cassandraBulkOperationResult) Value(key string) interface{} {
	if x, ok := m.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (m *cassandraBulkOperationResult) GetStatus(key string) bool {
	if x, ok := m.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (m *cassandraBulkOperationResult) GetError(key string) error {
	if x, ok := m.keyValues[key]; ok {
		return x.error
	}
	return errors.New("Key not found in bulk operation")
}

func (m *cassandraBulkOperationResult) GetExtra(key string) map[string]any {
	if _, ok := m.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (m *cassandraBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := m.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (m *cassandraBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		m.keyValues[x.Key] = perCassandraDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (m *cassandraBulkOperationResult) GetSize() int {
	return len(m.keyValues)
}

type perCassandraSubDocResult struct {
	keyValue []KeyValue
	error    error
	status   bool
	offset   int64
}

type cassandraSubDocOperationResult struct {
	key    string
	result perCassandraSubDocResult
}

func newCassandraSubDocOperationResult(key string, keyValue []KeyValue, err error, status bool, offset int64) *cassandraSubDocOperationResult {
	return &cassandraSubDocOperationResult{
		key: key,
		result: perCassandraSubDocResult{
			keyValue: keyValue,
			error:    err,
			status:   status,
			offset:   offset,
		},
	}
}

func (m *cassandraSubDocOperationResult) Key() string {
	return m.key
}

func (m *cassandraSubDocOperationResult) Value(subPath string) (interface{}, int64) {
	for _, x := range m.result.keyValue {
		if x.Key == subPath {
			return x.Doc, x.Offset
		}
	}
	return nil, -1
}

func (m *cassandraSubDocOperationResult) Values() []KeyValue {
	return m.result.keyValue
}

func (m *cassandraSubDocOperationResult) GetError() error {
	return m.result.error
}

func (m *cassandraSubDocOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *cassandraSubDocOperationResult) GetOffset() int64 {
	return m.result.offset
}

func (c *Cassandra) Connect(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	clusterConfig := &sdk_cassandra.CassandraClusterConfig{
		ClusterConfigOptions: sdk_cassandra.ClusterConfigOptions{
			KeyspaceName: extra.Keyspace,
			NumConns:     extra.NumOfConns,
		},
	}

	if _, err := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, clusterConfig); err != nil {
		log.Println("In Cassandra Connect(), error in GetCluster()")
		return err
	}

	return nil
}

func (c *Cassandra) Warmup(connStr, username, password string, extra Extras) error {
	log.Println("In Cassandra Warmup()")
	if err := validateStrings(connStr, username, password); err != nil {
		log.Println("In Cassandra Warmup(), error:", err)
		return err
	}

	cassSession, errSession := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if errSession != nil {
		log.Println("In Cassandra Warmup(), unable to create session, err:", errSession)
		return errors.New("In Cassandra Warmup(), unable to create session, err: " + errSession.Error())
	}

	// Checking if the cluster is reachable or not
	errQuery := cassSession.Query("SELECT cluster_name FROM system.local").Exec()
	if errQuery != nil {
		log.Println("unable to perform query on the cluster, err:", errQuery)
		return errors.New("In Cassandra Warmup(), unable to create cassSession, err: " + errQuery.Error())
	}
	return nil
}

func (c *Cassandra) Close(connStr string, extra Extras) error {
	return c.CassandraConnectionManager.Disconnect(connStr)
}

func (c *Cassandra) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	if err := validateStrings(extra.Keyspace); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Keyspace name is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Table); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Create(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Unable to connect to Cassandra!"), false,
			keyValue.Offset)
	}

	// Converting the Document to JSON
	jsonData, errDocToJSON := json.Marshal(keyValue.Doc)
	if errDocToJSON != nil {
		log.Println("In Cassandra Create(), error marshaling JSON:", errDocToJSON)
	}

	//insertQuery := "INSERT INTO " + extra.Table + " JSON '" + string(jsonData) + "'"
	insertQuery := "INSERT INTO " + extra.Table + " JSON ?"

	//errInsert := cassandraSession.Query(insertQuery).Exec()
	errInsert := cassandraSession.Query(insertQuery, jsonData).Exec()
	if errInsert != nil {
		log.Println("In Cassandra Create(), error inserting data:", errInsert)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errInsert, false, keyValue.Offset)
	}
	return newCassandraOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Cassandra) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	if err := validateStrings(extra.Keyspace); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Keyspace name is missing"), false,
			keyValue.Offset)
	}
	if err := validateStrings(extra.Table); err != nil {
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Update(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errors.New("Unable to connect to Cassandra!"), false,
			keyValue.Offset)
	}

	// Converting the Document to JSON
	jsonData, errDocToJSON := json.Marshal(keyValue.Doc)
	if errDocToJSON != nil {
		log.Println("In Cassandra Update(), error marshaling JSON:", errDocToJSON)
	}

	//updateQuery := "UPDATE " + extra.Table + " SET JSON '" + string(jsonData) + "' WHERE id = " + keyValue.Key
	//updateQuery := "INSERT INTO " + extra.Table + " JSON '" + string(jsonData) + "' DEFAULT UNSET"
	updateQuery := "INSERT INTO " + extra.Table + " JSON ? DEFAULT UNSET"

	errUpdate := cassandraSession.Query(updateQuery, jsonData).Exec()
	if errUpdate != nil {
		log.Println("In Cassandra Update(), error updating data:", errUpdate)
		return newCassandraOperationResult(keyValue.Key, keyValue.Doc, errUpdate, false, keyValue.Offset)
	}
	return newCassandraOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (c *Cassandra) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Keyspace is missing"), false, offset)
	}
	cassandraSessionObj, err1 := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if err1 != nil {
		return newCassandraOperationResult(key, nil, err1, false, offset)
	}
	var result map[string]interface{}

	query := "SELECT * FROM " + keyspaceName + "." + tableName + " WHERE ID = ?"
	iter := cassandraSessionObj.Query(query, key).Iter()
	result = make(map[string]interface{})
	success := iter.MapScan(result)
	if !success {
		if result == nil {
			return newCassandraOperationResult(key, nil,
				fmt.Errorf("result is nil even after successful READ operation %s ", connStr), false,
				offset)
		} else if err := iter.Close(); err != nil {
			return newCassandraOperationResult(key, nil,
				fmt.Errorf("Unsuccessful READ operation %s ", connStr), false,
				offset)
		}
	}
	return newCassandraOperationResult(key, result, nil, true, offset)
}

func (c *Cassandra) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Keyspace is missing"), false, offset)
	}

	cassandraSessionObj, err1 := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if err1 != nil {
		return newCassandraOperationResult(key, nil, err1, false, offset)
	}
	query := "DELETE FROM " + keyspaceName + "." + tableName + " WHERE ID = ?"
	if err2 := cassandraSessionObj.Query(query, key).Exec(); err2 != nil {
		return newCassandraOperationResult(key, nil,
			fmt.Errorf("unsuccessful Delete %s ", connStr), false, offset)
	}
	return newCassandraOperationResult(key, nil, nil, true, offset)
}

func (c *Cassandra) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCassandraOperationResult(key, nil, err, false, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Table name is missing"), false, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCassandraOperationResult(key, nil, errors.New("Keyspace is missing"), false, offset)
	}
	cassandraSessionObj, err1 := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
	if err1 != nil {
		return newCassandraOperationResult(key, nil, err1, false, offset)
	}
	query := fmt.Sprintf("UPDATE %s.%s USING TTL %d WHERE ID = ?", keyspaceName, tableName, extra.Expiry)
	fmt.Println(query)
	if err2 := cassandraSessionObj.Query(query, key).Exec(); err2 != nil {
		return newCassandraOperationResult(key, nil, err2, false, offset)
	}
	return newCassandraOperationResult(key, nil, nil, true, offset)
}

func (c *Cassandra) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra InsertSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for _, x := range keyValues {
		columnName := extra.SubDocPath
		if err := validateStrings(columnName); err != nil {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("SubDocPath is missing"), false, extra.Cas, offset)
		}
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			fmt.Println(alterQuery)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
			}
		}
		insertSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		errinsertSubDocQuery := cassandraSession.Query(insertSubDocQuery, key).Exec()
		if errinsertSubDocQuery != nil {
			log.Println("In Cassandra InsertSubDoc(), error inserting data:", errinsertSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errinsertSubDocQuery, false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra UpsertSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for _, x := range keyValues {
		columnName := extra.SubDocPath
		if err := validateStrings(columnName); err != nil {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("SubDocPath is missing"), false, extra.Cas, offset)
		}
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			fmt.Println(alterQuery)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
			}
		}
		upsertSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		errUpsertSubDocQuery := cassandraSession.Query(upsertSubDocQuery, key).Exec()
		if errUpsertSubDocQuery != nil {
			log.Println("In Cassandra UpsertSubDoc(), error inserting data:", errUpsertSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errUpsertSubDocQuery, false, extra.Cas, offset)
		}
		var currentValue float64
		mutationSubDocQuery := fmt.Sprintf("SELECT mutated FROM %s WHERE ID = ?", tableName)
		errMutationSubDocQuery := cassandraSession.Query(mutationSubDocQuery, key).Scan(&currentValue)
		if errMutationSubDocQuery != nil {
			log.Println("In Cassandra UpsertSubDoc(), error fetching current mutated field:", errMutationSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errMutationSubDocQuery, false, extra.Cas, offset)
		}
		mutationIncSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", currentValue+1)
		errMutationIncSubDocQuery := cassandraSession.Query(mutationIncSubDocQuery, key).Exec()
		if errMutationIncSubDocQuery != nil {
			log.Println("In Cassandra UpsertSubDoc(), error updating mutated field:", errMutationIncSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errMutationIncSubDocQuery, false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) Increment(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Cassandra) ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra ReplaceSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for _, x := range keyValues {
		columnName := extra.SubDocPath
		if err := validateStrings(columnName); err != nil {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("SubDocPath is missing"), false, extra.Cas, offset)
		}
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
			}
		}
		replaceSubDocQuery := fmt.Sprintf("UPDATE %s SET %s='%s' WHERE ID = ?", tableName, columnName, x.Doc)
		errreplaceSubDocQuery := cassandraSession.Query(replaceSubDocQuery, key).Exec()
		if errreplaceSubDocQuery != nil {
			log.Println("In Cassandra ReplaceSubDoc(), error inserting data:", errreplaceSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errreplaceSubDocQuery, false, extra.Cas, offset)
		}
		var currentValue float64
		mutationSubDocQuery := fmt.Sprintf("SELECT mutated FROM %s WHERE ID = ?", tableName)
		errMutationSubDocQuery := cassandraSession.Query(mutationSubDocQuery, key).Scan(&currentValue)
		if errMutationSubDocQuery != nil {
			log.Println("In Cassandra ReplaceSubDoc(), error fetching current mutated field:", errMutationSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errMutationSubDocQuery, false, extra.Cas, offset)
		}
		mutationIncSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", currentValue+1)
		errMutationIncSubDocQuery := cassandraSession.Query(mutationIncSubDocQuery, key).Exec()
		if errMutationIncSubDocQuery != nil {
			log.Println("In Cassandra UpsertSubDoc(), error updating mutated field:", errMutationIncSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errMutationIncSubDocQuery, false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra ReadSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for range keyValues {
		columnName := extra.SubDocPath
		if err := validateStrings(columnName); err != nil {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("SubDocPath is missing"), false, extra.Cas, offset)
		}
		var result map[string]interface{}
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("No subdocs field found."), false, extra.Cas, offset)
		}
		selectSubDocQuery := fmt.Sprintf("SELECT %s FROM %s WHERE ID = ?", columnName, tableName)
		iter := cassandraSession.Query(selectSubDocQuery, key).Iter()
		result = make(map[string]interface{})
		success := iter.MapScan(result)
		if !success {
			if result == nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("No documents found despite successful subdocread."), false, extra.Cas, offset)
			} else if err := iter.Close(); err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Unsuccessful READ operation."), false, extra.Cas, offset)
			}
		}
		if result[columnName] == "" {
			return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("No subdocs found."), false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	if err := validateStrings(connStr, username, password); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
	}
	tableName := extra.Table
	keyspaceName := extra.Keyspace
	if err := validateStrings(tableName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Table name is missing"), false, extra.Cas, offset)
	}
	if err := validateStrings(keyspaceName); err != nil {
		return newCouchbaseSubDocOperationResult(key, keyValues, errors.New("Keyspace is missing"), false, extra.Cas, offset)
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra DeleteSubDoc(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		return newCouchbaseSubDocOperationResult(key, keyValues, errSessionCreate, false, extra.Cas, offset)
	}
	for range keyValues {
		columnName := extra.SubDocPath
		if !cassandraColumnExists(cassandraSession, keyspaceName, tableName, columnName) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s.%s ADD %s text", keyspaceName, tableName, columnName)
			err := cassandraSession.Query(alterQuery).Exec()
			if err != nil {
				return newCouchbaseSubDocOperationResult(key, keyValues, err, false, extra.Cas, offset)
			}
		}
		delSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=? WHERE ID = ?", tableName, columnName)
		errdelSubDocQuery := cassandraSession.Query(delSubDocQuery, nil, key).Exec()
		if errdelSubDocQuery != nil {
			log.Println("In Cassandra DeleteSubDoc(), error inserting data:", errdelSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errdelSubDocQuery, false, extra.Cas, offset)
		}
		mutationClearSubDocQuery := fmt.Sprintf("UPDATE %s SET %s=%f WHERE ID = ?", tableName, "mutated", 0.0)
		errmutationClearSubDocQuery := cassandraSession.Query(mutationClearSubDocQuery, key).Exec()
		if errmutationClearSubDocQuery != nil {
			log.Println("In Cassandra DeleteSubDoc(), error updating mutated field:", errmutationClearSubDocQuery)
			return newCouchbaseSubDocOperationResult(key, keyValues, errmutationClearSubDocQuery, false, extra.Cas, offset)
		}
	}
	return newCassandraSubDocOperationResult(key, keyValues, nil, true, offset)
}

func (c *Cassandra) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {

	result := newCassandraBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Keyspace); err != nil {
		result.failBulk(keyValues, errors.New("Keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("Table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Create(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		result.failBulk(keyValues, errSessionCreate)
		return result
	}

	cassBatchOp := cassandraSession.NewBatch(gocql.LoggedBatch).WithContext(context.TODO())
	for _, x := range keyValues {

		// Converting the Document to JSON
		jsonData, errDocToJSON := json.Marshal(x.Doc)
		if errDocToJSON != nil {
			log.Println("In Cassandra Update(), error marshaling JSON:", errDocToJSON)
		}

		cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
			Stmt:       "INSERT INTO " + extra.Table + " JSON ?",
			Args:       []interface{}{jsonData},
			Idempotent: true,
		})
	}

	errBulkInsert := cassandraSession.ExecuteBatch(cassBatchOp)
	if errBulkInsert != nil {
		log.Println("In Cassandra CreateBulk(), ExecuteBatch() Error:", errBulkInsert)
		result.failBulk(keyValues, errBulkInsert)
		return result
	}
	cassBatchOp = nil

	for _, x := range keyValues {
		//log.Println("Successfully inserted document with id:", x.Key)
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (c *Cassandra) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newCassandraBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Keyspace); err != nil {
		result.failBulk(keyValues, errors.New("Keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("Table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Update(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		result.failBulk(keyValues, errSessionCreate)
		return result
	}

	for _, x := range keyValues {
		cassBatchSize := 10
		cassBatchOp := cassandraSession.NewBatch(gocql.LoggedBatch).WithContext(context.TODO())
		var docArg []interface{}
		for i := 0; i < cassBatchSize; i++ {
			// Converting the Document to JSON
			jsonData, errDocToJSON := json.Marshal(x.Doc)
			if errDocToJSON != nil {
				log.Println("In Cassandra UpdateBulk(), error marshaling JSON:", errDocToJSON)
			}

			docArg = append(docArg, jsonData)
			cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
				Stmt:       "INSERT INTO " + extra.Table + " JSON ? DEFAULT UNSET",
				Args:       docArg,
				Idempotent: true,
			})
			docArg = nil
		}
		errBulkUpsert := cassandraSession.ExecuteBatch(cassBatchOp)
		if errBulkUpsert != nil {
			log.Println("In Cassandra UpdateBulk(), ExecuteBatch() Error:", errBulkUpsert)
			result.failBulk(keyValues, errBulkUpsert)
			return result
		}
		cassBatchOp = nil
	}

	for _, x := range keyValues {
		//log.Println("Successfully inserted document with id:", x.Key)
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (c *Cassandra) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newCassandraBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	keysToString := "("
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		keysToString += "'" + x.Key + "'" + ","
	}
	keysToString = keysToString[:len(keysToString)-1] + ")"
	if err := validateStrings(extra.Keyspace); err != nil {
		result.failBulk(keyValues, errors.New("Keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("Table name is missing"))
		return result
	}
	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("In Cassandra Read(), unable to connect to Cassandra:")
		log.Println(errSessionCreate)
		result.failBulk(keyValues, errSessionCreate)
		return result
	}

	query := "SELECT * FROM " + extra.Table + " WHERE ID IN " + keysToString
	iter := cassandraSession.Query(query).Iter()
	if iter.NumRows() != len(keyValues) {
		result.failBulk(keyValues, errors.New("Unable to perform Bulk Read"))
	}
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		result.AddResult(row["id"].(string), nil, nil, false, keyToOffset[row["id"].(string)])
	}
	return result
}

func (c *Cassandra) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newCassandraBulkOperation()
	if err := validateStrings(connStr, username, password); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
	}

	if err := validateStrings(extra.Keyspace); err != nil {
		result.failBulk(keyValues, errors.New("bulk deleting in cassandra: keyspace name is missing"))
		return result
	}
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("bulk deleting in cassandra: table name is missing"))
		return result
	}

	cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
	if errSessionCreate != nil {
		log.Println("bulk deleting in cassandra: unable to connect to Cassandra:", errSessionCreate)
		result.failBulk(keyValues, errors.New("bulk deleting in cassandra: unable to connect to Cassandra:"+errSessionCreate.Error()))
		return result
	}

	for _, x := range keyValues {
		cassBatchSize := 10
		cassBatchOp := cassandraSession.NewBatch(gocql.UnloggedBatch).WithContext(context.TODO())
		for i := 0; i < cassBatchSize; i++ {
			cassBatchOp.Entries = append(cassBatchOp.Entries, gocql.BatchEntry{
				Stmt:       "DELETE FROM " + extra.Table + " WHERE ID=?",
				Args:       []interface{}{x.Key},
				Idempotent: true,
			})
		}
		errBulkUpdate := cassandraSession.ExecuteBatch(cassBatchOp)
		if errBulkUpdate != nil {
			log.Println("bulk deleting in cassandra: error while executing batch:", errBulkUpdate)
			result.failBulk(keyValues, errors.New("bulk deleting in cassandra: error while executing batch:"+errBulkUpdate.Error()))
			return result
		}
		cassBatchOp = nil
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}

	return result
}

func (c *Cassandra) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

// CreateDatabase creates a Keyspace in Cassandra cluster or Table in a Keyspace.
/*
 *	If only Keyspace name is provided, then a Keyspace will be created if it does not exist.
 *	If both Keyspace and Table name are provided, then a Table will be created in the Keyspace.
 *	NOTE: While creating Keyspace, make sure to provide Extras.CassandraClass and Extras.ReplicationFactor
 *	NOTE: While creating Table, make sure to provide OperationConfig.Template as it will be used to retrieve correct cassandra schema.
 */
func (c *Cassandra) CreateDatabase(connStr, username, password string, extra Extras, templateName string, docSize int) (string, error) {
	resultString := ""

	if connStr == "" || password == "" || username == "" {
		return "", errors.New("creating cassandra keyspace or table: connection string or auth parameters not provided")
	}

	if extra.Keyspace == "" {
		return "", errors.New("creating cassandra keyspace or table: keyspace name not provided")

	} else if extra.Keyspace != "" && extra.Table == "" {

		// Creating a new Keyspace
		if extra.CassandraClass == "" || extra.ReplicationFactor == 0 {
			log.Println("creating cassandra keyspace: cassandra class or replication factor not provided for creating keyspace")
			return "", errors.New("creating cassandra keyspace: cassandra class or replication factor not provided for creating keyspace")
		}

		cassandraSession, errCreateSession := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
		if errCreateSession != nil {
			log.Println("creating cassandra keyspace: unable to connect to cassandra", errCreateSession)
			return "", errors.New("creating cassandra keyspace: unable to connect to cassandra: " + errCreateSession.Error())
		}

		createKeyspaceQuery := fmt.Sprintf(`
							CREATE KEYSPACE IF NOT EXISTS %s
							WITH replication = {
								'class': '%s',
								'replication_factor': %v
							};`, extra.Keyspace, extra.CassandraClass, extra.ReplicationFactor)
		errCreateKeyspace := cassandraSession.Query(createKeyspaceQuery).Exec()
		if errCreateKeyspace != nil {
			log.Println("creating cassandra keyspace: unable to create keyspace:", errCreateKeyspace)
			return "", errors.New("creating cassandra keyspace: unable to create keyspace: " + errCreateKeyspace.Error())
		}

		resultString = fmt.Sprintf("Keyspace '%s' created successfully.", extra.Keyspace)

	} else if extra.Keyspace != "" && extra.Table != "" {

		// Creating a new Table. Need to have Template.
		// And, we have to check if Keyspace is created or not.
		if templateName == "" {
			log.Println("creating cassandra table: template name not provided")
			return "", errors.New("creating cassandra table: template name not provided")
		}

		// First getting client on Cluster and checking if the Keyspace exists or not
		cassandraSession, errCreateSession := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
		if errCreateSession != nil {
			log.Println("creating cassandra table: unable to connect to cassandra", errCreateSession)
			return "", errors.New("creating cassandra table: unable to connect to cassandra: " + errCreateSession.Error())
		}

		var count int
		checkKeyspaceQuery := fmt.Sprintf("SELECT count(*) FROM system_schema.keyspaces WHERE keyspace_name = '%s'", extra.Keyspace)
		if err := cassandraSession.Query(checkKeyspaceQuery).Scan(&count); err != nil {
			log.Println("creating cassandra table: unable run the query to check keyspace existence:", err.Error())
			return "", errors.New("creating cassandra table: unable run the query to check keyspace existence: " + err.Error())
		}

		if count <= 0 {

			// Creating the keyspace as it does not exist
			if extra.CassandraClass == "" || extra.ReplicationFactor == 0 {
				log.Println("creating cassandra table: cassandra class or replication factor not provided for creating keyspace")
				return "", errors.New("creating cassandra table: cassandra class or replication factor not provided for creating keyspace")
			}

			createKeyspaceQuery := fmt.Sprintf(`
							CREATE KEYSPACE IF NOT EXISTS %s
							WITH replication = {
								'class': '%s',
								'replication_factor': %v
							};`, extra.Keyspace, extra.CassandraClass, extra.ReplicationFactor)
			errCreateKeyspace := cassandraSession.Query(createKeyspaceQuery).Exec()
			if errCreateKeyspace != nil {
				log.Println("creating cassandra keyspace: unable to create keyspace:", errCreateKeyspace)
				return "", errors.New("creating cassandra keyspace: unable to create keyspace: " + errCreateKeyspace.Error())
			}
		}

		cassandraSession, errCreateSession = c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errCreateSession != nil {
			log.Println("creating cassandra table: unable to connect to cassandra", errCreateSession)
			return "", errors.New("creating cassandra table: unable to connect to cassandra: " + errCreateSession.Error())
		}

		cassQueries, err := template.GetCassandraSchema(templateName, extra.Table)
		if err != nil {
			log.Println("creating cassandra table: unable to get cassandra schema:", err)
			return "", errors.New("creating cassandra table: unable to get cassandra schema: " + err.Error())
		}

		for _, cassQuery := range cassQueries {
			err = cassandraSession.Query(cassQuery).Exec()
			if err != nil {
				log.Println("creating cassandra table: unable to create type or table:", err)
				return "", errors.New("creating cassandra table: unable to create type or table: " + err.Error())
			}
		}

		resultString = fmt.Sprintf("Table '%s' created successfully in Keyspace '%s'.", extra.Table, extra.Keyspace)
	}

	return resultString, nil
}

// DeleteDatabase deletes a keyspace or table in a cassandra cluster.
/*
 *	If only keyspace name is provided, then the whole keyspace along with all its tables will be deleted.
 *	If keyspace and table name both are provided, then only the table will be deleted.
 */
func (c *Cassandra) DeleteDatabase(connStr, username, password string, extra Extras) (string, error) {

	resultString := ""
	if connStr == "" || password == "" || username == "" {
		return "", errors.New("deleting cassandra keyspace or table: connection string or auth parameters not provided")
	}

	if extra.Keyspace == "" {
		return "", errors.New("deleting cassandra keyspace or table: keyspace name not provided")

	} else if extra.Keyspace != "" && extra.Table == "" {

		// Deleting the Keyspace in given Cassandra cluster
		cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errSessionCreate != nil {
			log.Println("deleting cassandra keyspace: unable to connect to cassandra:", errSessionCreate.Error())
			return "", errors.New("deleting cassandra keyspace: unable to connect to cassandra: " + errSessionCreate.Error())
		}

		dropKeyspaceQuery := fmt.Sprintf("DROP KEYSPACE %s", extra.Keyspace)
		errDropKeyspace := cassandraSession.Query(dropKeyspaceQuery).Exec()
		if errDropKeyspace != nil {
			log.Println("deleting cassandra keyspace: unable to delete keyspace:", errDropKeyspace)
			return "", errors.New("deleting cassandra keyspace: unable to delete keyspace: " + errDropKeyspace.Error())
		}

		resultString = fmt.Sprintf("Keyspace '%s' deleted successfully.", extra.Keyspace)

	} else if extra.Keyspace != "" && extra.Table != "" {

		// Deleting the Table in given Keyspace
		cassandraSession, errCreateSession := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errCreateSession != nil {
			log.Println("deleting cassandra table: unable to connect to cassandra:", errCreateSession.Error())
			return "", errors.New("deleting cassandra table: unable to connect to cassandra: " + errCreateSession.Error())
		}

		dropTableQuery := fmt.Sprintf("DROP TABLE %s", extra.Table)
		errDropTable := cassandraSession.Query(dropTableQuery).Exec()
		if errDropTable != nil {
			log.Println("deleting cassandra table: unable to delete table:", errDropTable)
			return "", errors.New("deleting cassandra table: unable to delete table: " + errDropTable.Error())
		}

		resultString = fmt.Sprintf("Table '%s' deleted successfully from Keyspace '%s'", extra.Table, extra.Keyspace)
	}
	return resultString, nil
}

func (c *Cassandra) ListDatabase(connStr, username, password string, extra Extras) (any, error) {

	dbList := make(map[string][]string)

	if connStr == "" || password == "" || username == "" {
		return nil, errors.New("listing cassandra keyspace(s) or table(s): connection string or auth parameters not provided")
	}

	if extra.Keyspace == "" {
		// Since, Keyspace name is not provided, returning all the Keyspaces in the cluster
		cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraCluster(connStr, username, password, nil)
		if errSessionCreate != nil {
			log.Println("listing cassandra keyspace(s): unable to connect to Cassandra:", errSessionCreate.Error())
			return nil, errors.New("listing cassandra keyspace(s): unable to connect to cassandra: " + errSessionCreate.Error())
		}

		var keyspaceName string
		keyspaces := make([]string, 0)

		//listKeyspaceQuery := "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name != 'system' AND keyspace_name != 'system_traces' AND keyspace_name != 'system_auth' AND keyspace_name != 'system_distributed'"
		listKeyspaceQuery := "SELECT keyspace_name FROM system_schema.keyspaces"
		iterKeyspaces := cassandraSession.Query(listKeyspaceQuery).Iter()

		for iterKeyspaces.Scan(&keyspaceName) {
			keyspaces = append(keyspaces, keyspaceName)
		}
		if err := iterKeyspaces.Close(); err != nil {
			log.Println("iterating keyspaces names:", err)
			return nil, errors.New("iterating keyspaces names: " + err.Error())
		}

		// Appending all the Keyspace names to output
		for _, keyspaceN := range keyspaces {
			dbList[extra.Keyspace] = append(dbList[extra.Keyspace], keyspaceN)
		}

		if dbList == nil || len(keyspaces) == 0 {
			return nil, errors.New("listing cassandra keyspace(s): no keyspaces found")
		}

	} else {
		// Since, Keyspace name is provided, returning all the Tables present in the Keyspace
		cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errSessionCreate != nil {
			log.Println("listing cassandra table(s): unable to connect to cassandra:", errSessionCreate.Error())
			return nil, errors.New("listing cassandra table(s): unable to connect to cassandra: " + errSessionCreate.Error())
		}

		var tableName string
		tables := make([]string, 0)

		listTableQuery := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
		iterTables := cassandraSession.Query(listTableQuery, extra.Keyspace).Iter()

		for iterTables.Scan(&tableName) {
			tables = append(tables, tableName)
		}
		if err := iterTables.Close(); err != nil {
			log.Println("iterating table names:", err)
			return nil, errors.New("iterating table names: " + err.Error())
		}

		for _, tableN := range tables {
			dbList[extra.Keyspace] = append(dbList[extra.Keyspace], tableN)
		}
		if dbList == nil {
			return nil, errors.New("listing cassandra table(s): no tables found for keyspace: " + extra.Keyspace)
		}
	}
	return dbList, nil
}

// Count returns the number of rows present in a Cassandra Keyspace.Table.
/*
 *	If there is an error returned then count is returned as -1.
 *	Extras.DbOnLocal == "true", states that Sirius is running on the same machine as Cassandra.
	This way we can get the count in a much faster way and for a large number of rows.
 *	If Sirius is not running on the same machine as Cassandra, then we have to query to get the count of rows.
*/
func (c *Cassandra) Count(connStr, username, password string, extra Extras) (int64, error) {

	var count int64
	if connStr == "" || password == "" || username == "" {
		return -1, errors.New("listing count of rows of cassandra table: connection string or auth parameters not provided")
	}
	if extra.Keyspace == "" {
		return -1, errors.New("listing count of rows of cassandra table: keyspace name not provided")
	}
	if extra.Table == "" {
		return -1, errors.New("listing count of rows of cassandra table: table name not provided")
	}
	if extra.DbOnLocal == "" {
		return -1, errors.New("listing count of rows of cassandra table: database on local is not provided")
	}

	if extra.DbOnLocal == "true" {
		// If cassandra is present on the same machine as sirius
		cmd := exec.Command("sh", "-c", "cqlsh -e \"copy "+extra.Keyspace+"."+extra.Table+" (id) to '/dev/null'\" | sed -n 5p | sed 's/ .*//'")
		cmdOutput, err := cmd.Output()
		if err != nil {
			return -1, errors.New("listing count of rows of cassandra table: unable to parse command output")
		}

		count, err = strconv.ParseInt(strings.TrimSpace(string(cmdOutput)), 10, 64)
		if err != nil {
			return -1, errors.New("listing count of rows of cassandra table: unable to convert command output to an 64 bit integer")
		}

	} else {
		// If cassandra is hosted on another machine
		cassandraSession, errSessionCreate := c.CassandraConnectionManager.GetCassandraKeyspace(connStr, username, password, nil, extra.Keyspace)
		if errSessionCreate != nil {
			log.Println("listing cassandra table(s): unable to connect to cassandra:", errSessionCreate.Error())
			return -1, errors.New("listing cassandra table(s): unable to connect to cassandra: " + errSessionCreate.Error())
		}

		countQuery := "SELECT COUNT(*) FROM " + extra.Table
		if errCount := cassandraSession.Query(countQuery).Scan(&count); errCount != nil {
			log.Println("Error while getting COUNT", errCount)
			return 0, errors.New("Error while getting COUNT" + errCount.Error())
		}
	}
	return count, nil
}
