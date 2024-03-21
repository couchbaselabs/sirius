package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/couchbaselabs/sirius/internal/sdk_mysql"
	"github.com/couchbaselabs/sirius/internal/template"
	_ "github.com/go-sql-driver/mysql"
)

type Sql struct {
	connectionManager *sdk_mysql.SqlConnectionManager
}

type perSqlDocResult struct {
	value  interface{}
	error  error
	status bool
	offset int64
}

type sqlOperationResult struct {
	key    string
	result perSqlDocResult
}

func NewSqlConnectionManager() *Sql {
	return &Sql{
		connectionManager: sdk_mysql.ConfigSqlConnectionManager(),
	}
}

// Operation Results for Single Operations like Create, Update, Touch and Delete

func newSqlOperationResult(key string, value interface{}, err error, status bool, offset int64) *sqlOperationResult {
	return &sqlOperationResult{
		key: key,
		result: perSqlDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (m *sqlOperationResult) Key() string {
	return m.key
}

func (m *sqlOperationResult) Value() interface{} {
	return m.result.value
}

func (m *sqlOperationResult) GetStatus() bool {
	return m.result.status
}

func (m *sqlOperationResult) GetError() error {
	return m.result.error
}

func (m *sqlOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *sqlOperationResult) GetOffset() int64 {
	return m.result.offset
}

// Operation Results for Bulk Operations like Bulk-Create, Bulk-Update, Bulk-Touch and Bulk-Delete

type sqlBulkOperationResult struct {
	keyValues map[string]perSqlDocResult
}

func newSqlBulkOperation() *sqlBulkOperationResult {
	return &sqlBulkOperationResult{
		keyValues: make(map[string]perSqlDocResult),
	}
}

func (m *sqlBulkOperationResult) AddResult(key string, value interface{}, err error, status bool, offset int64) {
	m.keyValues[key] = perSqlDocResult{
		value:  value,
		error:  err,
		status: status,
		offset: offset,
	}
}

func (m *sqlBulkOperationResult) Value(key string) interface{} {
	if x, ok := m.keyValues[key]; ok {
		return x.value
	}
	return nil
}

func (m *sqlBulkOperationResult) GetStatus(key string) bool {
	if x, ok := m.keyValues[key]; ok {
		return x.status
	}
	return false
}

func (m *sqlBulkOperationResult) GetError(key string) error {
	if x, ok := m.keyValues[key]; ok {
		return x.error
	}
	return errors.New("Key not found in bulk operation")
}

func (m *sqlBulkOperationResult) GetExtra(key string) map[string]any {
	if _, ok := m.keyValues[key]; ok {
		return map[string]any{}
	}
	return nil
}

func (m *sqlBulkOperationResult) GetOffset(key string) int64 {
	if x, ok := m.keyValues[key]; ok {
		return x.offset
	}
	return -1
}

func (m *sqlBulkOperationResult) failBulk(keyValue []KeyValue, err error) {
	for _, x := range keyValue {
		m.keyValues[x.Key] = perSqlDocResult{
			value:  x.Doc,
			error:  err,
			status: false,
		}
	}
}

func (m *sqlBulkOperationResult) GetSize() int {
	return len(m.keyValues)
}

// Operation Result for SubDoc Operations
type perSqlSubDocResult struct {
	keyValue []KeyValue
	error    error
	status   bool
	offset   int64
}

type sqlSubDocOperationResult struct {
	key    string
	result perSqlSubDocResult
}

func newSqlSubDocOperationResult(key string, keyValue []KeyValue, err error, status bool, offset int64) *sqlSubDocOperationResult {
	return &sqlSubDocOperationResult{
		key: key,
		result: perSqlSubDocResult{
			keyValue: keyValue,
			error:    err,
			status:   status,
			offset:   offset,
		},
	}
}

func (m *sqlSubDocOperationResult) Key() string {
	return m.key
}

func (m *sqlSubDocOperationResult) Value(subPath string) (interface{}, int64) {
	for _, x := range m.result.keyValue {
		if x.Key == subPath {
			return x.Doc, x.Offset
		}
	}
	return nil, -1
}

func (m *sqlSubDocOperationResult) Values() []KeyValue {
	return m.result.keyValue
}

func (m *sqlSubDocOperationResult) GetError() error {
	return m.result.error
}

func (m *sqlSubDocOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (m *sqlSubDocOperationResult) GetOffset() int64 {
	return m.result.offset
}

func (m Sql) Connect(connStr, username, password string, extra Extras) error {
	clusterConfig := &sdk_mysql.SqlClusterConfig{
		ConnectionString:   connStr,
		Username:           username,
		Password:           password,
		Database:           extra.Database,
		MaxIdleConnections: extra.MaxIdleConnections,
		MaxOpenConnections: extra.MaxOpenConnections,
		MaxIdleTime:        extra.MaxIdleTime,
		MaxLifeTime:        extra.MaxLifeTime,
		Port:               extra.Port,
	}

	if _, err := m.connectionManager.GetSqlClusterObject(connStr, username, password, clusterConfig); err != nil {
		return err
	}
	return nil
}

func (m Sql) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]

	if err := validateStrings(extra.Table); err != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}
	doc, ok := keyValue.Doc.([]interface{})
	if !ok {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, errors.New("unable to decode"), false, keyValue.Offset)
	}
	sqlQuery := fmt.Sprintf("INSERT INTO %s VALUES (%s)", extra.Table, strings.Repeat("?, ", len(doc)-1)+"?")

	result, err2 := sqlClient.ExecContext(context.TODO(), sqlQuery, doc...)
	if err2 != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, err2, false, keyValue.Offset)
	}
	if result == nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful CREATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newSqlOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (m Sql) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, err, false, keyValue.Offset)
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]

	if err := validateStrings(extra.Table); err != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, errors.New("Table name is missing"), false,
			keyValue.Offset)
	}
	doc := keyValue.Doc.([]interface{})
	sqlQuery := fmt.Sprintf("REPLACE INTO %s VALUES (%s)", extra.Table, strings.Repeat("?, ", len(doc)-1)+"?")
	result, err2 := sqlClient.ExecContext(context.TODO(), sqlQuery, doc...)
	if err2 != nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc, err2, false, keyValue.Offset)
	}
	if result == nil {
		return newSqlOperationResult(keyValue.Key, keyValue.Doc,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false,
			keyValue.Offset)
	}
	return newSqlOperationResult(keyValue.Key, keyValue.Doc, nil, true, keyValue.Offset)
}

func (m Sql) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]

	if err := validateStrings(extra.Table); err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}
	sqlQuery := fmt.Sprintf("Select * from %s where id = ?", extra.Table)
	result, err2 := sqlClient.QueryContext(context.TODO(), sqlQuery, key)
	if err2 != nil {
		return newSqlOperationResult(key, nil, err2, false, offset)
	}
	if result == nil {
		return newSqlOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful UPDATE operation %s ", connStr), false,
			offset)
	}
	defer result.Close()
	cols, err := result.Columns()
	if err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}
	results := make(map[string]any)
	row := make([]interface{}, len(cols))
	for i := range row {
		var val interface{}
		row[i] = &val
	}
	for result.Next() {
		err = result.Scan(row...)
		if err != nil {
			return newSqlOperationResult(key, nil, err, false, offset)
		}
		for i, col := range cols {
			if val, ok := (*row[i].(*interface{})).([]byte); ok {
				results[col] = string(val)
			} else {
				results[col] = *(row[i].(*interface{}))
			}
		}

	}
	//log.Println(results)
	return newSqlOperationResult(key, results, nil, true, offset)

}

func (m Sql) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {

	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]

	if err := validateStrings(extra.Table); err != nil {
		return newSqlOperationResult(key, nil, err, false, offset)
	}

	sqlQuery := fmt.Sprintf("DELETE FROM %s WHERE id = ?", extra.Table)
	result, err2 := sqlClient.ExecContext(context.TODO(), sqlQuery, key)
	if err2 != nil {
		return newSqlOperationResult(key, nil, err2, false, offset)
	}
	if result == nil {
		return newSqlOperationResult(key, nil,
			fmt.Errorf("result is nil even after successful CREATE operation %s ", connStr), false,
			offset)
	}
	return newSqlOperationResult(key, nil, nil, true, offset)

}

func (m Sql) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newSqlBulkOperation()
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		result.failBulk(keyValues, err)
		return result
	}

	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("empty table name"))
		return result
	}
	sqlQuery := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", extra.Table, strings.Repeat("?, ", len(keyValues)-1)+"?")
	stmt, err := sqlClient.PrepareContext(context.TODO(), sqlQuery)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	defer stmt.Close()

	// Extract the keys from keyValues
	keys := make([]interface{}, len(keyValues))
	keyToOffset := make(map[string]int64)
	for i, x := range keyValues {
		keys[i] = x.Key
		keyToOffset[x.Key] = x.Offset
	}
	bulkResult, err := stmt.ExecContext(context.TODO(), keys...)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if bulkResult == nil {
		result.failBulk(keyValues, errors.New("delete successful but result is nil"))
		return result
	}

	rowsAffected, err := bulkResult.RowsAffected()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if rowsAffected == 0 {
		result.failBulk(keyValues, errors.New("no IDs were deleted"))
		return result
	}
	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (m Sql) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newSqlBulkOperation()
	if err := validateStrings(connStr, username, password, extra.Database, extra.Table); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("empty table name"))
		return result
	}
	baseSQL := fmt.Sprintf("INSERT INTO %s VALUES ", extra.Table)

	// Prepare the VALUES clause
	var valueArgs []interface{}
	length := len(keyValues[0].Doc.([]interface{}))
	queryArg := strings.Repeat("("+strings.Repeat("?, ", length-1)+"?),", len(keyValues))
	queryArg = queryArg[:len(queryArg)-1]
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		doc := x.Doc.([]interface{})
		valueArgs = append(valueArgs, doc...)
	}

	sqlQuery := baseSQL + queryArg

	// Execute the bulk insert operation
	bulkResult, err := sqlClient.ExecContext(context.TODO(), sqlQuery, valueArgs...)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if bulkResult == nil {
		result.failBulk(keyValues, errors.New("create successful but result is nil"))
		return result
	}
	rowsAffected, err := bulkResult.RowsAffected()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if rowsAffected == 0 {
		result.failBulk(keyValues, errors.New("Zero rows affected"))
		return result
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (m Sql) Warmup(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		return err
	}
	if err := validateStrings(extra.Table); err != nil {
		return errors.New("table name is missing")
	}
	return nil
}

// extras should be a parameter. Needs change
func (m Sql) Close(connStr string, extra Extras) error {
	if _, ok := m.connectionManager.Clusters[connStr+"/"+extra.Database]; !ok {
		return fmt.Errorf("%w : %s", errors.New("invalid closing of connectionstring"), connStr)
	}
	if err := m.connectionManager.Clusters[connStr+"/"+extra.Database].Close(); err != nil {
		log.Println("Sql Close(): Disconnect failed!")
		return err
	}
	return nil
}

func (m Sql) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newSqlBulkOperation()
	if err := validateStrings(connStr, username, password, extra.Database); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]
	if err := validateStrings(extra.Table); err != nil {
		result.failBulk(keyValues, errors.New("empty table name"))
		return result
	}
	baseSQL := fmt.Sprintf("REPLACE INTO %s VALUES ", extra.Table)

	// Prepare the VALUES clause
	var valueArgs []interface{}
	if keyValues[0].Doc == nil {
		result.failBulk(keyValues, errors.New("empty doc"))
		return result
	}
	length := len(keyValues[0].Doc.([]interface{}))
	queryArg := strings.Repeat("("+strings.Repeat("?, ", length-1)+"?),", len(keyValues))
	queryArg = queryArg[:len(queryArg)-1]
	keyToOffset := make(map[string]int64)
	for _, x := range keyValues {
		keyToOffset[x.Key] = x.Offset
		doc := x.Doc.([]interface{})
		valueArgs = append(valueArgs, doc...)
	}

	sqlQuery := baseSQL + queryArg

	// Execute the bulk insert operation
	bulkResult, err := sqlClient.ExecContext(context.TODO(), sqlQuery, valueArgs...)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if bulkResult == nil {
		result.failBulk(keyValues, errors.New("create successful but result is nil"))
		return result
	}
	rowsAffected, err := bulkResult.RowsAffected()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if rowsAffected == 0 {
		result.failBulk(keyValues, errors.New("Zero rows affected"))
		return result
	}

	for _, x := range keyValues {
		result.AddResult(x.Key, nil, nil, true, keyToOffset[x.Key])
	}
	return result
}

func (m Sql) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	result := newSqlBulkOperation()
	if err := validateStrings(connStr, username, password, extra.Database, extra.Table); err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	clusterIdentifier := connStr + "/" + extra.Database
	sqlClient := m.connectionManager.Clusters[clusterIdentifier]
	sqlQuery := fmt.Sprintf("Select * FROM %s WHERE id IN (%s)", extra.Table, strings.Repeat("?, ", len(keyValues)-1)+"?")
	stmt, err := sqlClient.PrepareContext(context.TODO(), sqlQuery)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	defer stmt.Close()

	keys := make([]interface{}, len(keyValues))
	keyToOffset := make(map[string]int64)
	for i, x := range keyValues {
		keys[i] = x.Key
		keyToOffset[x.Key] = x.Offset
	}
	bulkResult, err := stmt.QueryContext(context.TODO(), keys...)
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	if bulkResult == nil {
		result.failBulk(keyValues, errors.New("read successful but result is nil"))
		return result
	}
	defer bulkResult.Close()
	cols, err := bulkResult.Columns()
	if err != nil {
		result.failBulk(keyValues, err)
		return result
	}
	results := make(map[string]interface{})
	row := make([]interface{}, len(cols))
	for i := range row {
		var val interface{}
		row[i] = &val
	}
	failed := make(map[string]bool)
	for bulkResult.Next() {
		err = bulkResult.Scan(row...)
		if err != nil {
			result.failBulk(keyValues, err)
			return result
		}
		for i, col := range cols {
			if val, ok := (*row[i].(*interface{})).([]byte); ok {
				results[col] = string(val)
			} else {
				results[col] = *(row[i].(*interface{}))
			}
		}
		failed[results["id"].(string)] = true
		result.AddResult(results["id"].(string), results, nil, true, keyToOffset[results["id"].(string)])
	}

	for _, x := range keyValues {
		_, ok := failed[x.Key]
		if !ok {
			result.AddResult(x.Key, nil, errors.New("document not found"), false, x.Offset)
		}
	}
	return result
}
func (m *Sql) CreateDatabase(connStr, username, password string, extra Extras, templateName string, docSize int) (string, error) {
	if err := validateStrings(connStr, username, password); err != nil {
		return "", err
	}
	err := m.Connect(connStr, username, password, extra)
	if err != nil {
		return "", err
	}

	clusterIdentifier := connStr + "/" + extra.Database
	db := m.connectionManager.Clusters[clusterIdentifier]
	if err != nil {
		return "", err
	} else if extra.Database == "" {
		return "", errors.New("Empty Database name")
	} else {
		query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", extra.Database)
		_, err = db.Exec(query)
		if err != nil {
			return "", err
		}
		if extra.Table == "" {
			return "Database Created Successfully", nil
		}
		query = "Use " + extra.Database
		_, err := db.ExecContext(context.TODO(), query)
		if err != nil {
			return "", err
		}
		query = template.GetSQLSchema(templateName, extra.Table, docSize)
		_, err = db.ExecContext(context.TODO(), query)
		if err != nil {
			return "", err
		} else {
			return "Table created successfully", nil
		}

	}
}

func (m *Sql) DeleteDatabase(connStr, username, password string, extra Extras) (string, error) {
	if err := validateStrings(connStr, username, password); err != nil {
		return "", err
	}
	err := m.Connect(connStr, username, password, extra)
	if err != nil {
		return "", err
	}
	clusterIdentifier := connStr + "/" + extra.Database
	db, ok := m.connectionManager.Clusters[clusterIdentifier]
	if db == nil || !ok {
		return "", errors.New("Database initialisation error")
	}
	if extra.Database == "" {
		return "", errors.New("Empty Database name")
	}
	if extra.Table != "" {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", extra.Database, extra.Table)
		_, err = db.Exec(query)
		if err != nil {
			return "", err
		}
		return "TABLE DELETED Successfully", nil
	} else {
		query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", extra.Database)
		_, err = db.ExecContext(context.TODO(), query)
		if err != nil {
			return "", err
		} else {
			return "DATABASE DELETED Successfully", nil
		}
	}
}

func (m *Sql) Count(connStr, username, password string, extra Extras) (int64, error) {
	var count int64
	if err := validateStrings(connStr, username, password); err != nil {
		return -1, err
	}
	err := m.Connect(connStr, username, password, extra)
	if err != nil {
		return -1, err
	}
	clusterIdentifier := connStr + "/" + extra.Database
	db := m.connectionManager.Clusters[clusterIdentifier]
	if extra.Database == "" {
		return -1, errors.New("Empty Database name")
	}
	if extra.Table == "" {
		return -1, errors.New("Empty Table name")
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", extra.Database, extra.Table)
	rows, err := db.QueryContext(context.TODO(), query)
	if err != nil {
		return -1, err
	}

	for rows.Next() {
		tempCount := int64(0)
		err = rows.Scan(&tempCount)
		if err != nil {
			return -1, err
		}
		count += tempCount

	}
	return count, nil
}

func (m *Sql) ListDatabase(connStr, username, password string, extra Extras) (any, error) {
	dblist := make(map[string][]string)
	if err := validateStrings(connStr, username, password); err != nil {
		return nil, err
	}
	err := m.Connect(connStr, username, password, extra)
	if err != nil {
		return nil, err
	}
	clusterIdentifier := connStr + "/" + extra.Database
	db := m.connectionManager.Clusters[clusterIdentifier]
	var query string
	databases, err := db.Query("SHOW DATABASES  WHERE `Database` NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')")
	if err != nil {
		return nil, err
	}
	var dbname string
	var tname string
	for databases.Next() {
		err := databases.Scan(&dbname)
		if err != nil {
			return nil, err
		}
		query = "USE " + dbname
		_, err = db.Exec(query)
		if err != nil {
			return nil, err
		}
		tables, err := db.Query("SHOW TABLES")
		if err != nil {
			return nil, err
		}
		var arr []string
		for tables.Next() {
			err := tables.Scan(&tname)
			if err != nil {
				return nil, err
			}
			arr = append(arr, tname)

		}
		dblist[dbname] = arr
	}
	return dblist, nil
}

func (m Sql) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO implement me
	panic("implement me")
}
func (m Sql) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	return nil
}

func (m Sql) UpsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Sql) Increment(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Sql) ReplaceSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")

}

func (m Sql) ReadSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	// TODO implement me
	return nil
}

func (m Sql) DeleteSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	return nil
}
func (m Sql) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	return nil
}
