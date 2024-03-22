
### Available Actions

Each task can be executed using REST endpoints. All tasks tags to provide additional
configuration that is also available on a per-task basis:

 * [/bulk-create](#bulk-create)
 * [/bulk-delete](#bulk-delete)
 * [/bulk-read](#bulk-read)
 * [/bulk-touch](#bulk-touch)
 * [/bulk-upsert](#bulk-upsert)
 * [/clear_data](#clear_data)
 * [/count](#count)
 * [/create](#create)
 * [/create-database](#create-database)
 * [/delete](#delete)
 * [/delete-database](#delete-database)
 * [/list-database](#list-database)
 * [/read](#read)
 * [/result](#result)
 * [/retry-exceptions](#retry-exceptions)
 * [/sub-doc-delete](#sub-doc-delete)
 * [/sub-doc-insert](#sub-doc-insert)
 * [/sub-doc-read](#sub-doc-read)
 * [/sub-doc-replace](#sub-doc-replace)
 * [/sub-doc-upsert](#sub-doc-upsert)
 * [/touch](#touch)
 * [/upsert](#upsert)
 * [/validate](#validate)
 * [/validate-columnar](#validate-columnar)
 * [/warmup-bucket](#warmup-bucket)

---
#### /bulk-create

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /bulk-delete

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /bulk-read

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /bulk-touch

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /bulk-upsert

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /count

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /create

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /create-database

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /delete

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /delete-database

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /list-database

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /read

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /retry-exceptions

 REST : POST

Description : Retry Exception reties failed operations.
IgnoreExceptions will ignore failed operation occurred in this category. 
RetryExceptions will retry failed operation occurred in this category. 
RetryAttempts is the number of retry attempts.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ResultSeed` | `string` | `json:resultSeed`  |
| `Exceptions` | `struct` | `json:exceptions`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /sub-doc-delete

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /sub-doc-insert

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /sub-doc-read

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /sub-doc-replace

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /sub-doc-upsert

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /touch

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /upsert

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /validate

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /validate-columnar

 REST : POST

Description : Do operation between range from [start,end)

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `OperationConfig` | `ptr` | `json:operationConfig`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
#### /warmup-bucket

 REST : POST

Description : Warming up a connection to database.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `DBType` | `string` | `json:dbType`  |
| `ConnStr` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Extra` | `struct` | `json:extra`  |

---
**Description of Extra Parameters**.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `CompressionDisabled` | `bool` | `json:compressionDisabled,omitempty`  |
| `CompressionMinSize` | `uint32` | `json:compressionMinSize,omitempty`  |
| `CompressionMinRatio` | `float64` | `json:compressionMinRatio,omitempty`  |
| `ConnectionTimeout` | `int` | `json:connectionTimeout,omitempty`  |
| `KVTimeout` | `int` | `json:KVTimeout,omitempty`  |
| `KVDurableTimeout` | `int` | `json:KVDurableTimeout,omitempty`  |
| `Bucket` | `string` | `json:bucket,omitempty`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `Expiry` | `int` | `json:expiry,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `OperationTimeout` | `int` | `json:operationTimeout,omitempty`  |
| `Cas` | `uint64` | `json:cas,omitempty`  |
| `IsXattr` | `bool` | `json:isXattr,omitempty`  |
| `StoreSemantic` | `int` | `json:storeSemantic,omitempty`  |
| `PreserveExpiry` | `bool` | `json:preserveExpiry,omitempty`  |
| `CreatePath` | `bool` | `json:createPath,omitempty`  |
| `SDKBatchSize` | `int` | `json:SDKBatchSize,omitempty`  |
| `Database` | `string` | `json:database,omitempty`  |
| `Query` | `string` | `json:query,omitempty`  |
| `ConnStr` | `string` | `json:connstr,omitempty`  |
| `Username` | `string` | `json:username,omitempty`  |
| `Password` | `string` | `json:password,omitempty`  |
| `ColumnarBucket` | `string` | `json:columnarBucket,omitempty`  |
| `ColumnarScope` | `string` | `json:columnarScope,omitempty`  |
| `ColumnarCollection` | `string` | `json:columnarCollection,omitempty`  |
| `Provisioned` | `bool` | `json:provisioned,omitempty`  |
| `ReadCapacity` | `int` | `json:readCapacity,omitempty`  |
| `WriteCapacity` | `int` | `json:writeCapacity,omitempty`  |
| `Keyspace` | `string` | `json:keyspace,omitempty`  |
| `Table` | `string` | `json:table,omitempty`  |
| `NumOfConns` | `int` | `json:numOfConns,omitempty`  |
| `SubDocPath` | `string` | `json:subDocPath,omitempty`  |
| `ReplicationFactor` | `int` | `json:replicationFactor,omitempty`  |
| `CassandraClass` | `string` | `json:cassandraClass,omitempty`  |
| `Port` | `string` | `json:port,omitempty`  |
| `MaxIdleConnections` | `int` | `json:maxIdleConnections,omitempty`  |
| `MaxOpenConnections` | `int` | `json:maxOpenConnections,omitempty`  |
| `MaxIdleTime` | `int` | `json:maxIdleTime,omitempty`  |
| `MaxLifeTime` | `int` | `json:maxLifeTime,omitempty`  |
| `DbOnLocal` | `string` | `json:dbOnLocal,omitempty`  |

---
Possible values for durability :-
1. NONE
2. MAJORITY
3. MAJORITY_AND_PERSIST_TO_ACTIVE
4. PERSIST_TO_MAJORITY


---
**APIs Response Description**.

1. Response after initiating a TASK.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Seed` | `string` | `json:seed`  |

---
2. Response which contains the TASK's result.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `ResultSeed` | `int64` | `json:resultSeed`  |
| `Operation` | `string` | `json:operation`  |
| `ErrorOther` | `string` | `json:otherErrors`  |
| `Success` | `int64` | `json:success`  |
| `Failure` | `int64` | `json:failure`  |
| `BulkError` | `map` | `json:bulkErrors`  |
| `RetriedError` | `map` | `json:retriedError`  |
| `QueryError` | `map` | `json:queryErrors`  |
| `SingleResult` | `map` | `json:singleResult`  |

---
**Helping nested json values n**.

 * [bulkError](#bulkerror)
 * [exceptions](#exceptions)
 * [operationConfig](#operationconfig)
 * [retriedError](#retriederror)
 * [sdkTimings](#sdktimings)
 * [singleResult](#singleresult)

---
#### bulkError

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `SDKTiming` | `struct` | `json:sdkTimings`  |
| `DocId` | `string` | `json:key`  |
| `Status` | `bool` | `json:status`  |
| `Extra` | `map` | `json:extra`  |
#### exceptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IgnoreExceptions` | `slice` | `json:ignoreExceptions,omitempty`  |
| `RetryExceptions` | `slice` | `json:retryExceptions,omitempty`  |
#### operationConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `DocSize` | `int` | `json:docSize`  |
| `DocType` | `string` | `json:docType,omitempty`  |
| `KeySize` | `int` | `json:keySize,omitempty`  |
| `TemplateName` | `string` | `json:template`  |
| `Start` | `int64` | `json:start`  |
| `End` | `int64` | `json:end`  |
| `FieldsToChange` | `slice` | `json:fieldsToChange`  |
| `Exceptions` | `struct` | `json:exceptions,omitempty`  |
#### retriedError

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `SDKTiming` | `struct` | `json:sdkTimings`  |
| `DocId` | `string` | `json:key`  |
| `Status` | `bool` | `json:status`  |
| `Extra` | `map` | `json:extra`  |
#### sdkTimings

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `SendTime` | `string` | `json:sendTime`  |
| `AckTime` | `string` | `json:ackTime`  |
#### singleResult

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `SDKTiming` | `struct` | `json:sdkTimings`  |
| `ErrorString` | `string` | `json:errorString`  |
| `Status` | `bool` | `json:status`  |
| `Cas` | `uint64` | `json:cas`  |

---
