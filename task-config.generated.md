
### Available Actions

Each task can be executed using REST endpoints. All tasks tags to provide additional
configuration that is also available on a per-task basis:

 * [/bulk-create](#bulk-create)
 * [/bulk-delete](#bulk-delete)
 * [/bulk-read](#bulk-read)
 * [/bulk-upsert](#bulk-upsert)
 * [/clear_data](#clear_data)
 * [/result](#result)
 * [/retry-exceptions](#retry-exceptions)
 * [/run-template-query](#run-template-query)
 * [/single-create](#single-create)
 * [/single-delete](#single-delete)
 * [/single-read](#single-read)
 * [/single-replace](#single-replace)
 * [/single-touch](#single-touch)
 * [/single-upsert](#single-upsert)
 * [/validate](#validate)

---
#### /bulk-create

 REST : POST

Description :  Insert task uploads documents in bulk into a bucket.
The durability while inserting a document can be set using following values in the 'durability' JSON tag :-
1. MAJORITY
2. MAJORITY_AND_PERSIST_TO_ACTIVE
3. PERSIST_TO_MAJORITY


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `InsertOptions` | `ptr` | `json:insertOptions,omitempty`  |
| `OperationConfig` | `ptr` | `json:operationConfig,omitempty`  |

---
#### /bulk-delete

 REST : POST

Description : Delete task deletes documents in bulk into a bucket.
The task will delete documents from [start,end] inclusive.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `RemoveOptions` | `ptr` | `json:removeOptions,omitempty`  |
| `OperationConfig` | `ptr` | `json:operationConfig,omitempty`  |

---
#### /bulk-read

 REST : POST

Description : Read Task get documents from bucket and validate them with the expected ones

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `OperationConfig` | `ptr` | `json:operationConfig,omitempty`  |

---
#### /bulk-upsert

 REST : POST

Description : Upsert task mutates documents in bulk into a bucket.
The task will update the fields in a documents ranging from [start,end] inclusive.
We need to share the fields we want to update in a json document using SQL++ syntax.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `InsertOptions` | `ptr` | `json:insertOptions,omitempty`  |
| `OperationConfig` | `ptr` | `json:operationConfig,omitempty`  |

---
#### /clear_data

 REST : POST

Description : The Task clear operation will remove the metadata from the bucket on the specific Couchbase server where
the test was executed.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |

---
#### /result

 REST : POST

Description :  Task result is retrieved via this endpoint.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Seed` | `string` | `json:seed`  |
| `DeleteRecord` | `bool` | `json:deleteRecord`  |

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

---
#### /run-template-query

 REST : POST

Description :  Query task runs N1QL query over a period of time over a bucket.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `QueryOperationConfig` | `ptr` | `json:operationConfig,omitempty`  |

---
#### /single-create

 REST : POST

Description : Single insert task create key value in Couchbase.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `InsertOptions` | `ptr` | `json:insertOptions,omitempty`  |
| `OperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /single-delete

 REST : POST

Description : Single delete task deletes key in Couchbase.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `RemoveOptions` | `ptr` | `json:removeOptions,omitempty`  |
| `OperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /single-read

 REST : POST

Description : Single read task reads key value in couchbase and validates.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `OperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /single-replace

 REST : POST

Description : Single replace task a document in the collection in Couchbase.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `ReplaceOptions` | `ptr` | `json:replaceOptions,omitempty`  |
| `OperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /single-touch

 REST : POST

Description : Single touch task specifies a new expiry time for a document in Couchbase.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `InsertOptions` | `ptr` | `json:insertOptions,omitempty`  |
| `OperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /single-upsert

 REST : POST

Description : Single insert task updates key value in Couchbase.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `InsertOptions` | `ptr` | `json:insertOptions,omitempty`  |
| `OperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /validate

 REST : POST

Description : Validates every document in the cluster's bucket

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `OperationConfig` | `ptr` | `json:operationConfig,omitempty`  |

---
**Description of JSON tags used in routes**.

 * [bulkError](#bulkerror)
 * [clusterConfig](#clusterconfig)
 * [compressionConfig](#compressionconfig)
 * [exceptions](#exceptions)
 * [insertOptions](#insertoptions)
 * [keyValue](#keyvalue)
 * [operationConfig](#operationconfig)
 * [queryOperationConfig](#queryoperationconfig)
 * [removeOptions](#removeoptions)
 * [replaceOption](#replaceoption)
 * [retriedError](#retriederror)
 * [singleOperationConfig](#singleoperationconfig)
 * [singleResult](#singleresult)
 * [timeoutsConfig](#timeoutsconfig)

---
#### bulkError

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `DocId` | `string` | `json:key`  |
| `Status` | `bool` | `json:status`  |
| `Cas` | `uint64` | `json:cas`  |
| `ErrorString` | `string` | `json:errorString`  |
#### clusterConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `ConnectionString` | `string` | `json:connectionString`  |
| `CompressionConfig` | `struct` | `json:compressionConfig,omitempty`  |
| `TimeoutsConfig` | `struct` | `json:timeoutsConfig,omitempty`  |
#### compressionConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Disabled` | `bool` | `json:disabled,omitempty`  |
| `MinSize` | `uint32` | `json:minSize,omitempty`  |
| `MinRatio` | `float64` | `json:minRatio,omitempty`  |
#### exceptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IgnoreExceptions` | `slice` | `json:ignoreExceptions,omitempty`  |
| `RetryExceptions` | `slice` | `json:retryExceptions,omitempty`  |
| `RetryAttempts` | `int` | `json:retryAttempts,omitempty`  |
#### insertOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Expiry` | `int64` | `json:expiry,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
#### keyValue

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Key` | `string` | `json:key`  |
| `Doc` | `interface` | `json:value,omitempty`  |
#### operationConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Count` | `int64` | `json:count,omitempty`  |
| `DocSize` | `int` | `json:docSize`  |
| `DocType` | `string` | `json:docType,omitempty`  |
| `KeySize` | `int` | `json:keySize,omitempty`  |
| `KeyPrefix` | `string` | `json:keyPrefix`  |
| `KeySuffix` | `string` | `json:keySuffix`  |
| `ReadYourOwnWrite` | `bool` | `json:readYourOwnWrite,omitempty`  |
| `TemplateName` | `string` | `json:template`  |
| `Start` | `int64` | `json:start`  |
| `End` | `int64` | `json:end`  |
| `FieldsToChange` | `slice` | `json:fieldsToChange`  |
| `Exceptions` | `struct` | `json:exceptions,omitempty`  |
#### queryOperationConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Template` | `string` | `json:template,omitempty`  |
| `Duration` | `int` | `json:duration,omitempty`  |
| `BuildIndex` | `bool` | `json:buildIndex`  |
| `BuildIndexViaSDK` | `bool` | `json:buildIndexViaSDK`  |
#### removeOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Cas` | `uint64` | `json:cas,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
#### replaceOption

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Expiry` | `int64` | `json:expiry,omitempty`  |
| `Cas` | `uint64` | `json:cas,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
#### retriedError

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `DocId` | `string` | `json:key`  |
| `Status` | `bool` | `json:status`  |
| `Cas` | `uint64` | `json:cas`  |
| `ErrorString` | `string` | `json:errorString`  |
#### singleOperationConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `KeyValue` | `slice` | `json:keyValue`  |
| `ReadYourOwnWrite` | `bool` | `json:readYourOwnWrite,omitempty`  |
#### singleResult

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `ErrorString` | `string` | `json:errorString`  |
| `Status` | `bool` | `json:status`  |
| `Cas` | `uint64` | `json:cas`  |
#### timeoutsConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `ConnectTimeout` | `int` | `json:connectTimeout,omitempty`  |
| `KVTimeout` | `int` | `json:KVTimeout,omitempty`  |
| `KVDurableTimeout` | `int` | `json:KVDurableTimeout,omitempty`  |

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
