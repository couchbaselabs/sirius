
### Available Actions

Each task can be executed using REST endpoints. All tasks tags to provide additional
configuration that is also available on a per-task basis:

 * [/clear_data](#clear_data)
 * [/delete](#delete)
 * [/fast-insert](#fast-insert)
 * [/insert](#insert)
 * [/read](#read)
 * [/result](#result)
 * [/upsert](#upsert)
 * [/validate](#validate)

---
#### /clear_data

 REST : POST

Description : The Task clear operation will remove the metadata from the bucket on the specific Couchbase server where
the test was executed.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Bucket` | `string` | `json:bucket,omitempty`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |

---
#### /delete

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
#### /fast-insert

 REST : Post

Description : Fast Insert task uploads documents in bulk into a bucket without maintaining intermediate state of task 
During fast operations, An incomplete task will be retied as whole if server dies in between of the operation.
 

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
#### /insert

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
#### /read

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
#### /result

 REST : POST

Description :  Task result is retrieved via this endpoint.


| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Seed` | `string` | `json:seed`  |
| `DeleteRecord` | `bool` | `json:deleteRecord`  |

---
#### /upsert

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

 * [clusterConfig](#clusterconfig)
 * [compressionConfig](#compressionconfig)
 * [insertOptions](#insertoptions)
 * [operationConfig](#operationconfig)
 * [removeOptions](#removeoptions)
 * [timeoutsConfig](#timeoutsconfig)

---
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
#### insertOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Expiry` | `int64` | `json:expiry,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
#### operationConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Count` | `int64` | `json:count,omitempty`  |
| `DocSize` | `int64` | `json:docSize`  |
| `DocType` | `string` | `json:docType,omitempty`  |
| `KeySize` | `int` | `json:keySize,omitempty`  |
| `KeyPrefix` | `string` | `json:keyPrefix`  |
| `KeySuffix` | `string` | `json:keySuffix`  |
| `RandomDocSize` | `bool` | `json:randomDocSize,omitempty`  |
| `RandomKeySize` | `bool` | `json:randomKeySize,omitempty`  |
| `ReadYourOwnWrite` | `bool` | `json:readYourOwnWrite,omitempty`  |
| `TemplateName` | `string` | `json:template`  |
| `Start` | `int64` | `json:start`  |
| `End` | `int64` | `json:end`  |
| `FieldsToChange` | `slice` | `json:fieldsToChange`  |
#### removeOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Cas` | `uint64` | `json:cas,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
#### timeoutsConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `ConnectTimeout` | `int64` | `json:connectTimeout,omitempty`  |
| `KVTimeout` | `int64` | `json:KVTimeout,omitempty`  |
| `KVDurableTimeout` | `int64` | `json:KVDurableTimeout,omitempty`  |

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
| `ErrorOther` | `string` | `json:other-errors,omitempty`  |
| `Success` | `int64` | `json:success`  |
| `Failure` | `int64` | `json:failure`  |
| `ValidationError` | `slice` | `json:validation-errors,omitempty`  |
| `Error` | `map` | `json:errors`  |

---
