
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
 * [/single-doc-validate](#single-doc-validate)
 * [/single-read](#single-read)
 * [/single-replace](#single-replace)
 * [/single-sub-doc-delete](#single-sub-doc-delete)
 * [/single-sub-doc-insert](#single-sub-doc-insert)
 * [/single-sub-doc-read](#single-sub-doc-read)
 * [/single-sub-doc-replace](#single-sub-doc-replace)
 * [/single-sub-doc-upsert](#single-sub-doc-upsert)
 * [/single-touch](#single-touch)
 * [/single-upsert](#single-upsert)
 * [/sub-doc-bulk-delete](#sub-doc-bulk-delete)
 * [/sub-doc-bulk-insert](#sub-doc-bulk-insert)
 * [/sub-doc-bulk-read](#sub-doc-bulk-read)
 * [/sub-doc-bulk-replace](#sub-doc-bulk-replace)
 * [/sub-doc-bulk-upsert](#sub-doc-bulk-upsert)
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
| `SingleOperationConfig` | `ptr` | `json:singleOperationConfig`  |

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
| `SingleOperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /single-doc-validate

 REST : POST

Description : validate the document integrity by document ID

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SingleOperationConfig` | `ptr` | `json:singleOperationConfig`  |

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
| `SingleOperationConfig` | `ptr` | `json:singleOperationConfig`  |

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
| `SingleOperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /single-sub-doc-delete

 REST : POST

Description : SingleSingleSubDocDelete inserts a Sub-Document as per user's input [No Random data]

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SingleSubDocOperationConfig` | `ptr` | `json:singleSubDocOperationConfig`  |
| `RemoveSpecOptions` | `ptr` | `json:removeSpecOptions`  |
| `MutateInOptions` | `ptr` | `json:mutateInOptions`  |

---
#### /single-sub-doc-insert

 REST : POST

Description : SingleSingleSubDocInsert inserts a Sub-Document as per user's input [No Random data]

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SingleSubDocOperationConfig` | `ptr` | `json:singleSubDocOperationConfig`  |
| `InsertSpecOptions` | `ptr` | `json:insertSpecOptions`  |
| `MutateInOptions` | `ptr` | `json:mutateInOptions`  |

---
#### /single-sub-doc-read

 REST : POST

Description : SingleSingleSubDocRead inserts a Sub-Document as per user's input [No Random data]

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SingleSubDocOperationConfig` | `ptr` | `json:singleSubDocOperationConfig`  |
| `LookupInOptions` | `ptr` | `json:lookupInOptions`  |
| `GetSpecOptions` | `ptr` | `json:getSpecOptions`  |

---
#### /single-sub-doc-replace

 REST : POST

Description : SingleSingleSubDocReplace inserts a Sub-Document as per user's input [No Random data]

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SingleSubDocOperationConfig` | `ptr` | `json:singleSubDocOperationConfig`  |
| `ReplaceSpecOptions` | `ptr` | `json:replaceSpecOptions`  |
| `MutateInOptions` | `ptr` | `json:mutateInOptions`  |

---
#### /single-sub-doc-upsert

 REST : POST

Description : SingleSingleSubDocUpsert inserts a Sub-Document as per user's input [No Random data]

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SingleSubDocOperationConfig` | `ptr` | `json:singleSubDocOperationConfig`  |
| `InsertSpecOptions` | `ptr` | `json:insertSpecOptions`  |
| `MutateInOptions` | `ptr` | `json:mutateInOptions`  |

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
| `SingleOperationConfig` | `ptr` | `json:singleOperationConfig`  |

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
| `SingleOperationConfig` | `ptr` | `json:singleOperationConfig`  |

---
#### /sub-doc-bulk-delete

 REST : POST

Description :  SubDocDelete deletes sub-documents in bulk

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SubDocOperationConfig` | `ptr` | `json:subDocOperationConfig`  |
| `RemoveSpecOptions` | `ptr` | `json:removeSpecOptions`  |
| `MutateInOptions` | `ptr` | `json:mutateInOptions`  |

---
#### /sub-doc-bulk-insert

 REST : POST

Description :  SubDocInsert inserts a Sub-Document

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SubDocOperationConfig` | `ptr` | `json:subDocOperationConfig`  |
| `InsertSpecOptions` | `ptr` | `json:insertSpecOptions`  |
| `MutateInOptions` | `ptr` | `json:mutateInOptions`  |

---
#### /sub-doc-bulk-read

 REST : POST

Description :  SubDocRead reads sub-document in bulk

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SubDocOperationConfig` | `ptr` | `json:subDocOperationConfig`  |
| `GetSpecOptions` | `ptr` | `json:getSpecOptions`  |
| `LookupInOptions` | `ptr` | `json:lookupInOptions`  |

---
#### /sub-doc-bulk-replace

 REST : POST

Description :  SubDocReplace upserts a Sub-Document

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SubDocOperationConfig` | `ptr` | `json:subDocOperationConfig`  |
| `ReplaceSpecOptions` | `ptr` | `json:replaceSpecOptions`  |
| `MutateInOptions` | `ptr` | `json:mutateInOptions`  |

---
#### /sub-doc-bulk-upsert

 REST : POST

Description :  SubDocUpsert upserts a Sub-Document

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IdentifierToken` | `string` | `json:identifierToken`  |
| `ClusterConfig` | `ptr` | `json:clusterConfig`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `SubDocOperationConfig` | `ptr` | `json:subDocOperationConfig`  |
| `InsertSpecOptions` | `ptr` | `json:insertSpecOptions`  |
| `MutateInOptions` | `ptr` | `json:mutateInOptions`  |

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
 * [getSpecOptions](#getspecoptions)
 * [insertOptions](#insertoptions)
 * [insertSpecOptions](#insertspecoptions)
 * [lookupInOptions](#lookupinoptions)
 * [mutateInOptions](#mutateinoptions)
 * [operationConfig](#operationconfig)
 * [queryOperationConfig](#queryoperationconfig)
 * [removeOptions](#removeoptions)
 * [removeSpecOptions](#removespecoptions)
 * [replaceOption](#replaceoption)
 * [replaceSpecOptions](#replacespecoptions)
 * [retriedError](#retriederror)
 * [singleOperationConfig](#singleoperationconfig)
 * [singleResult](#singleresult)
 * [singleSubDocOperationConfig](#singlesubdocoperationconfig)
 * [subDocOperationConfig](#subdocoperationconfig)
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
#### getSpecOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IsXattr` | `bool` | `json:isXattr,omitempty`  |
#### insertOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Expiry` | `int64` | `json:expiry,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
#### insertSpecOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `CreatePath` | `bool` | `json:createPath,omitempty`  |
| `IsXattr` | `bool` | `json:isXattr,omitempty`  |
#### lookupInOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Timeout` | `int` | `json:timeout,omitempty`  |
#### mutateInOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Expiry` | `int` | `json:expiry,omitempty`  |
| `Cas` | `uint64` | `json:cas,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `StoreSemantic` | `int` | `json:storeSemantic,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
| `PreserveExpiry` | `bool` | `json:preserveExpiry,omitempty`  |
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
#### removeSpecOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IsXattr` | `bool` | `json:isXattr,omitempty`  |
#### replaceOption

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Expiry` | `int64` | `json:expiry,omitempty`  |
| `Cas` | `uint64` | `json:cas,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
#### replaceSpecOptions

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `IsXattr` | `bool` | `json:isXattr,omitempty`  |
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
| `Keys` | `slice` | `json:keys`  |
| `Template` | `string` | `json:template`  |
| `DocSize` | `int` | `json:docSize`  |
#### singleResult

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `ErrorString` | `string` | `json:errorString`  |
| `Status` | `bool` | `json:status`  |
| `Cas` | `uint64` | `json:cas`  |
#### singleSubDocOperationConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Key` | `string` | `json:key`  |
| `Paths` | `slice` | `json:paths`  |
| `DocSize` | `int` | `json:docSize`  |
#### subDocOperationConfig

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Start` | `int64` | `json:start`  |
| `End` | `int64` | `json:end`  |
| `Exceptions` | `struct` | `json:exceptions,omitempty`  |
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
