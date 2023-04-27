
### Available Actions

Each task can be executed using REST endpoints. All tasks tags to provide additional
configuration that is also available on a per-task basis:

 * [/delete](#delete)
 * [/insert](#insert)
 * [/result](#result)
 * [/upsert](#upsert)
 * [/validate](#validate)

---
#### /delete

 REST : POST

Description : Delete task deletes documents in bulk into a bucket.
The task will delete documents from [start,end] inclusive.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `ConnectionString` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Host` | `string` | `json:host`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `Start` | `int64` | `json:start`  |
| `End` | `int64` | `json:end`  |

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
| `ConnectionString` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `Count` | `int64` | `json:count,omitempty`  |
| `DocSize` | `int64` | `json:docSize`  |
| `DocType` | `string` | `json:docType,omitempty`  |
| `KeySize` | `int` | `json:keySize,omitempty`  |
| `KeyPrefix` | `string` | `json:keyPrefix`  |
| `KeySuffix` | `string` | `json:keySuffix`  |
| `RandomDocSize` | `bool` | `json:randomDocSize,omitempty`  |
| `RandomKeySize` | `bool` | `json:randomKeySize,omitempty`  |
| `Expiry` | `int64` | `json:expiry,omitempty`  |
| `PersistTo` | `uint` | `json:persistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
| `ReadYourOwnWrite` | `bool` | `json:readYourOwnWrite,omitempty`  |
| `TemplateName` | `string` | `json:template`  |

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
| `ConnectionString` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `Start` | `int64` | `json:start`  |
| `End` | `int64` | `json:end`  |
| `FieldsToChange` | `slice` | `json:fieldsToChange`  |
| `TemplateName` | `string` | `json:template`  |
| `DocSize` | `int64` | `json:docSize`  |
| `KeyPrefix` | `string` | `json:keyPrefix`  |
| `KeySuffix` | `string` | `json:keySuffix`  |

---
#### /validate

 REST : POST

Description : validate every document in the cluster's bucket

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `ConnectionString` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `TemplateName` | `string` | `json:template`  |
| `DocSize` | `int64` | `json:docSize`  |
| `KeyPrefix` | `string` | `json:keyPrefix`  |
| `KeySuffix` | `string` | `json:keySuffix`  |

---
**API's Response Description**.

1. Response after initiating a TASK.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Seed` | `string` | `json:seed`  |

---
2. Response which contains the TASK's result.

| Name | Type | JSON Tag |
| ---- | ---- | -------- |
| `Seed` | `int64` | `json:seed`  |
| `Operation` | `string` | `json:operation`  |
| `ErrorOther` | `string` | `json:other-errors,omitempty`  |
| `Success` | `int64` | `json:success`  |
| `Failure` | `int64` | `json:failure`  |
| `ValidationError` | `slice` | `json:validation-errors,omitempty`  |
| `Error` | `map` | `json:errors`  |

---
