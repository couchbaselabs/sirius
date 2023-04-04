
### Available Actions

Each task can be executed using REST endpoints. All tasks tags to provide additional
configuration that is also available on a per-task basis:

 * [/delete](#/delete)
 * [/flush](#/flush)
 * [/insert](#/insert)
 * [/upsert](#/upsert)
 * [/validate](#/validate)

---
#### /delete

 REST : POST

| Name | Type | JSON Tag 
| ---- | ---- | -------- 
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
#### /flush

 REST : POST

| Name | Type | JSON Tag 
| ---- | ---- | -------- 
| `ConnectionString` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Host` | `string` | `json:host`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |

---
#### /insert

 REST : POST

| Name | Type | JSON Tag 
| ---- | ---- | -------- 
| `ConnectionString` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `Count` | `int64` | `json:count,omitempty`  |
| `DocSize` | `int64` | `json:docSize,omitempty`  |
| `DocType` | `string` | `json:docType,omitempty`  |
| `KeySize` | `int` | `json:keySize,omitempty`  |
| `KeyPrefix` | `string` | `json:keyPrefix,omitempty`  |
| `KeySuffix` | `string` | `json:keySuffix,omitempty`  |
| `RandomDocSize` | `bool` | `json:randomDocSize,omitempty`  |
| `RandomKeySize` | `bool` | `json:randomKeySize,omitempty`  |
| `Expiry` | `int64` | `json:expiry,omitempty`  |
| `PersistTo` | `uint` | `json:PersistTo,omitempty`  |
| `ReplicateTo` | `uint` | `json:replicateTo,omitempty`  |
| `Durability` | `string` | `json:durability,omitempty`  |
| `Timeout` | `int` | `json:timeout,omitempty`  |
| `ReadYourOwnWrite` | `bool` | `json:readYourOwnWrite,omitempty`  |
| `TemplateName` | `string` | `json:template`  |

---
#### /upsert

 REST : POST

| Name | Type | JSON Tag 
| ---- | ---- | -------- 
| `ConnectionString` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |
| `Start` | `int64` | `json:start`  |
| `End` | `int64` | `json:end`  |
| `FieldsToChange` | `slice` | `json:fieldsToChange,omitempty`  |

---
#### /validate

 REST : POST

| Name | Type | JSON Tag 
| ---- | ---- | -------- 
| `ConnectionString` | `string` | `json:connectionString`  |
| `Username` | `string` | `json:username`  |
| `Password` | `string` | `json:password`  |
| `Bucket` | `string` | `json:bucket`  |
| `Scope` | `string` | `json:scope,omitempty`  |
| `Collection` | `string` | `json:collection,omitempty`  |

---
