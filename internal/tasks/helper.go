package tasks

const (
	DefaultIdentifierToken         = "default"
	WatchIndexDuration      int    = 120
	InsertOperation         string = "insert"
	BulkInsertOperation     string = "bulkInsert"
	QueryOperation          string = "query"
	DeleteOperation         string = "delete"
	BulkDeleteOperation     string = "bulkDelete"
	UpsertOperation         string = "upsert"
	BulkUpsertOperation     string = "bulkUpsert"
	ReadOperation           string = "read"
	BulkReadOperation       string = "bulkRead"
	TouchOperation          string = "touch"
	BulkTouchOperation      string = "bulkTouch"
	ValidateOperation       string = "validate"
	CreatePrimaryIndex      string = "createPrimaryIndex"
	CreateIndex             string = "createIndex"
	BuildIndex              string = "buildIndex"
	RetryExceptionOperation string = "retryException"
	SubDocInsertOperation   string = "subDocInsert"
	SubDocDeleteOperation   string = "subDocDelete"
	SubDocUpsertOperation   string = "subDocUpsert"
	SubDocReadOperation     string = "subDocRead"
	SubDocReplaceOperation  string = "subDocReplace"
	BucketWarmUpOperation   string = "BucketWarmUp"
)

func CheckBulkOperation(operation string) bool {
	switch operation {
	case BulkInsertOperation, BulkUpsertOperation, BulkReadOperation, BulkDeleteOperation, BulkTouchOperation:
		return true
	default:
		return false
	}
}
