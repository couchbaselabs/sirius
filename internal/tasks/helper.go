package tasks

import (
	"reflect"
)

const (
	MaxConcurrentRoutines               = 128
	DefaultIdentifierToken              = "default"
	WatchIndexDuration           int    = 120
	InsertOperation              string = "insert"
	QueryOperation               string = "query"
	DeleteOperation              string = "delete"
	UpsertOperation              string = "upsert"
	ReadOperation                string = "read"
	TouchOperation               string = "touch"
	ValidateOperation            string = "validate"
	SingleInsertOperation        string = "singleInsert"
	SingleDeleteOperation        string = "singleDelete"
	SingleUpsertOperation        string = "singleUpsert"
	SingleReadOperation          string = "singleRead"
	SingleTouchOperation         string = "singleTouch"
	SingleReplaceOperation       string = "singleReplace"
	CreatePrimaryIndex           string = "createPrimaryIndex"
	CreateIndex                  string = "createIndex"
	BuildIndex                   string = "buildIndex"
	RetryExceptionOperation      string = "retryException"
	SubDocInsertOperation        string = "subDocInsert"
	SubDocDeleteOperation        string = "subDocDelete"
	SubDocUpsertOperation        string = "subDocUpsert"
	SubDocReadOperation          string = "subDocRead"
	SubDocReplaceOperation       string = "subDocReplace"
	SingleSubDocInsertOperation  string = "singleSubDocInsert"
	SingleSubDocUpsertOperation  string = "singleSubDocUpsert"
	SingleSubDocReplaceOperation string = "singleSubDocReplace"
	SingleSubDocDeleteOperation  string = "singleSubDocDelete"
	SingleSubDocReadOperation    string = "singleSubDocRead"
	SingleDocValidateOperation   string = "SingleDocValidate"
	BucketWarmUpOperation        string = "BucketWarmUp"
)

func buildKeyAndValues(doc map[string]any, result map[string]any, startString string) {
	for key, value := range doc {
		if subDoc, ok := value.(map[string]any); ok {
			buildKeyAndValues(subDoc, result, key+".")
		} else {
			result[startString+key] = value
		}
	}
}

func CompareDocumentsIsSame(host map[string]any, document1 map[string]any, document2 map[string]any) bool {

	hostMap := make(map[string]any)
	buildKeyAndValues(host, hostMap, "")

	document1Map := make(map[string]any)
	buildKeyAndValues(document1, document1Map, "")

	document2Map := make(map[string]any)
	buildKeyAndValues(document2, document2Map, "")

	for key, value := range hostMap {
		if v1, ok := document1Map[key]; ok {
			if reflect.DeepEqual(value, v1) == false {
				return false
			}
		} else if v2, ok := document2Map[key]; ok {
			if reflect.DeepEqual(v2, value) == false {
				return false
			}
		} else {
			// TODO  fix_the_validation_of_missing_Keys
			continue
		}
	}

	return true
}
