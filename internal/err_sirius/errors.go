package err_sirius

type Error string

func (e Error) Error() string { return string(e) }

const (
	RequestIsNil                       = Error("internal request.Request struct is nil")
	TaskStateIsNil                     = Error("task State is nil")
	InvalidInfo                        = Error("information(connection string,username,password) is nil")
	ParsingClusterConfig               = Error("unable to parse clusterConfig")
	CredentialMissing                  = Error("missing credentials for authentication")
	InvalidConnectionString            = Error("empty or invalid connection string")
	InvalidUsername                    = Error("empty or invalid username")
	ParsingSingleOperationConfig       = Error("unable to parse SingleOperationConfig")
	ParsingQueryConfig                 = Error("unable to parse QueryOperationConfig")
	ParsingOperatingConfig             = Error("unable to parse operationConfig")
	MalformedOperationRange            = Error("operation start to end range is malformed")
	ParsingInsertOptions               = Error("unable to parse InsertOptions")
	ParsingTouchOptions                = Error("unable to parse TouchOptions")
	ParsingRemoveOptions               = Error("unable to parse RemoveOptions")
	ParsingReplaceOptions              = Error("unable to parse ReplaceOptions")
	ParsingSubDocOperatingConfig       = Error("unable to parse SubDocOperatingConfig")
	ParsingGetSpecOptions              = Error("unable to parse GetSpecOptions")
	ParsingLookupInOptions             = Error("unable to parse LookupInOptions")
	ParsingInsertSpecOptions           = Error("unable to parse InsertSpecOptions")
	ParsingRemoveSpecOptions           = Error("unable to parse RemoveSpecOptions")
	ParsingReplaceSpecOptions          = Error("unable to parse ReplaceSpecOptions")
	ParsingSingleSubDocOperationConfig = Error("unable to parse SingleSubDocOperationConfig")
	ParsingMutateInOptions             = Error("unable to parse MutateInOptions")
	NilOperationConfig                 = Error("no operation found for the given offset")
	TaskingRetryFailed                 = Error("task is still in pending state before retrying")
	TaskInPendingState                 = Error("current task is still in progress")
	InvalidDatabase                    = Error("invalid database in sirius")
	BucketIsMisssing                   = Error("bucket value is missing in extra parameters for couchbase cluster")
	CollectionIsMissing                = Error("collection is in extra parameters for mongo cluster")
	InternalErrorSetOperationType      = Error(
		"operation type not set in the handler of route before configuring the generic loading task")
	IntegrityLost       = Error("document comparison failed")
	InvalidTemplateName = Error("invalid template name in operation config")
)
