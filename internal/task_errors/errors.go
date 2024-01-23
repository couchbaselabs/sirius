package task_errors

import "errors"

var (
	ErrRequestIsNil                       = errors.New("internal request.Request struct is nil")
	ErrTaskStateIsNil                     = errors.New("task State is nil")
	ErrParsingClusterConfig               = errors.New("unable to parse clusterConfig")
	ErrCredentialMissing                  = errors.New("missing credentials for authentication")
	ErrInvalidConnectionString            = errors.New("empty or invalid connection string")
	ErrParsingSingleOperationConfig       = errors.New("unable to parse SingleOperationConfig")
	ErrParsingQueryConfig                 = errors.New("unable to parse QueryOperationConfig")
	ErrParsingOperatingConfig             = errors.New("unable to parse operationConfig")
	ErrMalformedOperationRange            = errors.New("operation start to end range is malformed")
	ErrParsingInsertOptions               = errors.New("unable to parse InsertOptions")
	ErrParsingTouchOptions                = errors.New("unable to parse TouchOptions")
	ErrParsingRemoveOptions               = errors.New("unable to parse RemoveOptions")
	ErrParsingReplaceOptions              = errors.New("unable to parse ReplaceOptions")
	ErrParsingSubDocOperatingConfig       = errors.New("unable to parse SubDocOperatingConfig")
	ErrParsingGetSpecOptions              = errors.New("unable to parse GetSpecOptions")
	ErrParsingLookupInOptions             = errors.New("unable to parse LookupInOptions")
	ErrParsingInsertSpecOptions           = errors.New("unable to parse InsertSpecOptions")
	ErrParsingRemoveSpecOptions           = errors.New("unable to parse RemoveSpecOptions")
	ErrParsingReplaceSpecOptions          = errors.New("unable to parse ReplaceSpecOptions")
	ErrParsingSingleSubDocOperationConfig = errors.New("unable to parse SingleSubDocOperationConfig")
	ErrParsingMutateInOptions             = errors.New("unable to parse MutateInOptions")
	ErrNilOperationConfig                 = errors.New("no operation found for the given offset")
)
