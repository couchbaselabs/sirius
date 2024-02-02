package cb_sdk

import (
	"errors"
	"github.com/couchbase/gocb/v2"
)

func RegisteredErrors() map[error]struct{} {
	return map[error]struct{}{
		gocb.ErrCasMismatch:                    {},
		gocb.ErrCollectionNotFound:             {},
		gocb.ErrScopeNotFound:                  {},
		gocb.ErrDecodingFailure:                {},
		gocb.ErrDocumentExists:                 {},
		gocb.ErrDocumentNotFound:               {},
		gocb.ErrDocumentLocked:                 {},
		gocb.ErrDurabilityAmbiguous:            {},
		gocb.ErrDurabilityImpossible:           {},
		gocb.ErrUnambiguousTimeout:             {},
		gocb.ErrDurableWriteInProgress:         {},
		gocb.ErrFeatureNotAvailable:            {},
		gocb.ErrTimeout:                        {},
		gocb.ErrAmbiguousTimeout:               {},
		gocb.ErrUnambiguousTimeout:             {},
		gocb.ErrPathNotFound:                   {},
		gocb.ErrPathInvalid:                    {},
		gocb.ErrPathExists:                     {},
		gocb.ErrRequestCanceled:                {},
		gocb.ErrTemporaryFailure:               {},
		gocb.ErrValueTooLarge:                  {},
		gocb.ErrIndexExists:                    {},
		gocb.ErrIndexFailure:                   {},
		gocb.ErrIndexNotFound:                  {},
		gocb.ErrAttemptNotFoundOnQuery:         {},
		gocb.ErrPlanningFailure:                {},
		gocb.ErrCasMismatch:                    {},
		gocb.ErrBucketNotFound:                 {},
		gocb.ErrBucketNotFlushable:             {},
		gocb.ErrBucketExists:                   {},
		gocb.ErrAuthenticationFailure:          {},
		gocb.ErrDurableWriteReCommitInProgress: {},
		gocb.ErrDurabilityLevelNotAvailable:    {},
	}
}

// CheckSDKException returns SDK Exception on possible match.
func CheckSDKException(err error) (string, string) {

	for e, _ := range RegisteredErrors() {
		if errors.Is(err, e) {
			return e.Error(), err.Error()
		}
	}
	return "Unknown Exception", err.Error()
}
