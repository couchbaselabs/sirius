package sdk

import (
	"errors"
	"github.com/couchbase/gocb/v2"
)

func RegisteredErrors() map[error]struct{} {
	return map[error]struct{}{
		gocb.ErrCasMismatch:            {},
		gocb.ErrCollectionNotFound:     {},
		gocb.ErrScopeNotFound:          {},
		gocb.ErrDecodingFailure:        {},
		gocb.ErrDocumentExists:         {},
		gocb.ErrDocumentNotFound:       {},
		gocb.ErrDocumentLocked:         {},
		gocb.ErrDurabilityAmbiguous:    {},
		gocb.ErrDurabilityImpossible:   {},
		gocb.ErrDurableWriteInProgress: {},
		gocb.ErrFeatureNotAvailable:    {},
		gocb.ErrTimeout:                {},
		gocb.ErrAmbiguousTimeout:       {},
		gocb.ErrUnambiguousTimeout:     {},
		gocb.ErrPathNotFound:           {},
		gocb.ErrPathInvalid:            {},
		gocb.ErrPathExists:             {},
		gocb.ErrRequestCanceled:        {},
		gocb.ErrTemporaryFailure:       {},
		gocb.ErrValueTooLarge:          {},
	}
}

// CheckSDKException returns SDK Exception on possible match.
func CheckSDKException(err error) (string, error) {
	for e, _ := range RegisteredErrors() {
		if errors.Is(err, e) {
			return e.Error(), nil
		}
	}
	return "", errors.New("SDK Exception Not Found")
}
