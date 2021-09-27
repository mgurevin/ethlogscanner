package ethlogscanner

import (
	"errors"
	"strings"

	"github.com/cenkalti/backoff/v4"
)

var errInvalidHead = errors.New("invalid head block")

func errClasses(err error) (temporary, timeout, tooMuchResults bool) {
	if err == nil {
		return
	}

	temporary = errors.Is(err, errInvalidHead)

	// check if err implements Timeout()
	var timeoutErr interface{ Timeout() bool }
	timeout = errors.As(err, &timeoutErr) && timeoutErr.Timeout()

	// check if err implements Temporary()
	var tempErr interface{ Temporary() bool }
	temporary = temporary || errors.As(err, &tempErr) && tempErr.Temporary()

	// fallback to check error message
	errMsg := strings.TrimSpace(err.Error())

	// check infura max resultset size
	tooMuchResults = strings.HasPrefix(errMsg, "query returned more than") && strings.HasSuffix(errMsg, "results")

	// check geth max resultset size
	tooMuchResults = tooMuchResults || strings.Contains(errMsg, "read limit exceeded")

	// tooMuchResults means it is temporary if we decrease the range of blocks queried
	temporary = temporary || tooMuchResults

	// timeout means it is temporary
	temporary = temporary || timeout

	// check backoff package's permanent err
	permErr := &backoff.PermanentError{}
	if errors.As(err, &permErr) {
		temporary = false
	}

	return
}
