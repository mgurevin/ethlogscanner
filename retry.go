package ethlogscanner

import (
	"context"

	"github.com/cenkalti/backoff/v4"
)

type execFunc func(ctx context.Context, tooMuchResult bool) error

func retry(ctx context.Context, conf retryConf, f execFunc, n backoff.Notify) error {
	retry := backoff.NewExponentialBackOff()
	retry.InitialInterval = conf.initInterval
	retry.MaxInterval = conf.maxInterval
	retry.MaxElapsedTime = conf.maxElapsed

	var tooMuchResult bool

	return backoff.RetryNotify(func() error {
		err := f(ctx, tooMuchResult)
		if err != nil {
			var tempErr bool

			tempErr, _, tooMuchResult = errClasses(err)

			if !tempErr {
				return backoff.Permanent(err)
			}

			return err
		}

		return nil
	}, backoff.WithContext(retry, ctx), n)
}
