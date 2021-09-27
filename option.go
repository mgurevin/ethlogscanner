package ethlogscanner

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

type (
	Option interface {
		apply(*options) error
	}

	optStart      Cursor
	optStayBehind int

	optFilter struct {
		addresses []common.Address
		topics    [][]common.Hash
	}

	optRetryHead retryConf

	optRetryFilter retryConf

	optInitChunkSize int
	optMaxChunkSize  int

	optAdjThresholdLogs int
	optAdjThresholdDur  time.Duration

	retryConf struct {
		initInterval time.Duration
		maxInterval  time.Duration
		maxElapsed   time.Duration
	}

	options struct {
		start            Cursor
		stayBehind       int
		addresses        []common.Address
		topics           [][]common.Hash
		retryHead        retryConf
		retryFilter      retryConf
		initChunkSize    int
		maxChunkSize     int
		adjThresholdLogs int
		adjThresholdDur  time.Duration
	}
)

func WithStart(start Cursor) Option {
	return optStart(start)
}

func WithStayBehindToHead(stayBehindToHead int) Option {
	return optStayBehind(stayBehindToHead)
}

func WithFilter(addresses []common.Address, topics [][]common.Hash) Option {
	return &optFilter{addresses, topics}
}

func WithRetryHead(minInterval, maxInterval, maxElapsed time.Duration) Option {
	return &optRetryHead{minInterval, maxInterval, maxElapsed}
}

func WithRetryFilter(minInterval, maxInterval, maxElapsed time.Duration) Option {
	return &optRetryFilter{minInterval, maxInterval, maxElapsed}
}

func WithInitialChunkSize(initialChunkSize int) Option {
	return optInitChunkSize(initialChunkSize)
}

func WithMaxChunkSize(maxChunkSize int) Option {
	return optMaxChunkSize(maxChunkSize)
}

func WithThresholdChunkAdjByLogs(adjThresholdLogs int) Option {
	return optAdjThresholdLogs(adjThresholdLogs)
}

func WithThresholdChunkAdjByDur(adjThresholdDur time.Duration) Option {
	return optAdjThresholdDur(adjThresholdDur)
}

func (o optStart) apply(opts *options) error {
	opts.start = Cursor(o)

	return nil
}

func (o optStayBehind) apply(opts *options) error {
	opts.stayBehind = int(o)

	return nil
}

func (o *optFilter) apply(opts *options) error {
	opts.addresses = o.addresses
	opts.topics = o.topics

	return nil
}

func (o *optRetryHead) apply(opts *options) error {
	opts.retryHead = retryConf(*o)

	return nil
}

func (o *optRetryFilter) apply(opts *options) error {
	opts.retryFilter = retryConf(*o)

	return nil
}

func (o optInitChunkSize) apply(opts *options) error {
	opts.initChunkSize = int(o)

	return nil
}

func (o optMaxChunkSize) apply(opts *options) error {
	opts.maxChunkSize = int(o)

	return nil
}

func (o optAdjThresholdLogs) apply(opts *options) error {
	opts.adjThresholdLogs = int(o)

	return nil
}

func (o optAdjThresholdDur) apply(opts *options) error {
	opts.adjThresholdDur = time.Duration(o)

	return nil
}

func defaultOpts() *options {
	return &options{
		retryHead: retryConf{
			initInterval: 100 * time.Millisecond,
			maxInterval:  30 * time.Second,
			maxElapsed:   3 * time.Minute,
		},

		retryFilter: retryConf{
			initInterval: 250 * time.Millisecond,
			maxInterval:  45 * time.Second,
			maxElapsed:   5 * time.Minute,
		},

		initChunkSize: 1 << 4,
		maxChunkSize:  1 << 18,

		adjThresholdLogs: 1000,
		adjThresholdDur:  30 * time.Second,
	}
}

func (o *options) apply(opts []Option) (*options, error) {
	for _, opt := range opts {
		if err := opt.apply(o); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return o, nil
}
