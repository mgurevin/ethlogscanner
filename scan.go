package ethlogscanner

import (
	"context"
	"io"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
)

type Log types.Log

type Notification interface {
	keepInternal()

	Time() time.Time
}

type notification time.Time

type ChunkSizeUpdated struct {
	notification

	Previous int
	Updated  int
}

type FilterStarted struct {
	notification

	From uint64
	To   uint64

	ChunkSize int
}

type CursorUpdated struct {
	notification

	Next Cursor
}

type FilterCompleted struct {
	FilterStarted

	Duration     time.Duration
	NumberOfLogs int

	HasErr bool
}

func (n *notification) keepInternal()  {}
func (n notification) Time() time.Time { return time.Time(n) }

type Scanner interface {
	io.Closer

	Err() <-chan error
	Log() <-chan *Log
	Done() <-chan struct{}
	Notify() <-chan Notification

	Next() Cursor
}

type scanner struct {
	ctx context.Context

	ethC *ethclient.Client
	opts *options

	curr Cursor
	next Cursor

	chunk int

	chErr    chan error
	chLog    chan *Log
	chDone   chan struct{}
	chNotify chan Notification

	chClose   chan struct{}
	closeOnce sync.Once

	lastKnownHead           uint64
	lastFetchedNumberOfLogs int
}

func Scan(ctx context.Context, ethClient *ethclient.Client, options ...Option) (Scanner, error) {
	opts, err := defaultOpts().apply(options)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s := &scanner{
		ctx: ctx,

		ethC:  ethClient,
		opts:  opts,
		curr:  opts.start,
		next:  opts.start,
		chunk: opts.initChunkSize,

		chErr:    make(chan error),
		chLog:    make(chan *Log),
		chDone:   make(chan struct{}),
		chClose:  make(chan struct{}),
		chNotify: make(chan Notification),
	}

	go s.scan()

	return s, nil
}

func (s *scanner) scan() {
	defer func() {
		defer close(s.chDone)

		close(s.chLog)
		close(s.chErr)
		close(s.chNotify)

		s.closeOnce.Do(func() {
			close(s.chClose)
		})
	}()

	loop := func() bool {
		subCtx, subCtxCancel := context.WithCancel(s.ctx)
		defer subCtxCancel()

		subErr := s.subScan(subCtx)

		select {
		case <-s.ctx.Done():
			s.chErr <- errors.WithStack(s.ctx.Err())

			return false

		case <-s.chClose:
			return false

		case err := <-subErr:
			if err == nil {
				return s.curr.BlockNum() < s.lastKnownHead
			}

			temporary, _, tooMuchResults := errClasses(err)

			if tooMuchResults {
				s.decrChunkSize(subCtx)
			}

			select {
			case s.chErr <- errors.WithStack(err):
			case <-s.ctx.Done():
			}

			return temporary
		}
	}

	for loop() {
	}
}

func (s *scanner) subScan(ctx context.Context) <-chan error {
	chErr := make(chan error)

	go func() {
		defer close(chErr)

		head := s.lastKnownHead

		if l := s.lastKnownHead; l == 0 || l <= s.curr.BlockNum()+uint64(s.chunk) {
			err := retry(ctx, s.opts.retryHead, func(ctx context.Context, tooMuchResults bool) (err error) {
				head, err = s.ethC.BlockNumber(ctx)

				err = errors.WithStack(err)

				if err == nil && head == 0 {
					return errors.WithStack(errInvalidHead)
				}

				head -= uint64(s.opts.stayBehind)

				return

			}, func(err error, next time.Duration) {
				select {
				case s.chErr <- errors.Errorf("%s (it will be retried after %.2f secs.)", err.Error(), next.Seconds()):
				case <-ctx.Done():
					return
				}
			})

			if err != nil {
				chErr <- errors.WithStack(err)

				return
			}

			s.lastKnownHead = head
		}

		var logs []types.Log

		var from, to uint64

		var fetchDur time.Duration

		err := retry(ctx, s.opts.retryHead, func(ctx context.Context, tooMuchResults bool) (err error) {
			if tooMuchResults {
				s.decrChunkSize(ctx)
			}

			from = s.curr.BlockNum()
			to = from + uint64(s.chunk) - 1

			if head < to {
				to = head
			}

			if from > to {
				from = to
			}

			notifyStarted := &FilterStarted{
				notification: notification(time.Now()),
				From:         from,
				To:           to,
				ChunkSize:    s.chunk,
			}

			select {
			case <-ctx.Done():
				return

			default:
				select {
				case s.chNotify <- notifyStarted:
				case <-ctx.Done():
					return
				}
			}

			start := time.Now()

			logs, err = s.ethC.FilterLogs(ctx, ethereum.FilterQuery{
				FromBlock: new(big.Int).SetUint64(from),
				ToBlock:   new(big.Int).SetUint64(to),
				Addresses: s.opts.addresses,
				Topics:    s.opts.topics,
			})

			fetchDur = time.Since(start)

			notifyStarted.notification = notification(time.Now())

			select {
			case <-ctx.Done():
				return

			default:
				select {
				case s.chNotify <- &FilterCompleted{
					FilterStarted: *notifyStarted,
					Duration:      fetchDur,
					NumberOfLogs:  len(logs),
					HasErr:        err != nil,
				}:

				case <-ctx.Done():
					return
				}
			}

			err = errors.WithStack(err)

			return

		}, func(err error, next time.Duration) {
			select {
			case s.chErr <- errors.Errorf("%s (it will be retried after %.2f secs.)", err.Error(), next.Seconds()):
			case <-ctx.Done():
				return
			}
		})

		if err != nil {
			chErr <- errors.WithStack(err)

			return
		}

		numberOfLogs := len(logs)

		if fetchDur > s.opts.adjThresholdDur {
			s.decrChunkSize(ctx)

		} else if math.Abs(float64(numberOfLogs-s.lastFetchedNumberOfLogs)) > float64(s.lastFetchedNumberOfLogs) {
			s.decrChunkSize(ctx)

		} else {
			s.incrChunkSize(ctx)
		}

		s.lastFetchedNumberOfLogs = numberOfLogs

		for _, log := range logs {
			l := Log(log)

			if c := l.Cursor(); c < s.curr {
				if l.Removed {
					select {
					case s.chErr <- errors.Errorf("log removed due to chain re-org: %s", c.String()):
					case <-ctx.Done():
						return
					}
				}

				continue
			}

			s.curr = l.Cursor()

			select {
			case s.chLog <- &l:
				atomic.StoreUint64((*uint64)(&s.next), uint64(s.curr.Next()))

				select {
				case <-ctx.Done():
					return

				default:
					select {
					case s.chNotify <- &CursorUpdated{
						notification: notification(time.Now()),
						Next:         s.Next(),
					}:

					case <-ctx.Done():
						return
					}
				}

			case <-ctx.Done():
				return
			}
		}

		s.curr = MakeCursor(to+1, 0, 0)

		atomic.StoreUint64((*uint64)(&s.next), uint64(s.curr))

		select {
		case <-ctx.Done():
			return

		default:
			select {
			case s.chNotify <- &CursorUpdated{
				notification: notification(time.Now()),
				Next:         s.Next(),
			}:

			case <-ctx.Done():
				return
			}
		}
	}()

	return chErr
}

func (s *scanner) incrChunkSize(ctx context.Context) {
	defer s.notifyChunkSize(ctx, time.Now(), s.chunk)

	if s.chunk *= 2; s.chunk > s.opts.maxChunkSize {
		s.chunk = s.opts.maxChunkSize
	}
}

func (s *scanner) decrChunkSize(ctx context.Context) {
	defer s.notifyChunkSize(ctx, time.Now(), s.chunk)

	if s.chunk /= 2; s.chunk == 0 {
		s.chunk = 1
	}
}

func (s *scanner) notifyChunkSize(ctx context.Context, t time.Time, prev int) {
	if s.chunk != prev {
		select {
		case <-ctx.Done():
			return

		default:
			select {
			case s.chNotify <- &ChunkSizeUpdated{
				notification: notification(t),
				Previous:     prev,
				Updated:      s.chunk,
			}:

			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *scanner) Err() <-chan error {
	return s.chErr
}

func (s *scanner) Log() <-chan *Log {
	return s.chLog
}

func (s *scanner) Done() <-chan struct{} {
	return s.chDone
}

func (s *scanner) Notify() <-chan Notification {
	return s.chNotify
}

func (s *scanner) Next() Cursor {
	return Cursor(atomic.LoadUint64((*uint64)(&s.next)))
}

func (s *scanner) Close() error {
	s.closeOnce.Do(func() {
		close(s.chClose)
	})

	select {
	case <-s.chDone:
		return nil

	case <-s.ctx.Done():
		return errors.WithStack(s.ctx.Err())
	}
}

func (l *Log) Cursor() Cursor {
	if l == nil {
		return 0
	}

	return MakeCursor(l.BlockNumber, l.TxIndex, l.Index)
}
