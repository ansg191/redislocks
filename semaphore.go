package redislocks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Semaphore struct {
	baseLock
	limit int64

	// map[identifier string]*ctxEntry
	permits sync.Map
}

func NewSemaphore(client redis.UniversalClient, key string, limit int64, opts TimeoutOptions) *Semaphore {
	return &Semaphore{
		baseLock{
			client: client,
			key:    key,
			opts:   opts,
		},
		limit,
		sync.Map{},
	}
}

var acquireScript = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local identifier = ARGV[2]
local lockTimeout = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local expiredTimestamp = now - lockTimeout

redis.call('zremrangebyscore', key, '-inf', expiredTimestamp)

if redis.call('zcard', key) < limit then
	redis.call('zadd', key, now, identifier)
	redis.call('pexpire', key, lockTimeout)
	return 1
else
	return 0
end
`)

func (s *Semaphore) Lock(ctx context.Context, opts ...TimeoutOption) (context.Context, error) {
	o := s.opts
	for _, opt := range opts {
		opt(&o)
	}

	identifier, err := genIdentifier()
	if err != nil {
		return nil, err
	}

	aCtx, cancel := context.WithTimeoutCause(ctx, s.opts.AcquireTimeout, ErrAcquireTimeout)
	defer cancel()

	for attempt := uint64(0); attempt < s.opts.AcquireAttemptsLimit; attempt++ {
		resp, err := acquireScript.Run(
			aCtx,
			s.client,
			[]string{s.key},
			s.limit,
			identifier,
			s.opts.LockTimeout.Milliseconds(),
			time.Now().UnixMilli(),
		).Result()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) &&
				errors.Is(context.Cause(aCtx), ErrAcquireTimeout) {
				return nil, ErrAcquireTimeout
			}
			return nil, err
		}

		result, ok := resp.(int64)
		if !ok {
			return nil, fmt.Errorf("unexpected result type: %T", resp)
		}
		if result == 1 {
			// Acquired lock, return context that is canceled when the lock is lost.
			// Starts a goroutine to refresh the lock periodically.
			newCtx, newCancel := context.WithCancelCause(ctx)
			entry := &ctxEntry{
				stop:   make(chan struct{}),
				ctx:    ctx,
				cancel: newCancel,
			}
			s.permits.Store(identifier, entry)
			go s.refresh(newCtx, identifier, o)
			return newCtx, nil
		}

		// Lock wasn't acquired, wait before the next attempt.
		timer := time.NewTimer(s.opts.RetryInterval)

		select {
		case <-aCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			err = aCtx.Err()
			if errors.Is(err, context.DeadlineExceeded) &&
				errors.Is(context.Cause(aCtx), ErrAcquireTimeout) {
				return nil, ErrAcquireTimeout
			}
			return nil, err
		case <-timer.C:
		}
	}
	return nil, ErrAcquireAttemptsLimit
}

func (s *Semaphore) refresh(ctx context.Context, identifier string, o TimeoutOptions) {
	if o.RefreshInterval == 0 {
		o.RefreshInterval = time.Duration(refreshCoefficientNum * uint64(o.LockTimeout) / refreshCoefficientDen)
	}
	ticker := time.NewTicker(o.RefreshInterval)
	defer ticker.Stop()

	entryAny, ok := s.permits.Load(identifier)
	if !ok {
		return
	}
	entry := entryAny.(*ctxEntry)
	stopCh := entry.stop

	for {
		select {
		case <-ctx.Done():
			return // Context canceled, stop refreshing the lock.
		case <-ticker.C:
			refreshed, err := s.refreshInternal(ctx, identifier, o)
			if err != nil {
				// Error happened. Try again next time.
				continue
			}
			if !refreshed {
				// Lock lost: unlock and stop refreshing.
				_ = s.unlock(identifier)
				return
			}
		case <-stopCh:
			// Lock released, stop refreshing.
			return
		}
	}
}

var refreshScript = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local identifier = ARGV[2]
local lockTimeout = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local expiredTimestamp = now - lockTimeout

redis.call('zremrangebyscore', key, '-inf', expiredTimestamp)

if redis.call('zscore', key, identifier) then
	redis.call('zadd', key, now, identifier)
	redis.call('pexpire', key, lockTimeout)
	return 1
else
	return 0
end
`)

func (s *Semaphore) refreshInternal(ctx context.Context, identifier string, o TimeoutOptions) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, o.RefreshInterval)
	defer cancel()

	result, err := refreshScript.Run(
		ctx,
		s.client,
		[]string{s.key},
		s.limit,
		identifier,
		s.opts.LockTimeout.Milliseconds(),
		time.Now().UnixMilli(),
	).Result()
	if err != nil {
		return false, err
	}

	refreshed, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected result type: %T", result)
	}
	return refreshed == 1, nil
}

func (s *Semaphore) Unlock() error {
	var identifier string
	s.permits.Range(func(key, _ any) bool {
		identifier = key.(string)
		return false
	})
	if identifier == "" {
		// No permits, return nil.
		return nil
	}

	return s.unlock(identifier)
}

func (s *Semaphore) unlock(identifier string) error {
	entryAny, ok := s.permits.LoadAndDelete(identifier)
	if !ok {
		return nil
	}
	entry := entryAny.(*ctxEntry)

	// Cancel the context
	entry.cancel(ErrLockLost)
	// Stop the refresh goroutine
	close(entry.stop)

	// Unlock the lock
	return s.client.ZRem(entry.ctx, s.key, identifier).Err()
}

func (s *Semaphore) Acquire(ctx context.Context, n int, opts ...TimeoutOption) ([]context.Context, error) {
	ctxs := make([]context.Context, 0, n)
	for i := 0; i < n; i++ {
		newCtx, err := s.Lock(ctx, opts...)
		if err != nil {
			// Release all acquired permits
			_ = s.Release(i)
			return nil, err
		}
		ctxs = append(ctxs, newCtx)
	}
	return ctxs, nil
}

func (s *Semaphore) Release(n int) error {
	for i := 0; i < n; i++ {
		err := s.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
