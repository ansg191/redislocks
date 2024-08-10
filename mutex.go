package redislocks

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Mutex is a distributed mutual exclusion lock.
type Mutex struct {
	baseLock
	identifier string
	entry      atomic.Pointer[ctxEntry]
}

func NewMutex(client redis.UniversalClient, key string, opts TimeoutOptions) (*Mutex, error) {
	id, err := genIdentifier()
	if err != nil {
		return nil, err
	}
	return &Mutex{
		baseLock{
			client: client,
			key:    key,
			opts:   opts,
		},
		id,
		atomic.Pointer[ctxEntry]{},
	}, nil
}

func (m *Mutex) Lock(ctx context.Context, opts ...TimeoutOption) (context.Context, error) {
	o := m.opts
	for _, opt := range opts {
		opt(&o)
	}

	aCtx, cancel := context.WithTimeoutCause(ctx, m.opts.AcquireTimeout, ErrAcquireTimeout)
	defer cancel()

	for attempt := uint64(0); attempt < m.opts.AcquireAttemptsLimit; attempt++ {
		result, err := m.client.SetArgs(aCtx, m.key, m.identifier, redis.SetArgs{
			Mode: "NX",
			TTL:  m.opts.LockTimeout,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			if errors.Is(err, context.DeadlineExceeded) &&
				errors.Is(context.Cause(aCtx), ErrAcquireTimeout) {
				return nil, ErrAcquireTimeout
			}
			return nil, err
		}
		if result == "OK" {
			// Acquired lock, return context that is canceled when the lock is lost.
			// Starts a goroutine to refresh the lock periodically.
			newCtx, newCancel := context.WithCancelCause(ctx)
			entry := &ctxEntry{
				stop:   make(chan struct{}),
				ctx:    ctx,
				cancel: newCancel,
			}
			m.entry.Store(entry)
			go m.refresh(newCtx, o)
			return newCtx, nil
		}

		// Lock wasn't acquired, wait before the next attempt.
		timer := time.NewTimer(m.opts.RetryInterval)

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

func (m *Mutex) refresh(ctx context.Context, o TimeoutOptions) {
	if o.RefreshInterval == 0 {
		o.RefreshInterval = time.Duration(refreshCoefficientNum * uint64(o.LockTimeout) / refreshCoefficientDen)
	}
	ticker := time.NewTicker(o.RefreshInterval)
	defer ticker.Stop()

	entry := m.entry.Load()
	if entry == nil {
		return
	}
	stopCh := entry.stop

	for {
		select {
		case <-ctx.Done():
			return // Context canceled, stop refreshing the lock.
		case <-ticker.C:
			refreshed, err := m.refreshInternal(ctx, o)
			if err != nil {
				// Error happened. Try again next time.
				continue
			}
			if !refreshed {
				// Lock lost, release the lock.
				_ = m.Unlock()
				return
			}
		case <-stopCh:
			// Lock released, stop refreshing.
			return
		}
	}
}

var expireIfEqualScript = redis.NewScript(`
local key = KEYS[1]
local identifier = ARGV[1]
local lockTimeout = ARGV[2]

local value = redis.call('get', key)

if value == identifier then
	redis.call('pexpire', key, lockTimeout)
	return 1
end

return 0`)

func (m *Mutex) refreshInternal(ctx context.Context, o TimeoutOptions) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, o.RefreshInterval)
	defer cancel()

	result, err := expireIfEqualScript.Run(
		ctx,
		m.client,
		[]string{m.key},
		m.identifier,
		o.LockTimeout.Milliseconds(),
	).Result()
	if err != nil {
		return false, err
	}

	resultInt, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected result type: %T", result)
	}
	return resultInt == 1, nil
}

var delIfEqualScript = redis.NewScript(`
local key = KEYS[1]
local identifier = ARGV[1]

if redis.call('get', key) == identifier then
	return redis.call('del', key)
end

return 0`)

func (m *Mutex) Unlock() error {
	if m.entry.Load() == nil {
		return nil
	}
	entry := m.entry.Swap(nil)
	// Cancel the context
	entry.cancel(ErrLockLost)
	entry.cancel = nil
	// Stop the refresh goroutine
	close(entry.stop)

	// Unlock the lock
	return delIfEqualScript.Run(
		entry.ctx,
		m.client,
		[]string{m.key},
		m.identifier,
	).Err()
}
