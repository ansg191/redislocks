package redislocks

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

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

	err := m.lockInternal(ctx, func(ctx context.Context, s string) (bool, error) {
		result, err := m.client.SetArgs(ctx, s, m.identifier, redis.SetArgs{
			Mode: "NX",
			TTL:  o.LockTimeout,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return false, err
		}
		return result == "OK", nil
	}, o)
	if err != nil {
		return nil, err
	}

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

func (m *Mutex) refresh(ctx context.Context, o TimeoutOptions) {
	entry := m.entry.Load()
	if entry == nil {
		return
	}

	m.refreshInternal(
		ctx,
		o,
		entry.stop,
		m.tryRefresh,
		m.Unlock,
	)
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

func (m *Mutex) tryRefresh(ctx context.Context, o TimeoutOptions) (bool, error) {
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
