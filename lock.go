package redislocks

import (
	"context"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Locker interface {
	// Lock acquires the lock. Lock blocks until the lock is acquired, the context is
	// canceled, the operation times out, or an error occurs.
	//
	// On acquiring the lock, Lock returns a new child context of ctx that is
	// canceled when the lock is lost/released. Additionally, Lock starts a goroutine
	// to periodically check and refresh the lock.
	//
	// Unlock must be called when the lock is no longer necessary to release
	// the lock and stop the refresh goroutine.
	Lock(ctx context.Context, opts ...TimeoutOption) (context.Context, error)

	// Unlock releases the lock.
	//
	// Unlock stops the refresh goroutine and releases the lock. It returns an error
	// if the redis command fails. If the lock was already released, lost, or never
	// acquired, Unlock is a no-op and returns nil.
	Unlock() error
}

type SemaphoreLocker interface {
	Locker

	// Acquire acquires n permits. Acquire blocks until n permits are acquired, the
	// context is canceled, the operation times out, or an error occurs.
	//
	// On acquiring the permits, Acquire returns a new child context of ctx that is
	// canceled when any of the permits are released. Additionally, Acquire starts a
	// goroutine to periodically check and refresh the permits.
	//
	// Release must be called when the permits are no longer necessary to release the
	// permits and stop the refresh goroutine.
	//
	// Acquire(ctx, 1, opts...) is equivalent to Lock(ctx, opts...).
	Acquire(ctx context.Context, n int, opts ...TimeoutOption) (context.Context, error)

	// Release releases n permits.
	//
	// Release stops the refresh goroutine and releases n permits. It returns an
	// error if the redis command fails. If the permits were already released, lost,
	// or never acquired, Release is a no-op and returns nil.
	//
	// Passing n greater than the number of acquired permits is undefined
	// behavior.
	Release(n int) error
}

type baseLock struct {
	client redis.UniversalClient
	key    string
	opts   TimeoutOptions
}

type ctxEntry struct {
	stop   chan struct{}
	ctx    context.Context
	cancel context.CancelCauseFunc
}

func genIdentifier() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
