package redislocks

import (
	"context"
	"errors"
	"time"

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
	// On acquiring the permits, Acquire returns new child contexts of ctx that are
	// canceled when the permits are released. Additionally, Acquire starts a
	// goroutine to periodically check and refresh the permits.
	//
	// Release must be called when the permits are no longer necessary to release the
	// permits and stop the refresh goroutine.
	//
	// Acquire(ctx, 1, opts...) is equivalent to Lock(ctx, opts...).
	Acquire(ctx context.Context, n int, opts ...TimeoutOption) ([]context.Context, error)

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

func (l *baseLock) lockInternal(
	ctx context.Context,
	lockFn func(context.Context, string) (bool, error),
	o TimeoutOptions,
) error {
	ctx, cancel := context.WithTimeoutCause(ctx, o.AcquireTimeout, ErrAcquireTimeout)
	defer cancel()

	for attempt := uint64(0); attempt < o.AcquireAttemptsLimit; attempt++ {
		acquired, err := lockFn(ctx, l.key)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) &&
				errors.Is(context.Cause(ctx), ErrAcquireTimeout) {
				return ErrAcquireTimeout
			}
			return err
		}

		if acquired {
			// Acquired lock.
			return nil
		}

		// Lock wasn't acquired, wait before the next attempt.
		timer := time.NewTimer(o.RetryInterval)

		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			err = ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) &&
				errors.Is(context.Cause(ctx), ErrAcquireTimeout) {
				return ErrAcquireTimeout
			}
			return err
		case <-timer.C:
		}
	}
	return ErrAcquireAttemptsLimit
}

// refreshInternal runs while the lock is held and refreshes the lock periodically.
// It stops when the context is canceled, the lock is lost, or the stop channel is closed.
//
// Every o.RefreshInterval, refreshFn is called to refresh the lock.
// If refreshFn returns false, the lock is lost and unlockFn is called to release the lock.
func (l *baseLock) refreshInternal(
	ctx context.Context,
	o TimeoutOptions,
	stopCh <-chan struct{},
	refreshFn func(ctx context.Context, o TimeoutOptions) (bool, error),
	unlockFn func() error,
) {
	if o.RefreshInterval == 0 {
		o.RefreshInterval = time.Duration(refreshCoefficientNum * uint64(o.LockTimeout) / refreshCoefficientDen)
	}
	ticker := time.NewTicker(o.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return // Context canceled, stop refreshing the lock.
		case <-ticker.C:
			refreshed, err := refreshFn(ctx, o)
			if err != nil {
				// Error happened. Try again next time.
				continue
			}
			if !refreshed {
				// Lock lost, release the lock.
				_ = unlockFn()
				return
			}
		case <-stopCh:
			// Lock released, stop refreshing.
			return
		}
	}
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
