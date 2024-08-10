package redislocks

import (
	"math"
	"time"
)

const RefreshCoefficient = 0.8

const refreshCoefficientNum = uint64(RefreshCoefficient * float64(refreshCoefficientDen))
const refreshCoefficientDen = uint64(10)

type TimeoutOption func(*TimeoutOptions)

type TimeoutOptions struct {
	// LockTimeout is the time without refresh after which the lock is lost.
	LockTimeout time.Duration
	// AcquireTimeout is the time to wait for the lock to be acquired.
	AcquireTimeout time.Duration
	// AcquireAttemptsLimit is the maximum number of attempts to acquire the lock.
	AcquireAttemptsLimit uint64
	// RetryInterval is the time to wait before the next attempt to acquire the lock.
	RetryInterval time.Duration
	// RefreshInterval is the time to wait before the next attempt to refresh the lock.
	// If 0, the refresh interval is calculated as LockTimeout * RefreshCoefficient.
	RefreshInterval time.Duration
}

var DefaultTimeoutOptions = TimeoutOptions{
	LockTimeout:          10 * time.Second,
	AcquireTimeout:       10 * time.Second,
	AcquireAttemptsLimit: math.MaxUint64,
	RetryInterval:        10 * time.Millisecond,
	RefreshInterval:      0,
}

func WithLockTimeout(timeout time.Duration) TimeoutOption {
	return func(o *TimeoutOptions) {
		o.LockTimeout = timeout
	}
}

func WithAcquireTimeout(timeout time.Duration) TimeoutOption {
	return func(o *TimeoutOptions) {
		o.AcquireTimeout = timeout
	}
}

func WithAcquireAttemptsLimit(limit uint64) TimeoutOption {
	return func(o *TimeoutOptions) {
		o.AcquireAttemptsLimit = limit
	}
}

func WithRetryInterval(interval time.Duration) TimeoutOption {
	return func(o *TimeoutOptions) {
		o.RetryInterval = interval
	}
}

func WithRefreshInterval(interval time.Duration) TimeoutOption {
	return func(o *TimeoutOptions) {
		o.RefreshInterval = interval
	}
}
