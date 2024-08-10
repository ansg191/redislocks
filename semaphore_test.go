package redislocks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSemaphore_Binary tests the Semaphore as a binary semaphore.
func TestSemaphore_Binary(t *testing.T) {
	t.Parallel()
	t.Run("acquire and release", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		s := NewSemaphore(client, "test", 1, timeoutOptions)

		ctx, err := s.Lock(context.Background())
		require.NoError(t, err)
		require.NoError(t, ctx.Err())

		err = s.Unlock()
		require.NoError(t, err)

		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})
	t.Run("contention", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		s1 := NewSemaphore(client, "test", 1, timeoutOptions)
		s2 := NewSemaphore(client, "test", 1, timeoutOptions)

		ctx1, err := s1.Lock(context.Background())
		require.NoError(t, err)
		ctx2, err := s2.Lock(context.Background())
		assert.ErrorIs(t, err, ErrAcquireTimeout)
		assert.Nil(t, ctx2)

		err = s1.Unlock()
		require.NoError(t, err)

		require.ErrorIs(t, ctx1.Err(), context.Canceled)
		require.ErrorIs(t, context.Cause(ctx1), ErrLockLost)
	})
	t.Run("refresh", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		s := NewSemaphore(client, "test", 1, timeoutOptions)

		ctx, err := s.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		time.Sleep(2 * timeoutOptions.RefreshInterval)

		assert.NoError(t, ctx.Err())

		err = s.Unlock()
		require.NoError(t, err)
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})
	t.Run("lost", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		s := NewSemaphore(client, "test", 1, timeoutOptions)

		ctx, err := s.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		var identifier string
		s.permits.Range(func(key, _ any) bool {
			identifier = key.(string)
			return false
		})

		// Remove the lock to simulate a lost lock
		err = client.ZRem(ctx, s.key, identifier).Err()
		require.NoError(t, err)

		time.Sleep(2 * timeoutOptions.RefreshInterval)

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})
	t.Run("reusable acquire and release", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		s := NewSemaphore(client, "test", 1, timeoutOptions)

		ctx, err := s.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		err = s.Unlock()
		require.NoError(t, err)

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)

		ctx, err = s.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		err = s.Unlock()
		require.NoError(t, err)

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})

	t.Run("reentrant", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		s := NewSemaphore(client, "test", 1, timeoutOptions)

		ctx1, err := s.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx1.Err())

		ctx2, err := s.Lock(ctx1)
		assert.ErrorIs(t, err, ErrAcquireTimeout)
		assert.Nil(t, ctx2)

		err = s.Unlock()
		require.NoError(t, err)
	})
}

func TestSemaphore(t *testing.T) {
	t.Parallel()
	t.Run("acquire and release", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		s := NewSemaphore(client, "test", 2, timeoutOptions)

		ctxs, err := s.Acquire(context.Background(), 2)
		require.NoError(t, err)
		assert.Len(t, ctxs, 2)
		assert.NoError(t, ctxs[0].Err())
		assert.NoError(t, ctxs[1].Err())

		// Check that internal map has 2 permits
		var length int64 = 0
		s.permits.Range(func(_, _ any) bool {
			length++
			return true
		})
		assert.Equal(t, int64(2), length)

		// Check that sorted set has 2 permits
		length, err = client.ZCard(context.Background(), s.key).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(2), length)

		err = s.Release(1)
		require.NoError(t, err)

		// Assert that 1 context is canceled and 1 is not.
		assert.True(t, errors.Is(ctxs[0].Err(), context.Canceled) || errors.Is(ctxs[1].Err(), context.Canceled))
		assert.False(t, errors.Is(ctxs[0].Err(), context.Canceled) && errors.Is(ctxs[1].Err(), context.Canceled))
		assert.True(t, errors.Is(context.Cause(ctxs[0]), ErrLockLost) || errors.Is(context.Cause(ctxs[1]), ErrLockLost))
		assert.False(t, errors.Is(context.Cause(ctxs[0]), ErrLockLost) && errors.Is(context.Cause(ctxs[1]), ErrLockLost))

		// Check that internal map has 1 permit
		length = 0
		s.permits.Range(func(_, _ any) bool {
			length++
			return true
		})
		assert.Equal(t, int64(1), length)

		// Check that sorted set has 1 permit
		length, err = client.ZCard(context.Background(), s.key).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), length)

		err = s.Release(1)
		require.NoError(t, err)

		// Assert that both contexts are canceled.
		assert.ErrorIs(t, ctxs[0].Err(), context.Canceled)
		assert.ErrorIs(t, ctxs[1].Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctxs[0]), ErrLockLost)
		assert.ErrorIs(t, context.Cause(ctxs[1]), ErrLockLost)

		// Check that internal map has 0 permits
		length = 0
		s.permits.Range(func(_, _ any) bool {
			length++
			return true
		})
		assert.Equal(t, int64(0), length)

		// Check that sorted set has 0 permits
		length, err = client.ZCard(context.Background(), s.key).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), length)
	})
}
