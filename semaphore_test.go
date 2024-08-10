package redislocks

import (
	"context"
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
	//t.Run("lost", func(t *testing.T) {
	//	t.Parallel()
	//	client, i := getClient()
	//	defer returnClient(client, i)
	//
	//	s := NewSemaphore(client, "test", 1, timeoutOptions)
	//
	//	ctx, err := s.Lock(context.Background())
	//	require.NoError(t, err)
	//	assert.NoError(t, ctx.Err())
	//
	//	// Remove the lock to simulate a lost lock
	//	err = client.ZRem(ctx, s.key, s.identifier).Err()
	//	require.NoError(t, err)
	//
	//	time.Sleep(2 * timeoutOptions.RefreshInterval)
	//
	//	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	//	assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	//})
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
	//t.Run("acquire and release", func(t *testing.T) {
	//	t.Parallel()
	//	client, i := getClient()
	//	defer returnClient(client, i)
	//
	//	s1, err := NewSemaphore(client, "test", 2, timeoutOptions)
	//	require.NoError(t, err)
	//	s2, err := NewSemaphore(client, "test", 2, timeoutOptions)
	//
	//	ctx1, err := s.Lock(context.Background())
	//	require.NoError(t, err)
	//	assert.NoError(t, ctx1.Err())
	//	ctx2, err := s.Lock(context.Background())
	//	require.NoError(t, err)
	//	assert.NoError(t, ctx2.Err())
	//
	//	err = s.Unlock()
	//	require.NoError(t, err)
	//
	//})
}
