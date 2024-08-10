package redislocks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutex(t *testing.T) {
	t.Parallel()
	t.Run("acquire and release", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		m, err := NewMutex(client, "test", timeoutOptions)
		require.NoError(t, err)

		ctx, err := m.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		err = m.Unlock()
		require.NoError(t, err)

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})
	t.Run("contention", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		m1, err := NewMutex(client, "test", timeoutOptions)
		require.NoError(t, err)
		m2, err := NewMutex(client, "test", timeoutOptions)
		require.NoError(t, err)

		ctx1, err := m1.Lock(context.Background())
		require.NoError(t, err)
		ctx2, err := m2.Lock(context.Background())
		assert.ErrorIs(t, err, ErrAcquireTimeout)
		assert.Nil(t, ctx2)

		err = m1.Unlock()
		require.NoError(t, err)

		assert.ErrorIs(t, ctx1.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx1), ErrLockLost)
	})
	t.Run("refresh", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		m, err := NewMutex(client, "test", timeoutOptions)
		require.NoError(t, err)

		ctx, err := m.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		time.Sleep(2 * timeoutOptions.RefreshInterval)

		assert.NoError(t, ctx.Err())

		err = m.Unlock()
		require.NoError(t, err)
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})
	t.Run("lost", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		m1, err := NewMutex(client, "test", timeoutOptions)
		require.NoError(t, err)

		ctx, err := m1.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		// Change the lock identifier to simulate a lost lock.
		err = client.Set(context.Background(), "test", "other", 0).Err()
		require.NoError(t, err)

		time.Sleep(2 * timeoutOptions.RefreshInterval)

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})
	t.Run("lost by expiry", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		m1, err := NewMutex(client, "test", timeoutOptions)
		require.NoError(t, err)

		ctx, err := m1.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		// Change the lock identifier to simulate a lost lock.
		err = client.Del(context.Background(), "test").Err()
		require.NoError(t, err)

		time.Sleep(2 * timeoutOptions.RefreshInterval)

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})
	t.Run("reusable acquire and release", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		m, err := NewMutex(client, "test", timeoutOptions)
		require.NoError(t, err)

		ctx, err := m.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		err = m.Unlock()
		require.NoError(t, err)

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)

		ctx, err = m.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx.Err())

		err = m.Unlock()
		require.NoError(t, err)

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrLockLost)
	})
	t.Run("reentrant", func(t *testing.T) {
		t.Parallel()
		client, i := getClient()
		defer returnClient(client, i)

		m, err := NewMutex(client, "test", timeoutOptions)
		require.NoError(t, err)

		ctx1, err := m.Lock(context.Background())
		require.NoError(t, err)
		assert.NoError(t, ctx1.Err())

		ctx2, err := m.Lock(ctx1)
		assert.ErrorIs(t, err, ErrAcquireTimeout)
		assert.Nil(t, ctx2)

		err = m.Unlock()
		require.NoError(t, err)
	})
}
