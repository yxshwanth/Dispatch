package ratelimit_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/dispatch/internal/ratelimit"
)

func TestSlidingWindowAllowsThenDenies(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	limiter := ratelimit.New(rdb, 3, time.Minute, slog.Default())
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		allow, _, err := limiter.Allow(ctx, "tenant-1")
		require.NoError(t, err)
		assert.True(t, allow, "request %d should allow", i+1)
	}

	allow, retryAfter, err := limiter.Allow(ctx, "tenant-1")
	require.NoError(t, err)
	assert.False(t, allow)
	assert.Equal(t, time.Minute, retryAfter)
}

func TestFailOpenOnRedisDown(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	limiter := ratelimit.New(rdb, 1, time.Minute, slog.Default())
	allow, _, err := limiter.Allow(context.Background(), "tenant-1")
	require.NoError(t, err)
	assert.True(t, allow)
}
