package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter implements a Redis sliding-window log rate limiter.
// Keys: ratelimit:{tenantID} as a sorted set of request timestamps.
type Limiter struct {
	rdb      *redis.Client
	limit    int
	window   time.Duration
	log      *slog.Logger
}

func New(rdb *redis.Client, limit int, window time.Duration, log *slog.Logger) *Limiter {
	if log == nil {
		log = slog.Default()
	}
	return &Limiter{rdb: rdb, limit: limit, window: window, log: log}
}

// Allow returns whether the request is permitted and a Retry-After duration when denied.
// On Redis errors, fail-open (allow=true) and log a warning.
func (l *Limiter) Allow(ctx context.Context, tenantID string) (allow bool, retryAfter time.Duration, err error) {
	now := time.Now()
	windowStart := now.Add(-l.window)
	key := fmt.Sprintf("ratelimit:%s", tenantID)
	member := strconv.FormatInt(now.UnixNano(), 10)

	pipe := l.rdb.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(windowStart.UnixNano(), 10))
	countCmd := pipe.ZCard(ctx, key)
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now.UnixNano()), Member: member})
	pipe.Expire(ctx, key, l.window)
	_, err = pipe.Exec(ctx)
	if err != nil {
		l.log.Warn("rate limiter redis error; failing open", "err", err, "tenant_id", tenantID)
		return true, 0, nil
	}

	count := countCmd.Val()
	// count is before ZAdd in the same pipeline — after rem, before add.
	// So allowed if count < limit.
	if count >= int64(l.limit) {
		// Remove the member we just added since we're denying.
		_ = l.rdb.ZRem(ctx, key, member).Err()
		return false, l.window, nil
	}
	return true, 0, nil
}
