package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(rdb *redis.Client, ttl time.Duration) *Store {
	return &Store{rdb: rdb, ttl: ttl}
}

func key(tenantID, idemKey string) string {
	return fmt.Sprintf("idempotency:%s:%s", tenantID, idemKey)
}

// Reserve tries to claim an idempotency key. If already present, returns the
// existing event ID and exists=true. On Redis error, returns err (caller may
// fall through to Postgres unique index).
func (s *Store) Reserve(ctx context.Context, tenantID, idemKey, eventID string) (existingEventID string, exists bool, err error) {
	k := key(tenantID, idemKey)
	ok, err := s.rdb.SetNX(ctx, k, eventID, s.ttl).Result()
	if err != nil {
		return "", false, err
	}
	if ok {
		return "", false, nil
	}
	existing, err := s.rdb.Get(ctx, k).Result()
	if err != nil {
		return "", false, err
	}
	return existing, true, nil
}

// Set stores the mapping (used when Redis was down during reserve and PG won).
func (s *Store) Set(ctx context.Context, tenantID, idemKey, eventID string) error {
	return s.rdb.Set(ctx, key(tenantID, idemKey), eventID, s.ttl).Err()
}
