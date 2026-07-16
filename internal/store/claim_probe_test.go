package store_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/dispatch/internal/auth"
	"github.com/yash/dispatch/internal/store"
)

func TestClaimProbeSingleFlight(t *testing.T) {
	if os.Getenv("DISPATCH_INTEGRATION") != "1" {
		t.Skip("set DISPATCH_INTEGRATION=1 with Compose Postgres up")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, envOr("DATABASE_URL", "postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable"))
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	st := store.New(pool)
	tenant, err := st.CreateTenant(ctx, "claim-probe", auth.HashAPIKey("k-"+t.Name()))
	require.NoError(t, err)

	sub, err := st.CreateSubscription(ctx, tenant.ID, "http://127.0.0.1/hook", []string{"t"}, "secret")
	require.NoError(t, err)

	now := time.Now().UTC()
	cooldown := time.Minute
	// Force degraded with cooldown already elapsed.
	_, err = pool.Exec(ctx, `
		UPDATE subscriptions
		SET state = 'degraded', consecutive_failures = 5, state_changed_at = $1
		WHERE id = $2
	`, now.Add(-2*cooldown), sub.ID)
	require.NoError(t, err)

	var claimed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := st.ClaimProbe(ctx, sub.ID, now, cooldown)
			require.NoError(t, err)
			if ok {
				claimed.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), claimed.Load(), "exactly one goroutine must claim the probe")

	// Second wave: cooldown restarted by claim → nobody should claim yet.
	ok, err := st.ClaimProbe(ctx, sub.ID, now.Add(time.Second), cooldown)
	require.NoError(t, err)
	assert.False(t, ok)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
