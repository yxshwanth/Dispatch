package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/yash/dispatch/internal/circuitbreaker"
	"github.com/yash/dispatch/internal/store"
)

// RefreshCircuitBreakerGauges periodically sets CB state gauges from Postgres.
func RefreshCircuitBreakerGauges(ctx context.Context, st *store.Store, interval time.Duration, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	refresh := func() {
		counts, err := st.CountSubscriptionsByState(ctx)
		if err != nil {
			log.Warn("cb gauge refresh failed", "err", err)
			return
		}
		CircuitBreakerState.WithLabelValues(circuitbreaker.StateActive).Set(float64(counts[circuitbreaker.StateActive]))
		CircuitBreakerState.WithLabelValues(circuitbreaker.StateDegraded).Set(float64(counts[circuitbreaker.StateDegraded]))
		CircuitBreakerState.WithLabelValues(circuitbreaker.StatePaused).Set(float64(counts[circuitbreaker.StatePaused]))
	}
	refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			refresh()
		}
	}
}
