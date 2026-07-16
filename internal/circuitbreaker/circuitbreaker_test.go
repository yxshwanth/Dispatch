package circuitbreaker_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/dispatch/internal/circuitbreaker"
)

func TestAllowDelivery(t *testing.T) {
	now := time.Now()
	cooldown := 60 * time.Second

	allow, half := circuitbreaker.AllowDelivery(circuitbreaker.StateActive, now, now, cooldown)
	assert.True(t, allow)
	assert.False(t, half)

	allow, half = circuitbreaker.AllowDelivery(circuitbreaker.StatePaused, now, now, cooldown)
	assert.False(t, allow)
	assert.False(t, half)

	allow, half = circuitbreaker.AllowDelivery(circuitbreaker.StateDegraded, now, now, cooldown)
	assert.False(t, allow)
	assert.False(t, half)

	allow, half = circuitbreaker.AllowDelivery(circuitbreaker.StateDegraded, now.Add(-61*time.Second), now, cooldown)
	assert.True(t, allow)
	assert.True(t, half)
}

func TestNextAfterFailureThreshold(t *testing.T) {
	cfg := circuitbreaker.Config{FailureThreshold: 5, DLQPauseThreshold: 20}

	state, failures, transition, reason := circuitbreaker.NextAfterFailure(
		circuitbreaker.StateActive, false, 4, 0, cfg,
	)
	require.True(t, transition)
	assert.Equal(t, circuitbreaker.StateDegraded, state)
	assert.Equal(t, 5, failures)
	assert.Equal(t, "consecutive_failures_threshold", reason)
}

func TestNextAfterFailureBelowThreshold(t *testing.T) {
	cfg := circuitbreaker.Config{FailureThreshold: 5, DLQPauseThreshold: 20}

	state, failures, transition, _ := circuitbreaker.NextAfterFailure(
		circuitbreaker.StateActive, false, 2, 0, cfg,
	)
	assert.False(t, transition)
	assert.Equal(t, circuitbreaker.StateActive, state)
	assert.Equal(t, 3, failures)
}

func TestHalfOpenProbe(t *testing.T) {
	cfg := circuitbreaker.Config{FailureThreshold: 5, DLQPauseThreshold: 20}

	state, reset, transition, reason := circuitbreaker.NextAfterSuccess(circuitbreaker.StateDegraded, true)
	require.True(t, transition)
	assert.True(t, reset)
	assert.Equal(t, circuitbreaker.StateActive, state)
	assert.Equal(t, "half_open_probe_succeeded", reason)

	state, failures, transition, reason := circuitbreaker.NextAfterFailure(
		circuitbreaker.StateDegraded, true, 5, 0, cfg,
	)
	require.True(t, transition)
	assert.Equal(t, circuitbreaker.StateDegraded, state)
	assert.Equal(t, 6, failures)
	assert.Equal(t, "half_open_probe_failed", reason)
}

func TestPausedOnlyViaDLQThreshold(t *testing.T) {
	cfg := circuitbreaker.Config{FailureThreshold: 5, DLQPauseThreshold: 20}

	state, _, transition, reason := circuitbreaker.NextAfterFailure(
		circuitbreaker.StateDegraded, false, 10, 20, cfg,
	)
	require.True(t, transition)
	assert.Equal(t, circuitbreaker.StatePaused, state)
	assert.Equal(t, "dlq_threshold", reason)
}

func TestPausedStaysPaused(t *testing.T) {
	cfg := circuitbreaker.Config{FailureThreshold: 5, DLQPauseThreshold: 20}
	state, _, transition, _ := circuitbreaker.NextAfterFailure(
		circuitbreaker.StatePaused, false, 100, 50, cfg,
	)
	assert.False(t, transition)
	assert.Equal(t, circuitbreaker.StatePaused, state)
}
