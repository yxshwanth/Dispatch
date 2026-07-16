package circuitbreaker

import "time"

const (
	StateActive   = "active"
	StateDegraded = "degraded"
	StatePaused   = "paused"
)

type Config struct {
	FailureThreshold  int
	Cooldown          time.Duration
	DLQPauseThreshold int
}

// AllowDelivery decides whether a delivery attempt should proceed.
// For degraded subscriptions past cooldown, allow=true means half-open probe.
func AllowDelivery(state string, stateChangedAt time.Time, now time.Time, cooldown time.Duration) (allow bool, halfOpen bool) {
	switch state {
	case StateActive:
		return true, false
	case StateDegraded:
		if now.Sub(stateChangedAt) >= cooldown {
			return true, true
		}
		return false, false
	case StatePaused:
		return false, false
	default:
		return false, false
	}
}

// NextAfterSuccess returns the state after a successful delivery.
func NextAfterSuccess(state string, halfOpen bool) (newState string, resetFailures bool, transition bool, reason string) {
	if state == StateDegraded && halfOpen {
		return StateActive, true, true, "half_open_probe_succeeded"
	}
	if state == StateActive {
		return StateActive, true, false, ""
	}
	// paused stays paused; degraded without probe shouldn't get success deliveries
	return state, false, false, ""
}

// NextAfterFailure returns updated consecutive failure count and any transition.
func NextAfterFailure(
	state string,
	halfOpen bool,
	consecutiveFailures int,
	dlqCount int,
	cfg Config,
) (newState string, newFailures int, transition bool, reason string) {
	if state == StatePaused {
		return StatePaused, consecutiveFailures, false, ""
	}

	if state == StateDegraded && halfOpen {
		return StateDegraded, consecutiveFailures + 1, true, "half_open_probe_failed"
	}

	newFailures = consecutiveFailures + 1

	if state == StateActive && newFailures >= cfg.FailureThreshold {
		return StateDegraded, newFailures, true, "consecutive_failures_threshold"
	}

	if dlqCount >= cfg.DLQPauseThreshold && state != StatePaused {
		return StatePaused, newFailures, true, "dlq_threshold"
	}

	return state, newFailures, false, ""
}
