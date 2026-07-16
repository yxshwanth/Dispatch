package kafka

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const (
	HeaderEventID         = "event_id"
	HeaderAttemptNumber   = "attempt_number"
	HeaderRetryAfter      = "retry_after"
	HeaderSubscriptionID  = "subscription_id"
)

// EventMessage is produced to the ingest topic.
type EventMessage struct {
	EventID   uuid.UUID       `json:"event_id"`
	TenantID  uuid.UUID       `json:"tenant_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

// RetryMessage is produced to the retry topic for a single subscription.
type RetryMessage struct {
	EventID        uuid.UUID       `json:"event_id"`
	TenantID       uuid.UUID       `json:"tenant_id"`
	SubscriptionID uuid.UUID       `json:"subscription_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	AttemptNumber  int             `json:"attempt_number"`
	RetryAfterUnix int64           `json:"retry_after"`
	LastError      string          `json:"last_error,omitempty"`
}

func EncodeEvent(m EventMessage) ([]byte, error) {
	return json.Marshal(m)
}

func DecodeEvent(b []byte) (EventMessage, error) {
	var m EventMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return EventMessage{}, err
	}
	if m.EventID == uuid.Nil || m.TenantID == uuid.Nil {
		return EventMessage{}, fmt.Errorf("invalid event message: missing ids")
	}
	return m, nil
}

func EncodeRetry(m RetryMessage) ([]byte, error) {
	return json.Marshal(m)
}

func DecodeRetry(b []byte) (RetryMessage, error) {
	var m RetryMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return RetryMessage{}, err
	}
	if m.EventID == uuid.Nil || m.SubscriptionID == uuid.Nil {
		return RetryMessage{}, fmt.Errorf("invalid retry message: missing ids")
	}
	return m, nil
}

// NextBackoff returns the delay before the next attempt (1-indexed attempt after failure).
// attemptNumber is the attempt that just failed (1 = first delivery failed).
func NextBackoff(schedule []time.Duration, attemptNumber int) (delay time.Duration, exhausted bool) {
	if attemptNumber <= 0 {
		attemptNumber = 1
	}
	idx := attemptNumber - 1
	if idx >= len(schedule) {
		return 0, true
	}
	return schedule[idx], false
}

func RetryAfterUnix(now time.Time, delay time.Duration) int64 {
	return now.Add(delay).Unix()
}

func ParseAttemptHeader(v string) (int, error) {
	return strconv.Atoi(v)
}

func ParseRetryAfterHeader(v string) (int64, error) {
	return strconv.ParseInt(v, 10, 64)
}
