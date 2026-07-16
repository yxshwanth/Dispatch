package kafka_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/dispatch/internal/kafka"
)

func TestNextBackoff(t *testing.T) {
	schedule := []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}

	d, exhausted := kafka.NextBackoff(schedule, 1)
	require.False(t, exhausted)
	assert.Equal(t, 10*time.Second, d)

	d, exhausted = kafka.NextBackoff(schedule, 5)
	require.False(t, exhausted)
	assert.Equal(t, 15*time.Minute, d)

	_, exhausted = kafka.NextBackoff(schedule, 6)
	assert.True(t, exhausted)
}

func TestEncodeDecodeEvent(t *testing.T) {
	msg := kafka.EventMessage{
		EventID:   uuid.New(),
		TenantID:  uuid.New(),
		EventType: "order.created",
		Payload:   []byte(`{"id":1}`),
	}
	b, err := kafka.EncodeEvent(msg)
	require.NoError(t, err)
	got, err := kafka.DecodeEvent(b)
	require.NoError(t, err)
	assert.Equal(t, msg.EventID, got.EventID)
	assert.Equal(t, msg.EventType, got.EventType)
}

func TestEncodeDecodeRetry(t *testing.T) {
	msg := kafka.RetryMessage{
		EventID:        uuid.New(),
		TenantID:       uuid.New(),
		SubscriptionID: uuid.New(),
		EventType:      "order.created",
		Payload:        []byte(`{}`),
		AttemptNumber:  2,
		RetryAfterUnix: time.Now().Unix(),
	}
	b, err := kafka.EncodeRetry(msg)
	require.NoError(t, err)
	got, err := kafka.DecodeRetry(b)
	require.NoError(t, err)
	assert.Equal(t, msg.SubscriptionID, got.SubscriptionID)
	assert.Equal(t, 2, got.AttemptNumber)
}
