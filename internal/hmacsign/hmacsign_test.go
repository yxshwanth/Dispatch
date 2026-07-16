package hmacsign_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/dispatch/internal/hmacsign"
)

func TestSignAndVerify(t *testing.T) {
	secret := "test-secret"
	ts := time.Unix(1700000000, 0).UTC()
	payload := []byte(`{"hello":"world"}`)

	sig := hmacsign.Sign(secret, ts, payload)
	require.NotEmpty(t, sig)
	assert.True(t, hmacsign.Verify(secret, ts, payload, sig))
	assert.False(t, hmacsign.Verify(secret, ts, []byte(`{"hello":"nope"}`), sig))
	assert.False(t, hmacsign.Verify("other", ts, payload, sig))
}

func TestSignIncludesTimestamp(t *testing.T) {
	secret := "test-secret"
	payload := []byte(`{}`)
	ts1 := time.Unix(1700000000, 0).UTC()
	ts2 := time.Unix(1700000001, 0).UTC()

	sig1 := hmacsign.Sign(secret, ts1, payload)
	sig2 := hmacsign.Sign(secret, ts2, payload)
	assert.NotEqual(t, sig1, sig2)
}

func TestVerifyWithRotation(t *testing.T) {
	current := "current-secret"
	previous := "previous-secret"
	ts := time.Unix(1700000000, 0).UTC()
	payload := []byte(`{"a":1}`)
	now := time.Unix(1700000100, 0).UTC()
	expires := now.Add(time.Hour)
	expired := now.Add(-time.Minute)

	sigPrev := hmacsign.Sign(previous, ts, payload)
	assert.True(t, hmacsign.VerifyWithRotation(current, &previous, &expires, ts, payload, sigPrev, now))
	assert.False(t, hmacsign.VerifyWithRotation(current, &previous, &expired, ts, payload, sigPrev, now))

	sigCur := hmacsign.Sign(current, ts, payload)
	assert.True(t, hmacsign.VerifyWithRotation(current, &previous, &expires, ts, payload, sigCur, now))
}
