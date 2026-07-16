package hmacsign

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// Sign returns hex(HMAC-SHA256(secret, "timestamp.payload")).
func Sign(secret string, timestamp time.Time, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "%d.", timestamp.Unix())
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify checks signature against secret. Returns true on match.
func Verify(secret string, timestamp time.Time, payload []byte, signature string) bool {
	expected := Sign(secret, timestamp, payload)
	return hmac.Equal([]byte(expected), []byte(signature))
}

// VerifyWithRotation tries current secret, then previous if grace is still active.
func VerifyWithRotation(
	secret string,
	previousSecret *string,
	previousExpiresAt *time.Time,
	timestamp time.Time,
	payload []byte,
	signature string,
	now time.Time,
) bool {
	if Verify(secret, timestamp, payload, signature) {
		return true
	}
	if previousSecret == nil || previousExpiresAt == nil {
		return false
	}
	if now.After(*previousExpiresAt) {
		return false
	}
	return Verify(*previousSecret, timestamp, payload, signature)
}

// TimestampHeader formats the Unix timestamp for X-Dispatch-Timestamp.
func TimestampHeader(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}

// DefaultReplayWindow is the recommended max age for accepting signed payloads.
const DefaultReplayWindow = 5 * time.Minute

// VerifyFresh verifies the signature and rejects timestamps older than maxAge
// (or newer than a small clock-skew allowance of maxAge into the future).
func VerifyFresh(secret string, timestamp time.Time, payload []byte, signature string, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 {
		maxAge = DefaultReplayWindow
	}
	age := now.Sub(timestamp)
	if age > maxAge || age < -maxAge {
		return false
	}
	return Verify(secret, timestamp, payload, signature)
}
