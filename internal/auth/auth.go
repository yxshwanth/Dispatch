package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// HashAPIKey returns the SHA-256 hex digest of the plaintext API key.
func HashAPIKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// NewAPIKey generates a random API key (32 bytes, hex-encoded).
func NewAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// NewHMACSecret generates a random HMAC secret (32 bytes, hex-encoded).
func NewHMACSecret() (string, error) {
	return NewAPIKey()
}
