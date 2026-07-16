package httpapi

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/dispatch/internal/config"
	"github.com/yash/dispatch/internal/ratelimit"
	"github.com/yash/dispatch/internal/store"
)

func TestCreateEventRejectsBadContentType(t *testing.T) {
	s := newSecurityTestServer(t, 1024)
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(`{"event_type":"x","payload":{}}`))
	req.Header.Set("Content-Type", "text/plain")
	req = req.WithContext(contextWithTenant(req.Context(), store.Tenant{ID: uuid.New(), Name: "t"}))

	rec := httptest.NewRecorder()
	s.handleCreateEvent(rec, req)

	assert.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
}

func TestCreateEventRejectsOversizedPayload(t *testing.T) {
	s := newSecurityTestServer(t, 64)
	body := `{"event_type":"x","payload":"` + strings.Repeat("a", 200) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(contextWithTenant(req.Context(), store.Tenant{ID: uuid.New(), Name: "t"}))

	rec := httptest.NewRecorder()
	s.handleCreateEvent(rec, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func newSecurityTestServer(t *testing.T, maxBytes int64) *Server {
	t.Helper()
	cfg := config.Load()
	cfg.MaxPayloadBytes = maxBytes
	cfg.RateLimitPerMinute = 10_000

	// Unreachable Redis → limiter fails open (allow).
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	limiter := ratelimit.New(rdb, cfg.RateLimitPerMinute, cfg.RateLimitWindow, log)

	s := New(cfg, nil, limiter, nil, nil, nil, log)
	require.NotNil(t, s)
	return s
}
