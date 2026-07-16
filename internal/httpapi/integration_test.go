package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash/dispatch/internal/circuitbreaker"
	"github.com/yash/dispatch/internal/config"
	"github.com/yash/dispatch/internal/delivery"
	"github.com/yash/dispatch/internal/httpapi"
	"github.com/yash/dispatch/internal/idempotency"
	"github.com/yash/dispatch/internal/kafka"
	"github.com/yash/dispatch/internal/ratelimit"
	"github.com/yash/dispatch/internal/store"
)

func TestIntegrationDeliveryAndCircuitBreaker(t *testing.T) {
	if os.Getenv("DISPATCH_INTEGRATION") != "1" {
		t.Skip("set DISPATCH_INTEGRATION=1 with Compose up")
	}

	cfg := config.Load()
	cfg.CBFailureThreshold = 3
	cfg.RateLimitPerMinute = 1000
	cfg.RetryBackoff = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}
	cfg.IngestConsumerGroup = "dispatch-ingest-test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cfg.RetryConsumerGroup = "dispatch-retry-test-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Ping(ctx).Err())

	prod, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.IngestTopic, cfg.RetryTopic, slog.Default())
	require.NoError(t, err)
	t.Cleanup(prod.Close)

	st := store.New(pool)
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	limiter := ratelimit.New(rdb, cfg.RateLimitPerMinute, cfg.RateLimitWindow, log)
	idem := idempotency.New(rdb, cfg.IdempotencyTTL)
	cbCfg := circuitbreaker.Config{
		FailureThreshold:  cfg.CBFailureThreshold,
		Cooldown:          cfg.CBCooldown,
		DLQPauseThreshold: cfg.CBDLQPauseThreshold,
	}
	deliv := delivery.New(st, cfg.DeliveryTimeout, cbCfg, log)
	api := httpapi.New(cfg, st, limiter, idem, deliv, prod, log)

	worker := kafka.NewWorker(cfg, st, deliv, prod, log)
	go func() { _ = worker.Run(ctx) }()
	time.Sleep(500 * time.Millisecond) // allow consumer join

	var hits atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		assert.NotEmpty(t, r.Header.Get("X-Dispatch-Signature"))
		assert.NotEmpty(t, r.Header.Get("X-Dispatch-Timestamp"))
		assert.NotEmpty(t, r.Header.Get("X-Dispatch-Event-ID"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.Close)

	failer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(failer.Close)

	tenantBody := `{"name":"integration-test"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants", bytes.NewBufferString(tenantBody))
	req.Header.Set("Content-Type", "application/json")
	api.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	var tenantResp struct {
		APIKey string `json:"api_key"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tenantResp))
	authHeader := "Bearer " + tenantResp.APIKey

	rec = httptest.NewRecorder()
	subBody, _ := json.Marshal(map[string]any{"url": receiver.URL, "event_types": []string{"order.created"}})
	req = httptest.NewRequest(http.MethodPost, "/v1/subscriptions", bytes.NewReader(subBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	api.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	var subResp struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &subResp))

	rec = httptest.NewRecorder()
	evBody := `{"event_type":"order.created","payload":{"order_id":"1"}}`
	req = httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(evBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	api.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)

	require.Eventually(t, func() bool {
		return hits.Load() >= 1
	}, 15*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/subscriptions/"+subResp.ID+"/deliveries", nil)
		req.Header.Set("Authorization", authHeader)
		api.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}
		var deliveries struct {
			Items []map[string]any `json:"items"`
		}
		if json.Unmarshal(rec.Body.Bytes(), &deliveries) != nil {
			return false
		}
		return len(deliveries.Items) > 0
	}, 15*time.Second, 100*time.Millisecond)

	rec = httptest.NewRecorder()
	failSubBody, _ := json.Marshal(map[string]any{"url": failer.URL, "event_types": []string{"order.failed"}})
	req = httptest.NewRequest(http.MethodPost, "/v1/subscriptions", bytes.NewReader(failSubBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	api.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	var failSub struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &failSub))

	for i := 0; i < cfg.CBFailureThreshold; i++ {
		rec = httptest.NewRecorder()
		body := `{"event_type":"order.failed","payload":{"n":` + strconv.Itoa(i) + `}}`
		req = httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", authHeader)
		api.Handler().ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Code, "event %d", i)
	}

	require.Eventually(t, func() bool {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/subscriptions/"+failSub.ID, nil)
		req.Header.Set("Authorization", authHeader)
		api.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return false
		}
		var subState struct {
			State string `json:"state"`
		}
		if json.Unmarshal(rec.Body.Bytes(), &subState) != nil {
			return false
		}
		return subState.State == circuitbreaker.StateDegraded
	}, 30*time.Second, 200*time.Millisecond)
}
