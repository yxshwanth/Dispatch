package delivery

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/yash/dispatch/internal/circuitbreaker"
	"github.com/yash/dispatch/internal/hmacsign"
	"github.com/yash/dispatch/internal/store"
)

type Deliverer struct {
	client *http.Client
	store  *store.Store
	cb     circuitbreaker.Config
	log    *slog.Logger
}

func New(st *store.Store, timeout time.Duration, cb circuitbreaker.Config, log *slog.Logger) *Deliverer {
	if log == nil {
		log = slog.Default()
	}
	return &Deliverer{
		client: &http.Client{Timeout: timeout},
		store:  st,
		cb:     cb,
		log:    log,
	}
}

type Result struct {
	Success    bool
	StatusCode int
	Err        error
	Latency    time.Duration
	Skipped    bool
	HalfOpen   bool
}

func (d *Deliverer) Deliver(ctx context.Context, sub store.Subscription, eventID uuid.UUID, payload []byte) Result {
	now := time.Now().UTC()
	allow, halfOpen := circuitbreaker.AllowDelivery(sub.State, sub.StateChangedAt, now, d.cb.Cooldown)
	if !allow {
		d.log.Info("delivery skipped by circuit breaker",
			"event_id", eventID,
			"subscription_id", sub.ID,
			"state", sub.State,
		)
		return Result{Skipped: true}
	}

	ts := now
	sig := hmacsign.Sign(sub.Secret, ts, payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(payload))
	if err != nil {
		d.recordFailure(ctx, sub, eventID, halfOpen, nil, err.Error(), 0)
		return Result{Err: err, HalfOpen: halfOpen}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dispatch-Signature", sig)
	req.Header.Set("X-Dispatch-Timestamp", hmacsign.TimestampHeader(ts))
	req.Header.Set("X-Dispatch-Event-ID", eventID.String())

	start := time.Now()
	resp, err := d.client.Do(req)
	latency := time.Since(start)
	latencyMs := int(latency.Milliseconds())

	if err != nil {
		d.recordFailure(ctx, sub, eventID, halfOpen, nil, err.Error(), latencyMs)
		return Result{Err: err, Latency: latency, HalfOpen: halfOpen}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))

	code := resp.StatusCode
	if code >= 200 && code < 300 {
		sc := code
		_, _ = d.store.InsertDeliveryAttempt(ctx, eventID, sub.ID, &sc, nil, &latencyMs)
		if err := d.store.RecordSuccess(ctx, sub.ID, halfOpen); err != nil {
			d.log.Error("record success failed", "err", err, "event_id", eventID, "subscription_id", sub.ID)
		}
		d.log.Info("delivery succeeded",
			"event_id", eventID,
			"subscription_id", sub.ID,
			"status_code", code,
			"latency_ms", latencyMs,
		)
		return Result{Success: true, StatusCode: code, Latency: latency, HalfOpen: halfOpen}
	}

	errMsg := http.StatusText(code)
	d.recordFailure(ctx, sub, eventID, halfOpen, &code, errMsg, latencyMs)
	return Result{StatusCode: code, Latency: latency, HalfOpen: halfOpen}
}

func (d *Deliverer) recordFailure(ctx context.Context, sub store.Subscription, eventID uuid.UUID, halfOpen bool, statusCode *int, errMsg string, latencyMs int) {
	msg := errMsg
	var lm *int
	if latencyMs > 0 {
		lm = &latencyMs
	}
	_, _ = d.store.InsertDeliveryAttempt(ctx, eventID, sub.ID, statusCode, &msg, lm)
	if err := d.store.RecordFailure(ctx, sub.ID, halfOpen, d.cb); err != nil {
		d.log.Error("record failure failed", "err", err, "event_id", eventID, "subscription_id", sub.ID)
	}
	d.log.Info("delivery failed",
		"event_id", eventID,
		"subscription_id", sub.ID,
		"error", errMsg,
		"latency_ms", latencyMs,
	)
}
