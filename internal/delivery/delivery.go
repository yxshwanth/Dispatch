package delivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/yash/dispatch/internal/circuitbreaker"
	"github.com/yash/dispatch/internal/hmacsign"
	"github.com/yash/dispatch/internal/metrics"
	"github.com/yash/dispatch/internal/store"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Deliverer struct {
	client *http.Client
	store  *store.Store
	cb     circuitbreaker.Config
	log    *slog.Logger
	tracer trace.Tracer
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
		tracer: otel.Tracer("dispatch/delivery"),
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
	ctx, span := d.tracer.Start(ctx, "delivery.attempt",
		trace.WithAttributes(
			attribute.String("event_id", eventID.String()),
			attribute.String("subscription_id", sub.ID.String()),
		),
	)
	defer span.End()

	now := time.Now().UTC()
	allow, halfOpen := circuitbreaker.AllowDelivery(sub.State, sub.StateChangedAt, now, d.cb.Cooldown)
	if !allow {
		d.log.Info("delivery skipped by circuit breaker",
			"event_id", eventID,
			"subscription_id", sub.ID,
			"state", sub.State,
		)
		metrics.ObserveDelivery("skipped", 0)
		span.SetAttributes(attribute.Bool("skipped", true))
		return Result{Skipped: true}
	}

	ts := now
	sig := hmacsign.Sign(sub.Secret, ts, payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(payload))
	if err != nil {
		d.recordFailure(ctx, sub, eventID, halfOpen, nil, err.Error(), 0)
		metrics.ObserveDelivery("failure", 0)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
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
		status := "failure"
		if isTimeout(err) {
			status = "timeout"
		}
		d.recordFailure(ctx, sub, eventID, halfOpen, nil, err.Error(), latencyMs)
		metrics.ObserveDelivery(status, latency)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return Result{Err: err, Latency: latency, HalfOpen: halfOpen}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))

	code := resp.StatusCode
	span.SetAttributes(attribute.Int("http.status_code", code))
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
		metrics.ObserveDelivery("success", latency)
		return Result{Success: true, StatusCode: code, Latency: latency, HalfOpen: halfOpen}
	}

	errMsg := http.StatusText(code)
	d.recordFailure(ctx, sub, eventID, halfOpen, &code, errMsg, latencyMs)
	metrics.ObserveDelivery("failure", latency)
	span.SetStatus(codes.Error, errMsg)
	return Result{StatusCode: code, Latency: latency, HalfOpen: halfOpen}
}

func isTimeout(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
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
