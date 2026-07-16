package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yash/dispatch/internal/circuitbreaker"
	"github.com/yash/dispatch/internal/config"
	"github.com/yash/dispatch/internal/delivery"
	"github.com/yash/dispatch/internal/metrics"
	"github.com/yash/dispatch/internal/store"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Worker struct {
	cfg    config.Config
	store  *store.Store
	deliv  *delivery.Deliverer
	prod   *Producer
	log    *slog.Logger
	cb     circuitbreaker.Config
	tracer trace.Tracer
}

func NewWorker(cfg config.Config, st *store.Store, deliv *delivery.Deliverer, prod *Producer, log *slog.Logger) *Worker {
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		cfg:   cfg,
		store: st,
		deliv: deliv,
		prod:  prod,
		log:   log,
		cb: circuitbreaker.Config{
			FailureThreshold:  cfg.CBFailureThreshold,
			Cooldown:          cfg.CBCooldown,
			DLQPauseThreshold: cfg.CBDLQPauseThreshold,
		},
		tracer: otel.Tracer("dispatch/kafka"),
	}
}

// Run starts ingest and/or retry consumers based on CONSUMER_MODE until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	mode := w.cfg.ConsumerMode
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	start := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.log.Info("consumer starting", "mode", name)
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				errCh <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}

	switch mode {
	case "ingest":
		start("ingest", w.runIngest)
	case "retry":
		start("retry", w.runRetry)
	default: // all
		start("ingest", w.runIngest)
		start("retry", w.runRetry)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		<-done
		return nil
	case err := <-errCh:
		return err
	case <-done:
		return nil
	}
}

func (w *Worker) runIngest(ctx context.Context) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(w.cfg.KafkaBrokers...),
		kgo.ConsumerGroup(w.cfg.IngestConsumerGroup),
		kgo.ConsumeTopics(w.cfg.IngestTopic),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
			w.log.Info("ingest partitions revoked", "partitions", revoked)
			// Finish in-flight work before release: PollFetches is blocked by our process loop;
			// CommitOffsetsSync here flushes any completed work.
			if err := c.CommitUncommittedOffsets(ctx); err != nil {
				w.log.Error("commit on revoke failed", "err", err)
			}
		}),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.Canceled || e.Err == context.DeadlineExceeded {
					return nil
				}
				w.log.Error("ingest fetch error", "err", e.Err, "topic", e.Topic, "partition", e.Partition)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if err := w.processIngest(ctx, r); err != nil {
				w.log.Error("ingest process failed", "err", err)
				return
			}
			if err := cl.CommitRecords(ctx, r); err != nil {
				w.log.Error("ingest commit failed", "err", err)
			}
		})
	}
}

func (w *Worker) processIngest(ctx context.Context, r *kgo.Record) error {
	headers := r.Headers
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &headers})
	ctx, span := w.tracer.Start(ctx, "kafka.consume.ingest",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	msg, err := DecodeEvent(r.Value)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.String("event_id", msg.EventID.String()))
	log := w.log.With("event_id", msg.EventID, "tenant_id", msg.TenantID)
	log.Info("processing ingest message")

	subs, err := w.store.MatchingSubscriptions(ctx, msg.TenantID, msg.EventType)
	if err != nil {
		span.RecordError(err)
		return err
	}
	for _, sub := range subs {
		res := w.deliv.Deliver(ctx, sub, msg.EventID, msg.Payload)
		if res.Skipped || res.Success {
			continue
		}
		if err := w.enqueueRetry(ctx, msg, sub.ID, 1, errString(res)); err != nil {
			log.Error("enqueue retry failed", "err", err, "subscription_id", sub.ID)
		}
	}
	return nil
}

func (w *Worker) runRetry(ctx context.Context) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(w.cfg.KafkaBrokers...),
		kgo.ConsumerGroup(w.cfg.RetryConsumerGroup),
		kgo.ConsumeTopics(w.cfg.RetryTopic),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
			w.log.Info("retry partitions revoked", "partitions", revoked)
			if err := c.CommitUncommittedOffsets(ctx); err != nil {
				w.log.Error("retry commit on revoke failed", "err", err)
			}
		}),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.Canceled || e.Err == context.DeadlineExceeded {
					return nil
				}
				w.log.Error("retry fetch error", "err", e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if err := w.processRetry(ctx, r); err != nil {
				w.log.Error("retry process failed", "err", err)
				return
			}
			if err := cl.CommitRecords(ctx, r); err != nil {
				w.log.Error("retry commit failed", "err", err)
			}
		})
	}
}

func (w *Worker) processRetry(ctx context.Context, r *kgo.Record) error {
	headers := r.Headers
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &headers})
	ctx, span := w.tracer.Start(ctx, "kafka.consume.retry",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	msg, err := DecodeRetry(r.Value)
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(
		attribute.String("event_id", msg.EventID.String()),
		attribute.Int("attempt", msg.AttemptNumber),
	)
	log := w.log.With("event_id", msg.EventID, "subscription_id", msg.SubscriptionID, "attempt", msg.AttemptNumber)

	now := time.Now().Unix()
	if msg.RetryAfterUnix > now {
		wait := time.Until(time.Unix(msg.RetryAfterUnix, 0))
		log.Info("waiting for retry_after", "wait", wait)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	sub, err := w.store.GetSubscriptionByID(ctx, msg.SubscriptionID)
	if err != nil {
		span.RecordError(err)
		return err
	}

	res := w.deliv.Deliver(ctx, sub, msg.EventID, msg.Payload)
	if res.Skipped || res.Success {
		log.Info("retry delivery done", "skipped", res.Skipped, "success", res.Success)
		return nil
	}

	nextAttempt := msg.AttemptNumber + 1
	_, exhausted := NextBackoff(w.cfg.RetryBackoff, nextAttempt)
	if exhausted {
		errMsg := errString(res)
		if err := w.store.InsertDeadLetter(ctx, msg.EventID, msg.SubscriptionID, msg.AttemptNumber, errMsg, w.cb); err != nil {
			return err
		}
		metrics.DeadLetters.Inc()
		log.Info("moved to dead letters", "attempts", msg.AttemptNumber)
		return nil
	}

	return w.enqueueRetry(ctx, EventMessage{
		EventID:   msg.EventID,
		TenantID:  msg.TenantID,
		EventType: msg.EventType,
		Payload:   msg.Payload,
	}, msg.SubscriptionID, nextAttempt, errString(res))
}

func (w *Worker) enqueueRetry(ctx context.Context, ev EventMessage, subID uuid.UUID, attempt int, lastErr string) error {
	delay, exhausted := NextBackoff(w.cfg.RetryBackoff, attempt)
	if exhausted {
		if err := w.store.InsertDeadLetter(ctx, ev.EventID, subID, attempt-1, lastErr, w.cb); err != nil {
			return err
		}
		metrics.DeadLetters.Inc()
		return nil
	}
	return w.prod.ProduceRetry(ctx, RetryMessage{
		EventID:        ev.EventID,
		TenantID:       ev.TenantID,
		SubscriptionID: subID,
		EventType:      ev.EventType,
		Payload:        ev.Payload,
		AttemptNumber:  attempt,
		RetryAfterUnix: RetryAfterUnix(time.Now().UTC(), delay),
		LastError:      lastErr,
	})
}

func errString(res delivery.Result) string {
	if res.Err != nil {
		return res.Err.Error()
	}
	if res.StatusCode > 0 {
		return fmt.Sprintf("HTTP %d", res.StatusCode)
	}
	return "delivery failed"
}