package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/yash/dispatch/internal/idempotency"
	kafkapkg "github.com/yash/dispatch/internal/kafka"
	"github.com/yash/dispatch/internal/metrics"
	"github.com/yash/dispatch/internal/store"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Service is the shared ingestion path used by HTTP and gRPC adapters.
type Service struct {
	store *store.Store
	idem  *idempotency.Store
	prod  *kafkapkg.Producer
	log   *slog.Logger
	tracer trace.Tracer
}

func New(st *store.Store, idem *idempotency.Store, prod *kafkapkg.Producer, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		store:  st,
		idem:   idem,
		prod:   prod,
		log:    log,
		tracer: otel.Tracer("dispatch/ingest"),
	}
}

// Result is returned from CreateEvent.
type Result struct {
	Event    store.Event
	Replayed bool
}

// CreateEvent validates payload JSON, applies idempotency, persists, and produces to Kafka.
// Callers are responsible for auth and transport-level limits (rate limit, body size, Content-Type).
func (s *Service) CreateEvent(ctx context.Context, tenantID uuid.UUID, eventType string, payload []byte, idempotencyKey string) (Result, error) {
	ctx, span := s.tracer.Start(ctx, "ingest.CreateEvent")
	defer span.End()

	if eventType == "" {
		return Result{}, fmt.Errorf("%w: event_type is required", ErrInvalid)
	}
	if len(payload) == 0 || !json.Valid(payload) {
		return Result{}, fmt.Errorf("%w: invalid payload JSON", ErrInvalid)
	}

	var idemKey *string
	eventID := uuid.New()
	if idempotencyKey != "" {
		idemKey = &idempotencyKey
		existingID, exists, err := s.idem.Reserve(ctx, tenantID.String(), idempotencyKey, eventID.String())
		if err == nil && exists && existingID != "" {
			id, parseErr := uuid.Parse(existingID)
			if parseErr != nil {
				return Result{}, parseErr
			}
			ev, err := s.store.GetEvent(ctx, id)
			if err != nil {
				// Redis had the id; return a minimal replay result without PG row if missing.
				return Result{Event: store.Event{ID: id, TenantID: tenantID, EventType: eventType}, Replayed: true}, nil
			}
			span.SetAttributes(attribute.String("event_id", ev.ID.String()), attribute.Bool("replayed", true))
			return Result{Event: ev, Replayed: true}, nil
		}
	}

	ev, err := s.store.CreateEvent(ctx, eventID, tenantID, eventType, payload, idemKey)
	if err != nil {
		if errors.Is(err, store.ErrConflict) && idemKey != nil {
			existing, err2 := s.store.EventByIdempotencyKey(ctx, tenantID, *idemKey)
			if err2 == nil {
				_ = s.idem.Set(ctx, tenantID.String(), *idemKey, existing.ID.String())
				span.SetAttributes(attribute.String("event_id", existing.ID.String()), attribute.Bool("replayed", true))
				return Result{Event: existing, Replayed: true}, nil
			}
		}
		return Result{}, err
	}

	span.SetAttributes(attribute.String("event_id", ev.ID.String()))
	s.log.Info("event ingested", "event_id", ev.ID, "tenant_id", tenantID)
	metrics.EventsIngested.Inc()

	if s.prod == nil {
		return Result{}, errors.New("kafka producer not configured")
	}
	if err := s.prod.ProduceIngest(ctx, kafkapkg.EventMessage{
		EventID:   ev.ID,
		TenantID:  tenantID,
		EventType: ev.EventType,
		Payload:   payload,
	}); err != nil {
		return Result{}, fmt.Errorf("enqueue event: %w", err)
	}

	return Result{Event: ev, Replayed: false}, nil
}

var ErrInvalid = errors.New("invalid ingest request")
