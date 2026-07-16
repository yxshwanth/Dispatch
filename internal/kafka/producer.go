package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Producer struct {
	client      *kgo.Client
	ingestTopic string
	retryTopic  string
	log         *slog.Logger
	tracer      trace.Tracer
}

func NewProducer(brokers []string, ingestTopic, retryTopic string, log *slog.Logger) (*Producer, error) {
	if log == nil {
		log = slog.Default()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{
		client:      cl,
		ingestTopic: ingestTopic,
		retryTopic:  retryTopic,
		log:         log,
		tracer:      otel.Tracer("dispatch/kafka"),
	}, nil
}

func (p *Producer) Close() { p.client.Close() }

func (p *Producer) Client() *kgo.Client { return p.client }

func (p *Producer) ProduceIngest(ctx context.Context, msg EventMessage) error {
	ctx, span := p.tracer.Start(ctx, "kafka.produce.ingest",
		trace.WithAttributes(
			attribute.String("event_id", msg.EventID.String()),
			attribute.String("messaging.destination", p.ingestTopic),
		),
	)
	defer span.End()

	body, err := EncodeEvent(msg)
	if err != nil {
		span.RecordError(err)
		return err
	}
	headers := []kgo.RecordHeader{
		{Key: HeaderEventID, Value: []byte(msg.EventID.String())},
	}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	record := &kgo.Record{
		Topic:   p.ingestTopic,
		Key:     []byte(msg.TenantID.String()),
		Value:   body,
		Headers: headers,
	}
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("produce ingest: %w", err)
	}
	p.log.Info("produced ingest message", "event_id", msg.EventID, "tenant_id", msg.TenantID, "topic", p.ingestTopic)
	return nil
}

func (p *Producer) ProduceRetry(ctx context.Context, msg RetryMessage) error {
	ctx, span := p.tracer.Start(ctx, "kafka.produce.retry",
		trace.WithAttributes(
			attribute.String("event_id", msg.EventID.String()),
			attribute.String("messaging.destination", p.retryTopic),
		),
	)
	defer span.End()

	body, err := EncodeRetry(msg)
	if err != nil {
		span.RecordError(err)
		return err
	}
	headers := []kgo.RecordHeader{
		{Key: HeaderEventID, Value: []byte(msg.EventID.String())},
		{Key: HeaderSubscriptionID, Value: []byte(msg.SubscriptionID.String())},
		{Key: HeaderAttemptNumber, Value: []byte(fmt.Sprintf("%d", msg.AttemptNumber))},
		{Key: HeaderRetryAfter, Value: []byte(fmt.Sprintf("%d", msg.RetryAfterUnix))},
	}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	record := &kgo.Record{
		Topic:   p.retryTopic,
		Key:     []byte(msg.SubscriptionID.String()),
		Value:   body,
		Headers: headers,
	}
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("produce retry: %w", err)
	}
	p.log.Info("produced retry message",
		"event_id", msg.EventID,
		"subscription_id", msg.SubscriptionID,
		"attempt", msg.AttemptNumber,
		"retry_after", time.Unix(msg.RetryAfterUnix, 0).UTC(),
	)
	return nil
}

func (p *Producer) ProduceIngestRaw(ctx context.Context, eventID, tenantID uuid.UUID, eventType string, payload []byte) error {
	return p.ProduceIngest(ctx, EventMessage{
		EventID:   eventID,
		TenantID:  tenantID,
		EventType: eventType,
		Payload:   payload,
	})
}

// Ensure propagator interface used
var _ propagation.TextMapCarrier = headerCarrier{}
