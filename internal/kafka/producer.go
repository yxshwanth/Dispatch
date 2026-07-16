package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client      *kgo.Client
	ingestTopic string
	retryTopic  string
	log         *slog.Logger
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
	return &Producer{client: cl, ingestTopic: ingestTopic, retryTopic: retryTopic, log: log}, nil
}

func (p *Producer) Close() { p.client.Close() }

func (p *Producer) ProduceIngest(ctx context.Context, msg EventMessage) error {
	body, err := EncodeEvent(msg)
	if err != nil {
		return err
	}
	record := &kgo.Record{
		Topic: p.ingestTopic,
		Key:   []byte(msg.TenantID.String()),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: HeaderEventID, Value: []byte(msg.EventID.String())},
		},
	}
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce ingest: %w", err)
	}
	p.log.Info("produced ingest message", "event_id", msg.EventID, "tenant_id", msg.TenantID, "topic", p.ingestTopic)
	return nil
}

func (p *Producer) ProduceRetry(ctx context.Context, msg RetryMessage) error {
	body, err := EncodeRetry(msg)
	if err != nil {
		return err
	}
	record := &kgo.Record{
		Topic: p.retryTopic,
		Key:   []byte(msg.SubscriptionID.String()),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: HeaderEventID, Value: []byte(msg.EventID.String())},
			{Key: HeaderSubscriptionID, Value: []byte(msg.SubscriptionID.String())},
			{Key: HeaderAttemptNumber, Value: []byte(fmt.Sprintf("%d", msg.AttemptNumber))},
			{Key: HeaderRetryAfter, Value: []byte(fmt.Sprintf("%d", msg.RetryAfterUnix))},
		},
	}
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
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

// ProduceIngestRaw re-produces an existing event (recovery / replay).
func (p *Producer) ProduceIngestRaw(ctx context.Context, eventID, tenantID uuid.UUID, eventType string, payload []byte) error {
	return p.ProduceIngest(ctx, EventMessage{
		EventID:   eventID,
		TenantID:  tenantID,
		EventType: eventType,
		Payload:   payload,
	})
}
