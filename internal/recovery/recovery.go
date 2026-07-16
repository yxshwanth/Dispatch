package recovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/yash/dispatch/internal/kafka"
	"github.com/yash/dispatch/internal/store"
)

// Sweeper re-produces events that never received any delivery attempts.
type Sweeper struct {
	store    *store.Store
	producer *kafka.Producer
	interval time.Duration
	age      time.Duration
	log      *slog.Logger
}

func New(st *store.Store, prod *kafka.Producer, interval, age time.Duration, log *slog.Logger) *Sweeper {
	if log == nil {
		log = slog.Default()
	}
	return &Sweeper{store: st, producer: prod, interval: interval, age: age, log: log}
}

func (s *Sweeper) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.once(ctx)
		}
	}
}

func (s *Sweeper) once(ctx context.Context) {
	events, err := s.store.ListUndeliveredEvents(ctx, s.age, 100)
	if err != nil {
		s.log.Error("recovery sweep list failed", "err", err)
		return
	}
	for _, ev := range events {
		if err := s.producer.ProduceIngestRaw(ctx, ev.ID, ev.TenantID, ev.EventType, ev.Payload); err != nil {
			s.log.Error("recovery produce failed", "err", err, "event_id", ev.ID)
			continue
		}
		s.log.Info("recovery re-produced event", "event_id", ev.ID)
	}
}
