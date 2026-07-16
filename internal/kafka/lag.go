package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yash/dispatch/internal/metrics"
)

// ReportLag periodically updates consumer lag and retry queue depth gauges.
func ReportLag(ctx context.Context, brokers []string, ingestTopic, retryTopic, ingestGroup string, interval time.Duration, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Warn("lag reporter client failed", "err", err)
		return
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	tick := time.NewTicker(interval)
	defer tick.Stop()
	update := func() {
		end, err := admin.ListEndOffsets(ctx, ingestTopic, retryTopic)
		if err != nil {
			log.Warn("list end offsets failed", "err", err)
			return
		}
		if parts, ok := end[ingestTopic]; ok {
			var hw int64
			for _, p := range parts {
				if p.Err == nil {
					hw += p.Offset
				}
			}
			committed, err := admin.FetchOffsetsForTopics(ctx, ingestGroup, ingestTopic)
			if err == nil {
				var committedSum int64
				if byTopic, ok := committed[ingestTopic]; ok {
					for _, o := range byTopic {
						if o.Err == nil && o.At >= 0 {
							committedSum += o.At
						}
					}
				}
				lag := hw - committedSum
				if lag < 0 {
					lag = 0
				}
				metrics.ConsumerLag.Set(float64(lag))
			}
		}
		if parts, ok := end[retryTopic]; ok {
			var depth int64
			for _, p := range parts {
				if p.Err == nil {
					depth += p.Offset
				}
			}
			// Approximate queue depth as high watermark (demo-friendly).
			metrics.RetryQueueDepth.Set(float64(depth))
		}
	}
	update()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			update()
		}
	}
}
