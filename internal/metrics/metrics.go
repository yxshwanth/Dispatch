package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	DeliveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "dispatch_delivery_duration_seconds",
		Help: "Outbound webhook delivery latency",
		Buckets: []float64{
			0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"status"})

	DeliveryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatch_delivery_total",
		Help: "Outbound webhook deliveries by status",
	}, []string{"status"})

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dispatch_circuit_breaker_state",
		Help: "Count of subscriptions in each circuit breaker state",
	}, []string{"state"})

	EventsIngested = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatch_events_ingested_total",
		Help: "Events accepted by the ingestion API",
	})

	DeadLetters = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatch_dead_letters_total",
		Help: "Events written to the dead letter table",
	})

	ConsumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dispatch_consumer_lag",
		Help: "Kafka ingest consumer lag (messages behind high watermark)",
	})

	RetryQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dispatch_retry_queue_depth",
		Help: "Approximate pending messages in the retry topic",
	})
)

// ObserveDelivery records delivery metrics from a completed attempt.
func ObserveDelivery(status string, latency time.Duration) {
	DeliveryTotal.WithLabelValues(status).Inc()
	if status != "skipped" {
		DeliveryDuration.WithLabelValues(status).Observe(latency.Seconds())
	}
}

// ListenAndServe starts a dedicated metrics HTTP server on addr (e.g. :9090).
func ListenAndServe(ctx context.Context, addr string, log *slog.Logger) {
	if addr == "" {
		return
	}
	if log == nil {
		log = slog.Default()
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server error", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}
