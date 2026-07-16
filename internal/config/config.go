package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL string
	RedisAddr   string
	APIAddr     string

	MaxPayloadBytes int64
	DeliveryTimeout time.Duration
	ShutdownTimeout time.Duration

	RateLimitPerMinute int
	RateLimitWindow    time.Duration

	CBFailureThreshold  int
	CBCooldown          time.Duration
	CBDLQPauseThreshold int

	IdempotencyTTL time.Duration

	KafkaBrokers       []string
	IngestTopic        string
	RetryTopic         string
	IngestConsumerGroup string
	RetryConsumerGroup  string
	ConsumerMode       string // all | ingest | retry

	RetryBackoff       []time.Duration
	RecoveryInterval   time.Duration
	RecoveryAge        time.Duration
}

func Load() Config {
	return Config{
		DatabaseURL: getenv("DATABASE_URL", "postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable"),
		RedisAddr:   getenv("REDIS_ADDR", "localhost:6379"),
		APIAddr:     getenv("API_ADDR", ":8080"),

		MaxPayloadBytes: int64(getenvInt("MAX_PAYLOAD_BYTES", 256*1024)),
		DeliveryTimeout: getenvDuration("DELIVERY_TIMEOUT", 10*time.Second),
		ShutdownTimeout: getenvDuration("SHUTDOWN_TIMEOUT", 15*time.Second),

		RateLimitPerMinute: getenvInt("RATE_LIMIT_PER_MINUTE", 60),
		RateLimitWindow:    getenvDuration("RATE_LIMIT_WINDOW", time.Minute),

		CBFailureThreshold:  getenvInt("CB_FAILURE_THRESHOLD", 5),
		CBCooldown:          getenvDuration("CB_COOLDOWN", 60*time.Second),
		CBDLQPauseThreshold: getenvInt("CB_DLQ_PAUSE_THRESHOLD", 20),

		IdempotencyTTL: getenvDuration("IDEMPOTENCY_TTL", 24*time.Hour),

		KafkaBrokers:        splitCSV(getenv("KAFKA_BROKERS", "localhost:19092")),
		IngestTopic:         getenv("KAFKA_INGEST_TOPIC", "dispatch.ingest"),
		RetryTopic:          getenv("KAFKA_RETRY_TOPIC", "dispatch.retry"),
		IngestConsumerGroup: getenv("KAFKA_INGEST_GROUP", "dispatch-ingest"),
		RetryConsumerGroup:  getenv("KAFKA_RETRY_GROUP", "dispatch-retry"),
		ConsumerMode:        getenv("CONSUMER_MODE", "all"),

		RetryBackoff:     parseBackoff(getenv("RETRY_BACKOFF", "10s,30s,1m,5m,15m")),
		RecoveryInterval: getenvDuration("RECOVERY_INTERVAL", 30*time.Second),
		RecoveryAge:      getenvDuration("RECOVERY_AGE", 60*time.Second),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseBackoff(s string) []time.Duration {
	parts := splitCSV(s)
	out := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		d, err := time.ParseDuration(p)
		if err != nil {
			continue
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}
	}
	return out
}
