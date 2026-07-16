package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/yash/dispatch/internal/circuitbreaker"
	"github.com/yash/dispatch/internal/config"
	"github.com/yash/dispatch/internal/delivery"
	"github.com/yash/dispatch/internal/httpapi"
	"github.com/yash/dispatch/internal/idempotency"
	"github.com/yash/dispatch/internal/kafka"
	"github.com/yash/dispatch/internal/ratelimit"
	"github.com/yash/dispatch/internal/recovery"
	"github.com/yash/dispatch/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("postgres connect failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("postgres ping failed", "err", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	prod, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.IngestTopic, cfg.RetryTopic, log)
	if err != nil {
		log.Error("kafka producer failed", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	st := store.New(pool)
	limiter := ratelimit.New(rdb, cfg.RateLimitPerMinute, cfg.RateLimitWindow, log)
	idem := idempotency.New(rdb, cfg.IdempotencyTTL)
	cbCfg := circuitbreaker.Config{
		FailureThreshold:  cfg.CBFailureThreshold,
		Cooldown:          cfg.CBCooldown,
		DLQPauseThreshold: cfg.CBDLQPauseThreshold,
	}
	deliv := delivery.New(st, cfg.DeliveryTimeout, cbCfg, log)
	api := httpapi.New(cfg, st, limiter, idem, deliv, prod, log)

	sweeper := recovery.New(st, prod, cfg.RecoveryInterval, cfg.RecoveryAge, log)
	go sweeper.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
	}

	go func() {
		log.Info("api listening", "addr", cfg.APIAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "err", err)
		os.Exit(1)
	}
	log.Info("shutdown complete")
}
