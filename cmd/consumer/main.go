package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yash/dispatch/internal/circuitbreaker"
	"github.com/yash/dispatch/internal/config"
	"github.com/yash/dispatch/internal/delivery"
	"github.com/yash/dispatch/internal/kafka"
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

	prod, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.IngestTopic, cfg.RetryTopic, log)
	if err != nil {
		log.Error("kafka producer failed", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	st := store.New(pool)
	cbCfg := circuitbreaker.Config{
		FailureThreshold:  cfg.CBFailureThreshold,
		Cooldown:          cfg.CBCooldown,
		DLQPauseThreshold: cfg.CBDLQPauseThreshold,
	}
	deliv := delivery.New(st, cfg.DeliveryTimeout, cbCfg, log)
	worker := kafka.NewWorker(cfg, st, deliv, prod, log)

	log.Info("consumer starting", "mode", cfg.ConsumerMode, "brokers", cfg.KafkaBrokers)
	if err := worker.Run(ctx); err != nil {
		log.Error("consumer exited with error", "err", err)
		os.Exit(1)
	}
	log.Info("consumer shutdown complete")
}
