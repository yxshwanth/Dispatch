.PHONY: up down migrate-up migrate-down topics run-api run-consumer test test-integration

# Prefer system Docker Engine socket when Desktop socket is unavailable.
export DOCKER_HOST ?= unix:///var/run/docker.sock

DATABASE_URL ?= postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable
REDIS_ADDR ?= localhost:6379
KAFKA_BROKERS ?= localhost:19092
API_ADDR ?= :8080

up:
	docker compose up -d --wait postgres redis redpanda
	$(MAKE) topics

down:
	docker compose down

migrate-up:
	docker compose run --rm migrate up

migrate-down:
	docker compose run --rm migrate down 1

topics:
	docker compose exec -T redpanda rpk topic create dispatch.ingest -p 3 -r 1 || true
	docker compose exec -T redpanda rpk topic create dispatch.retry -p 1 -r 1 || true

run-api:
	DATABASE_URL="$(DATABASE_URL)" REDIS_ADDR="$(REDIS_ADDR)" KAFKA_BROKERS="$(KAFKA_BROKERS)" API_ADDR="$(API_ADDR)" go run ./cmd/api

run-consumer:
	DATABASE_URL="$(DATABASE_URL)" REDIS_ADDR="$(REDIS_ADDR)" KAFKA_BROKERS="$(KAFKA_BROKERS)" go run ./cmd/consumer

test:
	go test ./... -count=1

test-integration:
	DISPATCH_INTEGRATION=1 DATABASE_URL="$(DATABASE_URL)" REDIS_ADDR="$(REDIS_ADDR)" KAFKA_BROKERS="$(KAFKA_BROKERS)" go test ./... -count=1 -timeout 180s
