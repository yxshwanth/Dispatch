# Dispatch — Task List

Atomic execution checklist for the full project. Flip boxes as you go.
Design detail: [`project_overview.md`](project_overview.md). Phases and
gates: [`roadmap.md`](roadmap.md).

**Status legend:** `- [x]` todo · `- [x]` done

**Conventions:** IDs like `P1.3.a` are stable references. Each leaf task
has **Done when**; **Watch** and **Artifacts** where useful.

---

## Phase 0 — Repo bootstrap

### P0.1 — Version control and ignores

- [x] **P0.1.a** Initialize git repository  
  **Done when:**
  - `git status` works at repo root
  - Initial commit can include planning docs only

- [x] **P0.1.b** Add `.gitignore` for Go, Terraform, IDE, env, binaries  
  **Done when:**
  - Ignores `bin/`, `*.exe`, `.env`, `.idea/`, `.vscode/` (optional), `.terraform/`, `*.tfstate*`, `vendor/` if unused
  **Artifacts:** `.gitignore`

- [x] **P0.1.c** Optional `LICENSE` (e.g. MIT)  
  **Done when:**
  - License file present or explicitly skipped with a note in README stub

### P0.2 — Directory skeleton

- [x] **P0.2.a** Create Go binary directories  
  **Done when:**
  - `cmd/api/` and `cmd/consumer/` exist (placeholders OK)
  **Artifacts:** `cmd/api/`, `cmd/consumer/`

- [x] **P0.2.b** Create `internal/`, `migrations/`, `deploy/helm/`, `terraform/`, `.github/workflows/`  
  **Done when:**
  - Empty dirs or `.gitkeep` so layout is visible in git
  **Artifacts:** `internal/`, `migrations/`, `deploy/helm/`, `terraform/`, `.github/workflows/`

### P0.3 — Stub README pointer

- [x] **P0.3.a** Minimal README linking planning docs  
  **Done when:**
  - Points to `project_overview.md`, `roadmap.md`, `task_list.md`
  - States full README is Phase 4.6
  **Artifacts:** `README.md` (stub)

---

## Phase 1 — Core service (Week 1)

### P1.1 — Development environment

- [x] **P1.1.a** `docker-compose.yml` with Postgres 16  
  **Done when:**
  - Postgres container starts with persistent volume
  - Healthcheck verifies accept connections
  **Artifacts:** `docker-compose.yml`

- [x] **P1.1.b** Redis 7 service in Compose  
  **Done when:**
  - Redis healthy; reachable from other services and host as documented

- [x] **P1.1.c** Redpanda with dual listeners  
  **Done when:**
  - INTERNAL listener for container-to-container
  - EXTERNAL on `localhost:19092` for host-run Go processes
  **Watch:** Wrong dual-listener config → host Go cannot reach broker

- [x] **P1.1.d** Healthchecks + `depends_on: condition: service_healthy`  
  **Done when:**
  - Dependent services wait for Postgres/Redis/Redpanda health
  **Watch:** Without this, migrate races Postgres

- [x] **P1.1.e** One-shot migrate service (golang-migrate)  
  **Done when:**
  - Compose (or Make) runs migrations against healthy Postgres
  **Artifacts:** migrate service definition; ties to `migrations/`

- [x] **P1.1.f** Makefile developer interface  
  **Done when:**
  - Targets at minimum: `make up`, `make migrate-up`, `make run-api`, `make test`
  **Artifacts:** `Makefile`

### P1.2 — Schema and migrations

- [x] **P1.2.a** `tenants` table migration  
  **Done when:**
  - Stores API key hash (not plaintext); up/down scripts exist

- [x] **P1.2.b** `subscriptions` table with CB columns + dual secrets + `event_types`  
  **Done when:**
  - Columns cover state, consecutive_failures, state_changed_at, current/previous HMAC secrets + grace expiry, event type filter array

- [x] **P1.2.c** `subscription_state_transitions` audit table  
  **Done when:**
  - `from_state`, `to_state`, `reason`; `BIGINT GENERATED ALWAYS AS IDENTITY` PK
  **Watch:** Prefer IDENTITY over SERIAL

- [x] **P1.2.d** `events` table with optional `idempotency_key`  
  **Done when:**
  - Partial unique index: `(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL`

- [x] **P1.2.e** `delivery_attempts` table  
  **Done when:**
  - Records status_code, error, latency_ms; IDENTITY PK

- [x] **P1.2.f** `dead_letters` table + pending index  
  **Done when:**
  - Partial index on `WHERE replayed_at IS NULL`
  - Unique pending constraint per event/subscription as designed in overview
  **Watch:** Partial index is the replay list performance story

- [x] **P1.2.g** Apply migrations via `make migrate-up`  
  **Done when:**
  - Fresh Compose volume migrates cleanly; down scripts exist for initial migration set
  **Artifacts:** `migrations/*.up.sql`, `migrations/*.down.sql`

### P1.3 — Go project structure and graceful shutdown

- [x] **P1.3.a** `go.mod` module + Go 1.22+ toolchain  
  **Done when:**
  - Module builds; stdlib routing available
  **Artifacts:** `go.mod`

- [x] **P1.3.b** `cmd/api` main + `internal/` package layout  
  **Done when:**
  - Clear separation (http, store, delivery, etc. as needed)
  - `cmd/consumer` stub present for Week 2
  **Watch:** Don’t over-abstract; concrete code first

- [x] **P1.3.c** Structured logging with `log/slog` from day one  
  **Done when:**
  - JSON (or consistent structured) logs; no bare `fmt.Println` in hot paths

- [x] **P1.3.d** Graceful shutdown via `signal.NotifyContext`  
  **Done when:**
  - SIGINT/SIGTERM stop accepting connections, drain in-flight ≤ 15s, exit cleanly
  - `http.ErrServerClosed` not logged as crash
  **Watch:** `ListenAndServe` always errors on shutdown

- [x] **P1.3.e** `http.Server` timeouts  
  **Done when:**
  - `ReadHeaderTimeout` and `ReadTimeout` set (slowloris mitigation)

- [x] **P1.3.f** Health endpoints `/healthz` and `/readyz`  
  **Done when:**
  - Liveness always OK when process up; readiness reflects dependency checks as designed

### P1.4 — Tenant and subscription CRUD

- [x] **P1.4.a** `POST /v1/tenants`  
  **Done when:**
  - Creates tenant; returns plaintext API key **once**; stores SHA-256 hash only
  **Watch:** Never store plaintext key

- [x] **P1.4.b** `POST /v1/subscriptions`  
  **Done when:**
  - Creates subscription; generates HMAC secret; returns secret as designed

- [x] **P1.4.c** `GET /v1/subscriptions` cursor-paginated  
  **Done when:**
  - Cursor-based pagination (not OFFSET/LIMIT)
  **Watch:** `WHERE created_at < $cursor ORDER BY created_at DESC LIMIT $n` (or monotonic PK)

- [x] **P1.4.d** `GET /v1/subscriptions/{id}`  
  **Done when:**
  - Authz: tenant can only fetch own subscription; 404 otherwise

- [x] **P1.4.e** `DELETE /v1/subscriptions/{id}`  
  **Done when:**
  - Removes subscription for owning tenant

- [x] **P1.4.f** `POST /v1/subscriptions/{id}/rotate-secret`  
  **Done when:**
  - Accepts `grace_period`; keeps `previous_secret` valid until `previous_secret_expires_at`
  - Lazy or sweep cleanup after window
  **Watch:** Verify HMAC tries current then previous only if grace active

- [x] **P1.4.g** `POST /v1/subscriptions/{id}/activate`  
  **Done when:**
  - Manually resets paused circuit breaker to `active`; writes state transition

- [x] **P1.4.h** `GET /v1/subscriptions/{id}/deliveries`  
  **Done when:**
  - Paginated delivery log for subscription

- [x] **P1.4.i** API key auth middleware  
  **Done when:**
  - Hashes incoming key; looks up tenant by hash; protects tenant-scoped routes
  **Watch:** Hash comparison on stored hash is sufficient here

- [x] **P1.4.j** Stdlib Go 1.22 method-aware routing  
  **Done when:**
  - Routes like `"POST /v1/subscriptions"`; no framework dependency for HTTP

### P1.5 — Synchronous delivery path

- [x] **P1.5.a** `POST /v1/events` ingest validation  
  **Done when:**
  - Rejects non-`application/json`; enforces payload size limit (e.g. 256KB)

- [x] **P1.5.b** Redis idempotency (`Idempotency-Key`)  
  **Done when:**
  - `SET NX` + TTL (24h); duplicate returns original `event_id`
  - Postgres partial unique index remains safety net
  **Watch:** Redis is primary check; PG is fallback

- [x] **P1.5.c** Persist event then sync fan-out  
  **Done when:**
  - Event stored; matching subscriptions filtered by `event_types`; each attempted inline (Week 1)

- [x] **P1.5.d** HMAC-SHA256 over `timestamp.payload`  
  **Done when:**
  - Headers: `X-Dispatch-Signature`, `X-Dispatch-Timestamp`, `X-Dispatch-Event-ID`
  **Watch:** Sign `timestamp.payload`, not payload alone

- [x] **P1.5.e** Outbound HTTP client timeout 5–10s  
  **Done when:**
  - `http.Client{Timeout: ...}` used for all deliveries

- [x] **P1.5.f** Record `delivery_attempts`  
  **Done when:**
  - status_code, error, latency_ms written per attempt

- [x] **P1.5.g** Update consecutive_failures + trigger CB transitions  
  **Done when:**
  - Success/failure updates subscription; thresholds fire state machine (P1.6)

- [x] **P1.5.h** Correlation: `event_id` on logs and attempts  
  **Done when:**
  - Every post-ingest log line for the request carries `event_id`

### P1.6 — Circuit breaker state machine

- [x] **P1.6.a** States: `active` → `degraded` → `paused`  
  **Done when:**
  - Default: N consecutive failures (e.g. 5) → degraded; M DLQ entries (e.g. 20) → paused
  - Thresholds configurable via env or per-subscription

- [x] **P1.6.b** Half-open probe after cooldown  
  **Done when:**
  - Degraded + cooldown elapsed (e.g. 60s since `state_changed_at`) → next real event is probe
  - Success → active, consecutive_failures = 0; failure → cooldown restarts
  **Watch:** No active pingers / synthetic GETs

- [x] **P1.6.c** Write `subscription_state_transitions` on every transition  
  **Done when:**
  - from_state, to_state, reason recorded

- [x] **P1.6.d** Atomic transition under concurrency  
  **Done when:**
  - `UPDATE ... WHERE state = 'active' AND consecutive_failures >= $threshold RETURNING id` (or equivalent) so only one winner transitions
  **Watch:** Two workers seeing failures=4 must not double-fire side effects

- [x] **P1.6.e** Paused → active only via activate API  
  **Done when:**
  - Automatic recovery does not un-pause; `POST .../activate` required

### P1.7 — Per-tenant rate limiting

- [x] **P1.7.a** Redis sliding-window (or weighted dual fixed window) limiter  
  **Done when:**
  - Per-tenant X events/sec (or minute); algorithm documented in code/README
  **Watch:** Know sliding vs fixed; avoid silent 2× burst at boundaries if using fixed

- [x] **P1.7.b** `429` + `Retry-After` when exceeded  
  **Done when:**
  - Response includes Retry-After; no event persist on reject (or consistent documented behavior)

- [x] **P1.7.c** Redis failure = fail-open  
  **Done when:**
  - Redis down → allow request + log warning
  **Watch:** Rate limit is protection, not correctness

### P1.8 — Tests (Week 1)

- [x] **P1.8.a** Unit: circuit breaker transitions (testify)  
  **Done when:**
  - Covers active→degraded, half-open success/fail, paused rules

- [x] **P1.8.b** Unit: HMAC signing  
  **Done when:**
  - Verifies signature over `timestamp.payload`; rotation/grace cases if applicable

- [x] **P1.8.c** Unit: rate limiter logic  
  **Done when:**
  - Under/over limit behavior covered (may use miniredis or interface)

- [x] **P1.8.d** Integration: full delivery path against Compose  
  **Done when:**
  - Ingest → delivery_attempt row; uses real Postgres/Redis (testcontainers or compose-up)
  **Watch:** `t.Cleanup` for teardown; dedicated test DB or transactional rollback

- [x] **P1.8.e** Integration: failure path → degraded  
  **Done when:**
  - Test proves N consecutive failures trigger degraded
  **Watch:** Failure-path tests > happy-path volume for interview value

- [x] **P1.8.f** Name testify in docs/resume materials  
  **Done when:**
  - Dependency and README/stub mention testify explicitly

---

## Phase 2 — Kafka event pipeline (Week 2)

### P2.1 — Kafka producer integration

- [x] **P2.1.a** Choose client (`segmentio/kafka-go` or `twmb/franz-go`)  
  **Done when:**
  - Dependency locked; choice noted (franz-go preferred later for MSK IAM)

- [x] **P2.1.b** Replace sync delivery with produce-after-persist  
  **Done when:**
  - Persist event in Postgres transaction; then produce; return `202 Accepted`
  **Watch:** Do not produce inside the DB transaction

- [x] **P2.1.c** Producer `acks=all`  
  **Done when:**
  - Config waits for all ISR replicas

- [x] **P2.1.d** Partition key = `tenant_id`  
  **Done when:**
  - Message key is tenant_id bytes — not event_id
  **Watch:** event_id key scatters tenant ordering

- [x] **P2.1.e** Recovery sweep for undelivered events  
  **Done when:**
  - Background goroutine finds events without delivery progress after timeout and re-produces
  **Watch:** Simplified transactional outbox

- [x] **P2.1.f** Create ingest topic (local)  
  **Done when:**
  - Topic exists in Redpanda; partitioned appropriately for demo
  **Artifacts:** topic bootstrap script or Compose init

### P2.2 — Consumer group

- [x] **P2.2.a** `cmd/consumer` binary joins ingest consumer group  
  **Done when:**
  - Separate process from API; configurable brokers/group/topic
  **Artifacts:** `cmd/consumer/main.go`

- [x] **P2.2.b** Deserialize → match subscriptions → deliver (reuse Week 1 logic)  
  **Done when:**
  - Same HMAC, attempt recording, CB checks as sync path

- [x] **P2.2.c** Disable auto-commit; commit after successful processing  
  **Done when:**
  - Offsets committed only after processing completes
  **Watch:** Auto-commit is dangerous

- [x] **P2.2.d** Graceful shutdown: finish current message, commit, exit  
  **Done when:**
  - SIGTERM cancels poll loop cleanly; no silent mid-message kill without handling

- [x] **P2.2.e** Partition revocation handling  
  **Done when:**
  - On revoke: finish in-flight work for revoked partitions before release
  **Watch:** Franz-go `OnPartitionsRevoked` (or equivalent)

- [x] **P2.2.f** Correlation ID in Kafka headers + consumer logs  
  **Done when:**
  - `event_id` in message headers; every consumer log line includes it

### P2.3 — Retry topic with exponential backoff

- [x] **P2.3.a** On failure: produce to retry topic with headers  
  **Done when:**
  - Headers include `attempt_number` and `retry_after` (Unix ts)

- [x] **P2.3.b** Backoff schedule 10s, 30s, 1m, 5m, 15m (5 retries)  
  **Done when:**
  - Schedule configurable; documented
  **Watch:** Single retry topic — no tiered topics

- [x] **P2.3.c** Retry consumer skips/waits until `retry_after`  
  **Done when:**
  - Messages with future `retry_after` do not deliver early (sleep or pause)

- [x] **P2.3.d** Separate consumer group for retry vs ingest  
  **Done when:**
  - Ingest and retry scale independently
  **Watch:** Retry lag is expected — do not alarm as ingest lag

- [x] **P2.3.e** Max attempts → insert `dead_letters`, stop retrying  
  **Done when:**
  - Row in Postgres DLQ; no further retry produce

### P2.4 — DLQ replay API

- [x] **P2.4.a** `GET /v1/dead-letters?subscription_id=`  
  **Done when:**
  - Paginated unreplayed list; uses partial index

- [x] **P2.4.b** `POST /v1/dead-letters/{id}/replay`  
  **Done when:**
  - Sets `replayed_at`; re-produces original event to **ingest** topic
  **Watch:** Not inline delivery — full pipeline again

- [x] **P2.4.c** Reject already-replayed with `409 Conflict`  
  **Done when:**
  - Checks `replayed_at IS NULL` before acting

- [x] **P2.4.d** Unique pending DLQ semantics  
  **Done when:**
  - Same event/subscription cannot sit twice pending (until one replayed)

### P2.5 — CI pipeline (GitHub Actions)

- [x] **P2.5.a** Workflow on push to main and PRs  
  **Done when:**
  - `.github/workflows/ci.yml` (or equivalent) triggers correctly
  **Artifacts:** `.github/workflows/ci.yml`

- [x] **P2.5.b** `docker compose up -d --wait` → migrate → test  
  **Done when:**
  - Steps: compose up with wait, migrations, `go test ./... -race -count=1`, tear down

- [x] **P2.5.c** Cache Go modules (`actions/cache` on `~/go/pkg/mod`)  
  **Done when:**
  - Cache key includes go.sum / module files

---

## Phase 3 — AWS deployment (Week 3)

### P3.1 — VPC and networking

- [x] **P3.1.a** VPC via `terraform-aws-modules/vpc/aws`  
  **Done when:**
  - 2 public subnets (ALB), 2 private subnets (EKS/RDS/ElastiCache/MSK), NAT for egress
  **Artifacts:** `terraform/` VPC module usage
  **Watch:** Multi-AZ from the start

- [x] **P3.1.b** Least-privilege security groups  
  **Done when:**
  - EKS→RDS :5432, EKS→ElastiCache :6379, EKS→MSK :9092 (or IAM port as required)
  - No `0.0.0.0/0` for internal data-plane traffic

### P3.2 — EKS cluster

- [x] **P3.2.a** EKS + managed node group via `terraform-aws-modules/eks/aws`  
  **Done when:**
  - Cluster plans/applies; Kubernetes version pinned (not “latest”)
  **Watch:** 10–15m provision time

- [x] **P3.2.b** OIDC provider + IRSA wiring  
  **Done when:**
  - ServiceAccount annotations match IAM role trust policies

- [x] **P3.2.c** Distinct IAM roles: API (e.g. S3 write) vs consumer  
  **Done when:**
  - No shared broad node role for app permissions

### P3.3 — RDS Aurora PostgreSQL

- [x] **P3.3.a** Aurora cluster in private subnets, multi-AZ  
  **Done when:**
  - DB subnet group spans 2+ AZs; encrypted at rest (KMS); automated backups

- [x] **P3.3.b** Master password via Secrets Manager  
  **Done when:**
  - `manage_master_user_password = true` or Secrets Manager secret; not plaintext in state
  **Watch:** Prefer RDS-managed Secrets Manager password

- [x] **P3.3.c** Parameter group with `pg_stat_statements`  
  **Done when:**
  - `shared_preload_libraries` includes `pg_stat_statements`

- [x] **P3.3.d** App uses cluster endpoint (writes), not instance endpoint  
  **Done when:**
  - Connection string docs/config use cluster endpoint; reader noted if used

### P3.4 — MSK

- [x] **P3.4.a** MSK cluster 3 brokers, 2+ AZs, TLS in transit  
  **Done when:**
  - Terraform resource plans clean
  **Watch:** 15–25m create time; longest resource

- [x] **P3.4.b** IAM authentication (no plaintext Kafka user/pass)  
  **Done when:**
  - Auth mode IAM; Go client SASL signer wired or clearly documented
  **Watch:** franz-go native; kafka-go needs plugin

- [x] **P3.4.c** Topic auto-create disabled; topics explicit  
  **Done when:**
  - Ingest + retry topics created in Terraform or bootstrap script

- [x] **P3.4.d** Instance type: `kafka.t3.small` for demo + comment for prod  
  **Done when:**
  - Comment notes `kafka.m5.large` (or similar) for production-like

### P3.5 — ElastiCache Redis and S3

- [x] **P3.5.a** ElastiCache Redis in VPC subnet group  
  **Done when:**
  - Single node for demo; replication group documented for production

- [x] **P3.5.b** S3 payload archival bucket  
  **Done when:**
  - Name like `dispatch-{account_id}-{region}-payloads`; public access blocked; encryption enforced

- [x] **P3.5.c** Lifecycle policy → Glacier after 90 days  
  **Done when:**
  - Lifecycle rule committed in Terraform

- [x] **P3.5.d** Optional S3 versioning  
  **Done when:**
  - Enabled or explicitly declined in comments

### P3.6 — Helm chart

- [x] **P3.6.a** Chart with Deployments `api` and `consumer`  
  **Done when:**
  - Templates render; separate workloads
  **Artifacts:** `deploy/helm/`

- [x] **P3.6.b** ServiceAccounts annotated for IRSA  
  **Done when:**
  - API and consumer SAs carry role ARNs from values

- [x] **P3.6.c** API Service + Ingress (ALB)  
  **Done when:**
  - External HTTP path to API defined

- [x] **P3.6.d** Liveness `/healthz` and readiness `/readyz`  
  **Done when:**
  - Probes configured on both Deployments as appropriate

- [x] **P3.6.e** `preStop` sleep 5–10s  
  **Done when:**
  - Lifecycle hook present so LB can drain before SIGTERM
  **Watch:** Most commonly missed K8s detail

- [x] **P3.6.f** `terminationGracePeriodSeconds` ≥ preStop + app shutdown  
  **Done when:**
  - e.g. preStop 5s + shutdown 15s → grace ≥ 25s

- [x] **P3.6.g** Resource requests/limits  
  **Done when:**
  - CPU/memory set on both Deployments

- [x] **P3.6.h** ConfigMap + Secret + `values.yaml`  
  **Done when:**
  - Non-sensitive env in ConfigMap; DSN/Redis in Secret; env-specific knobs in values (replicas, images, tags)
  **Watch:** No hardcoded env-specific values in templates

---

## Phase 4 — Observability and hardening (Week 4)

### P4.1 — Prometheus metrics

- [ ] **P4.1.a** Instrument with `prometheus/client_golang`  
  **Done when:**
  - Dependency added; registry wired

- [ ] **P4.1.b** `dispatch_delivery_duration_seconds` (histogram)  
  **Done when:**
  - Labeled appropriately (status); buckets match realistic latency (10ms…10s)

- [ ] **P4.1.c** `dispatch_delivery_total` (counter)  
  **Done when:**
  - Labels: success/failure/timeout (or equivalent)

- [ ] **P4.1.d** `dispatch_circuit_breaker_state` (gauge)  
  **Done when:**
  - Per subscription: 1=active, 2=degraded, 3=paused

- [ ] **P4.1.e** `dispatch_events_ingested_total` (counter)  
  **Done when:**
  - Per tenant only if cardinality bounded; otherwise safer label strategy

- [ ] **P4.1.f** `dispatch_dead_letters_total` (counter)  
  **Done when:**
  - Incremented on DLQ insert

- [ ] **P4.1.g** `dispatch_consumer_lag` (gauge)  
  **Done when:**
  - From client or high-watermark vs committed offset

- [ ] **P4.1.h** `dispatch_retry_queue_depth` (gauge)  
  **Done when:**
  - Pending retry messages exposed

- [ ] **P4.1.i** `/metrics` on port 9090 (separate from API 8080)  
  **Done when:**
  - Metrics server distinct; K8s scrape annotations documented/applied
  **Watch:** Avoid high-cardinality labels (no event_id, no raw URL)

### P4.2 — Grafana dashboard

- [ ] **P4.2.a** Add Prometheus + Grafana to Compose with provisioning  
  **Done when:**
  - `make up` brings obs stack; scrape targets use service names (`api:9090`)
  **Artifacts:** provisioning configs under e.g. `deploy/observability/`

- [ ] **P4.2.b** Dashboard-as-code JSON committed  
  **Done when:**
  - Exported JSON in repo (prefer build in UI then export)
  **Watch:** Don’t hand-write huge JSON from scratch

- [ ] **P4.2.c** Panel: delivery success rate  
  **Done when:** Present on dashboard

- [ ] **P4.2.d** Panel: delivery latency p50/p95/p99  
  **Done when:** From histogram

- [ ] **P4.2.e** Panel: events ingested per minute  
  **Done when:** `rate()` of ingest counter

- [ ] **P4.2.f** Panel: consumer lag  
  **Done when:** Near zero steady-state narrative understood

- [ ] **P4.2.g** Panel: retry queue depth  
  **Done when:** Present; lag here expected under backoff

- [ ] **P4.2.h** Panel: circuit breaker state distribution  
  **Done when:** Active vs degraded vs paused counts

- [ ] **P4.2.i** Panel: dead letter rate  
  **Done when:** Present on dashboard

### P4.3 — OpenTelemetry distributed tracing

- [ ] **P4.3.a** OTel SDK (`go.opentelemetry.io/otel`) wired  
  **Done when:**
  - Tracer provider configured; exporter to Jaeger locally

- [ ] **P4.3.b** Spans: ingest, Kafka produce, Kafka consume, delivery attempt  
  **Done when:**
  - Full lifecycle visible as one trace when headers propagate

- [ ] **P4.3.c** Propagate W3C trace context via Kafka headers  
  **Done when:**
  - Inject on produce, extract on consume
  **Watch:** Kafka has no native W3C propagation

- [ ] **P4.3.d** `event_id` as span attribute  
  **Done when:**
  - Searchable in Jaeger by event_id

- [ ] **P4.3.e** Jaeger in Compose  
  **Done when:**
  - Local tracing backend up with `make up`

- [ ] **P4.3.f** Do not trace health/metrics endpoints  
  **Done when:**
  - `/healthz`, `/readyz`, `/metrics` excluded or sampled out

### P4.4 — Load test and benchmark

- [ ] **P4.4.a** Load script with k6 or vegeta  
  **Done when:**
  - Ramp scenario for ingest throughput
  **Artifacts:** e.g. `scripts/load/` or `loadtest/`

- [ ] **P4.4.b** Measure p50/p95/p99 delivery latency under load  
  **Done when:**
  - Numbers captured from metrics or test output

- [ ] **P4.4.c** Completeness check: events ≈ delivery_attempts + dead_letters  
  **Done when:**
  - Post-test SQL/script asserts no silent loss
  **Watch:** Any discrepancy is a bug

- [ ] **P4.4.d** Watch consumer lag during test  
  **Done when:**
  - Notes whether lag grows unboundedly; partition/consumer scaling called out if needed

- [ ] **P4.4.e** Publish “X K events/sec at sub-Y ms p99” in README  
  **Done when:**
  - Resume placeholders filled from Compose run (not cloud absolutes)

### P4.5 — HMAC replay protection and security hardening

- [ ] **P4.5.a** Document 5-minute replay window for consumers  
  **Done when:**
  - Consumer-facing docs say reject signatures older than 5 minutes

- [ ] **P4.5.b** Test: 6-minute-old timestamp signature rejected  
  **Done when:**
  - Explicit test case fails verification / receiver rejection path as designed

- [ ] **P4.5.c** Test: payload size limit enforced  
  **Done when:**
  - Oversize body → 4xx

- [ ] **P4.5.d** Test: Content-Type enforcement  
  **Done when:**
  - Non-JSON → 4xx

### P4.6 — README and documentation

- [ ] **P4.6.a** What the system does  
  **Done when:** Clear product statement at top of README

- [ ] **P4.6.b** Architecture diagram + how it works  
  **Done when:** Diagram (from overview) and short pipeline explanation

- [ ] **P4.6.c** Design decisions and why  
  **Done when:** Ordering tradeoff, CB half-open, DLQ in Postgres, HMAC model

- [ ] **P4.6.d** What it doesn’t handle (honest limitations)  
  **Done when:** Matches roadmap non-goals

- [ ] **P4.6.e** How to run locally  
  **Done when:** `make up`, migrate, run api/consumer, hit example curl flows

- [ ] **P4.6.f** Benchmark numbers  
  **Done when:** X/Y from P4.4 present

- [ ] **P4.6.g** Replace stub README with full portfolio README  
  **Done when:** Primary artifact ready for GitHub visitors
  **Artifacts:** `README.md`

---

## Cross-cutting (ongoing)

- [x] **CX.1** Every post-ingest log carries `event_id` (API + consumer)  
  **Done when:** Grep one ID shows full lifecycle

- [x] **CX.2** Graceful shutdown on API and consumer proven  
  **Done when:** Documented sequence; pairs with Helm preStop

- [x] **CX.3** Unit + integration + E2E coverage maintained as features land  
  **Done when:** CI green with `-race` on every push (from P2.5 onward)

- [ ] **CX.4** Atomic commits with descriptive messages  
  **Done when:** History reads as feature narrative (e.g. “add circuit breaker state machine with testify tests”)

- [x] **CX.5** Interfaces only when needed  
  **Done when:** No day-one Repository ceremony with a single impl

---

## Appendix A — API surface inventory

Use as a burn-down of HTTP surface (duplicates Phase tasks; check when endpoint is complete).

- [x] `POST /v1/tenants`
- [x] `POST /v1/subscriptions`
- [x] `GET /v1/subscriptions` (cursor)
- [x] `GET /v1/subscriptions/{id}`
- [x] `DELETE /v1/subscriptions/{id}`
- [x] `POST /v1/subscriptions/{id}/rotate-secret`
- [x] `POST /v1/subscriptions/{id}/activate`
- [x] `GET /v1/subscriptions/{id}/deliveries`
- [x] `POST /v1/events`
- [x] `GET /v1/dead-letters`
- [x] `POST /v1/dead-letters/{id}/replay`
- [x] `GET /healthz`
- [x] `GET /readyz`
- [ ] `GET /metrics` (port 9090)

---

## Appendix B — Data store ownership

### PostgreSQL (source of truth)

- [x] `tenants`
- [x] `subscriptions` (CB state, dual secrets, event_types)
- [x] `subscription_state_transitions`
- [x] `events` (+ partial unique idempotency index)
- [x] `delivery_attempts`
- [x] `dead_letters` (+ partial index on unreplayed)

### Redis (ephemeral)

- [x] Per-tenant rate limit keys (sliding window)
- [x] Idempotency keys (`SET NX` + TTL)

### Kafka / Redpanda / MSK

- [x] Ingest topic (partitioned by `tenant_id`)
- [x] Retry topic (single topic; backoff in headers)
- [x] Topics created explicitly in prod (auto-create off)

---

## Appendix C — Resume keyword burn-down

| Gap | Task IDs |
| --- | -------- |
| Kafka / event-driven | P2.1–P2.3, P2.2.e–f |
| AWS (EKS, RDS, MSK, S3, IAM) | P3.2–P3.5, P3.2.b–c |
| Terraform | P3.1–P3.5 |
| Helm | P3.6 |
| Grafana dashboards | P4.2 |
| Database migrations | P1.1.e, P1.2 |
| Testing frameworks (testify) | P1.8 |
| gRPC (optional stretch) | *(not required for M4)* |
| Rate limiting / backoff | P1.7, P2.3 |
| Circuit breaker / resilience | P1.6 |
| Observability / OpenTelemetry | P4.1–P4.3 |
| Structured logging | P1.3.c, CX.1 |

---

## Appendix D — Definition of Done (project)

Project is **done** when all of the following are true:

- [ ] **DoD.1** Local Compose demo: tenant → subscription → events → deliveries (Kafka path)
- [ ] **DoD.2** CI green: compose, migrate, `go test ./... -race -count=1`
- [ ] **DoD.3** `terraform plan` clean for VPC/EKS/Aurora/MSK/ElastiCache/S3/IAM
- [ ] **DoD.4** Helm values documented; probes, preStop, IRSA, grace period correct
- [ ] **DoD.5** Prometheus + Grafana + Jaeger work via `make up`
- [ ] **DoD.6** Load test run; completeness check passes; X/Y in README
- [ ] **DoD.7** Security tests (replay window, size, Content-Type) green
- [ ] **DoD.8** README honest: architecture, decisions, non-goals, local run, benchmarks
- [ ] **DoD.9** Roadmap coarse gates M0–M4 checked
- [ ] **DoD.10** Resume keyword table (Appendix C) satisfiable from the repo without stretching claims

---

## Optional stretch (post-M4)

- [ ] **OPT.1** Internal gRPC ingestion service (resume gap only)  
  **Done when:** Documented as optional; does not block DoD
