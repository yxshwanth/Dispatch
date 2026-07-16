# Dispatch — Project Overview

A multi-tenant webhook delivery platform. Accept events from producers,
reliably deliver them to subscriber endpoints with retries, exponential
backoff, ordering guarantees, and observability. Every company at scale
builds or buys this (Stripe, GitHub, Twilio all maintain one internally).

---

## System architecture

```
                              ┌──────────────────────────────┐
                              │         Tenant / Producer     │
                              └──────────┬───────────────────┘
                                         │ POST /v1/events
                                         │ (API key auth)
                                         ▼
                              ┌──────────────────────────────┐
                              │     Ingestion API (Go)        │
                              │  • payload size + type check  │
                              │  • idempotency (Redis)        │
                              │  • rate limiting (Redis)      │
                              │  • event persisted (Postgres) │
                              └──────────┬───────────────────┘
                                         │
                    ┌────────────────────┴────────────────────┐
                    │  Week 1: sync delivery   Week 2: Kafka  │
                    │  (direct HTTP POST)      (produce to    │
                    │                          ingest topic)   │
                    └────────────────────┬────────────────────┘
                                        │
                                        ▼
                              ┌──────────────────────────────┐
                              │   Kafka Ingest Topic          │
                              │   partitioned by tenant_id    │
                              └──────────┬───────────────────┘
                                         │
                                         ▼
                              ┌──────────────────────────────┐
                              │   Consumer Group (Go)         │
                              │  • fan out per subscription   │
                              │  • HMAC-sign payload          │
                              │  • HTTP POST to endpoint      │
                              │  • record delivery_attempt    │
                              │  • circuit breaker check      │
                              └────┬──────────────┬──────────┘
                                   │              │
                            success│              │failure
                                   │              │
                                   ▼              ▼
                              ┌─────────┐  ┌──────────────────┐
                              │  Done   │  │  Retry Topic     │
                              └─────────┘  │  (attempt count  │
                                           │   + backoff ts   │
                                           │   in headers)    │
                                           └────────┬─────────┘
                                                    │
                                                    ▼ after max attempts
                                           ┌────────────────────┐
                                           │  Dead Letters      │
                                           │  (Postgres table)  │
                                           │  replay via API    │
                                           └────────────────────┘
```


### Data stores and what they own

**PostgreSQL** — tenants, subscriptions (including circuit breaker state
and HMAC secrets), events, delivery_attempts, dead_letters,
subscription_state_transitions. The persistent source of truth.

**Redis** — per-tenant rate limiting (sliding window counter) and
idempotency key deduplication (SET NX with TTL). Ephemeral by nature;
losing Redis data means duplicate delivery is possible for a brief
window, which is acceptable because webhook consumers must be idempotent
anyway.

**Kafka (Redpanda locally, MSK in production)** — two topics. The
*ingest topic* (partitioned by tenant_id for per-tenant ordering) and
the *retry topic* (single partition is fine; backoff timestamp in message
headers, consumer skips messages whose time hasn't arrived). No tiered
retry topics — the complexity doesn't pay off at this scale.


### Key design decisions

**Ordering.** Events are partitioned by tenant ID in Kafka, giving
per-tenant ordering at the partition level. Delivery ordering per
subscription is best-effort under failure: when a delivery fails and
enters the retry path, subsequent events for that subscription are NOT
blocked. Blocking lets one dead endpoint stall a tenant's entire event
stream. Consumers that require strict ordering should use the timestamp
in the signed payload. This is the same tradeoff Stripe and GitHub make.
Document it clearly in the README and operator docs.

**Circuit breaker (half-open, not active pings).** Subscriptions move
active → degraded after N consecutive failures. In degraded state,
normal deliveries are held; after a cooldown period, the next real event
is let through as a half-open probe. Success closes the circuit (back to
active); failure extends the cooldown. We do NOT actively ping tenant
endpoints — most webhook receivers only accept signed POSTs, so
synthetic health checks return 404/405 and tell you nothing, and POSTing
fake events is rude. Crossing the DLQ threshold moves the subscription
to paused, requiring explicit re-activation via the API.

**DLQ in Postgres, not Kafka.** The replay API needs filtering,
pagination, and mark-as-replayed semantics. A Postgres table with a
partial index on `WHERE replayed_at IS NULL` handles all of this. A
Kafka topic can't.

**HMAC with replay protection.** Payloads are signed per-subscription
using the Stripe/GitHub model: the signature covers a timestamp, and
consumers should reject signatures older than 5 minutes to prevent
replay attacks. Secret rotation keeps the previous secret valid during a
configurable grace window.

**No authentication beyond API keys.** The goal is not to demonstrate
auth; that's already proven elsewhere. A simple middleware that checks a
hashed API key is enough. Spend the time on infrastructure and
operability instead.

---

## Phase 1 — Core service (Week 1)

The goal is an end-to-end working system with synchronous delivery
before Kafka enters the picture. This means you always have a working
demo at every stage of the month.

### Tasks

**1.1 — Development environment (Day 1 morning)**

Set up docker-compose.yml with Postgres 16, Redis 7, and Redpanda
(Kafka-compatible, single binary, no ZooKeeper). Add a one-shot migrate
service using golang-migrate. Write the Makefile as the developer
interface: `make up`, `make migrate-up`, `make run-api`, `make test`.
Everything local; no cloud dependencies until Week 3.

*Watch out for:*
- Redpanda's dual-listener configuration (INTERNAL for container-to-container,
  EXTERNAL on localhost:19092 for host access). Get this wrong and your Go
  code running on the host can't reach the broker.
- Health checks on every container with `depends_on: condition:
  service_healthy`. Without this, `make up` returns before Postgres is
  actually accepting connections and your migration runner races.

**1.2 — Schema and migrations (Day 1 afternoon)**

Write the initial migration: tenants, subscriptions (with circuit breaker
columns, two-secret rotation, event_types filter array),
subscription_state_transitions, events (with idempotency_key), 
delivery_attempts, dead_letters.

*Watch out for:*
- The partial unique index on `events (tenant_id, idempotency_key) WHERE
  idempotency_key IS NOT NULL`. This means tenants that don't send keys
  pay no constraint cost. If you use a regular unique index, NULLs are
  considered distinct in Postgres so it still works, but the partial
  index is cleaner.
- The partial index on `dead_letters WHERE replayed_at IS NULL` — this is
  what makes the replay endpoint fast.
- Use `BIGINT GENERATED ALWAYS AS IDENTITY` for delivery_attempts and
  state_transitions, not SERIAL. SERIAL is legacy.

**1.3 — Go project structure and graceful shutdown (Day 1)**

Two binaries: `cmd/api` (the REST service) and `cmd/consumer` (the Kafka
worker, built in Week 2). `internal/` for everything else. JSON
structured logging via `log/slog` (stdlib since Go 1.21) from line one.
Graceful shutdown via `signal.NotifyContext` handling SIGINT and SIGTERM.
On SIGTERM: stop accepting new connections, drain in-flight requests
within a 15-second timeout, exit cleanly.

*Watch out for:*
- Using `http.ErrServerClosed` to distinguish expected shutdown from
  real errors. The `ListenAndServe` goroutine always returns an error on
  shutdown; don't log it as a crash.
- Setting `ReadHeaderTimeout` and `ReadTimeout` on the http.Server.
  Without these you're vulnerable to slowloris.

**1.4 — Tenant and subscription CRUD (Days 2–3)**

REST API using stdlib `net/http` with Go 1.22 method-aware routing
(`"POST /v1/subscriptions"`). No framework — this is a defensible
choice. Endpoints:

- `POST /v1/tenants` — create tenant, return plaintext API key once, 
  store only the SHA-256 hash.
- `POST /v1/subscriptions` — create subscription, generate HMAC secret.
- `GET /v1/subscriptions` — list (paginated, cursor-based).
- `GET /v1/subscriptions/{id}` — fetch one.
- `DELETE /v1/subscriptions/{id}` — remove.
- `POST /v1/subscriptions/{id}/rotate-secret` — rotate HMAC secret,
  accept a `grace_period` parameter, keep previous_secret valid until
  previous_secret_expires_at.
- `POST /v1/subscriptions/{id}/activate` — manually reset a paused
  circuit breaker to active.
- `GET /v1/subscriptions/{id}/deliveries` — paginated delivery log.

API key auth middleware: hash the incoming key, look up the tenant by
hash. Simple, non-timing-attack-safe comparison is fine here (the hash
itself prevents timing leaks on the raw key).

*Watch out for:*
- Cursor-based pagination, not OFFSET/LIMIT. OFFSET scans and discards
  rows, so page 1000 is slow. Use `WHERE created_at < $cursor ORDER BY
  created_at DESC LIMIT $page_size` — or the primary key if it's
  monotonic.
- Returning the API key plaintext exactly once in the creation response
  and never again. The DB stores a hash. If you store the key in the
  clear, that's a security question waiting to happen.
- The secret rotation grace window: when checking an HMAC signature, try
  the current secret first, then fall back to previous_secret only if
  previous_secret_expires_at is still in the future. Don't forget to
  clear previous_secret and previous_secret_expires_at after the window
  closes (a background sweep or lazy check on read both work).

**1.5 — Synchronous delivery path (Days 3–4)**

`POST /v1/events` accepts an event, persists it, and immediately
attempts delivery to all matching subscriptions (filtered by
event_types). For each subscription:

1. Build the payload with a timestamp.
2. Compute HMAC-SHA256 signature over `timestamp.payload` using the
   subscription's secret.
3. HTTP POST to the subscription URL with headers:
   `X-Dispatch-Signature`, `X-Dispatch-Timestamp`, `X-Dispatch-Event-ID`.
4. Record the delivery_attempt row (status_code, error, latency_ms).
5. Update consecutive_failures on the subscription.
6. Fire circuit breaker state transitions if thresholds are crossed.

*Watch out for:*
- Set a hard timeout on outbound HTTP calls (5–10 seconds). A hanging
  endpoint should not block your event processing forever. Use
  `http.Client{Timeout: 10 * time.Second}`.
- Payload size limit on ingestion (e.g. 256KB). Validate
  Content-Type too (application/json only). This is the "request
  payload validation with size limits" security signal.
- The HMAC must sign `timestamp.payload`, not just the payload. The
  timestamp prevents replay. The consumer-facing documentation should
  say: "reject signatures where the timestamp is more than 5 minutes
  old."
- Idempotency check before persisting: if the tenant sends
  `Idempotency-Key` header, SET NX in Redis with a TTL (24h is
  standard). If the key already exists, return the original event_id.
  The Postgres partial unique index is a fallback safety net, not the
  primary check — Redis is faster and doesn't hold row locks.

**1.6 — Circuit breaker state machine (Day 4)**

State transitions: active → degraded (after N consecutive failures,
e.g. 5) → paused (after M total DLQ entries, e.g. 20). Degraded →
active on successful half-open probe delivery. Paused → active only via
explicit API call.

Every transition writes a row to subscription_state_transitions with
from_state, to_state, and reason. This audit trail is cheap to maintain
and invaluable for debugging.

Half-open probe logic: when a subscription is degraded and the cooldown
period (e.g. 60 seconds since state_changed_at) has elapsed, the next
delivery attempt is allowed through as a probe. If it succeeds,
consecutive_failures resets to 0 and state returns to active. If it
fails, the cooldown restarts.

*Watch out for:*
- Don't use an active health-check pinger. Most webhook receivers only
  accept signed POSTs; a GET health check will 404 and tell you nothing.
  Half-open probes using real events are the standard pattern.
- Make the thresholds configurable (env vars or per-subscription).
  Hardcoding 5 consecutive failures is fine for the default, but
  mentioning configurability in the README shows operational thinking.
- Race conditions: two concurrent deliveries for the same subscription
  can both see consecutive_failures=4, both increment to 5, both try to
  transition. Use an UPDATE ... WHERE state = 'active' AND
  consecutive_failures >= $threshold RETURNING id to make the transition
  atomic. Only the row that wins the UPDATE fires the side effects.

**1.7 — Per-tenant rate limiting (Day 5)**

Redis sliding window rate limiter. Each tenant gets X events/second (or
minute). Use a Lua script or Redis MULTI/EXEC with INCR and EXPIRE on a
key like `ratelimit:{tenant_id}:{window}`. Return 429 with a
Retry-After header when exceeded.

*Watch out for:*
- The sliding window vs. fixed window distinction. Fixed windows have a
  burst problem at window boundaries (2x the rate if requests straddle
  two windows). A sliding window log (ZADD with timestamp scores, ZRANGEBYSCORE
  to count, ZREMRANGEBYSCORE to trim) is cleaner. Or use the simpler
  "two fixed windows with weighted average" approximation — either is
  fine, but know which one you picked and why.
- Redis failure mode: if Redis is down, do you reject all requests (safe
  but hostile) or allow them through (permissive but could overload
  downstream)? Pick "allow through with a log warning" — rate limiting
  is a protection mechanism, not a correctness requirement.

**1.8 — Tests (throughout Week 1)**

Unit tests with testify for the circuit breaker state machine, HMAC
signing, rate limiter logic. Integration tests against real containers
(via docker compose) for the full delivery path: ingest event → delivery
attempt → verify delivery_attempts row. Use `testcontainers-go` or just
rely on docker compose being up (the simpler approach; CI will do
`docker compose up` before running tests).

*Watch out for:*
- Use `t.Cleanup` for test data teardown, not defer. Cleanup runs after
  subtests complete; defer runs when the parent function returns, which
  can be too early.
- Integration tests should use a dedicated test database or at minimum
  run in transactions that roll back. Don't pollute the dev database.
- Test the failure paths, not just the happy path. A test that verifies
  "5 consecutive failures trigger degraded state" is worth more than 10
  tests that verify successful delivery.

---

## Phase 2 — Kafka event pipeline (Week 2)

### Tasks

**2.1 — Kafka producer integration (Day 1)**

Replace the synchronous delivery path in the API: instead of delivering
inline, produce the event to the Kafka ingest topic after persisting it
to Postgres. Use `segmentio/kafka-go` or `twmb/franz-go` (franz-go is
lower-level but more performant; either is fine). Partition key =
tenant_id (gives per-tenant ordering).

*Watch out for:*
- Producer acknowledgment. Use `acks=all` (wait for all ISR replicas to
  confirm). `acks=1` risks data loss if the leader dies before
  replicating.
- Don't produce to Kafka inside the HTTP request transaction. The
  pattern is: persist event to Postgres (in a transaction), produce to
  Kafka, return 202 Accepted. If the Kafka produce fails, the event is
  still in Postgres and can be picked up by a recovery sweep (a
  background goroutine that scans for events without delivery_attempts
  after a timeout). This is the transactional outbox pattern, simplified.
- Message key must be a byte representation of tenant_id. Don't use the
  event_id — you'll scatter a tenant's events across partitions.

**2.2 — Consumer group (Days 1–3)**

A separate binary (`cmd/consumer`) that joins a Kafka consumer group,
reads from the ingest topic, and for each message: deserializes the
event, looks up matching subscriptions, attempts delivery (same logic as
Week 1's sync path), records results.

Graceful shutdown is critical here. On SIGTERM: stop the consumer poll
loop, finish processing the current message, commit the offset, and exit
cleanly. In Go this is `context.WithCancel` triggered by a signal
handler, wrapping the consume loop. If you don't do this, a Kubernetes
rolling deploy will kill the consumer mid-message and the event gets
redelivered (at-least-once is fine, but avoidable redelivery creates
noise).

*Watch out for:*
- Consumer group rebalancing. When a new consumer joins or one dies, Kafka
  reassigns partitions. Your consumer must handle the revocation callback:
  finish in-flight work for the revoked partitions before releasing them.
  Franz-go handles this with the `OnPartitionsRevoked` hook.
- Correlation ID propagation. The event_id generated at ingestion must
  flow into the Kafka message headers and appear in every log line the
  consumer produces. This is how you grep one ID and see the full event
  lifecycle.
- Commit offsets AFTER processing, not before. Auto-commit is the
  default in most clients and it's dangerous — it commits on a timer
  regardless of processing status. Disable auto-commit; commit manually
  after successful processing.

**2.3 — Retry topic with exponential backoff (Days 3–4)**

On delivery failure: produce the event to the retry topic with headers
for `attempt_number` and `retry_after` (a Unix timestamp). The retry
consumer reads the topic; if `retry_after` is in the future, pause
consumption on that partition briefly (or use a time.Sleep, simpler).
When the time arrives, reattempt delivery. Backoff schedule: 10s, 30s,
1m, 5m, 15m (5 retries total — adjust to taste).

After max attempts, insert a row into the dead_letters Postgres table
and do NOT produce back to the retry topic.

*Watch out for:*
- A single retry topic, not tiered topics (retry-10s, retry-30s, etc.).
  Tiered topics are clever but add complexity without proportional benefit
  at this scale. The timestamp-in-header approach is simpler and easier
  to explain.
- Consumer lag on the retry topic is expected and normal — events are
  intentionally sitting there waiting for their backoff window. Don't
  alarm on retry topic lag in your monitoring.
- The retry consumer and the ingest consumer can be the same binary with
  a flag, or separate consumer groups. Separate consumer groups is
  cleaner — they scale independently and a slow retry queue doesn't
  affect ingest throughput.

**2.4 — DLQ replay API (Day 4)**

`GET /v1/dead-letters?subscription_id=X` — paginated list of unreplayed
dead letters (uses the partial index). `POST /v1/dead-letters/{id}/replay`
— marks the row as replayed (sets replayed_at) and re-produces the
original event to the ingest topic for redelivery.

*Watch out for:*
- The replay endpoint should re-produce to the ingest topic, not attempt
  delivery inline. This way the event goes through the same pipeline
  (consumer, circuit breaker check, retry logic) as any new event.
- Don't allow replay of an already-replayed dead letter. Check
  replayed_at IS NULL before acting and return 409 Conflict otherwise.
- The `idx_dead_letters_unique_pending` index prevents the same event
  from sitting in the DLQ twice for the same subscription (until one is
  replayed). This matters when a replay itself fails and re-enters the
  DLQ path.

**2.5 — CI pipeline with GitHub Actions (Day 5)**

GitHub Actions workflow: on push to main and on PRs. Steps: start docker
compose (Postgres, Redis, Redpanda), wait for health checks, run
migrations, execute `go test ./... -race -count=1`, tear down. This is
how mature teams test infrastructure-heavy services — integration tests
against real dependencies, not mocks.

*Watch out for:*
- GitHub Actions runners have Docker and Docker Compose pre-installed.
  Use `docker compose up -d --wait` to block until health checks pass.
- The `-race` flag enables the race detector. It's slower but catches
  data races that are nightmares in production. Always run with it in CI.
- Cache Go modules between runs using `actions/cache` with
  `~/go/pkg/mod` as the path. This halves your CI time.

---

## Phase 3 — AWS deployment with Terraform and Helm (Week 3)

This is the highest-risk week. MSK alone takes 15–20 minutes to
provision and the Terraform feedback loop is slow. Write the Terraform
and make sure it plans cleanly, but do all actual development and
testing against Docker Compose locally.

### Tasks

**3.1 — VPC and networking (Day 1)**

Terraform module for a VPC with: 2 public subnets (for the ALB), 2
private subnets (for EKS nodes, RDS, ElastiCache, MSK), NAT gateway
for outbound internet from private subnets, security groups with
least-privilege ingress/egress rules.

*Watch out for:*
- Multi-AZ from the start. RDS Aurora, MSK, and ElastiCache all require
  subnets in at least 2 AZs. Don't create a single-AZ layout and bolt
  on the second later — the refactor is painful.
- Security group rules should be specific: EKS nodes can reach RDS on
  5432, ElastiCache on 6379, MSK on 9092. Don't use 0.0.0.0/0 for
  internal traffic.
- Use `terraform-aws-modules/vpc/aws` to avoid reinventing subnet math.
  Writing your own VPC module is not the skill being demonstrated.

**3.2 — EKS cluster (Day 1–2)**

Terraform for an EKS cluster with a managed node group. Use
`terraform-aws-modules/eks/aws`. IRSA (IAM Roles for Service Accounts)
for pod-level IAM — the API pod gets a role with S3 write access for
payload archival; the consumer pod gets its own role. No shared node
role with broad permissions.

*Watch out for:*
- EKS takes 10–15 minutes to provision. Plan your iteration cycles
  around this.
- IRSA requires an OIDC provider associated with the cluster. The EKS
  module handles this, but verify the annotation on the Kubernetes
  ServiceAccount matches the IAM role's trust policy.
- Use a specific Kubernetes version, not "latest." Pin it and note why
  in a comment.

**3.3 — RDS Aurora PostgreSQL (Day 2)**

Terraform for an Aurora PostgreSQL cluster in the private subnets.
Encrypted at rest (KMS), automated backups, multi-AZ. Parameter group
with tuned settings (e.g. `shared_preload_libraries = pg_stat_statements`
for query analysis).

*Watch out for:*
- The DB subnet group must span 2+ AZs, matching your VPC layout.
- Don't put the master password in Terraform state. Use
  `aws_secretsmanager_secret` or pass it via variable with
  `sensitive = true`. Better: use `manage_master_user_password = true`
  to let RDS manage it in Secrets Manager automatically.
- Aurora's connection endpoint vs. instance endpoint vs. reader endpoint.
  Your application should connect to the cluster endpoint for writes and
  the reader endpoint for read replicas (if used). Don't hardcode an
  instance endpoint.

**3.4 — MSK (managed Kafka) cluster (Day 2–3)**

Terraform for an MSK cluster with 3 brokers across 2+ AZs. TLS
encryption in transit. IAM authentication (no plaintext
username/password). Topic auto-creation disabled — create topics
explicitly in Terraform or via a bootstrap script.

*Watch out for:*
- MSK takes 15–25 minutes to create. The longest single resource.
- MSK broker instance types matter: `kafka.t3.small` for dev/demo,
  `kafka.m5.large` for anything resembling production. Use the small
  one but comment that production would use the larger type.
- IAM auth requires the Go client to use the AWS MSK IAM SASL signer.
  Franz-go supports this natively; segmentio/kafka-go needs a plugin.
  Make sure you test this or note it clearly.

**3.5 — ElastiCache Redis and S3 (Day 3)**

Terraform for an ElastiCache Redis cluster (single node for demo,
replication group for production — document both). S3 bucket for payload
archival with a lifecycle policy that moves objects to Glacier after 90
days. Bucket policy enforcing encryption and blocking public access.

*Watch out for:*
- ElastiCache in a VPC requires a subnet group, same multi-AZ pattern
  as RDS.
- S3 bucket names are globally unique. Use a naming convention like
  `dispatch-{account_id}-{region}-payloads`.
- Enable S3 versioning if you want to demonstrate that you know it
  exists, but it's not strictly necessary for an archival bucket.

**3.6 — Helm chart (Days 4–5)**

Package the application as a Helm chart. Two deployments: `api` and
`consumer`. Each gets a ServiceAccount annotated with its IRSA IAM role
ARN. The API deployment gets a Service and Ingress (ALB). Both
deployments get:

- Liveness probe (`/healthz`) and readiness probe (`/readyz`).
- `preStop` lifecycle hook with a short sleep (5–10 seconds) to let the
  load balancer drain connections before SIGTERM fires. This pairs with
  the graceful shutdown code from Week 1.
- Resource requests and limits.
- ConfigMap for non-sensitive env vars, Secret for the Postgres DSN and
  Redis address.

*Watch out for:*
- The `preStop` sleep is the single most commonly missed detail in
  Kubernetes deployments. Without it, the pod receives SIGTERM and starts
  draining, but the load balancer hasn't removed it from the target
  group yet — so new requests arrive at a draining pod and get 503s.
  The sleep gives the LB time to update. 5 seconds is usually enough.
- `terminationGracePeriodSeconds` on the pod spec must be longer than
  `preStop` sleep + your application's shutdown timeout. If your
  preStop is 5s and shutdown timeout is 15s, set
  terminationGracePeriodSeconds to at least 25s.
- Use `values.yaml` for anything environment-specific: replica counts,
  resource limits, image tags, DSN strings. Don't hardcode.

---

## Phase 4 — Observability and hardening (Week 4)

### Tasks

**4.1 — Prometheus metrics (Day 1–2)**

Instrument the Go services with `prometheus/client_golang`. Key metrics:

- `dispatch_delivery_duration_seconds` (histogram) — delivery latency
  per subscription, per status code.
- `dispatch_delivery_total` (counter) — total deliveries, labeled by
  status (success/failure/timeout).
- `dispatch_circuit_breaker_state` (gauge) — current state per
  subscription (1 = active, 2 = degraded, 3 = paused).
- `dispatch_events_ingested_total` (counter) — events received per
  tenant.
- `dispatch_dead_letters_total` (counter) — events dead-lettered.
- `dispatch_consumer_lag` (gauge) — Kafka consumer lag (messages behind
  head of partition).
- `dispatch_retry_queue_depth` (gauge) — pending messages in the retry
  topic.

Expose `/metrics` on a separate port (9090) from the main API (8080).
In Kubernetes, annotate the pod for Prometheus scraping.

*Watch out for:*
- Histogram buckets for delivery latency should match realistic
  expectations: 10ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s.
  The defaults (up to 10s) are actually fine here.
- High-cardinality labels kill Prometheus. Don't label by event_id or
  individual URL. Label by tenant_id only if the tenant count is bounded
  (< 100 for a demo). Otherwise label by subscription state or status
  code.
- Consumer lag can be pulled from the consumer client library or by
  comparing the consumer group's committed offset with the topic's high
  watermark. Franz-go exposes this; segmentio/kafka-go needs manual
  offset management.

**4.2 — Grafana dashboard (Day 2–3)**

A JSON-provisioned Grafana dashboard (no manual clicking — the
dashboard-as-code is the artifact). Panels:

- Delivery success rate (percentage over time).
- Delivery latency p50/p95/p99 (from the histogram).
- Events ingested per minute (rate of the counter).
- Consumer lag (should be near zero in steady state, spikes under load).
- Retry queue depth over time.
- Circuit breaker state distribution (how many subs are active vs.
  degraded vs. paused).
- Dead letter rate.

Add Grafana and Prometheus to docker-compose.yml with provisioning
configs so `make up` gives a fully working observability stack locally.

*Watch out for:*
- Grafana dashboard JSON is verbose. Export it from the UI after
  building it visually, then commit the JSON. Don't hand-write it.
- Prometheus scrape config needs to know where the Go services are. In
  Docker Compose, use the service name as the target (`api:9090`). In
  Kubernetes, use service discovery annotations.

**4.3 — OpenTelemetry distributed tracing (Day 3–4)**

Add OpenTelemetry tracing using `go.opentelemetry.io/otel`. Create spans
for: event ingestion, Kafka produce, Kafka consume, each delivery
attempt. Propagate trace context through Kafka message headers so the
entire lifecycle of an event (API → Kafka → consumer → delivery) is one
trace.

The correlation ID (event_id) should be an attribute on every span so
you can search by it in the tracing backend. Use Jaeger as the local
tracing backend (add to docker-compose.yml).

*Watch out for:*
- Kafka doesn't natively propagate W3C trace context. You need to
  inject/extract the trace context into Kafka message headers manually.
  The `go.opentelemetry.io/contrib/instrumentation` packages have Kafka
  helpers for this.
- Don't trace everything. Trace the event lifecycle (ingestion through
  delivery), not health checks or metrics endpoints.

**4.4 — Load test and benchmark (Day 4–5)**

Write a load test using `k6` or `vegeta` (Go-native, fits the stack).
Scenario: ramp up event ingestion to find the throughput ceiling, measure
p50/p95/p99 delivery latency under load, verify no data loss (every
ingested event eventually has a delivery_attempt or a dead_letter row).

Publish the numbers in the README: "sustained X K events/sec at sub-Y ms
p99 delivery latency under Z concurrent producers."

*Watch out for:*
- Run the benchmark against Docker Compose locally, not against a cloud
  deployment. Absolute cloud numbers vary; measure a reproducible local
  baseline and discuss bottlenecks.
- Verify data completeness after the load test: count events ingested vs.
  delivery_attempts + dead_letters. Any discrepancy is a bug.
- Watch for Kafka consumer lag during the test — it tells you whether
  consumption is keeping up with production. If lag grows unboundedly,
  you need more partitions or more consumer instances.

**4.5 — HMAC replay protection and security hardening (Day 5)**

Verify that the HMAC signature includes a timestamp and document the
5-minute replay window. Write a test that verifies a signature with a
6-minute-old timestamp is rejected. Verify payload size limits are
enforced. Verify Content-Type enforcement. Write these as explicit test
cases.

**4.6 — README and documentation (Day 5)**

Keep the README current with what the system does, how it works
architecturally (with a diagram), design decisions and why, explicit
non-goals, how to run locally, and benchmark numbers when available.

---

## Cross-cutting concerns (applies to every phase)

**Structured logging with correlation IDs.** Every log line after event
ingestion carries the event_id. Generate it at the API boundary, pass it
through context, write it into Kafka headers, include it in every
delivery_attempt record. When someone asks "what happened to event X,"
you grep one ID and see the full lifecycle.

**Graceful shutdown everywhere.** The API drains HTTP connections. The
consumer finishes in-flight messages and commits offsets. Both log the
shutdown sequence. This pairs with the Helm preStop hook and proves you
understand Kubernetes rolling deploys.

**Tests at every layer.** Unit tests for pure logic (HMAC signing,
circuit breaker transitions, rate limiter). Integration tests against
real Postgres/Redis/Kafka via Docker Compose. End-to-end tests for the
full delivery path. CI runs all of them on every push.

**Don't over-abstract early.** Write concrete code first; extract
interfaces only when you need them for testing or when a second
implementation appears. A `Repository` interface on day one with one
implementation is ceremony, not architecture.

**Commit history matters.** Prefer atomic commits with descriptive
messages over "wip" dumps.