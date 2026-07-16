# Dispatch

Multi-tenant webhook delivery platform (Go, Kafka, Postgres, Redis, AWS).

This is a stub README. The full portfolio README lands in **Phase 4.6**.

## Planning docs

- [docs/project_overview.md](docs/project_overview.md) — architecture and design decisions
- [docs/architecture.md](docs/architecture.md) — granular application deep dive
- [docs/roadmap.md](docs/roadmap.md) — phases, gates, calendar
- [docs/task_list.md](docs/task_list.md) — atomic checkboxes
- [docs/halfway_summary.md](docs/halfway_summary.md) — status after Phases 0–3

## Local quickstart

```bash
make up              # Postgres, Redis, Redpanda + topics
make migrate-up
make run-api         # terminal 1
make run-consumer    # terminal 2
```

Tests use [testify](https://github.com/stretchr/testify):

```bash
make test
make test-integration   # requires Compose up
```

## Infra

- Terraform (no apply required for local demos): [`terraform/`](terraform/)
- Helm chart: [`deploy/helm/dispatch/`](deploy/helm/dispatch/)
