#!/usr/bin/env bash
# Vegeta ingest load against a running Dispatch API.
# Prerequisites: stack up (make up), vegeta on PATH (auto-installed via go install).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

API_URL="${API_URL:-http://localhost:8080}"
RATE="${RATE:-50/s}"
DURATION="${DURATION:-15s}"
# webhook service in Compose; reachable from api/consumer containers.
SUB_URL="${SUB_URL:-http://webhook:8080/}"

if ! command -v vegeta >/dev/null 2>&1; then
  echo "installing vegeta..."
  go install github.com/tsenart/vegeta/v12@v12.12.0
  export PATH="$(go env GOPATH)/bin:$PATH"
fi

echo "==> creating tenant"
TENANT_JSON=$(curl -sS -X POST "$API_URL/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d '{"name":"loadtest"}')
API_KEY=$(echo "$TENANT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["api_key"])')
echo "api_key acquired"

echo "==> creating subscription -> $SUB_URL"
SUB_JSON=$(curl -sS -X POST "$API_URL/v1/subscriptions" \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"$SUB_URL\",\"event_types\":[\"load.test\"]}")
SUB_ID=$(echo "$SUB_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')
echo "subscription_id=$SUB_ID"

BODY='{"event_type":"load.test","payload":{"n":1}}'
BODY_FILE=$(mktemp)
printf '%s' "$BODY" >"$BODY_FILE"

REPORT_DIR="${REPORT_DIR:-/tmp/dispatch-load}"
mkdir -p "$REPORT_DIR"
BIN="$REPORT_DIR/results.bin"
TXT="$REPORT_DIR/report.txt"

echo "==> vegeta attack rate=$RATE duration=$DURATION"
echo "POST $API_URL/v1/events" | vegeta attack \
  -rate="$RATE" \
  -duration="$DURATION" \
  -body="$BODY_FILE" \
  -header="Authorization: Bearer $API_KEY" \
  -header="Content-Type: application/json" \
  | tee "$BIN" | vegeta report | tee "$TXT"

rm -f "$BODY_FILE"

echo ""
echo "==> ingest metrics (api :9090)"
curl -sS http://localhost:9090/metrics | grep -E '^dispatch_events_ingested_total|^dispatch_delivery_' || true

echo ""
echo "==> delivery / lag metrics (consumer :9091)"
curl -sS http://localhost:9091/metrics 2>/dev/null | grep -E '^dispatch_delivery_|^dispatch_consumer_lag|^dispatch_retry_queue_depth' || true

echo ""
echo "==> wait for deliveries to settle, then completeness"
sleep 15
docker compose exec -T postgres psql -U dispatch -d dispatch < scripts/load/completeness.sql || true

echo ""
echo "Lag note: dispatch_consumer_lag above should stay near 0 under this load;"
echo "if it grows unboundedly, add ingest partitions / consumer replicas."
echo "report: $TXT"
