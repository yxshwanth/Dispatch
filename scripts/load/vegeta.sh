#!/usr/bin/env bash
# Vegeta ingest load against a running Dispatch API (smoke; default 50/s).
# Ceiling / isolation / crash: make load-ceiling load-isolation load-crash.
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"

ensure_vegeta

RATE="${RATE:-50/s}"
DURATION="${DURATION:-15s}"
SUB_URL="${SUB_URL:-http://webhook:8080/}"
REPORT_DIR="${REPORT_DIR:-/tmp/dispatch-load}"
mkdir -p "$REPORT_DIR"

echo "==> creating tenant"
API_KEY="$(create_tenant loadtest)"
TENANT_ID="$(tenant_id_from_key "$API_KEY")"
SUB_ID="$(create_subscription "$API_KEY" "$SUB_URL" load.test)"
echo "tenant_id=$TENANT_ID subscription_id=$SUB_ID"

vegeta_attack "$API_KEY" "$RATE" "$DURATION" "$REPORT_DIR/results.json"

echo ""
echo "==> ingest metrics (api :9090)"
curl -sS http://localhost:9090/metrics | grep -E '^dispatch_events_ingested_total|^dispatch_delivery_' || true

echo ""
echo "==> delivery / lag metrics (consumer :9091)"
curl -sS http://localhost:9091/metrics 2>/dev/null | grep -E '^dispatch_delivery_|^dispatch_consumer_lag|^dispatch_retry_queue_depth' || true

SETTLE_SECONDS="${SETTLE_SECONDS:-35}"
settle_completeness
assert_completeness "$TENANT_ID"

echo ""
echo "Lag note: ingest lag should be 0 after settle;"
echo "if it grows unboundedly under higher RATE, use make load-ceiling."
echo "report: $REPORT_DIR/results.txt"
