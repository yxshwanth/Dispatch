#!/usr/bin/env bash
# Isolation proof: one tenant, two subscriptions (200 vs 500).
# 1) A single event on the dead URL exhausts retry → DLQ.
# 2) A few thousand events: healthy p99 stays on the hot path, ingest stays 202.
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"

ensure_vegeta

HEALTHY_URL="${HEALTHY_URL:-http://webhook:8080/}"
FAIL_URL="${FAIL_URL:-http://webhook-fail/}"
RATE="${RATE:-100/s}"
DURATION="${DURATION:-30s}"
REPORT_DIR="${REPORT_DIR:-/tmp/dispatch-isolation}"
mkdir -p "$REPORT_DIR"

echo "==> ensuring webhook-fail is up"
docker compose up -d --wait webhook-fail

echo "==> consumer: short retry backoff so one event can reach DLQ"
RETRY_BACKOFF=1s,1s,1s,1s,1s CB_FAILURE_THRESHOLD=10 \
  docker compose up -d --wait --no-deps --force-recreate consumer
sleep 3

echo "==> creating tenant + two subscriptions"
API_KEY="$(create_tenant isolation)"
TENANT_ID="$(tenant_id_from_key "$API_KEY")"
HEALTHY_ID="$(create_subscription "$API_KEY" "$HEALTHY_URL" load.test)"
FAIL_ID="$(create_subscription "$API_KEY" "$FAIL_URL" load.test)"
echo "tenant_id=$TENANT_ID"
echo "healthy_id=$HEALTHY_ID"
echo "fail_id=$FAIL_ID"

echo "==> path: 1 event, wait for failing subscription DLQ"
CODE="$(post_event "$API_KEY")"
echo "ingest HTTP $CODE"
if [[ "$CODE" != "202" ]]; then
  echo "ERROR: expected 202 for seed event, got $CODE" >&2
  cat /tmp/dispatch-event-resp.json >&2 || true
  exit 1
fi

DLQ=0
for i in $(seq 1 40); do
  DLQ="$(psql_q "SELECT COUNT(*) FROM dead_letters WHERE subscription_id = '$FAIL_ID'::uuid AND replayed_at IS NULL;")"
  DLQ="$(echo "$DLQ" | tr -d '[:space:]')"
  if [[ "$DLQ" != "0" ]]; then
    echo "failing subscription reached DLQ after ${i}s (pending=$DLQ)"
    break
  fi
  sleep 1
done
if [[ "$DLQ" == "0" ]]; then
  echo "ERROR: failing subscription did not reach DLQ within 40s" >&2
  psql_t -v tenant_id="$TENANT_ID" < "$LOAD_DIR/isolation.sql" || true
  exit 1
fi

echo "==> isolation: $RATE for $DURATION"
vegeta_attack "$API_KEY" "$RATE" "$DURATION" "$REPORT_DIR/vegeta.json"
if ! vegeta_success_ok "$REPORT_DIR/vegeta.json"; then
  echo "ERROR: ingest 202 dropped below 100% under isolation load" >&2
  exit 1
fi

settle_completeness
psql_t -v tenant_id="$TENANT_ID" < "$LOAD_DIR/isolation.sql" | tee "$REPORT_DIR/isolation.txt"
assert_completeness "$TENANT_ID"

python3 - "$REPORT_DIR/vegeta.json" "$REPORT_DIR/result.env" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
codes = d.get("status_codes") or {}
p99 = d["latencies"]["99th"] / 1e6
open(sys.argv[2], "w").write(
    "INGEST_SUCCESS={:.6f}\nINGEST_P99_MS={:.3f}\nINGEST_REQUESTS={}\nINGEST_N202={}\nINGEST_RATE={:.2f}\n".format(
        d["success"], p99, d["requests"], int(codes.get("202", 0) or 0), d["rate"]
    )
)
PY

HEALTHY_P99="$(psql_q "
  SELECT COALESCE(ROUND((percentile_cont(0.99) WITHIN GROUP (ORDER BY da.latency_ms))::numeric, 2)::text, 'null')
  FROM delivery_attempts da
  WHERE da.subscription_id = '$HEALTHY_ID'::uuid
    AND da.status_code BETWEEN 200 AND 299;
")"
HEALTHY_OK="$(psql_q "SELECT COUNT(*) FROM delivery_attempts WHERE subscription_id = '$HEALTHY_ID'::uuid AND status_code BETWEEN 200 AND 299;")"
FAIL_5XX="$(psql_q "SELECT COUNT(*) FROM delivery_attempts WHERE subscription_id = '$FAIL_ID'::uuid AND status_code >= 500;")"
FAIL_OK="$(psql_q "SELECT COUNT(*) FROM delivery_attempts WHERE subscription_id = '$FAIL_ID'::uuid AND status_code BETWEEN 200 AND 299;")"
EVENTS="$(psql_q "SELECT COUNT(*) FROM events WHERE tenant_id = '$TENANT_ID'::uuid;")"
FAIL_STATE="$(psql_q "SELECT state FROM subscriptions WHERE id = '$FAIL_ID'::uuid;")"
FAIL_DLQ="$(psql_q "SELECT COUNT(*) FROM dead_letters WHERE subscription_id = '$FAIL_ID'::uuid;")"
LAG="$(ingest_lag)"

{
  echo "TENANT_ID=$TENANT_ID"
  echo "HEALTHY_ID=$HEALTHY_ID"
  echo "FAIL_ID=$FAIL_ID"
  echo "HEALTHY_P99_MS=${HEALTHY_P99}"
  echo "HEALTHY_OK_ATTEMPTS=${HEALTHY_OK}"
  echo "FAIL_5XX_ATTEMPTS=${FAIL_5XX}"
  echo "FAIL_OK_ATTEMPTS=${FAIL_OK}"
  echo "EVENTS=${EVENTS}"
  echo "FAIL_STATE=${FAIL_STATE}"
  echo "PENDING_DLQ=${FAIL_DLQ}"
  echo "INGEST_LAG=${LAG}"
} >> "$REPORT_DIR/result.env"

echo ""
echo "==> restoring consumer defaults"
docker compose up -d --wait --no-deps --force-recreate consumer

echo ""
echo "Isolation report: $REPORT_DIR"
echo "  events=$EVENTS ingest 100% 202 healthy_p99=${HEALTHY_P99}ms healthy_ok=$HEALTHY_OK fail_5xx=$FAIL_5XX fail_state=$FAIL_STATE dlq=$DLQ lag=$LAG"
