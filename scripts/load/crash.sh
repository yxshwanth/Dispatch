#!/usr/bin/env bash
# Kill the consumer mid-run (SIGKILL). Bring it back. Lag returns to 0.
# Completeness still holds (at-least-once / manual commit).
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"

ensure_vegeta

if [[ -f /tmp/dispatch-ceiling/result.env ]]; then
  CEILING_RATE="$(grep -E '^CEILING_RATE=' /tmp/dispatch-ceiling/result.env | cut -d= -f2-)"
fi

RATE="${RATE:-${CEILING_RATE:-200/s}}"
DURATION="${DURATION:-40s}"
KILL_AFTER="${KILL_AFTER:-12s}"
SUB_URL="${SUB_URL:-http://webhook:8080/}"
REPORT_DIR="${REPORT_DIR:-/tmp/dispatch-crash}"
mkdir -p "$REPORT_DIR"

echo "==> creating tenant + subscription"
API_KEY="$(create_tenant crash)"
TENANT_ID="$(tenant_id_from_key "$API_KEY")"
SUB_ID="$(create_subscription "$API_KEY" "$SUB_URL" load.test)"
echo "tenant_id=$TENANT_ID rate=$RATE duration=$DURATION kill_after=$KILL_AFTER"

BODY_FILE="$(mktemp)"
printf '%s' '{"event_type":"load.test","payload":{"n":1}}' >"$BODY_FILE"
BIN="$REPORT_DIR/vegeta.bin"
JSON="$REPORT_DIR/vegeta.json"
TXT="$REPORT_DIR/vegeta.txt"

echo "==> vegeta in background"
echo "POST $API_URL/v1/events" | vegeta attack \
  -rate="$RATE" \
  -duration="$DURATION" \
  -workers="${WORKERS:-64}" \
  -body="$BODY_FILE" \
  -header="Authorization: Bearer $API_KEY" \
  -header="Content-Type: application/json" \
  >"$BIN" &
VEGETA_PID=$!

sleep_dur="${KILL_AFTER%s}"
echo "==> waiting ${sleep_dur}s then SIGKILL consumer"
sleep "$sleep_dur"

echo "==> docker compose kill -s SIGKILL consumer"
docker compose kill -s SIGKILL consumer
LAG_DEAD="$(ingest_lag || true)"
echo "lag after kill=${LAG_DEAD:-unknown}"

# Ingest must keep returning 202 while the consumer is down.
PROBE_CODE="$(post_event "$API_KEY")"
echo "ingest probe during outage HTTP $PROBE_CODE"
if [[ "$PROBE_CODE" != "202" ]]; then
  echo "ERROR: ingest dropped while consumer was dead (HTTP $PROBE_CODE)" >&2
  wait "$VEGETA_PID" || true
  exit 1
fi

echo "==> bringing consumer back"
docker compose up -d --wait --no-deps consumer

wait "$VEGETA_PID" || true
vegeta report <"$BIN" | tee "$TXT"
vegeta report -type=json <"$BIN" >"$JSON"
rm -f "$BODY_FILE"

if ! vegeta_success_ok "$JSON"; then
  echo "ERROR: ingest 202 dropped below 100% across the crash window" >&2
  exit 1
fi

wait_ingest_lag_zero "${LAG_TIMEOUT:-180}"
wait_all_attempted "$TENANT_ID" "${ATTEMPT_TIMEOUT:-180}"
SETTLE_SECONDS="${SETTLE_SECONDS:-35}"
echo "==> settle ${SETTLE_SECONDS}s so completeness age predicate (30s) is valid"
sleep "$SETTLE_SECONDS"
assert_completeness "$TENANT_ID"

LAG="$(ingest_lag)"
EVENTS="$(psql_q "SELECT COUNT(*) FROM events WHERE tenant_id = '$TENANT_ID'::uuid;")"
ATTEMPTS="$(psql_q "SELECT COUNT(*) FROM delivery_attempts da JOIN events e ON e.id = da.event_id WHERE e.tenant_id = '$TENANT_ID'::uuid;")"

python3 - "$JSON" "$REPORT_DIR/result.env" "$RATE" "$TENANT_ID" "$EVENTS" "$ATTEMPTS" "$LAG" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
codes = d.get("status_codes") or {}
p99 = d["latencies"]["99th"] / 1e6
open(sys.argv[2], "w").write(
    "RATE={}\nINGEST_P99_MS={:.3f}\nINGEST_REQUESTS={}\nINGEST_N202={}\nINGEST_SUCCESS={:.6f}\nTENANT_ID={}\nEVENTS={}\nATTEMPTS={}\nINGEST_LAG={}\n".format(
        sys.argv[3], p99, d["requests"], int(codes.get("202", 0) or 0),
        d["success"], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]
    )
)
PY

echo "Crash report: $REPORT_DIR events=$EVENTS attempts=$ATTEMPTS lag=$LAG"
echo "lag returned to 0; completeness holds after SIGKILL + restart"
