# Shared helpers for Dispatch load / isolation / completeness scripts.
# Sourced, not executed. Callers should `set -euo pipefail`.

LOAD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$LOAD_DIR/../.." && pwd)"
cd "$ROOT"

export DOCKER_HOST="${DOCKER_HOST:-unix:///var/run/docker.sock}"

API_URL="${API_URL:-http://localhost:8080}"

ensure_vegeta() {
  export PATH="$(go env GOPATH)/bin:$PATH"
  if command -v vegeta >/dev/null 2>&1; then
    return 0
  fi
  echo "installing vegeta..."
  go install github.com/tsenart/vegeta/v12@v12.12.0
}

json_field() {
  python3 -c 'import sys,json; print(json.load(sys.stdin)['"$1"'])'
}

psql_t() {
  docker compose exec -T postgres psql -U dispatch -d dispatch -v ON_ERROR_STOP=1 "$@"
}

psql_q() {
  docker compose exec -T postgres psql -U dispatch -d dispatch -v ON_ERROR_STOP=1 -tAc "$1" | tr -d '[:space:]'
}

ingest_lag() {
  local out
  out="$(docker compose exec -T redpanda rpk group describe dispatch-ingest 2>/dev/null || true)"
  if [[ -z "$out" ]]; then
    echo ""
    return 0
  fi
  echo "$out" | awk '
    $1 == "TOTAL-LAG" && $2 ~ /^[0-9]+$/ { print $2; found=1 }
    END { if (!found) print "" }
  '
}

wait_ingest_lag_zero() {
  local timeout="${1:-180}"
  local i lag
  echo "==> waiting for ingest lag = 0 (timeout ${timeout}s)"
  for ((i = 0; i < timeout; i += 2)); do
    lag="$(ingest_lag)"
    if [[ "$lag" == "0" ]]; then
      echo "ingest lag=0"
      return 0
    fi
    echo "  lag=${lag:-unknown}  t=${i}s"
    sleep 2
  done
  echo "ERROR: ingest lag did not return to 0 within ${timeout}s (last=${lag:-unknown})" >&2
  return 1
}

wait_all_attempted() {
  local tenant="$1"
  local timeout="${2:-180}"
  local i missing
  echo "==> waiting for every event to have a delivery_attempt (timeout ${timeout}s)"
  for ((i = 0; i < timeout; i += 3)); do
    missing="$(psql_q "
      SELECT COUNT(*) FROM (
        SELECT e.id
        FROM events e
        LEFT JOIN delivery_attempts da ON da.event_id = e.id
        WHERE e.tenant_id = '$tenant'::uuid
        GROUP BY e.id
        HAVING COUNT(da.id) = 0
      ) missing;
    ")"
    if [[ "$missing" == "0" ]]; then
      echo "all events have at least one attempt"
      return 0
    fi
    echo "  missing_attempts=$missing  t=${i}s"
    sleep 3
  done
  echo "ERROR: $missing event(s) still have zero attempts after ${timeout}s" >&2
  return 1
}

settle_completeness() {
  local extra="${SETTLE_SECONDS:-35}"
  wait_ingest_lag_zero "${LAG_TIMEOUT:-180}"
  echo "==> settle ${extra}s so completeness age predicate (30s) is valid"
  sleep "$extra"
}

# Fails if any event older than 30s has zero delivery_attempts.
# When tenant_id is set, counts and the orphan check are scoped to that tenant.
assert_completeness() {
  local tenant="${1:-}"
  local filter=""
  if [[ -n "$tenant" ]]; then
    filter="AND e.tenant_id = '$tenant'::uuid"
  fi

  echo "==> completeness report"
  if [[ -n "$tenant" ]]; then
    psql_t -v tenant_id="$tenant" < "$LOAD_DIR/completeness.sql"
  else
    psql_t < "$LOAD_DIR/completeness.sql"
  fi

  local orphans
  orphans="$(psql_q "
    SELECT COUNT(*) FROM (
      SELECT e.id
      FROM events e
      LEFT JOIN delivery_attempts da ON da.event_id = e.id
      WHERE e.created_at < NOW() - INTERVAL '30 seconds'
        $filter
      GROUP BY e.id
      HAVING COUNT(da.id) = 0
    ) orphans;
  ")"
  orphans="$(echo "$orphans" | tr -d '[:space:]')"
  echo "orphan_count=$orphans"
  if [[ "$orphans" != "0" ]]; then
    echo "ERROR: $orphans aged event(s) with zero delivery_attempts" >&2
    return 1
  fi
  echo "completeness ok: 0 aged events with zero attempts"
}

create_tenant() {
  local name="${1:-loadtest}"
  local json
  json="$(curl -sS -X POST "$API_URL/v1/tenants" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\"}")"
  python3 -c 'import sys,json; print(json.load(sys.stdin)["api_key"])' <<<"$json"
}

create_subscription() {
  local api_key="$1"
  local url="$2"
  local event_type="${3:-load.test}"
  local json
  json="$(curl -sS -X POST "$API_URL/v1/subscriptions" \
    -H "Authorization: Bearer $api_key" \
    -H 'Content-Type: application/json' \
    -d "{\"url\":\"$url\",\"event_types\":[\"$event_type\"]}")"
  python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' <<<"$json"
}

tenant_id_from_key() {
  local api_key="$1"
  local hash
  hash="$(python3 -c 'import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' "$api_key")"
  psql_q "SELECT id FROM tenants WHERE api_key_hash = '$hash' LIMIT 1;"
}

post_event() {
  local api_key="$1"
  local body_file
  body_file="$(mktemp)"
  printf '%s' '{"event_type":"load.test","payload":{"n":1}}' >"$body_file"
  curl -sS -o /tmp/dispatch-event-resp.json -w "%{http_code}" \
    -X POST "$API_URL/v1/events" \
    -H "Authorization: Bearer $api_key" \
    -H 'Content-Type: application/json' \
    --data-binary @"$body_file"
  rm -f "$body_file"
}

# vegeta_attack API_KEY RATE DURATION OUT_JSON [workers]
# Writes a text report next to OUT_JSON (.txt) and prints a one-line summary:
# success_ratio p99_ms requests n202 rate
vegeta_attack() {
  local api_key="$1"
  local rate="$2"
  local duration="$3"
  local out_json="$4"
  local workers="${5:-64}"
  local body_file
  body_file="$(mktemp)"
  printf '%s' '{"event_type":"load.test","payload":{"n":1}}' >"$body_file"
  mkdir -p "$(dirname "$out_json")"
  local bin="${out_json%.json}.bin"
  local txt="${out_json%.json}.txt"

  echo "==> vegeta attack rate=$rate duration=$duration workers=$workers"
  echo "POST $API_URL/v1/events" | vegeta attack \
    -rate="$rate" \
    -duration="$duration" \
    -workers="$workers" \
    -body="$body_file" \
    -header="Authorization: Bearer $api_key" \
    -header="Content-Type: application/json" \
    | tee "$bin" | vegeta report | tee "$txt"
  vegeta report -type=json <"$bin" >"$out_json"
  rm -f "$body_file"

  python3 - "$out_json" <<'PY'
import json, sys
p = sys.argv[1]
with open(p) as f:
    d = json.load(f)
codes = d.get("status_codes") or {}
n202 = int(codes.get("202", 0) or 0)
p99_ms = d["latencies"]["99th"] / 1e6
print(f"summary success={d['success']:.6f} p99_ms={p99_ms:.3f} requests={d['requests']} n202={n202} rate={d['rate']:.2f}")
PY
}

vegeta_success_ok() {
  local json="$1"
  python3 - "$json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
codes = d.get("status_codes") or {}
n202 = int(codes.get("202", 0) or 0)
ok = d["success"] >= 0.9999 and n202 == d["requests"]
sys.exit(0 if ok else 1)
PY
}
