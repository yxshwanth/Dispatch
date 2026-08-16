#!/usr/bin/env bash
# Raise ingest rate until consumer lag grows or 202 falls below 100%.
# Then settle and assert completeness (0 aged events with zero attempts).
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"

ensure_vegeta

DURATION="${DURATION:-20s}"
WORKERS="${WORKERS:-64}"
RATES="${RATES:-100/s 200/s 400/s 800/s 1200/s 1600/s}"
# Lag at vegeta end above this, and higher than mid-run, counts as "growing".
LAG_GROW_THRESHOLD="${LAG_GROW_THRESHOLD:-200}"
SUB_URL="${SUB_URL:-http://webhook:8080/}"
REPORT_DIR="${REPORT_DIR:-/tmp/dispatch-ceiling}"
mkdir -p "$REPORT_DIR"

echo "==> creating tenant + healthy subscription"
API_KEY="$(create_tenant ceiling)"
TENANT_ID="$(tenant_id_from_key "$API_KEY")"
SUB_ID="$(create_subscription "$API_KEY" "$SUB_URL" load.test)"
echo "tenant_id=$TENANT_ID subscription_id=$SUB_ID"

last_good_rate=""
last_good_json=""
cliff_reason=""

for rate in $RATES; do
  tag="${rate//\//_}"
  json="$REPORT_DIR/${tag}.json"
  echo ""
  echo "======== sweep $rate ========"
  lag_before="$(ingest_lag)"
  echo "lag before=${lag_before:-unknown}"

  vegeta_attack "$API_KEY" "$rate" "$DURATION" "$json" "$WORKERS"

  lag_after="$(ingest_lag)"
  echo "lag after=${lag_after:-unknown}"

  success_ok=0
  if vegeta_success_ok "$json"; then
    success_ok=1
  fi

  lag_before_n="${lag_before:-0}"
  lag_after_n="${lag_after:-0}"
  [[ "$lag_before_n" =~ ^[0-9]+$ ]] || lag_before_n=0
  [[ "$lag_after_n" =~ ^[0-9]+$ ]] || lag_after_n=0

  lag_growing=0
  if (( lag_after_n > LAG_GROW_THRESHOLD && lag_after_n > lag_before_n + 50 )); then
    lag_growing=1
  fi

  {
    echo "rate=$rate success_ok=$success_ok lag_before=$lag_before_n lag_after=$lag_after_n lag_growing=$lag_growing"
  } | tee -a "$REPORT_DIR/sweep.txt"

  if [[ "$success_ok" != "1" ]]; then
    cliff_reason="202 below 100%"
    echo "==> cliff: $cliff_reason at $rate"
    break
  fi

  last_good_rate="$rate"
  last_good_json="$json"

  if [[ "$lag_growing" == "1" ]]; then
    cliff_reason="ingest lag grew (before=$lag_before_n after=$lag_after_n)"
    echo "==> cliff: $cliff_reason at $rate (this is X — 202 still 100%)"
    break
  fi
done

if [[ -z "$last_good_rate" ]]; then
  echo "ERROR: no rate accepted 100% 202" >&2
  exit 1
fi

echo ""
echo "==> ceiling X=$last_good_rate reason=${cliff_reason:-sweep finished within bounds}"

# If the last good run was not the cliff itself (202 dropped on the next step),
# re-run X once more so N / p99 / completeness share one measurement window.
if [[ -n "$cliff_reason" && "$cliff_reason" == "202 below 100%" ]]; then
  echo "==> re-measuring at $last_good_rate for the completeness window"
  last_good_json="$REPORT_DIR/final.json"
  vegeta_attack "$API_KEY" "$last_good_rate" "$DURATION" "$last_good_json" "$WORKERS"
  if ! vegeta_success_ok "$last_good_json"; then
    echo "ERROR: re-measure at $last_good_rate dropped 202" >&2
    exit 1
  fi
fi

settle_completeness
assert_completeness "$TENANT_ID"

python3 - "$last_good_json" "$REPORT_DIR/result.env" "$last_good_rate" "$TENANT_ID" "${cliff_reason:-within bounds}" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
codes = d.get("status_codes") or {}
p99 = d["latencies"]["99th"] / 1e6
open(sys.argv[2], "w").write(
    "CEILING_RATE={}\nINGEST_P99_MS={:.3f}\nINGEST_REQUESTS={}\nINGEST_N202={}\nINGEST_RATE={:.2f}\nINGEST_SUCCESS={:.6f}\nTENANT_ID={}\nCLIFF={}\n".format(
        sys.argv[3], p99, d["requests"], int(codes.get("202", 0) or 0),
        d["rate"], d["success"], sys.argv[4], json.dumps(sys.argv[5])
    )
)
print("ceiling {} ingest p99 {:.3f} ms requests {} 202 {} actual_rate {:.2f}/s".format(
    sys.argv[3], p99, d["requests"], int(codes.get("202", 0) or 0), d["rate"]
))
PY

EVENTS="$(psql_q "SELECT COUNT(*) FROM events WHERE tenant_id = '$TENANT_ID'::uuid;")"
echo "EVENTS=$EVENTS" >> "$REPORT_DIR/result.env"
LAG="$(ingest_lag)"
echo "INGEST_LAG=$LAG" >> "$REPORT_DIR/result.env"
echo "Ceiling report: $REPORT_DIR events=$EVENTS lag=$LAG"
