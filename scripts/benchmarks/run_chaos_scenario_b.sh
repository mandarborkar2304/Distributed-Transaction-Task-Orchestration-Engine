#!/usr/bin/env bash
# run_chaos_scenario_b.sh
# Executes 1,000-VU aggressive collision storm with 3-second Redis fault injection
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"

REDIS_CONTAINER=$(docker ps -q -f name=redis)
GATEWAY_URL="http://localhost:8080"

echo "================================================================="
echo "   SCENARIO B: 1,000-VU Collision Storm + Redis Fault Injection"
echo "================================================================="
echo "Target: ${GATEWAY_URL} | Redis Container: ${REDIS_CONTAINER}"
echo "Keyspace: 20 hot keys | Collision rate: 98%"
echo "Fault Injection: 3-second Docker pause on Redis @ t=20s"
echo ""

# Record memory/goroutines before
echo "==> Baseline gateway metrics:"
curl -sf "${GATEWAY_URL}/metrics" | grep -E "(go_goroutines|process_resident_memory_bytes|gateway_requests_total)" || true

# Launch k6 Scenario B in background
echo ""
echo "==> Launching k6 1,000-VU collision storm..."
k6 run \
  --summary-export "${RESULTS_DIR}/k6_scenario_b_collision.json" \
  -e SCENARIO=collision \
  "${SCRIPT_DIR}/k6_hardened.js" > "${RESULTS_DIR}/k6_scenario_b_collision.txt" 2>&1 &
K6_PID=$!

echo "  k6 started with PID: ${K6_PID}"
echo "  Ramping up to 1,000 VUs... Waiting 20 seconds for steady state..."
sleep 20

# Fault Injection: Pause Redis for 3 seconds
echo ""
echo "==> [CHAOS INJECTION] Pausing Redis container for 3 seconds..."
docker pause "${REDIS_CONTAINER}"
PAUSE_START=$(date +%s%N)
echo "  Redis container PAUSED at $(date -u '+%H:%M:%S.%3NZ')"

sleep 3

docker unpause "${REDIS_CONTAINER}"
PAUSE_END=$(date +%s%N)
PAUSE_MS=$(( (PAUSE_END - PAUSE_START) / 1000000 ))
echo "  Redis container UNPAUSED after ${PAUSE_MS} ms"
echo "==> [CHAOS RESTORED] Redis active. Observing gateway recovery..."

# Wait for k6 to finish
wait "${K6_PID}"
echo "==> k6 benchmark run finished."

# Record memory/goroutines after
echo ""
echo "==> Post-chaos gateway metrics:"
curl -sf "${GATEWAY_URL}/metrics" | grep -E "(go_goroutines|process_resident_memory_bytes|gateway_requests_total|gateway_idempotency_hits)" || true

echo ""
echo "================================================================="
echo "k6 Scenario B Summary Output:"
echo "================================================================="
cat "${RESULTS_DIR}/k6_scenario_b_collision.txt" | tail -n 35
