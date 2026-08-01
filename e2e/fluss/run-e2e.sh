#!/usr/bin/env bash
# E2E scenario runner for the SpiceAI Fluss connector.
#
# Runs on the host (Git Bash on Windows or any Linux shell) and drives
# everything through podman: the Fluss cluster via podman-compose, the
# deterministic producer and curl as one-shot containers, and spiced as a
# long-lived container with persistent .spice state.
#
# Usage:
#   ./run-e2e.sh build        # build spiced + producer images
#   ./run-e2e.sh all          # full suite: up -> scenarios 1..7 -> summary
#   ./run-e2e.sh s3           # run a single scenario (assumes prior state)
#   ./run-e2e.sh down|clean   # teardown (clean also removes the state volume)
#
# Scenario matrix:
#   s1  bootstrap        seed BEFORE spiced starts; verify full replay (append + CDC)
#   s2  realtime append  live appends visible via SQL within the latency budget
#   s3  cdc live         live insert/update/delete on the PK table; exact final state
#   s4  graceful resume  SIGTERM spiced; restart; exact counts (checkpoint resume)
#   s5  crash resume     SIGKILL spiced; restart; at-least-once appends, exact PK state
#   s6  tablet fault     restart a tablet server under continuous load; convergence
#   s7  chaos            pause coordinator + restart tablet under load; convergence

set -u
export MSYS_NO_PATHCONV=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/podman-compose.yaml"
# podman-compose on Windows is a native Python process — hand it a Windows
# path, not the MSYS /c/... form.
if command -v cygpath > /dev/null 2>&1; then
  COMPOSE_FILE="$(cygpath -m "${COMPOSE_FILE}")"
fi

SPICE_HTTP="${SPICE_HTTP:-http://localhost:18090}"
BOOTSTRAP="${FLUSS_BOOTSTRAP_SERVERS:-localhost:9123}"
CURL_IMAGE="docker.io/curlimages/curl:8.5.0"
PRODUCER_IMAGE="localhost/fluss-producer:dev"
SPICED_IMAGE="localhost/spiced-fluss:dev"

PASS_COUNT=0
FAIL_COUNT=0
FAILED_SCENARIOS=""

# Expected cumulative state, updated by scenarios so later scenarios can build
# on earlier ones when running `all`.
EXPECTED_ORDERS=0
EXPECTED_USERS=0

log()  { printf '\n\033[1;34m[e2e]\033[0m %s\n' "$*"; }
pass() { printf '\033[1;32m  PASS\033[0m %s\n' "$*"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { printf '\033[1;31m  FAIL\033[0m %s\n' "$*"; FAIL_COUNT=$((FAIL_COUNT + 1)); FAILED_SCENARIOS="${FAILED_SCENARIOS} ${CURRENT_SCENARIO:-?}"; }

compose() { podman-compose -f "${COMPOSE_FILE}" "$@"; }

producer() {
  podman run --rm --network host -e FLUSS_BOOTSTRAP_SERVERS="${BOOTSTRAP}" "${PRODUCER_IMAGE}" "$@"
}

curl_c() {
  podman run --rm --network host "${CURL_IMAGE}" "$@"
}

# Run a SQL statement against the SpiceAI HTTP API; prints the JSON response.
sql() {
  curl_c -sf -X POST "${SPICE_HTTP}/v1/sql" \
    -H "Content-Type: application/json" \
    -d "{\"sql\": \"$1\", \"parameters\": []}"
}

# Extract the first integer field named `cnt` from a SQL JSON response.
extract_cnt() { grep -o '"cnt":[0-9]*' | head -1 | cut -d: -f2; }

get_count() {
  sql "SELECT COUNT(*) AS cnt FROM $1" 2>/dev/null | extract_cnt
}

# wait_count <table> <op> <expected> <timeout_secs>  (op: -eq or -ge)
wait_count() {
  local table="$1" op="$2" expected="$3" timeout="$4" start now count
  start=$(date +%s)
  while true; do
    count=$(get_count "${table}")
    if [ -n "${count}" ] && [ "${count}" "${op}" "${expected}" ]; then
      echo "${count}"
      return 0
    fi
    now=$(date +%s)
    if [ $((now - start)) -ge "${timeout}" ]; then
      echo "${count:-none}"
      return 1
    fi
    sleep 2
  done
}

wait_spice_healthy() {
  local timeout="${1:-180}" start now
  start=$(date +%s)
  while true; do
    if curl_c -sf "${SPICE_HTTP}/health" > /dev/null 2>&1; then
      return 0
    fi
    now=$(date +%s)
    [ $((now - start)) -ge "${timeout}" ] && return 1
    sleep 2
  done
}

# Retry `producer setup` until the coordinator accepts it — doubles as the
# cluster readiness probe (setup is idempotent).
wait_cluster_ready() {
  local timeout="${1:-180}" start now
  start=$(date +%s)
  log "waiting for Fluss cluster (setup probe)..."
  while true; do
    if producer setup --buckets 3 > /dev/null 2>&1; then
      log "Fluss cluster ready"
      return 0
    fi
    now=$(date +%s)
    [ $((now - start)) -ge "${timeout}" ] && { log "cluster not ready after ${timeout}s"; return 1; }
    sleep 3
  done
}

build_images() {
  log "building spiced image (first build takes a while; cached afterwards)"
  podman build -f "${SCRIPT_DIR}/Containerfile.spiced" -t "${SPICED_IMAGE}" "${REPO_ROOT}" || return 1
  log "building producer image"
  podman build -f "${SCRIPT_DIR}/Containerfile.producer" -t "${PRODUCER_IMAGE}" "${SCRIPT_DIR}/producer" || return 1
}

up_cluster() {
  log "starting Fluss cluster (zookeeper + coordinator + 2 tablet servers)"
  compose up -d zookeeper coordinator tablet-server-0 tablet-server-1 || return 1
  wait_cluster_ready 180
}

start_spiced() {
  compose --profile spice up -d spiced || return 1
  wait_spice_healthy 180
}

down() { compose --profile spice down; }

# Force-remove by name: podman-compose down can fail on depends_on ordering,
# leaving stale containers that then collide on the next up.
clean() {
  compose --profile spice down -v > /dev/null 2>&1 || true
  podman rm -f spiced-fluss fluss-mixed \
    fluss-tablet-server-1 fluss-tablet-server-0 fluss-coordinator fluss-zookeeper \
    > /dev/null 2>&1 || true
  podman volume rm -f fluss_spice-state > /dev/null 2>&1 || true
}

# --- Scenarios -------------------------------------------------------------

s1_bootstrap() {
  CURRENT_SCENARIO=s1
  log "s1: bootstrap — seed BEFORE spiced starts, then verify full replay"
  # orders: ids [0..20) ; users: insert 1..10, update 1..3, delete 9..10 -> 8 rows
  producer append --count 20 --start-id 0 || { fail "s1 producer append"; return; }
  producer cdc --inserts 10 --updates 3 --deletes 2 --start-id 1 || { fail "s1 producer cdc"; return; }
  EXPECTED_ORDERS=20
  EXPECTED_USERS=8

  start_spiced || { fail "s1 spiced did not become healthy"; return; }

  local count
  count=$(wait_count fluss_orders -eq ${EXPECTED_ORDERS} 90) \
    && pass "s1 orders bootstrap count=${count}" \
    || fail "s1 orders bootstrap expected ${EXPECTED_ORDERS}, got ${count}"

  count=$(wait_count fluss_users -eq ${EXPECTED_USERS} 90) \
    && pass "s1 users bootstrap count=${count}" \
    || fail "s1 users bootstrap expected ${EXPECTED_USERS}, got ${count}"

  # user 1 was updated: name user_1_v2, score 1010; users 9,10 deleted.
  local resp
  resp=$(sql "SELECT name FROM fluss_users WHERE user_id = 1")
  echo "${resp}" | grep -q '"name":"user_1_v2"' \
    && pass "s1 update applied during bootstrap (user_1_v2)" \
    || fail "s1 update not applied: ${resp}"

  resp=$(sql "SELECT COUNT(*) AS cnt FROM fluss_users WHERE user_id IN (9, 10)")
  [ "$(echo "${resp}" | extract_cnt)" = "0" ] \
    && pass "s1 deletes applied during bootstrap (9,10 absent)" \
    || fail "s1 deletes not applied: ${resp}"
}

s2_realtime_append() {
  CURRENT_SCENARIO=s2
  log "s2: realtime append — 100 live rows must appear within the latency budget"
  producer append --count 100 --start-id 1000 --rate 50 || { fail "s2 producer"; return; }
  EXPECTED_ORDERS=$((EXPECTED_ORDERS + 100))

  local start end count
  start=$(date +%s)
  count=$(wait_count fluss_orders -eq ${EXPECTED_ORDERS} 30)
  end=$(date +%s)
  if [ "${count}" = "${EXPECTED_ORDERS}" ]; then
    pass "s2 realtime append visible in $((end - start))s (count=${count})"
  else
    fail "s2 expected ${EXPECTED_ORDERS}, got ${count} after 30s"
  fi
}

s3_cdc_live() {
  CURRENT_SCENARIO=s3
  log "s3: live CDC — insert 40, update 5, delete 5 on the PK table"
  # ids [100..140): update 100..104, delete 135..139 -> +35 rows
  producer cdc --inserts 40 --updates 5 --deletes 5 --start-id 100 || { fail "s3 producer"; return; }
  EXPECTED_USERS=$((EXPECTED_USERS + 35))

  local count resp
  count=$(wait_count fluss_users -eq ${EXPECTED_USERS} 30) \
    && pass "s3 users count=${count}" \
    || { fail "s3 expected ${EXPECTED_USERS} users, got ${count}"; return; }

  resp=$(sql "SELECT name, score FROM fluss_users WHERE user_id = 100")
  echo "${resp}" | grep -q '"name":"user_100_v2"' \
    && pass "s3 live update applied (user_100_v2)" \
    || fail "s3 live update not applied: ${resp}"

  resp=$(sql "SELECT COUNT(*) AS cnt FROM fluss_users WHERE user_id = 139")
  [ "$(echo "${resp}" | extract_cnt)" = "0" ] \
    && pass "s3 live delete applied (139 absent)" \
    || fail "s3 live delete not applied: ${resp}"
}

s4_graceful_resume() {
  CURRENT_SCENARIO=s4
  log "s4: graceful restart — SIGTERM spiced, restart, verify exact checkpoint resume"
  podman stop spiced-fluss || { fail "s4 podman stop"; return; }
  podman start spiced-fluss || { fail "s4 restart"; return; }
  wait_spice_healthy 180 || { fail "s4 spiced not healthy after restart"; return; }

  local count
  count=$(wait_count fluss_orders -eq ${EXPECTED_ORDERS} 90) \
    && pass "s4 orders exact after graceful restart (count=${count}, no duplicates)" \
    || fail "s4 orders expected exactly ${EXPECTED_ORDERS}, got ${count} (checkpoint resume broken?)"

  count=$(wait_count fluss_users -eq ${EXPECTED_USERS} 90) \
    && pass "s4 users exact after graceful restart (count=${count})" \
    || fail "s4 users expected ${EXPECTED_USERS}, got ${count}"

  # Stream must still be live after resume.
  producer append --count 10 --start-id 2000 || { fail "s4 post-restart producer"; return; }
  EXPECTED_ORDERS=$((EXPECTED_ORDERS + 10))
  count=$(wait_count fluss_orders -eq ${EXPECTED_ORDERS} 30) \
    && pass "s4 stream live after resume (count=${count})" \
    || fail "s4 stream dead after resume: expected ${EXPECTED_ORDERS}, got ${count}"
}

s5_crash_resume() {
  CURRENT_SCENARIO=s5
  log "s5: crash — SIGKILL spiced, restart, verify at-least-once resume"
  podman kill spiced-fluss || { fail "s5 podman kill"; return; }
  podman start spiced-fluss || { fail "s5 restart"; return; }
  wait_spice_healthy 180 || { fail "s5 spiced not healthy after crash"; return; }

  # Appends are at-least-once across a hard crash (offsets commit after the
  # batch is applied); PK state is idempotent so it must be exact.
  local count
  count=$(wait_count fluss_orders -ge ${EXPECTED_ORDERS} 90) \
    && pass "s5 orders at-least-once after crash (count=${count} >= ${EXPECTED_ORDERS})" \
    || fail "s5 orders lost data after crash: expected >= ${EXPECTED_ORDERS}, got ${count}"
  # Track actual so later scenarios use the real base.
  [ -n "${count}" ] && [ "${count}" != "none" ] && EXPECTED_ORDERS=${count}

  count=$(wait_count fluss_users -eq ${EXPECTED_USERS} 90) \
    && pass "s5 users exact after crash (PK idempotent, count=${count})" \
    || fail "s5 users expected ${EXPECTED_USERS}, got ${count}"

  producer append --count 5 --start-id 3000 || { fail "s5 post-crash producer"; return; }
  EXPECTED_ORDERS=$((EXPECTED_ORDERS + 5))
  count=$(wait_count fluss_orders -ge ${EXPECTED_ORDERS} 30) \
    && pass "s5 stream live after crash resume (count=${count})" \
    || fail "s5 stream dead after crash resume"
  [ -n "${count}" ] && [ "${count}" != "none" ] && EXPECTED_ORDERS=${count}
}

# Runs the mixed workload in a named background container and returns (echoes)
# its appended count once it exits.
run_mixed_bg() {
  podman rm -f fluss-mixed > /dev/null 2>&1 || true
  podman run -d --name fluss-mixed --network host \
    -e FLUSS_BOOTSTRAP_SERVERS="${BOOTSTRAP}" \
    "${PRODUCER_IMAGE}" mixed --duration-secs "$1" --rate "$2" --start-id "$3" > /dev/null
}

mixed_appended() {
  podman logs fluss-mixed 2>&1 | grep -o 'appended=[0-9]*' | tail -1 | cut -d= -f2
}

s6_tablet_fault() {
  CURRENT_SCENARIO=s6
  log "s6: tablet fault — restart tablet-server-0 under continuous load"
  run_mixed_bg 40 10 1000000

  sleep 8
  log "s6 restarting tablet-server-0..."
  podman restart -t 30 fluss-tablet-server-0 || { fail "s6 tablet restart"; return; }

  podman wait fluss-mixed > /dev/null 2>&1
  local appended
  appended=$(mixed_appended)
  podman logs fluss-mixed 2>&1 | tail -3
  [ -n "${appended}" ] || { fail "s6 could not read producer stats"; return; }
  pass "s6 producer survived tablet restart (appended=${appended})"

  EXPECTED_ORDERS=$((EXPECTED_ORDERS + appended))
  local count
  count=$(wait_count fluss_orders -ge ${EXPECTED_ORDERS} 120) \
    && pass "s6 converged after tablet fault (count=${count} >= ${EXPECTED_ORDERS})" \
    || fail "s6 did not converge: expected >= ${EXPECTED_ORDERS}, got ${count}"
  [ -n "${count}" ] && [ "${count}" != "none" ] && EXPECTED_ORDERS=${count}

  curl_c -sf "${SPICE_HTTP}/health" > /dev/null \
    && pass "s6 spiced healthy after tablet fault" \
    || fail "s6 spiced unhealthy after tablet fault"
}

s7_chaos() {
  CURRENT_SCENARIO=s7
  log "s7: chaos — pause coordinator + restart tablet-server-1 under load"
  run_mixed_bg 45 10 2000000

  sleep 5
  log "s7 pausing coordinator for 15s..."
  podman pause fluss-coordinator || { fail "s7 pause"; return; }
  sleep 15
  podman unpause fluss-coordinator || { fail "s7 unpause"; return; }

  sleep 3
  log "s7 restarting tablet-server-1..."
  podman restart -t 30 fluss-tablet-server-1 || { fail "s7 tablet restart"; return; }

  podman wait fluss-mixed > /dev/null 2>&1
  local appended
  appended=$(mixed_appended)
  podman logs fluss-mixed 2>&1 | tail -3
  [ -n "${appended}" ] || { fail "s7 could not read producer stats"; return; }
  pass "s7 producer survived chaos (appended=${appended})"

  EXPECTED_ORDERS=$((EXPECTED_ORDERS + appended))
  local count
  count=$(wait_count fluss_orders -ge ${EXPECTED_ORDERS} 120) \
    && pass "s7 converged after chaos (count=${count} >= ${EXPECTED_ORDERS})" \
    || fail "s7 did not converge: expected >= ${EXPECTED_ORDERS}, got ${count}"

  curl_c -sf "${SPICE_HTTP}/health" > /dev/null \
    && pass "s7 spiced healthy after chaos" \
    || fail "s7 spiced unhealthy after chaos"
}

summary() {
  printf '\n\033[1m=== E2E summary: %d passed, %d failed ===\033[0m\n' "${PASS_COUNT}" "${FAIL_COUNT}"
  [ "${FAIL_COUNT}" -gt 0 ] && printf 'Failed in:%s\n' "${FAILED_SCENARIOS}"
  [ "${FAIL_COUNT}" -eq 0 ]
}

all() {
  clean
  up_cluster || exit 1
  s1_bootstrap
  s2_realtime_append
  s3_cdc_live
  s4_graceful_resume
  s5_crash_resume
  s6_tablet_fault
  s7_chaos
  summary
}

case "${1:-all}" in
  build) build_images ;;
  up) up_cluster ;;
  spice) start_spiced ;;
  all) all ;;
  s1) s1_bootstrap; summary ;;
  s2) s2_realtime_append; summary ;;
  s3) s3_cdc_live; summary ;;
  s4) s4_graceful_resume; summary ;;
  s5) s5_crash_resume; summary ;;
  s6) s6_tablet_fault; summary ;;
  s7) s7_chaos; summary ;;
  down) down ;;
  clean) clean ;;
  *) echo "usage: $0 {build|up|spice|all|s1..s7|down|clean}"; exit 2 ;;
esac
