#!/bin/sh
# final-snapshot.sh v1 — preStop hook of the nginx container of every benchmark arm (prodcluster campaign).
#
# Records the final Envoy request counters of a terminating arm pod, so requests it served after its last Prometheus
# scrape are not lost to the hourly load evidence (Codex Task 03 r18/r19). Order:
#   1. wait SETTLE s: the pod is already terminating, its endpoint is being removed; requests still arriving are served
#      and counted;
#   2. stop the inbound listeners (Envoy admin drain_listeners?inboundonly) and require listener_manager.listener_stopped
#      to increase; a request routed here afterwards is refused and shows as a k6 failure, never silently;
#   3. require the inbound connection gauges (accepted, and still in listener-filter inspection: an idle socket that has
#      sent no byte is only "pre" connected) and every inbound HTTP request gauge to be present and zero;
#   4. read the istio_* counters twice, 1 s apart, and require identical reads (telemetry settled);
#   5. push them renamed bench_final_istio_* (never part of ordinary istio_* sums), stamped with the capture time;
#   6. only after that upload succeeded, push bench_final_snapshot_receipt (series count, payload sha256, proxy uptime
#      and hot-restart epoch, capture time).
# Any failure: log it and exit 1 without a receipt; the collector then treats the pod's tail as unobserved.
# Bounded by wall time: SETTLE (6 s) + WAIT (10 s) of polling + <= 3 read pairs + two uploads of <= 5 s each; 51 s if
# every request timed out, ~12-20 s measured. The overlay keeps Envoy up 60 s after SIGTERM (terminationDrainDuration)
# and gives the pod a 75-s grace period; an upload cut short leaves no receipt.
set -u
ADMIN=${FINAL_SNAPSHOT_ADMIN:-http://localhost:15000}
LOG=${FINAL_SNAPSHOT_LOG:-/proc/1/fd/1}
SETTLE=${FINAL_SNAPSHOT_SETTLE:-6}
WAIT=${FINAL_SNAPSHOT_WAIT:-10}
TMP=${FINAL_SNAPSHOT_TMP:-/tmp}
URL=${SNAPSHOT_URL:-}

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) final-snapshot v1 pod=${POD_NAME:-?} $*" >> "$LOG" 2>/dev/null || true; }
fail() { log "FAILED: $*"; exit 1; }
# stat_sum REGEX -> "<sum> <number of matching stats>" (integer stats only); empty output if the read failed
stat_sum() {
  curl -sf -m 2 -G --data-urlencode "filter=$1" "$ADMIN/stats" |
    awk -F': ' 'NF == 2 && $2 ~ /^[0-9]+$/ { s += $2; n++ } END { print s + 0, n + 0 }'
}
# pick REGEX -> "<sum> <count>" of the integer stats in $out whose name matches (RE via ENVIRON: no escape processing)
pick() {
  printf '%s\n' "$out" | RE="$1" awk -F': ' '$1 ~ ENVIRON["RE"] && $2 ~ /^[0-9]+$/ { s += $2; n++ } END { print s + 0, n + 0 }'
}

[ -n "$URL" ] || fail "SNAPSHOT_URL not set"
[ -n "${POD_NAME:-}" ] && [ -n "${POD_UID:-}" ] && [ -n "${POD_NAMESPACE:-}" ] || fail "pod identity not set"
sleep "$SETTLE"

set -- $(stat_sum '^listener_manager\.listener_stopped$')
[ "${2:-0}" = 1 ] || fail "listener_manager.listener_stopped not readable"
stopped0=$1
curl -sf -m 2 -X POST "$ADMIN/drain_listeners?inboundonly" > /dev/null || fail "drain_listeners request failed"

GAUGES='^(listener_manager\.listener_stopped|listener\.0\.0\.0\.0_15006\.downstream_(pre_)?cx_active|http\.inbound_.*\.downstream_rq_active)$'
deadline=$(( $(date -u +%s) + WAIT ))
polls=0; state=""
while :; do
  out=$(curl -sf -m 2 -G --data-urlencode "filter=$GAUGES" "$ADMIN/stats") || out=""
  set -- $(pick '^listener_manager\.listener_stopped$'); stopped=$1; ns=$2
  set -- $(pick '^listener\.0\.0\.0\.0_15006\.downstream_cx_active$'); cx=$1; ncx=$2
  set -- $(pick '^listener\.0\.0\.0\.0_15006\.downstream_pre_cx_active$'); pre=$1; npre=$2
  set -- $(pick '^http\.inbound_.*\.downstream_rq_active$'); rq=$1; nrq=$2
  polls=$((polls + 1))
  state="stopped=$stopped/$stopped0 cx=$cx($ncx) pre=$pre($npre) rq=$rq($nrq) polls=$polls"
  [ "$ns" = 1 ] && [ "$ncx" = 1 ] && [ "$npre" = 1 ] && [ "$nrq" -ge 1 ] || fail "inbound gauges missing: $state"
  if [ "$stopped" -gt "$stopped0" ] && [ "$cx" = 0 ] && [ "$pre" = 0 ] && [ "$rq" = 0 ]; then
    break
  fi
  [ "$(date -u +%s)" -lt "$deadline" ] || fail "inbound side not quiescent within ${WAIT} s: $state"
  sleep 0.2
done

ok=""
for try in 1 2 3; do
  curl -sf -m 2 "$ADMIN/stats/prometheus" | grep '^istio_' > "$TMP/fs.a" || fail "counter read failed"
  sleep 1
  curl -sf -m 2 "$ADMIN/stats/prometheus" | grep '^istio_' > "$TMP/fs.b" || fail "counter read failed"
  if [ -s "$TMP/fs.b" ] && cmp -s "$TMP/fs.a" "$TMP/fs.b"; then ok=1; break; fi
done
[ -n "$ok" ] || fail "counters still changing after the drain"

set -- $(stat_sum '^server\.uptime$'); uptime=${1:-}; [ "${2:-0}" = 1 ] || fail "server.uptime not readable"
set -- $(stat_sum '^server\.hot_restart_epoch$'); epoch=${1:-}; [ "${2:-0}" = 1 ] || fail "server.hot_restart_epoch not readable"
ts=$(date -u +%s)
awk -v ms="${ts}000" '{ print "bench_final_" $0 " " ms }' "$TMP/fs.b" > "$TMP/fs.payload"
n=$(wc -l < "$TMP/fs.payload" | tr -d ' ')
h=$(sha256sum "$TMP/fs.payload" | cut -d' ' -f1)
q="extra_label=pod=$POD_NAME&extra_label=pod_uid=$POD_UID&extra_label=namespace=$POD_NAMESPACE&extra_label=snapshot_version=1"
curl -sf -m 5 --data-binary @"$TMP/fs.payload" "$URL?$q" > /dev/null || fail "payload upload failed"
printf 'bench_final_snapshot_receipt{series="%s",sha256="%s",proxy_uptime_s="%s",hot_restart_epoch="%s",settle_s="%s",polls="%s"} %s %s000\n' \
  "$n" "$h" "$uptime" "$epoch" "$SETTLE" "$polls" "$ts" "$ts" > "$TMP/fs.receipt"
curl -sf -m 5 --data-binary @"$TMP/fs.receipt" "$URL?$q" > /dev/null || fail "receipt upload failed"
log "ok series=$n sha256=$h capture=$ts polls=$polls"
