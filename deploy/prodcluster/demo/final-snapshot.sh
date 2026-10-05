#!/bin/sh
# final-snapshot.sh v2 — preStop hook of the nginx container of every benchmark arm (prodcluster campaign).
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
#   5. push them renamed bench_final_istio_* (never part of ordinary istio_* sums), stamped with the capture second;
#   6. only after that upload succeeded, push bench_final_snapshot_receipt (series count, payload sha256, a CANONICAL
#      sha256 the collector recomputes from the stored series, pod UID, proxy uptime and hot-restart epoch).
# v2 (Codex r20): every curl's own exit status is checked (a truncated read can no longer pass through a pipe), and
# the canonical digest covers the integer counter families (*_total, *_bucket, *_count): one line per series,
# 'name{k="v",...} value' with labels sorted by key (C collation) and empty-valued labels dropped (VictoriaMetrics
# drops them), lines sorted, sha256.
# Any failure: log it and exit 1 without a receipt; the collector then treats the pod's tail as unobserved.
# Bounded by wall time: SETTLE (6 s) + WAIT (10 s) of polling + <= 3 read pairs + two uploads of <= 5 s each; the
# calculated budget of these operations is 51 s if every request timed out (shell work and scheduling come on top);
# ~12-20 s were measured on prodcluster. The overlay keeps Envoy up 60 s after SIGTERM (terminationDrainDuration)
# and gives the pod a 75-s grace period; an upload cut short leaves no receipt.
set -u
ADMIN=${FINAL_SNAPSHOT_ADMIN:-http://localhost:15000}
LOG=${FINAL_SNAPSHOT_LOG:-/proc/1/fd/1}
SETTLE=${FINAL_SNAPSHOT_SETTLE:-6}
WAIT=${FINAL_SNAPSHOT_WAIT:-10}
TMP=${FINAL_SNAPSHOT_TMP:-/tmp}
URL=${SNAPSHOT_URL:-}

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) final-snapshot v2 pod=${POD_NAME:-?} $*" >> "$LOG" 2>/dev/null || true; }
fail() { log "FAILED: $*"; exit 1; }
# stat_sum REGEX -> "<sum> <number of matching stats>" (integer stats only); empty output if the read failed
stat_sum() {
  curl -sf -m 2 -o "$TMP/fs.stat" -G --data-urlencode "filter=$1" "$ADMIN/stats" || { echo "0 0"; return; }
  awk -F': ' 'NF == 2 && $2 ~ /^[0-9]+$/ { s += $2; n++ } END { print s + 0, n + 0 }' "$TMP/fs.stat"
}
# canonical: integer counter families only, labels sorted by key, empty values dropped; exit 3 on anything unparsable
canonical() {
  LC_ALL=C awk '
    { line = $0; b = index(line, "{"); e = index(line, "} ")
      if (b == 0 || e < b) { bad = 1; next }
      name = substr(line, 1, b - 1); rest = substr(line, b + 1, e - b - 1); value = substr(line, e + 2)
      if (name !~ /(_total|_bucket|_count)$/) next
      if (value !~ /^[0-9]+$/) { bad = 1; next }
      n = 0
      while (length(rest) > 0) {
        if (!match(rest, /^[a-zA-Z_][a-zA-Z0-9_]*="[^"]*"/)) { bad = 1; break }
        kv = substr(rest, 1, RLENGTH); rest = substr(rest, RLENGTH + 1)
        if (substr(rest, 1, 1) == ",") rest = substr(rest, 2)
        q = index(kv, "="); k = substr(kv, 1, q - 1); v = substr(kv, q + 2, length(kv) - q - 2)
        if (v == "") continue
        n++; K[n] = k; V[n] = v
      }
      for (i = 2; i <= n; i++) { kk = K[i]; vv = V[i]; j = i - 1
        while (j >= 1 && K[j] > kk) { K[j + 1] = K[j]; V[j + 1] = V[j]; j-- }
        K[j + 1] = kk; V[j + 1] = vv }
      out = ""; for (i = 1; i <= n; i++) out = out (i > 1 ? "," : "") K[i] "=\"" V[i] "\""
      print name "{" out "} " value }
    END { if (bad) exit 3 }' "$1" > "$TMP/fs.canon.unsorted" || return 3
  LC_ALL=C sort "$TMP/fs.canon.unsorted"
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
read_counters() {  # $1 = output file; curl's own status is checked before filtering
  curl -sf -m 2 -o "$TMP/fs.raw" "$ADMIN/stats/prometheus" || fail "counter read failed (curl)"
  grep '^istio_' "$TMP/fs.raw" > "$1" || fail "counter read failed (no istio_ lines)"
}
for try in 1 2 3; do
  read_counters "$TMP/fs.a"
  sleep 1
  read_counters "$TMP/fs.b"
  if [ -s "$TMP/fs.b" ] && cmp -s "$TMP/fs.a" "$TMP/fs.b"; then ok=1; break; fi
done
[ -n "$ok" ] || fail "counters still changing after the drain"

set -- $(stat_sum '^server\.uptime$'); uptime=${1:-}; [ "${2:-0}" = 1 ] || fail "server.uptime not readable"
set -- $(stat_sum '^server\.hot_restart_epoch$'); epoch=${1:-}; [ "${2:-0}" = 1 ] || fail "server.hot_restart_epoch not readable"
ts=$(date -u +%s)
awk -v ms="${ts}000" '{ print "bench_final_" $0 " " ms }' "$TMP/fs.b" > "$TMP/fs.payload" || fail "payload generation failed"
canonical "$TMP/fs.b" > "$TMP/fs.canonical" || fail "counters not canonicalizable"
n=$(wc -l < "$TMP/fs.payload" | tr -d ' ')
nc=$(wc -l < "$TMP/fs.canonical" | tr -d ' ')
[ "$n" -gt 0 ] && [ "$nc" -gt 0 ] || fail "empty payload"
h=$(sha256sum "$TMP/fs.payload") || fail "sha256 failed"; h=${h%% *}
hc=$(sha256sum "$TMP/fs.canonical") || fail "sha256 failed"; hc=${hc%% *}
q="extra_label=pod=$POD_NAME&extra_label=pod_uid=$POD_UID&extra_label=namespace=$POD_NAMESPACE&extra_label=snapshot_version=2"
curl -sf -m 5 -o /dev/null --data-binary @"$TMP/fs.payload" "$URL?$q" || fail "payload upload failed"
printf 'bench_final_snapshot_receipt{series="%s",sha256="%s",canonical_series="%s",canonical_sha256="%s",proxy_uptime_s="%s",hot_restart_epoch="%s",settle_s="%s",polls="%s"} %s %s000\n' \
  "$n" "$h" "$nc" "$hc" "$uptime" "$epoch" "$SETTLE" "$polls" "$ts" "$ts" > "$TMP/fs.receipt" || fail "receipt generation failed"
curl -sf -m 5 -o /dev/null --data-binary @"$TMP/fs.receipt" "$URL?$q" || fail "receipt upload failed"
log "ok series=$n canonical=$nc canonical_sha256=$hc capture=$ts polls=$polls"
