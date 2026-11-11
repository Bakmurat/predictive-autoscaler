#!/usr/bin/env bash
# C2.8 demonstration-stage smoke (LIVE-DEMO-PLAN sections 5 and 6). The kind bed is for functionality, never evidence.
#
#   deploy/eval-c28/smoke/demo-kind.sh --render-only   OFFLINE (no cluster, no docker, no network; scripts/verify.sh
#                                                      runs it): validate the demonstration params stage and its
#                                                      refusals, render the stage, audit names, run labels,
#                                                      sidecar-free pods, RBAC, writers and residency, and run the REAL
#                                                      generator script under node on a virtual clock through
#                                                      `python -m c28 demo-reduce` (skipped with a message when node is
#                                                      not installed)
#   deploy/eval-c28/smoke/demo-kind.sh --k6-local      needs k6 0.57 on PATH, nothing else: runs the generator with a
#                                                      short smoke segment against a dummy HTTP server on 127.0.0.1
#                                                      for about three minutes, twice (two incarnations), and checks
#                                                      its exit status, its records and the server's own counts
#   deploy/eval-c28/smoke/demo-kind.sh                 FULL mode on a throwaway kind cluster with its own kubeconfig:
#                                                      reserve two workers for an "owner" run, apply the rendered
#                                                      demonstration objects of ANOTHER run id, run the generator from
#                                                      the reviewed k6 image by digest with a 3-minute smoke segment
#                                                      (a StatefulSet with one replica: at most one generator pod),
#                                                      and check placement, the single generator pod, a planned
#                                                      restart with exit 0, the records in the pod log, and the
#                                                      reduction against the ingress's raw samples
#   deploy/eval-c28/smoke/demo-kind.sh --images images.json
#                                                      FULL mode with the reviewed images of a c28-images/v1 file
#                                                      (fixture and generator): pulled BY DIGEST on the nodes from
#                                                      their registries, no build, no local registry
#   deploy/eval-c28/smoke/demo-kind.sh --replacement [--images images.json]
#                                                      FULL mode as the generator's RESTART AND REPLACEMENT smoke, at
#                                                      the schedule's daily peak (a smoke-only origin) and a higher
#                                                      rate (DEMO_CAPACITY_RPS per replica, default 17.5), with the
#                                                      generator's remote write into the cluster's VictoriaMetrics:
#                                                        1. three consecutive planned restarts (the k6 container
#                                                           exits 0 at its segment end, the kubelet restarts it in
#                                                           the same pod): the gap of each, back-off included, and
#                                                           resume without replay;
#                                                        2. `kubectl delete pod` with the default grace period
#                                                           (never forced): the replacement pod exists only after the
#                                                           old pod object is gone, never two pod objects at once;
#                                                        3. one target stops answering (its pods' responses are
#                                                           dropped on the workload node, or with
#                                                           DEMO_BLACKHOLE=requests the packets to them, so every
#                                                           request to it runs into the timeout); the generator
#                                                           container's memory is read from its cgroup on the node
#                                                           against its limit, and the ingress container's too, with
#                                                           its restart count and OOM kills;
#                                                        4. the reducer over the log of the WHOLE smoke (collected
#                                                           continuously: the API keeps only the current and the
#                                                           previous container's log), with its verdicts per phase.
#                                                      Its analysis (smoke/demo_kind.py) is self-tested offline by
#                                                      --render-only. The work directory keeps the evidence when
#                                                      WORK is given.
#   KEEP=1 deploy/eval-c28/smoke/demo-kind.sh          keep the cluster for inspection
#
# The full mode only ever talks to its own cluster: the kubeconfig in its work directory, checked (context, local
# endpoint and cluster UID) before every step, as smoke/kind.sh does. It installs the PredictiveAutoscaler CRD only,
# NOT the operator or the forecaster: the two PredictiveAutoscalers are created and stay unreconciled. The Active
# transition, the qualification and the scale write are not exercised here.
# Needs (full mode): docker, kind, kubectl, python3 with PyYAML.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
HARNESS="$ROOT/deploy/eval-c28/harness"
PARAMS="$ROOT/deploy/eval-c28/params/kind-demo.json"
SCRIPT="$ROOT/deploy/eval-c28/load/c28-demo.js"
SIM="$HARNESS/tests/demo_sim.js"
PY="${PYTHON:-python3}"
case "$PY" in */*) PY="$(cd "$(dirname "$PY")" && pwd)/$(basename "$PY")" ;; esac
c28() { (cd "$HARNESS" && KUBECONFIG=/dev/null "$PY" -m c28 "$@"); }
SMOKE_DIR="$ROOT/deploy/eval-c28/smoke"
MODE=full IMAGES_FILE="" REPLACEMENT=0
while [ $# -gt 0 ]; do
  case "$1" in
    --render-only) MODE=render ;;
    --k6-local) MODE=k6 ;;
    --images) [ $# -ge 2 ] || { echo "demo smoke: --images needs a file" >&2; exit 2; }; IMAGES_FILE="$2"; shift ;;
    --images=*) IMAGES_FILE="${1#--images=}" ;;
    --replacement) REPLACEMENT=1 ;;
    *) echo "demo smoke: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
if [ -n "$IMAGES_FILE" ]; then
  [ -f "$IMAGES_FILE" ] || { echo "demo smoke: --images $IMAGES_FILE is not a file" >&2; exit 2; }
  IMAGES_FILE="$(cd "$(dirname "$IMAGES_FILE")" && pwd)/$(basename "$IMAGES_FILE")"
fi

# images_read <images.json>: print "<fixture ref> <generator ref>" of a c28-images/v1 record of a reviewed, verified
# build, or refuse (exit 1). Every ref must be <repo>@sha256:<64 hex> (no tag), and the generator must be the k6 image
# the stage admits (demo.GENERATOR_DIGEST).
images_read() {
  "$PY" - "$1" "$HARNESS" <<'PY'
import json, re, sys
sys.path.insert(0, sys.argv[2])
from c28 import demo
def refuse(why):
    sys.exit(f"images: {sys.argv[1]}: {why}; refusing")
try:
    doc = json.load(open(sys.argv[1]))
except (OSError, ValueError) as e:
    refuse(f"not a readable JSON file ({e.__class__.__name__})")
if not isinstance(doc, dict) or doc.get("schema") != "c28-images/v1":
    refuse("schema is not c28-images/v1")
if doc.get("verified") is not True:
    refuse("verified is not true (an unverified build record)")
refs = []
for role in ("fixture", "generator"):
    ref = ((doc.get("images") or {}).get(role) or {}).get("ref")
    if not isinstance(ref, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/:-]*@sha256:[0-9a-f]{64}", ref) \
            or ":" in ref.split("@")[0].split("/")[-1]:
        refuse(f"images.{role}.ref must be <repository>@sha256:<64 hex> without a tag")
    refs.append(ref)
if not refs[1].endswith("@" + demo.GENERATOR_DIGEST):
    refuse(f"images.generator.ref is not the reviewed k6 image ({demo.GENERATOR_DIGEST})")
print(*refs)
PY
}

# edit_params <in> <out> <python statements over p>: a scratch copy of a params file (never the tracked one).
edit_params() {
  "$PY" - "$1" "$2" "$3" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))
exec(sys.argv[3])
json.dump(p, open(sys.argv[2], "w"), indent=1)
PY
}

# refused <params> <expected text>: the stage validation must fail and name the reason.
refused() {
  local out
  if out="$(c28 validate --params "$1" --stage demonstration)"; then
    echo "demo smoke: expected a refusal ($2), but the stage validated" >&2; return 1
  fi
  grep -qF -- "$2" <<<"$out" || { echo "demo smoke: refused, but not for: $2" >&2; echo "$out" >&2; return 1; }
}

render_only() (                                     # a subshell: its EXIT trap removes only its own scratch directory
  out="$(mktemp -d)"
  trap 'rm -rf -- "${out:?}"' EXIT
  c28 validate --params "$PARAMS" --stage demonstration >/dev/null
  echo "render-only: the demonstration params stage validates (no c0_rps, HPA target or KEDA setting in the file)"

  # --- refusals of the params stage -----------------------------------------------------------------------------------
  n=0
  while IFS='|' read -r edit want; do
    [ -n "$edit" ] || continue
    edit_params "$PARAMS" "$out/bad.json" "$edit"
    refused "$out/bad.json" "$want" || exit 1
    n=$((n + 1))
  done <<'EOF'
p["demonstration"]["cadence"]["update_interval_seconds"] = 30|demonstration.cadence must be exactly
p["demonstration"]["cadence"]["grid_seconds"] = 30|demonstration.cadence must be exactly
p["demonstration"]["schedule_origin_utc"] = "<set at start>"|still placeholders: demonstration.schedule_origin_utc
p["demonstration"]["capacity_per_replica_rps"] = "__CAPACITY__"|still placeholders: demonstration.capacity_per_replica_rps
p["fixture"]["cpu_request"] = "<calibrated>"|fixture.cpu_request is still a placeholder
p["demonstration"]["targets"].append({"name": "c28-demo-pa-x", "role": "reactive"})|exactly two targets
p["demonstration"]["targets"][1]["role"] = "forecast"|roles forecast and reactive
p["demonstration"]["profile"] = "s90"|one startup profile s20
p["demonstration"]["bounds"]["max"] = 16|demonstration.bounds must be exactly
p["demonstration"]["shadow_replicas"] = 13|within the bounds 1..12
p["demonstration"]["capacity_label"] = "calibrated"|demonstration.capacity_label must be the literal
p["demonstration"]["schedule_origin_utc"] = "2026-11-01T06:00:00Z"|must be a UTC midnight
p["reservation"]["taint_value"] = p["run_id"]|reservation.taint_value must be demonstration.reservation_ref.taint_value
p["demonstration"]["reservation_ref"]["nodes"]["workload"]["name"] = "another-node"|nodes.workload.name must be nodes.workload
p["demonstration"]["reservation_ref"]["nodes"]["support"]["uid"] = "not-a-uid"|must be the reserved node's UID
p["demonstration"]["generator"]["image"] = "grafana/k6:0.57.0"|must be pinned by digest
p["demonstration"]["targets"][0]["name"] = "demo-pa-n"|must be a c28-* DNS label
p.pop("residency")|residency is required
p["demonstration"]["program"]["days"] = 29|demonstration.program.days must be an integer in 1..28
p["demonstration"]["program"].pop("days")|demonstration.program is missing days
p["demonstration"]["program"]["seed"] = 180|must not be a tuning or confirmation seed of the comparison
p["demonstration"]["min_lead_seconds"] = 180|demonstration.min_lead_seconds must be 600
p["demonstration"].pop("min_lead_seconds")|demonstration is missing min_lead_seconds
EOF
  echo "render-only: $n refusals of the params stage (cadence, the minimum lead, placeholders, targets, profile, bounds, reservation reference, image, names, residency, the schedule's days and seeds)"
  edit_params "$PARAMS" "$out/extra.json" 'p.update(c0_rps="<measured c0>", hpa_cpu_target_percent="__HPA__", keda="<unused>")'
  c28 validate --params "$out/extra.json" --stage demonstration >/dev/null
  echo "render-only: experiment-only fields are not consumed (placeholders there do not block the stage)"
  "$PY" "$SMOKE_DIR/demo_kind.py" selftest | sed 's/^/render-only: /'
  # --images: a reviewed record is read, anything else is refused (offline; no image is pulled here).
  h="$(printf 'a%.0s' $(seq 64))"
  printf '{"schema":"c28-images/v1","verified":true,"images":{"fixture":{"ref":"harbor.example/x/c28-fixture@sha256:%s"},"generator":{"ref":"grafana/k6@sha256:70af91f86cd8e142e0544a4edaf79835a80033f71974b92edd5ac36fd4442a7b"}}}' "$h" > "$out/images.json"
  [ "$(images_read "$out/images.json")" = "harbor.example/x/c28-fixture@sha256:$h grafana/k6@sha256:70af91f86cd8e142e0544a4edaf79835a80033f71974b92edd5ac36fd4442a7b" ] \
    || { echo "demo smoke: a valid images file was not read" >&2; exit 1; }
  n=0
  for bad in 's/"verified":true/"verified":false/' 's/c28-images\/v1/c28-images\/v0/' 's/c28-fixture@sha256/c28-fixture:r2@sha256/' \
             's/70af91f8/80af91f8/' 's/@sha256:aaaa/@sha256:aaa/'; do
    sed "$bad" "$out/images.json" > "$out/images-bad.json"
    if images_read "$out/images-bad.json" >/dev/null 2>&1; then echo "demo smoke: images file accepted after $bad" >&2; exit 1; fi
    n=$((n + 1))
  done
  echo "render-only: --images reads a verified c28-images/v1 record (fixture and generator by digest) and refuses $n altered ones"

  # --- rendering and its audits ---------------------------------------------------------------------------------------
  c28 render --params "$PARAMS" --stage demonstration --out "$out/all.yaml"
  c28 residency --params "$PARAMS" --stage demonstration --out "$out/residency.json"
  "$PY" - "$out/all.yaml" "$PARAMS" "$HARNESS" "$out/residency.json" <<'PY'
import json, sys, yaml
sys.path.insert(0, sys.argv[3])
from c28 import demo, render
objs = list(yaml.safe_load_all(open(sys.argv[1])))
p = json.load(open(sys.argv[2]))
run, names = p["run_id"], demo.target_names(p)
render.audit_rendered(objs)                      # every target has exactly one writer
render.audit_sidecar_free(objs)
render.audit_rbac(objs)
for o in objs:
    assert o["metadata"]["name"].startswith("c28-"), o["metadata"]["name"]
    assert o["metadata"]["labels"][render.RUN_LABEL] == run, o["metadata"]["name"]
    for t in render.pod_templates(o):
        assert t["metadata"]["labels"][render.RUN_LABEL] == run
        assert t["spec"]["tolerations"][0]["value"] == p["demonstration"]["reservation_ref"]["run_id"]
kinds = [o["kind"] for o in objs]
assert "HorizontalPodAutoscaler" not in kinds and "ScaledObject" not in kinds
pas = [o for o in objs if o["kind"] == "PredictiveAutoscaler"]
assert sorted(o["metadata"]["name"] for o in pas) == sorted(names)
assert all(o["spec"]["mode"] == "Recommend" for o in pas)
assert all(o["spec"]["prediction"]["sampleIntervalSeconds"] == 60 == o["spec"]["prediction"]["updateIntervalSeconds"]
           for o in pas)
# The ONE qualified forecast profile: the minimum lead explicit, no calibration or trust override.
assert all(o["spec"]["prediction"]["minLeadSeconds"] == 600 and "calibration" not in o["spec"]["prediction"]
           and "trust" not in o["spec"]["prediction"] and o["spec"]["prediction"]["families"] == ["seasonalNaive"]
           and demo.qualified_prediction_problems(o["spec"]["prediction"], ["seasonalNaive"]) == [] for o in pas)
extras = {o["metadata"]["name"]: o["spec"]["prediction"]["predictiveExtra"] for o in pas}
assert extras[names[1]] == {"maxReplicas": 0, "maxPercent": 0} and extras[names[0]]["maxReplicas"] > 0
# At most one generator pod: ONE StatefulSet with one replica (its pod is always <name>-0), no Deployment.
gens = [o for o in objs if o["metadata"]["name"] == demo.GENERATOR_NAME and render.pod_templates(o)]
assert len(gens) == 1 and gens[0]["kind"] == "StatefulSet" and gens[0]["spec"]["replicas"] == 1
assert gens[0]["spec"]["serviceName"] == demo.GENERATOR_NAME
assert [o["spec"].get("clusterIP") for o in objs if o["kind"] == "Service"
        and o["metadata"]["name"] == demo.GENERATOR_NAME] == ["None"]
assert gens[0]["spec"]["template"]["spec"]["containers"][0]["image"].endswith("@" + demo.GENERATOR_DIGEST)
# The generator's configuration carries the WHOLE finite schedule: 28 days, one row and one predeclared seed per day.
cms = [o for o in objs if o["kind"] == "ConfigMap" and o["metadata"]["name"] == demo.GENERATOR_NAME]
cfg = json.loads(cms[0]["data"][demo.GENERATOR_CONFIG])
assert len(cms) == 1 and demo.config_problems(cfg) == [] and cfg["label"] == "demonstration"
assert cfg["days"] == 28 == len(cfg["per_minute_days"]) and all(len(r) == 1440 for r in cfg["per_minute_days"])
assert cfg["seeds"] == list(range(cfg["program"]["seed"], cfg["program"]["seed"] + 28))
assert all(a != b for a, b in zip(cfg["per_minute_days"], cfg["per_minute_days"][1:]))      # no day repeats its neighbour
assert cfg["schedule_end_ms"] == cfg["origin_ms"] + 28 * 86_400_000
rep = json.load(open(sys.argv[4]))
assert rep["problems"] == [], rep["problems"]
w = {c["name"]: c["pods"] for c in rep["nodes"]["workload"]["contributions"]}
assert all(w[f"Deployment/{n}"] == 12 and w[f"Deployment/{n} overlap"] == 2 for n in names)
for values, want in (({"operator": {"rbac": {"scaleWrite": True, "scaleTargets": []}}}, "scaleTargets is empty"),
                     ({"operator": {"rbac": {"scaleWrite": True, "scaleTargets": [names[0]]}}}, "must equal"),
                     ({"operator": {"rbac": {"scaleWrite": True, "scaleTargets": names}}}, None)):
    got = demo.active_values_problems(values, p)
    assert (got == []) if want is None else any(want in x for x in got), got
frac = {r: rep["nodes"][r]["fraction"] for r in ("workload", "support")}
print(f"render-only: {len(objs)} objects, all c28-* under run label {run}, sidecar-free, least-privilege, one "
      f"Recommend PA per target at the qualified profile (60 s grid and update, minimum lead 600 s), one generator "
      f"StatefulSet with one replica and a finite schedule of {cfg['days']} days (seeds "
      f"{cfg['seeds'][0]}..{cfg['seeds'][-1]}); residency {frac} within 75 %; the Active configuration "
      "refuses an empty or partial scaleTargets list")
PY

  # --- the generator's own logic through the reducer (node; no k6) ---------------------------------------------------
  if ! command -v node >/dev/null 2>&1; then
    echo "render-only: node is not installed: the generator simulation was SKIPPED (the script's logic is untested here)"
  else
    c28 demo-plan --params "$PARAMS" --smoke-segment-minutes 3 --out "$out/config.json"
    "$PY" - "$out" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1] + "/config.json"))
o = cfg["origin_ms"]
start = o + 10 * 86_400_000 + 18 * 3_600_000 + 4_000           # 4 s after a minute boundary, at the daily peak
sc = {"incarnations": [{"pod_uid": "smoke-pod", "start_ms": start},
                       {"pod_uid": "smoke-pod", "start_ms": start - 4_000 + 180_000 + 2_000}],
      "until_ms": start + 8 * 60_000, "ingress_initial": 0,
      "scrape": {"interval_ms": 15_000, "offset_ms": 3_000, "from_ms": start - 120_000, "to_ms": start + 8 * 60_000}}
json.dump(sc, open(sys.argv[1] + "/scenario.json", "w"))
json.dump({"start": start - 4_000, "end": start - 4_000 + 5 * 60_000}, open(sys.argv[1] + "/range.json", "w"))
PY
    node "$SIM" "$SCRIPT" "$out/config.json" "$out/scenario.json" > "$out/sim.json"
    "$PY" - "$out" <<'PY'
import json, sys
d = sys.argv[1]
sim = json.load(open(d + "/sim.json"))
open(d + "/pod.log", "w").write("".join(json.dumps(l) + "\n" for l in sim["lines"]))
json.dump(sim["ingress"], open(d + "/ingress.json", "w"))
PY
    read -r START END < <("$PY" -c '
import json, sys, datetime
r = json.load(open(sys.argv[1]))
f = lambda ms: datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
print(f(r["start"]), f(r["end"]))' "$out/range.json")
    c28 demo-reduce --config "$out/config.json" --log "$out/pod.log" --ingress "$out/ingress.json" \
      --start "$START" --end "$END" --out "$out/reduction.json"
    "$PY" - "$out/reduction.json" <<'PY'
import json, sys
red = json.load(open(sys.argv[1]))
for target, t in red["targets"].items():
    states = [r["state"] for r in t["rows"]]
    reasons = [r["reasons"] for r in t["rows"]]
    # minute 0: started 4 s late; minute 3: the planned segment boundary (exit, restart 2 s later); the rest observed
    assert states == ["partial", "observed", "observed", "partial", "observed"], (target, states, reasons)
    assert reasons[0] == ["incarnation_boundary"] == reasons[3], reasons
assert len({i["incarnation"] for i in red["incarnations"]}) == 2
assert {i["ended"] for i in red["incarnations"]} == {"segment-end"}       # both ran to their segment boundary
print("render-only: the real generator script under node (virtual clock): a start 4 s late and a planned segment "
      "restart are PARTIAL with exact counts, every other minute OBSERVED, binned from the recorded origin")
PY
  fi
  if command -v k6 >/dev/null 2>&1; then
    echo "render-only: k6 is installed ($(k6 version 2>/dev/null | head -1)): run '$0 --k6-local' for the real-k6 dry run"
  else
    echo "render-only: k6 is not installed here: the script was NOT run under k6 (see --k6-local and the full mode)"
  fi
)

# --- --k6-local: the real k6 binary against a dummy server on 127.0.0.1 (no cluster, no registry) -------------------
K6_WORK="" SERVER_PID=""
k6_cleanup() {
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || true; fi      # only the server this run started
  if [ -n "$K6_WORK" ] && [ -d "$K6_WORK" ]; then rm -rf -- "${K6_WORK:?}"; fi
}
k6_local() {
  command -v k6 >/dev/null 2>&1 || { echo "demo smoke: k6 is not installed; nothing was run" >&2; exit 2; }
  local work port
  K6_WORK="$(mktemp -d)"; work="$K6_WORK"
  trap k6_cleanup EXIT
  port="${DEMO_SMOKE_PORT:-18999}"
  "$PY" - "$port" "$work/counts.json" <<'PY' &
import json, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
counts, lock = {}, threading.Lock()
class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        with lock:
            counts[self.path] = counts.get(self.path, 0) + 1
            with open(sys.argv[2], "w") as fh:
                json.dump(counts, fh)
        self.send_response(200); self.send_header("Content-Length", "3"); self.end_headers(); self.wfile.write(b"ok\n")
    def log_message(self, *a): pass
ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
  SERVER_PID=$!
  sleep 1
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "demo smoke: the dummy server did not start on 127.0.0.1:$port" >&2; exit 1; }
  # A smoke configuration (2-minute segments, today's UTC midnight as the origin) pointed at the dummy server.
  edit_params "$PARAMS" "$work/params.json" \
    'import datetime; p["demonstration"]["schedule_origin_utc"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT00:00:00Z")'
  c28 demo-plan --params "$work/params.json" --smoke-segment-minutes 2 --out "$work/plan.json"
  "$PY" - "$work/plan.json" "$work/config.json" "$port" "$HARNESS" <<'PY'
import json, sys
sys.path.insert(0, sys.argv[4])
from c28 import schedule
cfg = json.load(open(sys.argv[1]))
cfg["base_url"] = f"http://127.0.0.1:{sys.argv[3]}"       # the local dry run only; never a rendered configuration
cfg["digest"] = schedule.digest(cfg)
json.dump(cfg, open(sys.argv[2], "w"))
PY
  local i rc
  for i in 1 2; do
    echo "k6-local: incarnation $i (runs to the next 2-minute boundary of the origin, at least 30 s away)"
    rc=0
    POD_UID=k6-local POD_NAME="k6-local-$i" k6 run --quiet --no-color --no-usage-report --no-summary --no-thresholds \
      --log-format raw -e "C28D_CONFIG=$work/config.json" "$SCRIPT" >>"$work/pod.log" 2>&1 || rc=$?
    [ "$rc" = 0 ] || { echo "k6-local: k6 exited $rc (expected 0 at the segment end)" >&2; tail -20 "$work/pod.log" >&2; exit 1; }
  done
  "$PY" - "$work" "$HARNESS" <<'PY'
import json, sys
sys.path.insert(0, sys.argv[2])
from c28 import demo_reduce as DR
d = sys.argv[1]
cfg = json.load(open(d + "/config.json"))
recs, stats = DR.parse_lines(open(d + "/pod.log", errors="replace").read().splitlines())
starts = [r for r in recs if r.get("event") == "start"]
ends = [r for r in recs if r.get("event") == "end"]
mins = [r for r in recs if r["schema"] == DR.MINUTE]
n = len(cfg["targets"])
assert len(starts) == 2 * n and len(ends) == 2 * n, (len(starts), len(ends), stats)
assert all(e["reason"] == "segment-end" and e["inflight"] == 0 for e in ends), ends
assert len({r["inc"] for r in starts}) == 2, "two incarnations expected"
bad = [(r["target"], r["minute"], DR._line_problems(cfg, r)) for r in mins if DR._line_problems(cfg, r)]
assert not bad, bad
assert all(r["clock"] is not None for r in mins if r["ok"]), "every minute with a response carries clock evidence"
served = json.load(open(d + "/counts.json"))
for t in cfg["targets"]:
    sent = sum(r["dispatched"] for r in mins if r["target"] == t)
    ok = sum(r["ok"] for r in mins if r["target"] == t)
    got = served.get(f"/t/{t}/work", 0)
    assert sent == ok == got and sent > 0, (t, sent, ok, got)
    assert sum(sum(r["dropped"].values()) for r in mins if r["target"] == t) == 0
print(f"k6-local: PASS - two incarnations exited 0 at their segment boundary, {len(mins)} minute records all "
      f"self-consistent, dispatched == answered == the server's own count for every target; "
      f"{stats['other']} other log lines")
PY
}

if [ "$MODE" = render ]; then render_only; exit 0; fi
if [ "$MODE" = k6 ]; then k6_local; exit 0; fi

# =====================================================================================================================
# FULL mode: a throwaway kind cluster. Ownership: only what this run created is ever removed; a pre-existing cluster,
# registry container or image tag this run would create makes it refuse to start (as smoke/kind.sh).
# =====================================================================================================================
CLUSTER="${CLUSTER:-pa-c28-demo}"
OWNER_RUN=c28-smoke-owner              # the run that "owns" the reservation; the demonstration is another run id
OWN_WORK=0 OWN_CLUSTER=0 OWN_REG=0
OWN_IMAGES=()
if [ -z "${WORK:-}" ]; then WORK="$(mktemp -d)"; OWN_WORK=1; fi
KCFG="$WORK/kubeconfig"
KIND="${KIND:-kind}"
NS=demo
CTRL=pa-eval-system
REG_NAME=pa-c28-demo-registry
REG_PORT="${REG_PORT:-5002}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
REGISTRY_IMAGE=registry:2.8.3
VM_IMAGE=victoriametrics/victoria-metrics@sha256:5eff7af5341e401471002f58d106d399e614a62f3d240f2dbc21901e49eed5dd   # v1.153.0
SEGMENT_MINUTES="${SEGMENT_MINUTES:-3}"
# --replacement: the per-replica rate and the fixture's work per request for the smoke's own params (a light fixture,
# so the healthy target carries the rate on a small bed), where in the schedule's day the generator starts (the daily
# peak is at 18:00), how many consecutive planned restarts are required and how long one target does not answer.
DEMO_CAPACITY_RPS="${DEMO_CAPACITY_RPS:-17.5}"
DEMO_WORK_ITERATIONS="${DEMO_WORK_ITERATIONS:-100}"
DEMO_START_AT="${DEMO_START_AT:-17:40}"
DEMO_MIN_RESTARTS="${DEMO_MIN_RESTARTS:-3}"
DEMO_TIMEOUT_SECONDS="${DEMO_TIMEOUT_SECONDS:-110}"
# How the target is made to stop answering in the timeout phase, on the workload node:
#   responses (default)  the data segments its pods SEND from port 8080 are dropped: connections are made, requests
#                        arrive, answers never do;
#   requests             the packets TO its pods' port 8080 are dropped: not even the SYN arrives (a network
#                        blackhole), so every connection attempt of the ingress hangs.
DEMO_BLACKHOLE="${DEMO_BLACKHOLE:-responses}"
DEMO_INGRESS_MEMORY="${DEMO_INGRESS_MEMORY:-}"     # empty: the params' ingress memory (the stage's floor)
case "$DEMO_BLACKHOLE" in responses|requests) ;; *) echo "demo smoke: DEMO_BLACKHOLE must be responses or requests" >&2; exit 2 ;; esac
# drop_rule <pod IP>: the iptables match of the chosen direction.
drop_rule() {
  if [ "$DEMO_BLACKHOLE" = requests ]; then echo "-d $1 -p tcp --dport 8080 -j DROP"
  else echo "-s $1 -p tcp --sport 8080 --tcp-flags PSH PSH -j DROP"; fi
}
UNANSWERED=c28-demo-pa-r
FOLLOW_PID="" UNANSWERED_IPS=""
export KUBECONFIG="$KCFG"
unset KUBERNETES_SERVICE_HOST KUBERNETES_SERVICE_PORT
log() { printf '%s  %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
pass() { log "PASS $*"; }
die() { log "FAIL $*"; k get pods -A -o wide 2>/dev/null | tail -40 || true; exit 1; }
k() { kubectl --request-timeout=30s --kubeconfig "$KCFG" --context "kind-$CLUSTER" "$@"; }
CLUSTER_UID=""
guard() {
  [ "$(kubectl --kubeconfig "$KCFG" config current-context 2>/dev/null)" = "kind-$CLUSTER" ] \
    || { echo "demo smoke: the kubeconfig does not point at kind-$CLUSTER; refusing" >&2; exit 2; }
  case "$(kubectl --kubeconfig "$KCFG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" in
    https://127.0.0.1:*|https://localhost:*) ;;
    *) echo "demo smoke: the API server is not a local kind endpoint; refusing" >&2; exit 2 ;;
  esac
  if [ -n "$CLUSTER_UID" ]; then
    [ "$(k get namespace kube-system -o jsonpath='{.metadata.uid}')" = "$CLUSTER_UID" ] \
      || { echo "demo smoke: the cluster identity changed; refusing" >&2; exit 2; }
  fi
}
wait_for() {
  local secs=$1 what=$2; shift 2
  local deadline=$(( $(date +%s) + secs ))
  until "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || die "$what (after ${secs}s)"
    sleep 5
  done
}
cleanup() {
  if [ -n "$FOLLOW_PID" ]; then kill "$FOLLOW_PID" 2>/dev/null || true; fi      # only the follower this run started
  for ip in $UNANSWERED_IPS; do                                                # only rules this run added, in its node
    docker exec "$W" iptables -D FORWARD $(drop_rule "$ip") >/dev/null 2>&1 || true
  done
  if [ "${KEEP:-0}" = 1 ]; then
    log "kept: KUBECONFIG=$KCFG kubectl get pods -A ; delete with: $KIND delete cluster --name $CLUSTER"
    return
  fi
  [ "$OWN_CLUSTER" = 1 ] && "$KIND" delete cluster --name "$CLUSTER" --kubeconfig "$KCFG" >/dev/null 2>&1 || true
  [ "$OWN_REG" = 1 ] && docker rm -f "$REG_NAME" >/dev/null 2>&1 || true
  for img in ${OWN_IMAGES[@]+"${OWN_IMAGES[@]}"}; do docker image rm "$img" >/dev/null 2>&1 || true; done
  if [ "$OWN_WORK" = 1 ] && [ -d "$WORK" ]; then rm -rf -- "${WORK:?}"; fi
}
trap cleanup EXIT
for tool in docker "$KIND" kubectl "$PY"; do command -v "$tool" >/dev/null || die "missing tool: $tool"; done
if "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER"; then die "a kind cluster named $CLUSTER already exists; refusing"; fi
GENERATOR_REF=""
if [ -n "$IMAGES_FILE" ]; then
  render_only                                       # the offline checks first: nothing is created on a refusal
  # --- the reviewed images, by digest: no build, no local registry (they are pulled on the nodes below) -------------
  reviewed="$(images_read "$IMAGES_FILE")" || die "the images file was refused"
  read -r FIXTURE_IMAGE GENERATOR_REF <<<"$reviewed"
  pass "reviewed images from $IMAGES_FILE (no build): fixture $FIXTURE_IMAGE, generator $GENERATOR_REF"
else
if docker container inspect "$REG_NAME" >/dev/null 2>&1; then die "a container named $REG_NAME already exists; refusing"; fi
FIXTURE_TAG="localhost:$REG_PORT/c28-fixture:demo-smoke"
if docker image inspect "$FIXTURE_TAG" >/dev/null 2>&1; then die "an image tagged $FIXTURE_TAG already exists; refusing"; fi
render_only                                         # the offline checks first: nothing is created on a refusal

# --- the fixture image through a local registry, so the manifests pin a real digest ----------------------------------
docker image inspect "$REGISTRY_IMAGE" >/dev/null 2>&1 || OWN_IMAGES+=("$REGISTRY_IMAGE")
OWN_REG=1
docker run -d --restart=no --name "$REG_NAME" -p "127.0.0.1:$REG_PORT:5000" "$REGISTRY_IMAGE" >/dev/null
OWN_IMAGES+=("$FIXTURE_TAG")
docker build -q -t "$FIXTURE_TAG" "$ROOT/deploy/eval-c28/fixture" >/dev/null
docker push -q "$FIXTURE_TAG" >/dev/null
FIXTURE_DIGEST="$(docker inspect --format '{{index .RepoDigests 0}}' "$FIXTURE_TAG")"
FIXTURE_DIGEST="${FIXTURE_DIGEST#*@}"
[[ "$FIXTURE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || die "no registry digest for the fixture image"
FIXTURE_IMAGE="localhost:$REG_PORT/c28-fixture@$FIXTURE_DIGEST"
# What was built, for the record: the image's ID (its config digest) and the registry digest the manifests pin.
printf '{"tag": "%s", "image_id": "%s", "digest": "%s", "ref": "%s"}\n' "$FIXTURE_TAG" \
  "$(docker inspect --format '{{.Id}}' "$FIXTURE_TAG")" "$FIXTURE_DIGEST" "$FIXTURE_IMAGE" > "$WORK/fixture-image.json"
pass "fixture image built and pinned at $FIXTURE_DIGEST"
fi

# --- the cluster: one control plane, a workload node and a support node ---------------------------------------------
cat > "$WORK/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
EOF
[ -n "$IMAGES_FILE" ] || cat >> "$WORK/kind.yaml" <<EOF
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "/etc/containerd/certs.d"
EOF
OWN_CLUSTER=1
"$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --config "$WORK/kind.yaml" --kubeconfig "$KCFG" --wait 120s >/dev/null
if [ -z "$IMAGES_FILE" ]; then
  # The local registry, by its container name on the kind network, as the nodes' mirror of localhost:$REG_PORT
  # (containerd's hosts.toml; the older registry.mirrors table is gone from containerd 2.x).
  docker network connect kind "$REG_NAME" 2>/dev/null || true
  for node in $("$KIND" get nodes --name "$CLUSTER"); do
    docker exec "$node" mkdir -p "/etc/containerd/certs.d/localhost:$REG_PORT"
    printf '[host."http://%s:5000"]\n' "$REG_NAME" | docker exec -i "$node" cp /dev/stdin "/etc/containerd/certs.d/localhost:$REG_PORT/hosts.toml"
  done
fi
guard
CLUSTER_UID="$(k get namespace kube-system -o jsonpath='{.metadata.uid}')"
W="$CLUSTER-worker"; S="$CLUSTER-worker2"
if [ -n "$IMAGES_FILE" ]; then
  # The reviewed images reach the nodes BY DIGEST from their registries (crictl pull inside each node that runs
  # them), as smoke/kind.sh does: containerd verifies the manifest digest, and every manifest names the
  # <repository>@sha256 reference itself. The metrics backend is pulled by its digest too.
  while read -r node ref; do
    docker exec "$node" crictl pull "$ref" >/dev/null || die "node $node: pull $ref"
    docker exec "$node" crictl inspecti -o json "$ref" | "$PY" -c '
import json, sys
st = json.load(sys.stdin)["status"]
want = sys.argv[1].split("@")[1]
assert any(d.endswith("@" + want) for d in st.get("repoDigests") or []), (sys.argv[1], st.get("repoDigests"))
' "$ref" || die "node $node: $ref is not present under its digest"
  done <<EOF
$W $FIXTURE_IMAGE
$S $FIXTURE_IMAGE
$S $GENERATOR_REF
$CLUSTER-control-plane $VM_IMAGE
EOF
  pass "fixture on W and S, k6 on S and the metrics backend on the control plane pulled by digest inside the nodes"
fi
# The reservation belongs to the OWNER run (its id is the taint value); the demonstration never retags the nodes.
k taint node "$CLUSTER-control-plane" node-role.kubernetes.io/control-plane:NoSchedule- >/dev/null
k label node "$W" c28.devkuban.com/role=workload --overwrite >/dev/null
k label node "$S" c28.devkuban.com/role=support --overwrite >/dev/null
k taint node "$W" "c28.devkuban.com/reserved=$OWNER_RUN:NoSchedule" --overwrite >/dev/null
k taint node "$S" "c28.devkuban.com/reserved=$OWNER_RUN:NoSchedule" --overwrite >/dev/null
for ns in "$NS" "$CTRL" monitoring; do k create namespace "$ns" >/dev/null; done
W_UID="$(k get node "$W" -o jsonpath='{.metadata.uid}')"; S_UID="$(k get node "$S" -o jsonpath='{.metadata.uid}')"
pass "cluster $CLUSTER_UID: W $W ($W_UID) and S $S ($S_UID) reserved by $OWNER_RUN"

# --- a metrics backend: scrapes the ingress every 15 s and accepts the generator's remote write ---------------------
guard
k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ConfigMap
metadata: {name: vm-scrape, namespace: monitoring}
data:
  scrape.yaml: |
    scrape_configs:
      - job_name: c28-ingress
        scrape_interval: 15s
        static_configs: [{targets: ["c28-ingress.$NS.svc:80"]}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: vm, namespace: monitoring}
spec:
  replicas: 1
  selector: {matchLabels: {app: vm}}
  template:
    metadata: {labels: {app: vm}}
    spec:
      containers:
        - name: vm
          image: $VM_IMAGE
          args: ["-promscrape.config=/etc/vm/scrape.yaml", "-retentionPeriod=1d"]
          ports: [{containerPort: 8428}]
          volumeMounts: [{name: cfg, mountPath: /etc/vm}]
      volumes: [{name: cfg, configMap: {name: vm-scrape}}]
---
apiVersion: v1
kind: Service
metadata: {name: vm, namespace: monitoring}
spec: {selector: {app: vm}, ports: [{port: 8428}]}
EOF
wait_for 180 "the metrics backend is ready" k -n monitoring rollout status deploy/vm --timeout=5s
k apply --server-side -f "$ROOT/charts/predictive-autoscaler/crds" >/dev/null
wait_for 60 "the PredictiveAutoscaler CRD is established" \
  k wait --for=condition=Established crd/predictiveautoscalers.autoscaling.devkuban.com --timeout=5s
pass "metrics backend and the PredictiveAutoscaler CRD (no operator, no forecaster: the PAs stay unreconciled)"

# --- the demonstration's own params: this cluster's nodes and UIDs, the built fixture, today's origin ---------------
PRM="$WORK/params.json"
edit_params "$PARAMS" "$PRM" "
import datetime
p['image'] = '$FIXTURE_IMAGE'
p['nodes'] = {'workload': '$W', 'support': '$S'}
ref = p['demonstration']['reservation_ref']
ref['nodes'] = {'workload': {'name': '$W', 'uid': '$W_UID'}, 'support': {'name': '$S', 'uid': '$S_UID'}}
p['demonstration']['schedule_origin_utc'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT00:00:00Z')
if '$GENERATOR_REF':
    p['demonstration']['generator']['image'] = '$GENERATOR_REF'
if '$REPLACEMENT' == '1':        # the replacement smoke: a higher rate on a light fixture
    p['demonstration']['capacity_per_replica_rps'] = float('$DEMO_CAPACITY_RPS')
    p['fixture']['work_iterations'] = int('$DEMO_WORK_ITERATIONS')
    # The ingress keeps the params' memory limit (the stage's floor, 128Mi): the smoke exercises the limit the
    # stage admits, not a larger one. DEMO_INGRESS_MEMORY overrides it for a comparison run.
    if '$DEMO_INGRESS_MEMORY':
        p['ingress']['memory_limit'] = p['ingress']['memory_request'] = '$DEMO_INGRESS_MEMORY'
"
c28 validate --params "$PRM" --stage demonstration >/dev/null || die "the smoke params do not validate"
RUN="$("$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$PRM")"
[ "$RUN" != "$OWNER_RUN" ] || die "the demonstration must not be the reservation's owner"
for part in common targets pas; do
  guard
  c28 render --params "$PRM" --stage demonstration --part "$part" --out "$WORK/$part.yaml"
  k apply -f "$WORK/$part.yaml" >/dev/null || die "apply $part"
done
wait_for 240 "the ingress is ready" k -n "$NS" rollout status deploy/c28-ingress --timeout=5s
for t in c28-demo-pa-n c28-demo-pa-r; do
  wait_for 300 "$t is ready" k -n "$NS" rollout status "deploy/$t" --timeout=5s
done
[ "$(k -n "$NS" get predictiveautoscalers.autoscaling.devkuban.com -o jsonpath='{range .items[*]}{.spec.mode}{"\n"}{end}' | sort -u)" = Recommend ] \
  || die "a PredictiveAutoscaler is not in Recommend"
pass "common objects, two targets and two Recommend PredictiveAutoscalers applied under run $RUN"

# --- the generator: one pod, the reviewed k6 image by digest, a ${SEGMENT_MINUTES}-minute smoke segment ---------------
guard
SMOKE_ORIGIN=()
if [ "$REPLACEMENT" = 1 ]; then
  # A smoke-only origin: the generator starts at $DEMO_START_AT of the schedule's day 0, so the smoke runs across the
  # daily peak (18:00) whatever the wall clock says. Never a demonstration's origin.
  ORIGIN_S=$(( ($(date +%s) / 60) * 60 - 10#${DEMO_START_AT%%:*} * 3600 - 10#${DEMO_START_AT##*:} * 60 ))
  SMOKE_ORIGIN=(--smoke-origin-utc "$(date -u -d "@$ORIGIN_S" +%Y-%m-%dT%H:%M:%SZ)")
  log "replacement smoke: origin ${SMOKE_ORIGIN[1]} (the generator starts at $DEMO_START_AT of day 0), $DEMO_CAPACITY_RPS rps per replica, ${SEGMENT_MINUTES}-minute segments"
fi
c28 render --params "$PRM" --stage demonstration --part generator --smoke-segment-minutes "$SEGMENT_MINUTES" \
  ${SMOKE_ORIGIN[@]+"${SMOKE_ORIGIN[@]}"} --out "$WORK/generator.yaml"
# Server-side: the ConfigMap carries the whole 28-day table (about 0.2 MiB); a client-side apply would copy it into
# the last-applied annotation, which is limited to 256 KiB.
k apply --server-side -f "$WORK/generator.yaml" >/dev/null || die "apply generator"
wait_for 300 "the generator is running" k -n "$NS" rollout status statefulset/c28-demo-generator --timeout=5s
GEN_STARTED="$(date +%s)"
gen_pods() { k -n "$NS" get pods -l app.kubernetes.io/name=c28-demo-generator --no-headers 2>/dev/null | wc -l; }
[ "$(gen_pods)" = 1 ] || die "expected exactly one generator pod, found $(gen_pods)"
[ "$(k -n "$NS" get pods -l app.kubernetes.io/name=c28-demo-generator -o jsonpath='{.items[0].metadata.name}')" = c28-demo-generator-0 ] \
  || die "the generator pod is not the StatefulSet's c28-demo-generator-0"

# --- placement: every demonstration pod on its role's node ----------------------------------------------------------
bad="$(k get pods -A -l "c28.devkuban.com/run=$RUN" -o json | "$PY" -c '
import json, sys
w, s = sys.argv[1], sys.argv[2]
on_w = ("c28-ingress", "c28-demo-pa-n", "c28-demo-pa-r")
for p in json.load(sys.stdin)["items"]:
    name, node = p["metadata"]["name"], p["spec"].get("nodeName")
    want = w if name.startswith(on_w) else s
    if node != want:
        print(f"{name} on {node}, expected {want}")
' "$W" "$S")"
[ -z "$bad" ] || die "placement: $bad"
pass "every demonstration pod is on its role's node (targets and ingress on W; routes and generator on S)"

if [ "$REPLACEMENT" = 1 ]; then
  # ===================================================================================================================
  # The generator's restart and replacement smoke. Everything it collects is in $WORK/replacement.
  # ===================================================================================================================
  EV="$WORK/replacement"; mkdir "$EV"
  GEN=c28-demo-generator; POD="$GEN-0"; SEG_S=$(( SEGMENT_MINUTES * 60 ))
  ms() { date +%s%3N; }
  pod_uid() { k -n "$NS" get pod "$POD" -o jsonpath='{.metadata.uid}' 2>/dev/null; }
  "$PY" "$SMOKE_DIR/demo_kind.py" follow --kubeconfig "$KCFG" --context "kind-$CLUSTER" --namespace "$NS" \
    --name "$GEN" --node "$S" --ingress-node "$W" --out "$EV" --interval 3 > "$EV/follow.log" 2>&1 &
  FOLLOW_PID=$!
  T0=$(( GEN_STARTED * 1000 ))
  FIRST_UID="$(pod_uid)"

  # --- 1. planned restarts: the segment ends, k6 exits 0, the kubelet restarts the container in the same pod -------
  restarts_done() {
    local st
    st="$(k -n "$NS" get pod "$POD" -o jsonpath='{.metadata.uid} {.status.containerStatuses[0].restartCount} {.status.containerStatuses[0].state.running.startedAt}')"
    set -- $st
    [ "${1:-}" = "$FIRST_UID" ] && [ "${2:-0}" -ge "$DEMO_MIN_RESTARTS" ] && [ -n "${3:-}" ]
  }
  log "1. waiting for $DEMO_MIN_RESTARTS consecutive planned restarts of the k6 container in pod $POD ($FIRST_UID)"
  wait_for $(( (SEGMENT_MINUTES * (DEMO_MIN_RESTARTS + 1) + 15) * 60 )) "$DEMO_MIN_RESTARTS planned restarts in the same pod" restarts_done
  sleep 75                                           # at least one whole minute of the incarnation after the last one
  [ "$(gen_pods)" = 1 ] && [ "$(pod_uid)" = "$FIRST_UID" ] || die "1. the planned restarts did not keep the one pod"
  pass "1. $DEMO_MIN_RESTARTS planned restarts in the same pod ($(k -n "$NS" get pod "$POD" -o jsonpath='{.status.containerStatuses[0].restartCount}') so far)"
  T1="$(ms)"

  # --- 2. ordinary pod deletion with its termination grace period: NEVER forced -------------------------------------
  guard
  log "2. kubectl delete pod $POD (the default grace period; no --force, no --grace-period)"
  k -n "$NS" delete pod "$POD" --wait=false >/dev/null || die "2. delete pod"
  replaced() {
    local st
    st="$(k -n "$NS" get pod "$POD" -o jsonpath='{.metadata.uid} {.status.phase} {.status.containerStatuses[0].state.running.startedAt}' 2>/dev/null)"
    set -- $st
    [ -n "${1:-}" ] && [ "$1" != "$FIRST_UID" ] && [ "${2:-}" = Running ] && [ -n "${3:-}" ]
  }
  wait_for 300 "a replacement generator pod is running" replaced
  [ "$(gen_pods)" = 1 ] || die "2. $(gen_pods) generator pods after the replacement"
  pass "2. the deleted pod was replaced by $(pod_uid) (the order of the objects is judged from the watch events)"
  sleep 150
  T2="$(ms)"

  # --- 3. timeout load: one target stops answering while the other keeps being served ------------------------------
  # Begin 25 s after the next segment boundary, so the unanswered stretch lies inside ONE incarnation (one container,
  # one cgroup) and ends before that segment does.
  into=$(( ($(date +%s) - ORIGIN_S) % SEG_S ))
  sleep $(( SEG_S - into + 25 ))
  guard
  IPS="$(k -n "$NS" get pods -l "c28.devkuban.com/target=$UNANSWERED" -o jsonpath='{range .items[*]}{.status.podIP}{"\n"}{end}')"
  [ -n "$IPS" ] || die "3. no pod IP of $UNANSWERED"
  # The ingress before the fault: the pod and container the verdict expects to find again after the recovery.
  k -n "$NS" get pods -l app.kubernetes.io/name=c28-ingress -o json > "$EV/ingress-pods-before.json"
  T3="$(ms)"
  # The drop rules go into the workload node's FORWARD chain (the node is this run's own kind container). The
  # kubelet's probes are local to the node and keep passing, so the endpoints stay ready and the ingress keeps
  # routing to the target: every request to it hangs until a timeout. Direction "responses": the pods accept
  # connections and requests, but the data segments they send never arrive. Direction "requests": nothing reaches
  # the pods, so every connection attempt of the ingress hangs (an ingress that does not bound its dials piles them
  # up: the first run of this smoke lost its ingress container that way).
  UNANSWERED_IPS="$IPS"
  for ip in $IPS; do
    docker exec "$W" iptables -I FORWARD 1 $(drop_rule "$ip") || die "3. iptables on $W"
  done
  log "3. $UNANSWERED blackholed (direction $DEMO_BLACKHOLE) on $W for ${DEMO_TIMEOUT_SECONDS}s ($(echo $IPS | wc -w) pods)"
  sleep "$DEMO_TIMEOUT_SECONDS"
  for ip in $IPS; do
    docker exec "$W" iptables -D FORWARD $(drop_rule "$ip") || die "3. iptables -D on $W"
  done
  UNANSWERED_IPS=""
  T4="$(ms)"
  pass "3. the drop rules were removed after ${DEMO_TIMEOUT_SECONDS}s"
  # The ingress as the timeout load left it: container statuses (restart count, last termination), the previous
  # container's log, the pod's events and its description.
  k -n "$NS" get pods -l app.kubernetes.io/name=c28-ingress -o json > "$EV/ingress-pods.json"
  IPOD="$(k -n "$NS" get pods -l app.kubernetes.io/name=c28-ingress -o jsonpath='{.items[0].metadata.name}')"
  k -n "$NS" logs "$IPOD" --previous > "$EV/ingress-previous.log" 2> "$EV/ingress-previous.err" || true
  k -n "$NS" logs "$IPOD" --tail=200 > "$EV/ingress-current.log" 2>/dev/null || true
  k -n "$NS" get events --field-selector "involvedObject.name=$IPOD" -o json > "$EV/ingress-events.json" || true
  k -n "$NS" describe pod "$IPOD" > "$EV/ingress-describe.txt" || true
  log "collecting five more minutes (recovery, and the last lines)"
  sleep 300
  # The ingress after the recovery: the status the verdict is decided on (a restart during the recovery counts too).
  k -n "$NS" get pods -l app.kubernetes.io/name=c28-ingress -o json > "$EV/ingress-pods-final.json"
  T5=$(( $(ms) - 120000 ))
  touch "$EV/stop"; wait "$FOLLOW_PID" || true; FOLLOW_PID=""

  # --- 4. the records of the whole smoke, the ingress's raw samples, remote write, the analysis ---------------------
  guard
  k -n "$NS" get configmap "$GEN" -o jsonpath='{.data.config\.json}' > "$EV/config.json"
  k -n "$NS" get statefulset "$GEN" -o json > "$EV/statefulset.json"
  k -n "$NS" get pods -o wide > "$EV/pods-final.txt"
  for t in c28-demo-pa-n c28-demo-pa-r; do
    k get --raw "/api/v1/namespaces/monitoring/services/vm:8428/proxy/api/v1/query?query=c28_ingress_requests_total%7Btarget%3D%22$t%22%7D%5B90m%5D" \
      > "$EV/ingress-$t.json" || die "the ingress samples of $t could not be read"
  done
  # Remote write: what the generator's k6 output delivered to the cluster's VictoriaMetrics.
  k get --raw "/api/v1/namespaces/monitoring/services/vm:8428/proxy/api/v1/query?query=sum%20by%20(__name__)%20(%7B__name__%3D~%22k6_c28d_.%2B%22%7D)" \
    > "$EV/remote-write.json" || die "the remote-write series could not be read"
  "$PY" - "$EV" "$T0" "$T1" "$T2" "$T3" "$T4" "$T5" "$UNANSWERED" "$DEMO_MIN_RESTARTS" "$DEMO_BLACKHOLE" <<'PY'
import json, sys
d, t0, t1, t2, t3, t4, t5 = sys.argv[1], *map(int, sys.argv[2:8])
out = {}
for t in ("c28-demo-pa-n", "c28-demo-pa-r"):
    res = json.load(open(f"{d}/ingress-{t}.json"))["data"]["result"]
    assert len(res) == 1, f"{t}: expected one raw series, got {len(res)}"
    out[t] = [[float(ts), float(v)] for ts, v in res[0]["values"]]
json.dump(out, open(d + "/ingress.json", "w"))
sts = json.load(open(d + "/statefulset.json"))
limit = sts["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["memory"]
units = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30}
limit_bytes = int(limit[:-2]) * units[limit[-2:]] if limit[-2:] in units else int(limit)
rw = {r["metric"]["__name__"]: float(r["value"][1]) for r in json.load(open(d + "/remote-write.json"))["data"]["result"]}
def ingress_statuses(name):
    return [{"pod": p["metadata"]["name"], "uid": p["metadata"].get("uid"), "container_id": cs.get("containerID"),
             "restarts": cs.get("restartCount"), "last_terminated": (cs.get("lastState") or {}).get("terminated")}
            for p in json.load(open(d + "/" + name))["items"] for cs in p["status"].get("containerStatuses", [])]
ing = ingress_statuses("ingress-pods.json")                      # right after the fault (with the logs and events)
assert sts["spec"]["replicas"] == 1 and sts["kind"] == "StatefulSet"
json.dump({"phases": {"restarts": [t0, t1], "replacement": [t1, t2], "steady": [t2, t3], "timeout": [t3, t4],
                      "recovery": [t4, t5]},
           "unanswered_target": sys.argv[8], "min_restarts": int(sys.argv[9]), "memory_limit_bytes": limit_bytes,
           "remote_write_series": rw, "ingress": ing, "ingress_before": ingress_statuses("ingress-pods-before.json"),
           "ingress_final": ingress_statuses("ingress-pods-final.json"),
           "blackhole": {"direction": sys.argv[10], "from_ms": t3, "to_ms": t4}},
          open(d + "/phases.json", "w"), indent=1)
print("remote write:", json.dumps(rw))
assert any(v > 0 for v in rw.values()), "no k6_c28d_* series reached the metrics backend: remote write did not work"
PY
  rc=0
  "$PY" "$SMOKE_DIR/demo_kind.py" analyse --dir "$EV" --harness "$HARNESS" --config "$EV/config.json" \
    --ingress "$EV/ingress.json" --phases "$EV/phases.json" --out "$EV/summary.json" || rc=$?
  [ "$rc" = 0 ] || die "the replacement smoke's analysis found problems (see $EV/summary.json)"
  pass "restart and replacement smoke complete: $EV/summary.json (functionality only; no evidence, no operator, no forecaster, no Active)"
  exit 0
fi

# --- a planned restart: the segment ends, k6 exits 0, the kubelet starts the next incarnation -------------------------
restarted() {
  [ "$(k -n "$NS" get pods -l app.kubernetes.io/name=c28-demo-generator \
        -o jsonpath='{.items[0].status.containerStatuses[0].lastState.terminated.exitCode}')" = 0 ]
}
wait_for $(( (SEGMENT_MINUTES * 2 + 2) * 60 )) "the generator's first segment ended with exit 0" restarted
[ "$(gen_pods)" = 1 ] || die "the planned restart left $(gen_pods) generator pods"
pass "the generator exited 0 at its segment boundary and was restarted in place (one pod throughout)"
log "collecting seven more minutes of the second incarnation"
sleep 420

# --- the records and the reduction -----------------------------------------------------------------------------------
guard
POD="$(k -n "$NS" get pods -l app.kubernetes.io/name=c28-demo-generator -o jsonpath='{.items[0].metadata.name}')"
k -n "$NS" logs "$POD" --previous > "$WORK/pod-previous.log" 2>/dev/null || true
k -n "$NS" logs "$POD" > "$WORK/pod.log"
k -n "$NS" get configmap c28-demo-generator -o jsonpath='{.data.config\.json}' > "$WORK/config.json"
for t in c28-demo-pa-n c28-demo-pa-r; do
  k get --raw "/api/v1/namespaces/monitoring/services/vm:8428/proxy/api/v1/query?query=c28_ingress_requests_total%7Btarget%3D%22$t%22%7D%5B30m%5D" \
    > "$WORK/ingress-$t.json" || die "the ingress samples of $t could not be read"
done
"$PY" - "$WORK" <<'PY'
import json, sys
d = sys.argv[1]
out = {}
for t in ("c28-demo-pa-n", "c28-demo-pa-r"):
    res = json.load(open(f"{d}/ingress-{t}.json"))["data"]["result"]
    assert len(res) == 1, f"{t}: expected one raw series, got {len(res)}"
    out[t] = [[float(ts), float(v)] for ts, v in res[0]["values"]]
json.dump(out, open(d + "/ingress.json", "w"))
PY
START="$(date -u -d "@$(( (GEN_STARTED / 60) * 60 ))" +%Y-%m-%dT%H:%M:%SZ)"
END="$(date -u -d "@$(( ($(date +%s) / 60 - 3) * 60 ))" +%Y-%m-%dT%H:%M:%SZ)"
c28 demo-reduce --config "$WORK/config.json" --log "$WORK/pod-previous.log" --log "$WORK/pod.log" \
  --ingress "$WORK/ingress.json" --start "$START" --end "$END" --out "$WORK/reduction.json"
"$PY" - "$WORK/reduction.json" <<'PY'
import json, sys
red = json.load(open(sys.argv[1]))
allowed = {"incarnation_boundary", "ingress_gap", "before_first_record"}
for target, t in red["targets"].items():
    s = t["summary"]
    assert s["states"]["observed"] >= 2, (target, s)
    odd = {k: v for k, v in s["reasons"].items() if k not in allowed}
    assert not odd, (target, odd, [(r["minute"], r["state"], r["reasons"]) for r in t["rows"]])
    tot = s["observed_totals"]
    assert tot["dropped"] == 0 and tot["dispatched"] == tot["due"] == tot["intended"] > 0, (target, tot)
incs = {i["incarnation"]: i for i in red["incarnations"]}
assert len(incs) >= 2 and any(i["ended"] == "segment-end" for i in incs.values()), incs
assert red["log"]["records"] > 0
print("reduction:", json.dumps({t: v["summary"]["states"] for t, v in red["targets"].items()}),
      f"incarnations {len(incs)}, other log lines {red['log']['other']}")
PY
pass "records in the pod log reduce to observed minutes from the recorded origin; two incarnations, the first ended at its segment boundary"

# --- memory (R2-18): what the generator really uses at this rate, for the record -------------------------------------
MEM="$(k -n "$NS" exec "$POD" -- cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
if [ -n "$MEM" ]; then
  log "generator memory.current = $MEM bytes ($(c28 demo-sizing --params "$PRM" | "$PY" -c 'import json,sys; s=json.load(sys.stdin); print("estimate", s["memory_estimate_bytes"], "limit", s["memory_limit_bytes"], "peak rps/target", s["peak_rps_per_target"])'))"
else
  log "generator memory could not be read from inside the container (no cat in the image?): NOT MEASURED"
fi
pass "demonstration-stage smoke complete (functionality only; no evidence, no operator, no forecaster, no Active)"
