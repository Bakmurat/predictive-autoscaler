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
#                                                      the reviewed k6 image by digest with a 3-minute smoke segment,
#                                                      and check placement, the single generator pod, a planned
#                                                      restart with exit 0, the records in the pod log, and the
#                                                      reduction against the ingress's raw samples
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
MODE=full
while [ $# -gt 0 ]; do
  case "$1" in
    --render-only) MODE=render ;;
    --k6-local) MODE=k6 ;;
    *) echo "demo smoke: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

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
EOF
  echo "render-only: $n refusals of the params stage (cadence, placeholders, targets, profile, bounds, reservation reference, image, names, residency)"
  edit_params "$PARAMS" "$out/extra.json" 'p.update(c0_rps="<measured c0>", hpa_cpu_target_percent="__HPA__", keda="<unused>")'
  c28 validate --params "$out/extra.json" --stage demonstration >/dev/null
  echo "render-only: experiment-only fields are not consumed (placeholders there do not block the stage)"

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
extras = {o["metadata"]["name"]: o["spec"]["prediction"]["predictiveExtra"] for o in pas}
assert extras[names[1]] == {"maxReplicas": 0, "maxPercent": 0} and extras[names[0]]["maxReplicas"] > 0
gens = [o for o in objs if o["kind"] == "Deployment" and o["metadata"]["name"] == demo.GENERATOR_NAME]
assert len(gens) == 1 and gens[0]["spec"]["replicas"] == 1 and gens[0]["spec"]["strategy"] == {"type": "Recreate"}
assert gens[0]["spec"]["template"]["spec"]["containers"][0]["image"].endswith("@" + demo.GENERATOR_DIGEST)
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
      f"Recommend PA per target, one Recreate generator; residency {frac} within 75 %; the Active configuration "
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
    assert sum(r["dropped"]["late"] + r["dropped"]["inflight"] for r in mins if r["target"] == t) == 0
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
VM_IMAGE=victoriametrics/victoria-metrics:v1.153.0
SEGMENT_MINUTES=3
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
pass "fixture image built and pinned at $FIXTURE_DIGEST"

# --- the cluster: one control plane, a workload node and a support node ---------------------------------------------
cat > "$WORK/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:$REG_PORT"]
      endpoint = ["http://$REG_NAME:5000"]
EOF
OWN_CLUSTER=1
"$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --config "$WORK/kind.yaml" --kubeconfig "$KCFG" --wait 120s >/dev/null
docker network connect kind "$REG_NAME" 2>/dev/null || true
guard
CLUSTER_UID="$(k get namespace kube-system -o jsonpath='{.metadata.uid}')"
W="$CLUSTER-worker"; S="$CLUSTER-worker2"
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
c28 render --params "$PRM" --stage demonstration --part generator --smoke-segment-minutes "$SEGMENT_MINUTES" --out "$WORK/generator.yaml"
k apply -f "$WORK/generator.yaml" >/dev/null || die "apply generator"
wait_for 300 "the generator is running" k -n "$NS" rollout status deploy/c28-demo-generator --timeout=5s
GEN_STARTED="$(date +%s)"
gen_pods() { k -n "$NS" get pods -l app.kubernetes.io/name=c28-demo-generator --no-headers 2>/dev/null | wc -l; }
[ "$(gen_pods)" = 1 ] || die "expected exactly one generator pod, found $(gen_pods)"

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
