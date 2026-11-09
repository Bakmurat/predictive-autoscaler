#!/usr/bin/env bash
# C2.8 kind smoke (EVAL-C28-MANIFEST section 2: the aspire kind bed is for functionality, not evidence). On a throwaway
# kind cluster with its own kubeconfig, it deploys the evaluation fixture, the ingress, the fault routes and the five
# arms for both startup profiles, and checks:
#   1. every fixture pod lands on its role node (workload / support) through the run taint and required affinity;
#   2. every target Deployment has exactly one writer (python -m c28 audit over the live HPAs, ScaledObjects and
#      PredictiveAutoscalers of both API groups), including KEDA's generated HPA;
#   3. the PredictiveAutoscaler arms run in Recommend mode and write nothing (replicas stay at 2);
#   4. an accelerated (labelled accelerated-smoke, never a day) open-loop k6 run reaches the ingress, which counts it;
#   5. a scheduled metrics-route outage refuses connections while the ingress counter keeps counting;
#   6. the PriorityClass-scoped quota denies new fixture pods (ReplicaFailure, then ScaleOutBlocked on a PA arm) and
#      leaves a non-fixture pod alone;
#   7. the forecast API answers through the certificate-verified forecast route (a 503 DataUnavailable counts: this is
#      reachability only; v2 acceptance with sufficient lead is NOT tested and is reported as PENDING);
#   8. every container of every pod of the deployed operator, forecaster, fixture and k6 workloads (enumerated from the
#      rendered manifests, not by image) names its role's image, and its runtime identity
#      (status.containerStatuses[].imageID) is recorded; with --images it must be exactly the reviewed digest.
# It only ever talks to its own cluster: the kubeconfig in its work directory, checked (context and cluster UID) before
# every step. Nothing here reads or writes a lab cluster, and no evidence produced here counts toward C2.8.
#
#   deploy/eval-c28/smoke/kind.sh               build, create the cluster, run, delete it
#   KEEP=1 deploy/eval-c28/smoke/kind.sh        keep the cluster for inspection
#   deploy/eval-c28/smoke/kind.sh --render-only render and audit the manifests offline and self-test check 7's
#                                               classifier and the --images validation (no cluster, no docker;
#                                               scripts/verify.sh runs it); with --images it also validates that file
#   deploy/eval-c28/smoke/kind.sh --images images.json
#                                               run the EXACT reviewed images of a c28-images/v1 file (schema
#                                               c28-images/v1, verified true, all four of operator, statistical,
#                                               fixture, generator as <repo>@sha256:<64 hex>): pulled by digest, no
#                                               docker build at all, and check 8 fails on any other running digest
# Needs: docker, kind, kubectl, helm, python3 with PyYAML. OPERATOR_IMAGE / FORECASTER_IMAGE (local tags) skip builds
# (not with --images, which supplies every image).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
HARNESS="$ROOT/deploy/eval-c28/harness"
PARAMS="$ROOT/deploy/eval-c28/params/kind-smoke.json"
PY="${PYTHON:-python3}"
case "$PY" in */*) PY="$(cd "$(dirname "$PY")" && pwd)/$(basename "$PY")" ;; esac
c28() { (cd "$HARNESS" && KUBECONFIG=/dev/null "$PY" -m c28 "$@"); }
RENDER_ONLY=0 IMAGES_FILE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --render-only) RENDER_ONLY=1 ;;
    --images) [ $# -ge 2 ] || { echo "smoke: --images needs a file" >&2; exit 2; }; IMAGES_FILE="$2"; shift ;;
    --images=*) IMAGES_FILE="${1#--images=}" ;;
    *) echo "smoke: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

# images_read <images.json>: print "<role> <ref>" for operator, statistical, fixture and generator (in that order), or
# refuse (exit 1, reason on stderr). The file is the c28-images/v1 record of the reviewed build (extra fields such as
# tag, at or source_commit are allowed and ignored); every ref must be <repo>@sha256:<64 hex>: no tag, not even beside
# the digest.
images_read() {
  "$PY" - "$1" <<'PY'
import json, re, sys
comp = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
ref_re = re.compile(rf"^(?:[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*(?::[0-9]+)?/)?{comp}(?:/{comp})*@sha256:[0-9a-f]{{64}}$")
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
images = doc.get("images")
if not isinstance(images, dict):
    refuse("no images object")
for role in ("operator", "statistical", "fixture", "generator"):
    ref = images.get(role, {}).get("ref") if isinstance(images.get(role), dict) else None
    if not isinstance(ref, str) or not ref_re.fullmatch(ref):
        refuse(f"images.{role}.ref is not pinned by digest (<repo>@sha256:<64 hex>): {ref!r}")
    print(role, ref)
PY
}
images_self_test() {
  local dir bad=0 f want got
  dir="$(mktemp -d)"
  "$PY" - "$dir" <<'PY'
import json, os, sys
d = sys.argv[1]
h = "0123456789abcdef" * 4
good = {"schema": "c28-images/v1", "verified": True, "source_commit": "0" * 40, "images": {
    "operator": {"ref": f"harbor.devkuban.com/autoscaler/operator@sha256:{h}", "tag": "harbor.devkuban.com/autoscaler/operator:x"},
    "statistical": {"ref": f"harbor.devkuban.com/autoscaler/statistical-evaluation@sha256:{h}"},
    "fixture": {"ref": f"localhost:5001/c28-fixture@sha256:{h}"},
    "generator": {"ref": f"grafana/k6@sha256:{h}"}}}
def case(name, mutate):
    doc = json.loads(json.dumps(good))
    mutate(doc)
    json.dump(doc, open(os.path.join(d, name + ".json"), "w"))
def setref(role, ref):
    def m(doc): doc["images"][role]["ref"] = ref
    return m
case("accept-good", lambda doc: None)
case("accept-rebuild-attempt", lambda doc: doc.update(attempt="r2", run_id="c28-prod-1"))
case("refuse-tag", setref("generator", "grafana/k6:0.57.0"))
case("refuse-tag-and-digest", setref("generator", f"grafana/k6:0.57.0@sha256:{h}"))
case("refuse-short-digest", setref("operator", f"harbor.devkuban.com/autoscaler/operator@sha256:{h[:63]}"))
case("refuse-uppercase-digest", setref("fixture", f"localhost:5001/c28-fixture@sha256:{h.upper()}"))
case("refuse-port-only", setref("fixture", f"localhost:5001@sha256:{h}"))
case("refuse-schema", lambda doc: doc.update(schema="c28-images/v0"))
case("refuse-no-schema", lambda doc: doc.pop("schema"))
case("refuse-unverified", lambda doc: doc.update(verified=False))
case("refuse-verified-string", lambda doc: doc.update(verified="true"))
case("refuse-missing-fixture", lambda doc: doc["images"].pop("fixture"))
case("refuse-ref-missing", lambda doc: doc["images"]["statistical"].pop("ref"))
open(os.path.join(d, "refuse-not-json.json"), "w").write("{schema: c28-images/v1")
PY
  for f in "$dir"/*.json; do
    want="${f##*/}"; want="${want%%-*}"
    if images_read "$f" >/dev/null 2>&1; then got=accept; else got=refuse; fi
    [ "$got" = "$want" ] || { echo "images self-test: ${f##*/} gave $got, want $want" >&2; bad=1; }
  done
  [ "$(images_read "$dir/accept-good.json" | cut -d' ' -f1 | tr '\n' ' ')" = "operator statistical fixture generator " ] \
    || { echo "images self-test: roles not printed in order" >&2; bad=1; }
  rm -rf -- "${dir:?}"
  return "$bad"
}

# Check 7's classifier: does a PredictionReady (reason, message) prove an answer from the forecast API over the route?
# Only what the operator derives from a response counts: the trust reasons it copies from the service's answer, a 422
# refusal, Ready (a verified answer), and an HTTP error status ("ML API returned status", built only from a response read
# over the verified connection). LeadInsufficient is decided by the operator before any request (v2_reconcile.go
# readiness), so it proves nothing about the route; a TLS or dial failure is DataUnavailable with a transport error.
forecast_api_answered() {
  case "$1" in
    HistoryInsufficient|CalibrationInsufficient|ShadowInsufficient|CoverageLow|RequestRefused|Ready) return 0 ;;
    DataUnavailable) [[ "$2" == "ML API returned status "* ]] ;;
    *) return 1 ;;
  esac
}
classifier_self_test() {
  local bad=0 want r m got
  while IFS='|' read -r want r m; do
    if forecast_api_answered "$r" "$m"; then got=yes; else got=no; fi
    [ "$got" = "$want" ] || { echo "check-7 classifier: reason '$r' ($m) gave $got, want $want" >&2; bad=1; }
  done <<'CASES'
no|LeadInsufficient|no startup budget: set spec.behavior.startup.fallbackDelaySeconds until enough startups are observed
no|DataUnavailable|Post "https://c28-forecast-route.pa-eval-system.svc:8443/v2/predict": tls: failed to verify certificate: x509: certificate is valid for pa-eval-predictive-autoscaler-forecaster.pa-eval-system.svc, not c28-forecast-route.pa-eval-system.svc
no|DataUnavailable|Post "https://c28-forecast-route.pa-eval-system.svc:8443/v2/predict": dial tcp 10.96.12.34:8443: connect: connection refused
no|DataUnavailable|no forecast
no|CapacityMissing|no per-replica capacity: no forecast is requested
no|SettingsInvalid|invalid settings
no|EvidenceStale|no qualifying outcome within 900 s
no||
yes|DataUnavailable|ML API returned status 503: {"detail":{"error":"DataUnavailable","message":"the newest finalized observation is too old"}}
yes|HistoryInsufficient|trust state Untrusted, reason HistoryInsufficient, service eligibility false
yes|RequestRefused|forecast refused
yes|Ready|ready to activate: the forecast passed every check; Recommend mode still makes no writes
CASES
  return "$bad"
}

# check8_classify <expected.json> <live.json> <out.json>: check 8's verdict. expected.json names the workloads the smoke
# deployed (kind, namespace, name, role, container names: taken from the rendered manifests, never from an image) and
# per role the image the specs must name plus, with reviewed=true, the reviewed digest the runtime must report.
# live.json is a List of the Pods, Deployments and Jobs in the run's namespaces. Every pod a workload's selector picks
# is examined, every container in it: an unexpected or missing container, a spec image other than the role's, or a
# runtime imageID other than the reviewed digest is a HARD failure (exit 1); a container with no runtime identity yet
# or fewer pods than the workload reports is SOFT (exit 3: the caller may look again, and fails if it persists). The
# full inventory (every examined container with its spec image, imageID and verdict) goes to out.json.
# A runtime that reports only a bare sha256:<id> is compared with the config digest docker verified for the digest
# reference (docker image inspect .Id): the manifest names that config, and the config pins every layer by its
# diff_id, so both identify the same image content.
check8_classify() {
  "$PY" - "$1" "$2" "$3" <<'PY'
import json, sys
exp, live, out = json.load(open(sys.argv[1])), json.load(open(sys.argv[2])), sys.argv[3]
reviewed, roles = exp["reviewed"] is True, exp["roles"]
def short(r):
    r = r[len("docker.io/"):] if r.startswith("docker.io/") else r
    return r[len("library/"):] if r.startswith("library/") else r
items = live.get("items") or []
objs = {(o["kind"], o["metadata"]["namespace"], o["metadata"]["name"]): o for o in items if o.get("kind") in ("Deployment", "Job")}
pods = [o for o in items if o.get("kind") == "Pod"]
rows, hard, soft, seen = [], [], [], []
for w in exp["workloads"]:
    wid = f'{w["kind"]} {w["namespace"]}/{w["name"]}'
    o = objs.get((w["kind"], w["namespace"], w["name"]))
    if o is None:
        hard.append(f"{wid}: not found"); continue
    sel = (o.get("spec") or {}).get("selector") or {}
    if sel.get("matchExpressions") or not sel.get("matchLabels"):
        hard.append(f"{wid}: selector is not plain matchLabels"); continue
    st = o.get("status") or {}
    want_pods = max(1, st.get("replicas") or 0) if w["kind"] == "Deployment" else \
        max(1, (st.get("active") or 0) + (st.get("succeeded") or 0) + (st.get("failed") or 0))
    mine = [p for p in pods if p["metadata"]["namespace"] == w["namespace"]
            and all((p["metadata"].get("labels") or {}).get(k) == v for k, v in sel["matchLabels"].items())]
    seen.append({"workload": wid, "role": w["role"], "pods": len(mine), "expected_pods": want_pods})
    if len(mine) < want_pods:
        soft.append(f"{wid}: {len(mine)} pods, expected at least {want_pods}")
    want_image, want_id = roles[w["role"]]["image"], roles[w["role"]].get("id") or ""
    for pod in mine:
        pid = f'{pod["metadata"]["namespace"]}/{pod["metadata"]["name"]}'
        spec = pod.get("spec") or {}
        images = {c["name"]: c.get("image", "") for c in (spec.get("containers") or []) + (spec.get("initContainers") or [])}
        status = pod.get("status") or {}
        stats = {c["name"]: c for c in (status.get("containerStatuses") or []) + (status.get("initContainerStatuses") or [])}
        for name in sorted(set(images) | set(w["containers"])):
            got = (stats.get(name) or {}).get("imageID") or ""
            row = {"role": w["role"], "workload": wid, "pod": pid, "container": name,
                   "spec_image": images.get(name), "imageID": got}
            if name not in w["containers"]:
                row["verdict"] = "unexpected-container"; hard.append(f"{pid} {name}: not a container of {wid}")
            elif name not in images:
                row["verdict"] = "missing-container"; hard.append(f"{pid} {name}: missing from the pod spec")
            elif images[name] != want_image:
                row["verdict"] = "wrong-spec-image"; hard.append(f"{pid} {name}: spec image {images[name]} is not {want_image}")
            elif not got:
                row["verdict"] = "no-runtime-identity"; soft.append(f"{pid} {name}: no containerStatus imageID")
            elif not reviewed:
                row["verdict"] = "recorded"
            elif "@" in got and short(got) == short(want_image):
                row["verdict"] = "ok-repo-digest"
            elif "@" not in got and want_id and got == want_id:
                row["verdict"] = "ok-config-digest"
            else:
                row["verdict"] = "wrong-runtime-image"; hard.append(f"{pid} {name}: imageID {got} is not {want_image}")
            rows.append(row)
for role in roles:
    if not any(r["role"] == role and r["verdict"].startswith(("ok", "recorded")) for r in rows):
        soft.append(f"role {role}: no examined container")
json.dump({"mode": "reviewed" if reviewed else "build", "workloads": seen, "containers": rows,
           "hard": hard, "soft": soft}, open(out, "w"), indent=1)
for m in hard + soft:
    print(("HARD " if m in hard else "SOFT ") + m, file=sys.stderr)
sys.exit(1 if hard else 3 if soft else 0)
PY
}
check8_self_test() {
  local dir bad=0 f want rc
  dir="$(mktemp -d)"
  "$PY" - "$dir" <<'PY'
import copy, json, os, sys
d = sys.argv[1]
h = {r: c * 64 for r, c in (("operator", "a"), ("forecaster", "b"), ("fixture", "c"), ("generator", "d"))}
img = {"operator": f"reg.example/op@sha256:{h['operator']}", "forecaster": f"reg.example/fc@sha256:{h['forecaster']}",
       "fixture": f"reg.example/fx@sha256:{h['fixture']}", "generator": f"grafana/k6@sha256:{h['generator']}"}
cfg = {r: "sha256:" + str(i) * 64 for i, r in enumerate(img, 1)}
exp = {"reviewed": True, "roles": {r: {"image": img[r], "id": cfg[r]} for r in img}, "workloads": [
    {"kind": "Deployment", "namespace": "ctl", "name": "op", "role": "operator", "containers": ["operator"]},
    {"kind": "Deployment", "namespace": "ctl", "name": "fc", "role": "forecaster", "containers": ["forecaster"]},
    {"kind": "Deployment", "namespace": "ctl", "name": "route", "role": "fixture", "containers": ["proxy"]},
    {"kind": "Deployment", "namespace": "demo", "name": "arm", "role": "fixture", "containers": ["fixture"]},
    {"kind": "Job", "namespace": "demo", "name": "k6", "role": "generator", "containers": ["k6"]}]}
def workload(kind, ns, name, n):
    st = {"replicas": n} if kind == "Deployment" else {"succeeded": n}
    return {"kind": kind, "metadata": {"namespace": ns, "name": name},
            "spec": {"selector": {"matchLabels": {"app": name}}}, "status": st}
def pod(ns, owner, i, container, role, image_id=None):
    return {"kind": "Pod", "metadata": {"namespace": ns, "name": f"{owner}-{i}", "labels": {"app": owner}},
            "spec": {"containers": [{"name": container, "image": img[role]}]},
            "status": {"containerStatuses": [{"name": container, "imageID": image_id or ("docker.io/" + img[role] if role == "generator" else img[role])}]}}
good = {"kind": "List", "items": [workload("Deployment", "ctl", "op", 1), workload("Deployment", "ctl", "fc", 1),
        workload("Deployment", "ctl", "route", 1), workload("Deployment", "demo", "arm", 2), workload("Job", "demo", "k6", 1),
        pod("ctl", "op", 0, "operator", "operator"), pod("ctl", "fc", 0, "forecaster", "forecaster"),
        pod("ctl", "route", 0, "proxy", "fixture"), pod("demo", "arm", 0, "fixture", "fixture"),
        pod("demo", "arm", 1, "fixture", "fixture"), pod("demo", "k6", 0, "k6", "generator"),
        {"kind": "Pod", "metadata": {"namespace": "demo", "name": "unrelated", "labels": {"app": "other"}},
         "spec": {"containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10"}]}, "status": {}}]}
def arm1(live):
    return next(o for o in live["items"] if o["metadata"]["name"] == "arm-1")
cases = {"0-all-correct": lambda live: None,
         "0-config-digest": lambda live: arm1(live)["status"]["containerStatuses"][0].update(imageID=cfg["fixture"]),
         "1-wrong-spec-image": lambda live: arm1(live)["spec"]["containers"][0].update(image=f"reg.example/fx@sha256:{'e' * 64}"),
         "1-tag-spec-image": lambda live: arm1(live)["spec"]["containers"][0].update(image="reg.example/fx:latest"),
         "1-wrong-runtime": lambda live: arm1(live)["status"]["containerStatuses"][0].update(imageID=f"reg.example/fx@sha256:{'e' * 64}"),
         "1-wrong-config-digest": lambda live: arm1(live)["status"]["containerStatuses"][0].update(imageID="sha256:" + "9" * 64),
         "3-missing-status": lambda live: arm1(live)["status"].update(containerStatuses=[]),
         "3-empty-imageid": lambda live: arm1(live)["status"]["containerStatuses"][0].update(imageID=""),
         "1-extra-container": lambda live: arm1(live)["spec"]["containers"].append({"name": "sidecar", "image": img["fixture"]}),
         "1-extra-init-container": lambda live: arm1(live)["spec"].update(initContainers=[{"name": "init", "image": img["fixture"]}]),
         "1-renamed-container": lambda live: arm1(live)["spec"]["containers"][0].update(name="other"),
         "3-fewer-pods": lambda live: live["items"].remove(arm1(live)),
         "1-missing-workload": lambda live: live["items"].pop(0)}
for name, mutate in cases.items():
    live = copy.deepcopy(good)
    mutate(live)
    json.dump(live, open(os.path.join(d, name + ".live.json"), "w"))
json.dump(exp, open(os.path.join(d, "expected.json"), "w"))
PY
  for f in "$dir"/*.live.json; do
    want="${f##*/}"; want="${want%%-*}"
    rc=0; check8_classify "$dir/expected.json" "$f" "$dir/out.json" 2>/dev/null || rc=$?
    [ "$rc" = "$want" ] || { echo "check-8 self-test: ${f##*/} gave exit $rc, want $want" >&2; bad=1; }
  done
  check8_classify "$dir/expected.json" "$dir/0-all-correct.live.json" "$dir/out.json"
  "$PY" -c 'import json, sys; d = json.load(open(sys.argv[1])); assert len(d["containers"]) == 6 and len(d["workloads"]) == 5, d' \
    "$dir/out.json" || { echo "check-8 self-test: the inventory is incomplete" >&2; bad=1; }
  rm -rf -- "${dir:?}"
  return "$bad"
}

if [ "$RENDER_ONLY" = 1 ]; then
  classifier_self_test || { echo "render-only: check 7's classifier self-test failed" >&2; exit 1; }
  echo "render-only: check 7's classifier accepts only answers from the forecast API"
  images_self_test || { echo "render-only: the --images validation self-test failed" >&2; exit 1; }
  echo "render-only: --images refuses tag refs, a wrong schema and an unverified file"
  check8_self_test || { echo "render-only: check 8's classifier self-test failed" >&2; exit 1; }
  echo "render-only: check 8 fails on a wrong spec image, a wrong runtime image, a missing status, an extra container and too few pods"
  if [ -n "$IMAGES_FILE" ]; then
    images_read "$IMAGES_FILE" | sed 's/^/render-only: reviewed image /'   # pipefail: a refusal exits here
  fi
  out="$(mktemp -d)"
  c28 render --params "$PARAMS" --out "$out/all.yaml"
  "$PY" - "$out/all.yaml" "$PARAMS" "$HARNESS" <<'PY'
import json, sys, yaml
sys.path.insert(0, sys.argv[3])
from c28 import render
objs = list(yaml.safe_load_all(open(sys.argv[1])))
render.audit_rendered(objs)
live = [o for o in objs if o["kind"] in ("HorizontalPodAutoscaler", "ScaledObject", "PredictiveAutoscaler")]
problems = render.audit_live(render.expected_writers(json.load(open(sys.argv[2]))), live)
assert not problems, problems
print(f"render-only: {len(objs)} objects, every target has exactly one writer")
PY
  exit 0
fi

CLUSTER="${CLUSTER:-pa-c28-smoke}"
# Ownership: only what this run created is ever removed. A supplied WORK is never deleted; a pre-existing cluster,
# registry container or image tag this run would build makes the smoke refuse to start. Image tags it builds and a
# registry image it pulls are untagged at the end (never forced, so layers another tag uses stay).
OWN_WORK=0 OWN_CLUSTER=0 OWN_REG=0
OWN_IMAGES=()
if [ -z "${WORK:-}" ]; then WORK="$(mktemp -d)"; OWN_WORK=1; fi
KCFG="$WORK/kubeconfig"
KIND="${KIND:-kind}"
NS=demo
CTRL=pa-eval-system
REG_NAME=pa-c28-registry
REG_PORT="${REG_PORT:-5001}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0}"
REGISTRY_IMAGE=registry:2.8.3
VM_IMAGE=victoriametrics/victoria-metrics:v1.153.0
K6_IMAGE=grafana/k6:0.57.0
K6_PULL=IfNotPresent
KEDA_CHART_VERSION="${KEDA_CHART_VERSION:-2.16.1}"
METRICS_SERVER_URL=https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
OPERATOR_IMAGE="${OPERATOR_IMAGE:-}"
FORECASTER_IMAGE="${FORECASTER_IMAGE:-}"
export KUBECONFIG="$KCFG"
unset KUBERNETES_SERVICE_HOST KUBERNETES_SERVICE_PORT

log() { printf '%s  %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
pass() { log "PASS $*"; }
die() { log "FAIL $*"; k get pods -A -o wide 2>/dev/null | tail -40 || true; exit 1; }
k() { kubectl --request-timeout=30s --kubeconfig "$KCFG" --context "kind-$CLUSTER" "$@"; }
h() { helm --kubeconfig "$KCFG" --kube-context "kind-$CLUSTER" "$@"; }
CLUSTER_UID=""
guard() {
  [ "$(kubectl --kubeconfig "$KCFG" config current-context 2>/dev/null)" = "kind-$CLUSTER" ] \
    || { echo "smoke: the kubeconfig does not point at kind-$CLUSTER; refusing" >&2; exit 2; }
  case "$(kubectl --kubeconfig "$KCFG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" in
    https://127.0.0.1:*|https://localhost:*) ;;
    *) echo "smoke: the API server is not a local kind endpoint; refusing" >&2; exit 2 ;;
  esac
  if [ -n "$CLUSTER_UID" ]; then
    [ "$(k get namespace kube-system -o jsonpath='{.metadata.uid}')" = "$CLUSTER_UID" ] \
      || { echo "smoke: the cluster identity changed; refusing" >&2; exit 2; }
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
for tool in docker "$KIND" kubectl helm "$PY"; do command -v "$tool" >/dev/null || die "missing tool: $tool"; done
if "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER"; then die "a kind cluster named $CLUSTER already exists; refusing"; fi

# repo_digest_id <ref>: the local image's config digest (docker's Id), but only if its RepoDigests hold exactly <ref>
# (docker shows Docker Hub names in their short form, so docker.io/ and library/ are compared without). A local tag or
# the first RepoDigest is never taken on trust.
repo_digest_id() {
  docker image inspect "$1" | "$PY" -c '
import json, sys
def short(r):
    r = r[len("docker.io/"):] if r.startswith("docker.io/") else r
    return r[len("library/"):] if r.startswith("library/") else r
img = json.load(sys.stdin)[0]
digests = img.get("RepoDigests") or []
if short(sys.argv[1]) not in [short(d) for d in digests]:
    sys.exit(f"{sys.argv[1]} is not among the pulled image RepoDigests {digests}")
print(img["Id"])
' "$1"
}

if [ -n "$IMAGES_FILE" ]; then
  # --- the reviewed images, pulled by digest: no build, no local registry ---------------------------------------------
  [ -z "$OPERATOR_IMAGE$FORECASTER_IMAGE" ] || die "OPERATOR_IMAGE / FORECASTER_IMAGE cannot be combined with --images"
  reviewed="$(images_read "$IMAGES_FILE")" || die "the images file was refused"
  while read -r role ref; do
    case "$role" in
      operator) OPERATOR_REF="$ref" ;; statistical) STATISTICAL_REF="$ref" ;;
      fixture) FIXTURE_REF="$ref" ;; generator) GENERATOR_REF="$ref" ;;
    esac
  done <<<"$reviewed"
  for ref in "$OPERATOR_REF" "$STATISTICAL_REF" "$FIXTURE_REF" "$GENERATOR_REF"; do
    docker image inspect "$ref" >/dev/null 2>&1 || OWN_IMAGES+=("$ref")   # only a digest reference this run adds
    docker pull -q "$ref" >/dev/null || die "pull $ref"
  done
  OPERATOR_ID="$(repo_digest_id "$OPERATOR_REF")" || die "operator image identity"
  STATISTICAL_ID="$(repo_digest_id "$STATISTICAL_REF")" || die "statistical image identity"
  FIXTURE_ID="$(repo_digest_id "$FIXTURE_REF")" || die "fixture image identity"
  GENERATOR_ID="$(repo_digest_id "$GENERATOR_REF")" || die "generator image identity"
  OPERATOR_IMAGE="$OPERATOR_REF" FORECASTER_IMAGE="$STATISTICAL_REF" K6_IMAGE="$GENERATOR_REF" K6_PULL=Never
  FIXTURE_IMAGE="$FIXTURE_REF"
  pass "reviewed images pulled by digest from $IMAGES_FILE (no build): operator $OPERATOR_ID, statistical $STATISTICAL_ID, fixture $FIXTURE_ID, generator $GENERATOR_ID (config digests)"
else
  if docker container inspect "$REG_NAME" >/dev/null 2>&1; then die "a container named $REG_NAME already exists; refusing"; fi
  FIXTURE_TAG="localhost:$REG_PORT/c28-fixture:smoke"
  BUILD_TAGS=("$FIXTURE_TAG")
  [ -n "$OPERATOR_IMAGE" ] || BUILD_TAGS+=(pa-operator:c28-smoke)
  [ -n "$FORECASTER_IMAGE" ] || BUILD_TAGS+=(pa-forecaster-statistical:c28-smoke)
  for img in "${BUILD_TAGS[@]}"; do
    if docker image inspect "$img" >/dev/null 2>&1; then die "an image tagged $img already exists; refusing"; fi
  done
  docker image inspect "$REGISTRY_IMAGE" >/dev/null 2>&1 || OWN_IMAGES+=("$REGISTRY_IMAGE")

  # --- images and a local registry (so every manifest pins a real digest) -------------------------------------------
  OWN_REG=1   # set before run: a half-started container of this name is ours (its absence was checked above)
  docker run -d --restart=no --name "$REG_NAME" -p "127.0.0.1:$REG_PORT:5000" "$REGISTRY_IMAGE" >/dev/null
  OWN_IMAGES+=("${BUILD_TAGS[@]}")   # absent above, so every one of these tags is this run's
  docker build -q -t "$FIXTURE_TAG" "$ROOT/deploy/eval-c28/fixture" >/dev/null
  docker push -q "$FIXTURE_TAG" >/dev/null
  FIXTURE_DIGEST="$(docker inspect --format '{{index .RepoDigests 0}}' "$FIXTURE_TAG")"
  FIXTURE_DIGEST="${FIXTURE_DIGEST#*@}"
  [[ "$FIXTURE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || die "no registry digest for the fixture image"
  FIXTURE_IMAGE="localhost:$REG_PORT/c28-fixture@$FIXTURE_DIGEST"
  if [ -z "$OPERATOR_IMAGE" ]; then
    OPERATOR_IMAGE=pa-operator:c28-smoke
    docker build -q -t "$OPERATOR_IMAGE" "$ROOT/k8s-operator" >/dev/null
  fi
  if [ -z "$FORECASTER_IMAGE" ]; then
    FORECASTER_IMAGE=pa-forecaster-statistical:c28-smoke
    docker build -q -t "$FORECASTER_IMAGE" -f "$ROOT/ml-engine/Dockerfile.statistical" "$ROOT/ml-engine" >/dev/null
  fi
  pass "images built; fixture pinned at $FIXTURE_DIGEST"
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
    [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:$REG_PORT"]
      endpoint = ["http://$REG_NAME:5000"]
EOF
OWN_CLUSTER=1   # set before create: a half-created cluster of this name is ours (its absence was checked above)
"$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --config "$WORK/kind.yaml" --kubeconfig "$KCFG" --wait 120s >/dev/null
[ -n "$IMAGES_FILE" ] || docker network connect kind "$REG_NAME" 2>/dev/null || true
guard
CLUSTER_UID="$(k get namespace kube-system -o jsonpath='{.metadata.uid}')"
W="$CLUSTER-worker"; S="$CLUSTER-worker2"
if [ -n "$IMAGES_FILE" ]; then
  # The reviewed images reach the nodes by digest from their registry (crictl pull inside each node that runs them),
  # not with kind load, and every manifest names the <repo>@sha256 reference itself (operator, forecaster and k6 with
  # pull policy Never; the fixture's digest reference defaults to IfNotPresent and is already present). Why: on a classic docker store, kind load of a digest-only image imports it UNNAMED
  # (docker.io/library/import-<date>) under a manifest digest docker save recomputes (seen on aspire with kind v0.33
  # and Docker 29: k6 70af91f8... arrived as 5e482a64...), so a pod naming the digest would not find it and a
  # run-scoped tag would make the manifests name a tag. A node pull lets containerd verify the manifest digest, and the
  # pod status then reports imageID <repo>@sha256:<reviewed digest> (check 8). The node's config digest must also
  # equal the one docker verified on the host (the same image content, both sides).
  while read -r node ref id; do
    docker exec "$node" crictl pull "$ref" >/dev/null || die "node $node: pull $ref"
    docker exec "$node" crictl inspecti -o json "$ref" | "$PY" -c '
import json, sys
def short(r):
    r = r[len("docker.io/"):] if r.startswith("docker.io/") else r
    return r[len("library/"):] if r.startswith("library/") else r
st = json.load(sys.stdin)["status"]
ref, want_id = sys.argv[1], sys.argv[2]
digests, node_id = st.get("repoDigests") or [], st["id"]
assert short(ref) in [short(d) for d in digests], f"{ref} not in the node repoDigests {digests}"
assert node_id == want_id, f"node config digest {node_id} != host {want_id}"
' "$ref" "$id" || die "node $node: $ref is not the image verified on the host"
  done <<EOF
$S $OPERATOR_REF $OPERATOR_ID
$S $STATISTICAL_REF $STATISTICAL_ID
$S $GENERATOR_REF $GENERATOR_ID
$W $FIXTURE_REF $FIXTURE_ID
$S $FIXTURE_REF $FIXTURE_ID
EOF
  pass "reviewed images pulled by digest on the nodes; repo digests and config digests match the host"
else
  "$KIND" load docker-image "$OPERATOR_IMAGE" "$FORECASTER_IMAGE" --name "$CLUSTER" >/dev/null
fi
# Both workers carry the run taint, so the shared platform (metrics-server, KEDA, the metrics backend), which tolerates
# nothing, runs on this throwaway cluster's control plane.
k taint node "$CLUSTER-control-plane" node-role.kubernetes.io/control-plane:NoSchedule- >/dev/null
k label node "$W" c28.devkuban.com/role=workload --overwrite >/dev/null
k label node "$S" c28.devkuban.com/role=support --overwrite >/dev/null
k taint node "$W" c28.devkuban.com/reserved=c28-smoke:NoSchedule --overwrite >/dev/null
k taint node "$S" c28.devkuban.com/reserved=c28-smoke:NoSchedule --overwrite >/dev/null
k create namespace "$NS" >/dev/null
k create namespace "$CTRL" >/dev/null
k create namespace monitoring >/dev/null
pass "cluster $CLUSTER_UID with workload node $W and support node $S (run taint, role labels)"

# --- platform: metrics-server (H-CPU), KEDA (H-RPS), a metrics backend scraping the ingress ---------------------------
guard
k apply -f "$METRICS_SERVER_URL" >/dev/null
k -n kube-system patch deploy metrics-server --type json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' >/dev/null
h repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
h install keda kedacore/keda --version "$KEDA_CHART_VERSION" -n keda --create-namespace --wait --timeout 6m >/dev/null \
  || die "KEDA install"
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
pass "metrics-server, KEDA $KEDA_CHART_VERSION and the metrics backend"

# --- the evaluation release (statistical engine; metrics and forecasts through the evaluation-owned routes) ----------
jq_params="$WORK/params.json"
"$PY" - "$PARAMS" "$jq_params" "$FIXTURE_IMAGE" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))
p["image"] = sys.argv[3]
json.dump(p, open(sys.argv[2], "w"))
PY
c28 release-values --params "$jq_params" --out "$WORK/release-values.yaml"   # routes, and the route's TLS name
if [ -n "$IMAGES_FILE" ]; then   # the chart renders <repository>@<digest>: exactly the reviewed reference
  IMG_OPERATOR="{repository: ${OPERATOR_IMAGE%@*}, digest: ${OPERATOR_IMAGE#*@}}"
  IMG_FORECASTER="{repository: ${FORECASTER_IMAGE%@*}, digest: ${FORECASTER_IMAGE#*@}}"
else
  IMG_OPERATOR="{repository: ${OPERATOR_IMAGE%:*}, tag: ${OPERATOR_IMAGE##*:}}"
  IMG_FORECASTER="{repository: ${FORECASTER_IMAGE%:*}, tag: ${FORECASTER_IMAGE##*:}}"
fi
cat > "$WORK/values.yaml" <<EOF
images:
  operator: $IMG_OPERATOR
  statisticalForecaster: $IMG_FORECASTER
  pullPolicy: Never
operator:
  tolerations: [{key: c28.devkuban.com/reserved, operator: Equal, value: c28-smoke, effect: NoSchedule}]
  nodeSelector: {c28.devkuban.com/role: support}
forecaster:
  tolerations: [{key: c28.devkuban.com/reserved, operator: Equal, value: c28-smoke, effect: NoSchedule}]
  nodeSelector: {c28.devkuban.com/role: support}
networkPolicy: {enabled: false}
EOF
guard
h install pa-eval "$ROOT/charts/predictive-autoscaler" -n "$CTRL" -f "$WORK/release-values.yaml" -f "$WORK/values.yaml" \
  --wait --timeout 8m >/dev/null \
  || die "evaluation release install"
pass "evaluation release in $CTRL watching $NS only"

# --- the fixture and the five arms (both profiles) --------------------------------------------------------------------
c28 render --params "$jq_params" --out "$WORK/all.yaml"
guard
k apply -f "$WORK/all.yaml" >/dev/null
for d in $(k -n "$NS" get deploy -l c28.devkuban.com/run=c28-smoke -o name); do
  wait_for 300 "$d is available" k -n "$NS" rollout status "$d" --timeout=5s
done
wait_for 120 "the fault routes are available" k -n "$CTRL" rollout status deploy/c28-metrics-route --timeout=5s
misplaced="$(k get pods -A -l c28.devkuban.com/run=c28-smoke -o json | "$PY" -c '
import json, sys
W, S = sys.argv[1], sys.argv[2]
for p in json.load(sys.stdin)["items"]:
    want = S if p["metadata"]["name"].startswith("c28-") and "-route-" in p["metadata"]["name"] else W
    if p["spec"].get("nodeName") != want:
        print(p["metadata"]["name"], p["spec"].get("nodeName"))
' "$W" "$S")"
[ -z "$misplaced" ] || die "pods on the wrong node: $misplaced"
pass "1. every fixture pod is on its role node"

# --- 2. exclusive writers ---------------------------------------------------------------------------------------------
wait_for 120 "KEDA created its HPAs" sh -c "[ \"\$(kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n $NS get hpa -o name | grep -c keda-hpa)\" = 2 ]"
{
  echo '['
  first=1
  for kind in hpa scaledobjects.keda.sh predictiveautoscalers.autoscaling.devkuban.com; do
    k get "$kind" -A -o json | "$PY" -c 'import json,sys; [print(json.dumps(i)) for i in json.load(sys.stdin)["items"]]'
  done | while read -r line; do
    [ "$first" = 1 ] || echo ','
    first=0
    echo "$line"
  done
  echo ']'
} > "$WORK/live.json"
c28 audit --params "$jq_params" --live "$WORK/live.json" || die "exclusive-writer audit"
pass "2. every target has exactly its one writer"

# --- 3. Recommend writes nothing ----------------------------------------------------------------------------------------
sleep 90
for d in $(k -n "$NS" get pa -o jsonpath='{.items[*].spec.targetDeployment.name}'); do
  [ "$(k -n "$NS" get deploy "$d" -o jsonpath='{.spec.replicas}')" = 2 ] || die "Recommend arm $d changed its replicas"
  [ "$(k -n "$NS" get pa "$d" -o jsonpath='{.status.mode}')" = Recommend ] || die "$d is not in Recommend"
done
pass "3. the six PredictiveAutoscaler arms are in Recommend and wrote nothing"

# --- 4. accelerated open-loop load through the ingress ------------------------------------------------------------------
# accelerate 120: each program minute lasts 0.5 s, the whole scored program 12 minutes (labelled accelerated-smoke).
c28 plan --kind scored --seed 201 --capacity 7 --target c28-h-rps-s20 --accelerate 120 --out "$WORK/plan.json"
k -n "$NS" create configmap c28-k6 --from-file=plan.json="$WORK/plan.json" --from-file=c28.js="$ROOT/deploy/eval-c28/load/c28.js" >/dev/null
cat > "$WORK/k6-job.yaml" <<EOF
apiVersion: batch/v1
kind: Job
metadata: {name: c28-k6-smoke, namespace: $NS}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 900
  template:
    spec:
      restartPolicy: Never
      tolerations: [{key: c28.devkuban.com/reserved, operator: Equal, value: c28-smoke, effect: NoSchedule}]
      nodeSelector: {c28.devkuban.com/role: support}
      containers:
        - name: k6
          image: $K6_IMAGE
          imagePullPolicy: $K6_PULL
          args: ["run", "-e", "C28_PLAN=/plan/plan.json", "-e", "C28_BASE_URL=http://c28-ingress.$NS.svc",
                 "-e", "C28_SUMMARY=/tmp/summary.json", "/plan/c28.js"]
          volumeMounts: [{name: plan, mountPath: /plan}]
      volumes: [{name: plan, configMap: {name: c28-k6}}]
EOF
k apply -f "$WORK/k6-job.yaml" >/dev/null
# fetch_matches <namespace> <resource> <local port> <remote port> <path> <pattern>: fetch through a short
# port-forward (the images carry no tools) and match the answer.
fetch_matches() {
  local ns=$1 res=$2 lport=$3 rport=$4 path=$5 pattern=$6 out pf
  k -n "$ns" port-forward "$res" "$lport:$rport" >/dev/null 2>&1 & pf=$!
  sleep 2
  out="$("$PY" -c 'import sys, urllib.request; print(urllib.request.urlopen(sys.argv[1], timeout=5).read().decode())' \
    "http://127.0.0.1:$lport$path" 2>/dev/null || true)"
  kill "$pf" 2>/dev/null || true
  grep -Eq "$pattern" <<<"$out"
}
ingress_counted() { fetch_matches "$NS" deploy/c28-ingress 19181 8080 /metrics '^c28_ingress_requests_total\{target="c28-h-rps-s20"\} [1-9]'; }
wait_for 300 "the ingress counts the arrivals" ingress_counted
pass "4. accelerated-smoke load reaches the ingress, which counts it per target"

# --- 5. a scheduled metrics-route outage ---------------------------------------------------------------------------------
start="$(date -u -d '+90 seconds' +%Y-%m-%dT%H:%M:%SZ)"; end="$(date -u -d '+150 seconds' +%Y-%m-%dT%H:%M:%SZ)"
k -n "$CTRL" create configmap c28-metrics-route-faults --dry-run=client -o yaml \
  --from-literal=faults.json="{\"outages\":[{\"start\":\"$start\",\"end\":\"$end\"}]}" | k apply -f - >/dev/null
route_in_outage() { fetch_matches "$CTRL" deploy/c28-metrics-route 19191 9091 /metrics '^c28_proxy_outage 1'; }
wait_for 240 "the route reports its outage" route_in_outage
wait_for 60 "the ingress still counts during the outage" ingress_counted
pass "5. the metrics route refuses during its scheduled outage (the ingress counter is unaffected)"

# --- 6. the scoped quota -------------------------------------------------------------------------------------------------
guard
# A hard limit of 0 (below the pods already counted, which stay) denies every new fixture pod. A limit equal to the
# current count would not hold: the active arms keep scaling, and one scale-down frees a slot (seen on aspire: an H-CPU
# arm stepped down and the denied pod was created before the PA's next decision).
k -n "$NS" patch resourcequota c28-fixture-quota --type merge -p '{"spec":{"hard":{"pods":"0"}}}' >/dev/null
k -n "$NS" scale deploy c28-pa-r-s20 --replicas 3 >/dev/null   # a manual scale of a Recommend arm (it never writes)
wait_for 120 "the quota denies the new fixture pod" sh -c "kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n $NS get deploy c28-pa-r-s20 -o jsonpath='{.status.conditions[?(@.type==\"ReplicaFailure\")].status}' | grep -q True"
wait_for 180 "the PA arm reports ScaleOutBlocked" sh -c "kubectl --kubeconfig '$KCFG' --context 'kind-$CLUSTER' -n $NS get pa c28-pa-r-s20 -o jsonpath='{.status.conditions[?(@.type==\"ScaleOutBlocked\")].reason}' | grep -q QuotaExceeded"
k -n "$NS" run c28-not-fixture --image=registry.k8s.io/pause:3.10 --restart=Never \
  --overrides='{"spec":{"tolerations":[{"key":"c28.devkuban.com/reserved","operator":"Exists","effect":"NoSchedule"}]}}' >/dev/null \
  || die "a non-fixture pod was denied by the scoped quota"
k -n "$NS" patch resourcequota c28-fixture-quota --type merge -p '{"spec":{"hard":{"pods":"64"}}}' >/dev/null
pass "6. the scoped quota denies fixture pods only; ScaleOutBlocked=QuotaExceeded"

# --- 7. the forecast route keeps TLS verification -------------------------------------------------------------------------
# The operator dials https://c28-forecast-route...:8443 (a TCP pass-through) and verifies the forecaster's certificate
# against that name (forecaster.tls.extraDNSNames). Only an answer from the forecast API counts (forecast_api_answered,
# above). The smoke's load reaches only the H-RPS arm, so the PA arms' series are empty and the usual answer here is the
# service's 503.
service_reason() {
  local r m
  r="$(k -n "$NS" get pa c28-pa-n-s20 -o jsonpath='{.status.conditions[?(@.type=="PredictionReady")].reason}')"
  m="$(k -n "$NS" get pa c28-pa-n-s20 -o jsonpath='{.status.conditions[?(@.type=="PredictionReady")].message}')"
  forecast_api_answered "$r" "$m"
}
wait_for 300 "a forecast answer through the verified route" service_reason
log "check 7 saw PredictionReady $(k -n "$NS" get pa c28-pa-n-s20 -o jsonpath='{.status.conditions[?(@.type=="PredictionReady")].reason}: {.status.conditions[?(@.type=="PredictionReady")].message}' | cut -c1-160)"
pass "7. forecast API reachable through the certificate-verified route"
log "v2-acceptance: PENDING (not tested by this smoke: check 7 proves route reachability only, a 503 DataUnavailable included; acceptance with sufficient lead is not exercised)"

# --- 8. the images the pods run ----------------------------------------------------------------------------------------
# The expected set comes from what the smoke deployed, never from an image value: every Deployment of the rendered
# fixture (arms, ingress, routes), the release's operator and forecaster Deployments (any other workload in the release
# fails) and the k6 Job, each with its container names. Every pod those workloads' selectors pick and every container
# in it is then checked by check8_classify (above): the spec must name the role's image and, with --images, the
# runtime must report the reviewed digest. A container still starting (no imageID yet, or fewer pods than the
# workload reports) is looked at again for up to 3 minutes; a hard failure stops at once. The inventory is
# $WORK/running-images.json, and the pod/workload snapshot it was decided on (env values dropped) is
# $WORK/running-images-pods.json.
h get manifest pa-eval -n "$CTRL" > "$WORK/release-manifest.yaml"
"$PY" - "$WORK/check8-expected.json" "${IMAGES_FILE:+reviewed}" "$WORK/all.yaml" "$WORK/release-manifest.yaml" \
  "$WORK/k6-job.yaml" "$OPERATOR_IMAGE" "${OPERATOR_ID:-}" "$FORECASTER_IMAGE" "${STATISTICAL_ID:-}" \
  "$FIXTURE_IMAGE" "${FIXTURE_ID:-}" "$K6_IMAGE" "${GENERATOR_ID:-}" <<'PY' || die "8. the expected workload set"
import json, sys, yaml
out, reviewed, fixture_f, release_f, k6_f = sys.argv[1], sys.argv[2] == "reviewed", *sys.argv[3:6]
a = sys.argv[6:]
roles = {r: {"image": a[2 * i], "id": a[2 * i + 1]} for i, r in enumerate(("operator", "forecaster", "fixture", "generator"))}
WORKLOADS = ("Deployment", "Job", "CronJob", "StatefulSet", "DaemonSet", "ReplicaSet", "Pod")
def load(path):
    return [o for o in yaml.safe_load_all(open(path)) if o and o.get("kind") in WORKLOADS]
def entry(o, role):
    tpl = o["spec"]["template"]["spec"]
    return {"kind": o["kind"], "namespace": o["metadata"]["namespace"], "name": o["metadata"]["name"], "role": role,
            "containers": [c["name"] for c in tpl.get("containers", []) + tpl.get("initContainers", [])]}
workloads = []
for o in load(fixture_f):
    assert o["kind"] == "Deployment", f"unexpected fixture workload {o['kind']} {o['metadata']['name']}"
    workloads.append(entry(o, "fixture"))
for o in load(release_f):
    comp = o["spec"]["template"]["metadata"]["labels"].get("app.kubernetes.io/component") if o["kind"] == "Deployment" else None
    assert comp in ("operator", "forecaster"), f"unexpected release workload {o['kind']} {o['metadata']['name']}"
    workloads.append(entry(o, comp))
for o in load(k6_f):
    assert o["kind"] == "Job", o["kind"]
    workloads.append(entry(o, "generator"))
missing = {r for r in roles} - {w["role"] for w in workloads}
assert not missing, f"no workload for {sorted(missing)}"
json.dump({"reviewed": reviewed, "roles": roles, "workloads": workloads}, open(out, "w"), indent=1)
PY
check8_snapshot() {   # the run's Pods, Deployments and Jobs as one List, env values and managedFields dropped
  local raw="$WORK/.check8-raw"
  k get pods,deployments,jobs -n "$NS" -o json > "$raw-ns.json"
  k get pods,deployments,jobs -n "$CTRL" -o json > "$raw-ctrl.json"
  "$PY" - "$WORK/running-images-pods.json" "$raw-ns.json" "$raw-ctrl.json" <<'PY'
import json, sys
def clean(spec):
    for c in (spec or {}).get("containers", []) + (spec or {}).get("initContainers", []):
        c["env"] = [{"name": e.get("name")} for e in c.get("env") or []]
items = []
for path in sys.argv[2:]:
    for o in json.load(open(path))["items"]:
        o["metadata"].pop("managedFields", None)
        (o["metadata"].get("annotations") or {}).pop("kubectl.kubernetes.io/last-applied-configuration", None)
        clean(o.get("spec") if o["kind"] == "Pod" else ((o.get("spec") or {}).get("template") or {}).get("spec"))
        items.append(o)
json.dump({"apiVersion": "v1", "kind": "List", "items": items}, open(sys.argv[1], "w"), indent=1)
PY
  rm -f -- "$raw-ns.json" "$raw-ctrl.json"
}
attempt=0
while :; do
  attempt=$((attempt + 1))
  guard
  check8_snapshot
  rc=0; check8_classify "$WORK/check8-expected.json" "$WORK/running-images-pods.json" "$WORK/running-images.json" || rc=$?
  { [ "$rc" = 3 ] && [ "$attempt" -lt 18 ]; } || break
  sleep 10
done
"$PY" - "$WORK/running-images.json" "$attempt" <<'PY'
import collections, json, sys
d = json.load(open(sys.argv[1]))
for r in d["containers"]:
    print(f'running-image {r["role"]} {r["pod"]} {r["container"]} verdict={r["verdict"]} spec={r["spec_image"]} imageID={r["imageID"]}')
print("running-images " + json.dumps({"mode": d["mode"], "attempt": int(sys.argv[2]), "workloads": len(d["workloads"]),
      "pods": sum(w["pods"] for w in d["workloads"]), "containers": len(d["containers"]),
      "verdicts": dict(collections.Counter(r["verdict"] for r in d["containers"])),
      "hard": len(d["hard"]), "soft": len(d["soft"])}))
PY
[ "$rc" = 0 ] || die "8. the running images are not exactly the expected ones (see $WORK/running-images.json)"
if [ -n "$IMAGES_FILE" ]; then
  pass "8. every container of every operator, forecaster, fixture and k6 pod names and runs exactly the reviewed digest ($WORK/running-images.json)"
else
  pass "8. every expected container names its image; running image IDs recorded (build mode: no reviewed digests to compare)"
fi
log "smoke complete (accelerated, engineering only: no C2.8 evidence)"
