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
# With --shadow the release is installed as the S3 shadow install runs it (operator.rbac.scaleWrite false: the operator
# may read deployments/scale and nothing more; the forecast/decision ledger on, here on an emptyDir), a second
# accelerated k6 run drives demand at one Recommend arm (c28-pa-r-s90, which no other check touches), and five more
# checks run:
#   S1. effective permissions, once the autoscalers exist (after check 2). The live release roles give
#       deployments/scale exactly [get] and none of them may write a scale subresource. The API server's authorizer,
#       asked as the operator's ServiceAccount (kubectl auth can-i --subresource=scale under impersonation), answers no
#       to update and to patch and yes to get, for the question without a name AND for the Deployment NAME of every
#       PredictiveAutoscaler target (a grant restricted by resourceNames answers only the named question). Only the
#       exact answers count: an impersonation or API error is "unproven" and fails, never a no. And every RoleBinding
#       and ClusterRoleBinding of the cluster whose subjects name that ServiceAccount, its username or one of its
#       groups (system:serviceaccounts, system:serviceaccounts:<namespace>, system:authenticated), whatever its labels,
#       is resolved to its role: no rule there may write a scale subresource of Deployments, named or not.
#       S1's own negative self-test follows: a Role WITHOUT the release's labels that grants update on
#       deployments/scale for one named target (c28-pa-n-s90), bound to the operator's ServiceAccount, is created in
#       this cluster; S1 must then REFUSE (the named question answers yes while the unnamed one still answers no, and
#       the binding scan finds the grant); the Role and RoleBinding are deleted and S1 must pass again, all before any
#       load;
#   S2. under that demand the arm's PredictiveAutoscaler recommends at least 4 replicas (status.calculatedReplicas; the
#       current count is 2) while its Deployment keeps spec.replicas 2, its UID and its metadata.generation from before
#       the load; status.mode is Recommend, ScalingActive is False with reason RecommendMode, Ready and
#       TelemetryAvailable are True and the status records no write (appliedReplicas, lastScaleTime). An unavailable or
#       unqualified forecast (ForecastAvailable=False, PredictionReady not Ready) is EXPECTED here and is reported, not
#       failed: the reactive rule recommends from the request rate;
#   S3. the operator's log (every pod, a restarted container's previous log included), read twice. Right after S2,
#       before checks 5 and 6: no line with "forbidden" (any case), none with "deployments/scale", and the arm's
#       "Recommend mode: not scaling" lines are there (so the zero is not an empty log). At the end: still no
#       "deployments/scale" line, and the only "forbidden" lines are check 6's own quota denial, which the operator
#       quotes in its ScaleOutBlocked event (pods ... is forbidden: exceeded quota: c28-fixture-quota); they are
#       counted and reported, and any other "forbidden" line fails;
#   S4. the ledger, read twice some minutes apart out of the operator's emptyDir through the kind node (the image has no
#       shell): the second read extends the first, the arm's decision records and their newest timestamp advance, at
#       least one recommends more than the current count, and every decision of every autoscaler has action recommend
#       with applied_replicas equal to current_replicas (no scale_up, scale_down or scale_error);
#   S5. at the end the arm's Deployment still has 2 replicas and its generation, and no PredictiveAutoscaler of the run
#       left Recommend or records a write.
# The observed values go to $WORK/shadow-summary.json and a shadow-summary log line. Without --shadow nothing of this
# runs and the release keeps the chart's default (scale writes authorized, no ledger).
# It only ever talks to its own cluster: the kubeconfig in its work directory, checked (context and cluster UID) before
# every step. Nothing here reads or writes a lab cluster, and no evidence produced here counts toward C2.8.
#
#   deploy/eval-c28/smoke/kind.sh               build, create the cluster, run, delete it
#   KEEP=1 deploy/eval-c28/smoke/kind.sh        keep the cluster for inspection
#   deploy/eval-c28/smoke/kind.sh --render-only render and audit the manifests offline and self-test check 7's
#                                               classifier and the --images validation (no cluster, no docker;
#                                               scripts/verify.sh runs it); with --images it also validates that file;
#                                               with --shadow it also renders the chart (helm template, no cluster)
#                                               with and without the shadow values and self-tests S1's rule check
#                                               (get only with them, get and update without), S1's verdict (per-name
#                                               answers and the binding scan, a name-restricted outside grant
#                                               included) and S2's and S4's classifiers (needs helm)
#   deploy/eval-c28/smoke/kind.sh --images images.json
#                                               run the EXACT reviewed images of a c28-images/v1 file (schema
#                                               c28-images/v1, verified true, all four of operator, statistical,
#                                               fixture, generator as <repo>@sha256:<64 hex>): pulled by digest, no
#                                               docker build at all, and check 8 fails on any other running digest
#   deploy/eval-c28/smoke/kind.sh --shadow [--images images.json]
#                                               the shadow variant: checks 1-8 and S1-S5 (above)
# Needs: docker, kind, kubectl, helm, python3 with PyYAML. OPERATOR_IMAGE / FORECASTER_IMAGE (local tags) skip builds
# (not with --images, which supplies every image).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
HARNESS="$ROOT/deploy/eval-c28/harness"
PARAMS="$ROOT/deploy/eval-c28/params/kind-smoke.json"
PY="${PYTHON:-python3}"
case "$PY" in */*) PY="$(cd "$(dirname "$PY")" && pwd)/$(basename "$PY")" ;; esac
c28() { (cd "$HARNESS" && KUBECONFIG=/dev/null "$PY" -m c28 "$@"); }
RENDER_ONLY=0 IMAGES_FILE="" SHADOW=0
while [ $# -gt 0 ]; do
  case "$1" in
    --render-only) RENDER_ONLY=1 ;;
    --shadow) SHADOW=1 ;;
    --images) [ $# -ge 2 ] || { echo "smoke: --images needs a file" >&2; exit 2; }; IMAGES_FILE="$2"; shift ;;
    --images=*) IMAGES_FILE="${1#--images=}" ;;
    *) echo "smoke: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

# The shadow variant (--shadow). The release's operator values gain exactly SHADOW_OPERATOR_VALUES (what the S3 shadow
# preparation sets, staging/prepare.py: no replica write, the ledger on; S3 keeps the ledger on a claim, the smoke on an
# emptyDir). RELEASE_FULLNAME is the chart's fullname for release pa-eval (its ServiceAccount and roles carry it).
# SHADOW_ARM gets the second load: a PA-R arm, because no forecast qualifies within a smoke and the reactive rule
# recommends from the request rate alone; the s90 one, because check 6 scales c28-pa-r-s20 by hand. SHADOW_MIN is the
# recommendation S2 waits for: twice the fixed 2 replicas, so never a rounding of the program's low 2c phase.
# SHADOW_SELFTEST_TARGET is the one target S1's negative self-test grants a scale update for, for a few seconds: another
# Recommend arm than the one S2 measures.
SHADOW_OPERATOR_VALUES='  rbac: {scaleWrite: false}
  forecastLedger: {enabled: true}'
RELEASE_FULLNAME=pa-eval-predictive-autoscaler
SHADOW_ARM=c28-pa-r-s90
SHADOW_MIN=4
SHADOW_SELFTEST_TARGET=c28-pa-n-s90
SHADOW_SELFTEST_GRANT=c28-shadow-selftest-scale-writer

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

# smoke_values <operator image> <forecaster image> <shadow: 0|1>: the smoke's own release values (on top of `c28
# release-values`), the images as chart image maps. With shadow 1 the operator block gains SHADOW_OPERATOR_VALUES and
# nothing else changes; with 0 the output is what the smoke has always installed.
smoke_values() {
  local extra=""
  [ "$3" = 1 ] && extra=$'\n'"$SHADOW_OPERATOR_VALUES"
  cat <<EOF
images:
  operator: $1
  statisticalForecaster: $2
  pullPolicy: Never
operator:
  tolerations: [{key: c28.devkuban.com/reserved, operator: Equal, value: c28-smoke, effect: NoSchedule}]
  nodeSelector: {c28.devkuban.com/role: support}$extra
forecaster:
  tolerations: [{key: c28.devkuban.com/reserved, operator: Equal, value: c28-smoke, effect: NoSchedule}]
  nodeSelector: {c28.devkuban.com/role: support}
networkPolicy: {enabled: false}
EOF
}

# scale_rules shadow|default <RBAC objects> <write ClusterRole>: S1's rule check, on rendered manifests (YAML documents)
# or on live objects (kubectl Lists). The write role must hold exactly one rule that reaches deployments/scale, for
# apiGroup apps and that resource alone, with verbs exactly [get] (shadow) or [get, update] (default); and the roles
# that may write a scale subresource of apps (named or through a wildcard) must be none (shadow) or that role alone
# (default). Prints the verdict, or the reason on stderr with exit 1.
scale_rules() {
  "$PY" - "$1" "$2" "$3" <<'PY'
import sys, yaml
want, path, write_role = sys.argv[1:4]
docs = []
for d in yaml.safe_load_all(open(path)):
    if d:
        docs.extend((d.get("items") or []) if str(d.get("kind", "")).endswith("List") else [d])
roles = [d for d in docs if d.get("kind") in ("ClusterRole", "Role")]
def reaches_scale(rule):
    groups, res = set(rule.get("apiGroups") or []), set(rule.get("resources") or [])
    return bool(groups & {"apps", "*"}) and bool(res & {"deployments/scale", "*/scale", "*"})
WRITES = {"update", "patch", "create", "delete", "deletecollection", "*"}
mine = [r for d in roles if d["kind"] == "ClusterRole" and d["metadata"]["name"] == write_role
        for r in d.get("rules") or [] if reaches_scale(r)]
if len(mine) != 1 or mine[0].get("apiGroups") != ["apps"] or mine[0].get("resources") != ["deployments/scale"]:
    sys.exit(f"scale rule: ClusterRole {write_role} does not hold exactly one apps deployments/scale rule: {mine}")
verbs, expected = mine[0].get("verbs"), ["get"] if want == "shadow" else ["get", "update"]
if verbs != expected:
    sys.exit(f"scale rule: ClusterRole {write_role} gives deployments/scale {verbs}, want exactly {expected} ({want})")
writers = sorted({f'{d["kind"]}/{d["metadata"]["name"]}' for d in roles for r in d.get("rules") or []
                  if reaches_scale(r) and WRITES & set(r.get("verbs") or [])})
if writers != ([] if want == "shadow" else [f"ClusterRole/{write_role}"]):
    sys.exit(f"scale rule: roles that may write a scale subresource: {writers} ({want})")
print(f"ClusterRole {write_role} gives deployments/scale exactly {verbs}; of {len(roles)} roles, those that may write "
      f"a scale subresource: {', '.join(writers) or 'none'}")
PY
}

# shadow_permission_verdict <answers> <targets> <ServiceAccount namespace> <ServiceAccount name> <out.json> <RBAC
# list>...: S1's verdict on the operator ServiceAccount's permission to write replicas.
#   answers: one "<target or -> <verb> <answer>" line per question asked of the authorizer (sa_can; "-" is the question
#     without a name, anything other than yes or no is an unclear answer);
#   targets: the PredictiveAutoscalers' target Deployment names, one per line;
#   RBAC lists: EVERY ClusterRole, Role, ClusterRoleBinding and RoleBinding of the cluster (kubectl Lists as JSON),
#     whatever their labels.
# "-" and every target need update no, patch no and get yes (the control: the release's own scale rule grants it, so a
# no there means the question did not reach the scale rule). Every binding with a subject that is the ServiceAccount,
# its username or one of its groups is resolved to its role (a RoleBinding's Role in its own namespace), and a rule
# there that reaches a scale subresource of apps Deployments (named or by wildcard) with a write verb is a finding,
# with or without resourceNames: a name-restricted grant is still a grant.
# REFUSED (exit 1): a yes to update or patch, or such a grant. UNPROVEN (exit 3): no target, a missing or unclear
# answer, or a failed control. The answers, the identity's bindings, the grants and the reasons go to out.json.
shadow_permission_verdict() {
  "$PY" - "$@" <<'PY'
import json, sys
answers_p, targets_p, sa_ns, sa_name, out = sys.argv[1:6]
targets = [line.strip() for line in open(targets_p) if line.strip()]
answers = {}
for line in open(answers_p):
    parts = line.split()
    if len(parts) == 3:
        answers.setdefault(parts[0], {})[parts[1]] = parts[2]
refused, unproven = [], []
if not targets:
    unproven.append("no PredictiveAutoscaler target to ask about")
for name in ["-"] + targets:
    label = "of any Deployment (no name)" if name == "-" else f"of Deployment {name}"
    for verb in ("update", "patch", "get"):
        got = answers.get(name, {}).get(verb)
        if got not in ("yes", "no"):
            unproven.append(f"{verb} deployments/scale {label}: no clear answer ({got})")
        elif verb != "get" and got == "yes":
            refused.append(f"the authorizer allows {verb} on deployments/scale {label}")
        elif verb == "get" and got == "no":
            unproven.append(f"get deployments/scale {label} answered no: the control failed")
objs = []
for path in sys.argv[6:]:
    doc = json.load(open(path))
    objs.extend(doc.get("items") or [] if str(doc.get("kind", "")).endswith("List") else [doc])
user = f"system:serviceaccount:{sa_ns}:{sa_name}"
groups = {"system:serviceaccounts", f"system:serviceaccounts:{sa_ns}", "system:authenticated"}
def names_identity(subject, binding_ns):
    kind, name = subject.get("kind"), subject.get("name")
    if kind == "ServiceAccount":
        return name == sa_name and (subject.get("namespace") or binding_ns) == sa_ns
    return (kind == "User" and name == user) or (kind == "Group" and name in groups)
def reaches_scale(rule):
    rule_groups, res = set(rule.get("apiGroups") or []), set(rule.get("resources") or [])
    return bool(rule_groups & {"apps", "*"}) and bool(res & {"deployments/scale", "*/scale", "*"})
WRITES = {"update", "patch", "create", "delete", "deletecollection", "*"}
roles = {(o["kind"], o["metadata"].get("namespace") or "", o["metadata"]["name"]): o
         for o in objs if o.get("kind") in ("ClusterRole", "Role")}
bindings, grants = [], []
for b in objs:
    if b.get("kind") not in ("ClusterRoleBinding", "RoleBinding"):
        continue
    ns = b["metadata"].get("namespace") or ""
    subjects = [s for s in b.get("subjects") or [] if names_identity(s, ns)]
    if not subjects:
        continue
    ref = b.get("roleRef") or {}
    role = roles.get((ref.get("kind"), ns if ref.get("kind") == "Role" else "", ref.get("name")))
    bid = f'{b["kind"]}/{ns + "/" if ns else ""}{b["metadata"]["name"]}'
    rid = f'{ref.get("kind")}/{ns + "/" if ref.get("kind") == "Role" and ns else ""}{ref.get("name")}'
    bindings.append({"binding": bid, "role": rid, "role_found": role is not None,
                     "subjects": sorted(f'{s.get("kind")}:{s.get("name")}' for s in subjects)})
    for rule in (role or {}).get("rules") or []:
        if reaches_scale(rule) and WRITES & set(rule.get("verbs") or []):
            g = {"binding": bid, "role": rid, "where": ns or "(every namespace)", "verbs": rule.get("verbs"),
                 "resources": rule.get("resources"), "resourceNames": rule.get("resourceNames") or []}
            grants.append(g)
            refused.append(f'{bid} -> {rid} authorizes {g["verbs"]} on {g["resources"]} in {g["where"]}'
                           + (f', resourceNames {g["resourceNames"]}' if g["resourceNames"] else ""))
verdict = "refused" if refused else "unproven" if unproven else "pass"
json.dump({"identity": user, "groups": sorted(groups), "targets": targets, "answers": answers,
           "bindings_of_the_identity": sorted(bindings, key=lambda x: x["binding"]), "scale_write_grants": grants,
           "verdict": verdict, "refused": refused, "unproven": unproven}, open(out, "w"), indent=1)
for m in refused + unproven:
    print(("REFUSED " if m in refused else "UNPROVEN ") + m, file=sys.stderr)
sys.exit(1 if refused else 3 if unproven else 0)
PY
}

# shadow_verdict <pa.json> <deployment.json> <generation before the load> <uid before the load> <minimum
# recommendation>: S2's verdict on one snapshot of the shadow arm, printed as one JSON line (the observed values, the
# conditions, hard and soft). HARD (exit 1) is anything a replica write or a broken shadow would leave: a Deployment
# with other than 2 replicas, another generation or another UID; a mode other than Recommend; a status that records a
# write (appliedReplicas, lastScaleTime); ScalingActive other than False/RecommendMode (ScalingError lands there).
# SOFT (exit 3: look again) is a recommendation not there yet: no status, Ready or TelemetryAvailable not True, or
# calculatedReplicas below the minimum. ForecastAvailable and PredictionReady are reported and never judged: in a smoke
# no forecast qualifies, and the reactive rule recommends without one.
shadow_verdict() {
  "$PY" - "$@" <<'PY'
import json, sys
pa, dep = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
gen0, uid0, want = int(sys.argv[3]), sys.argv[4], int(sys.argv[5])
st = pa.get("status") or {}
cond = {c.get("type"): c for c in st.get("conditions") or []}
def cs(t):
    return f'{cond[t].get("status")}/{cond[t].get("reason")}' if t in cond else "absent"
d = {"spec_replicas": (dep.get("spec") or {}).get("replicas"), "status_replicas": (dep.get("status") or {}).get("replicas"),
     "generation": dep["metadata"].get("generation"), "uid": dep["metadata"].get("uid")}
obs = {"autoscaler": pa["metadata"]["name"], "resourceVersion": pa["metadata"].get("resourceVersion"),
       "mode": st.get("mode"), "calculatedReplicas": st.get("calculatedReplicas", 0),
       "currentReplicas": st.get("currentReplicas", 0), "appliedReplicas": st.get("appliedReplicas", 0),
       "lastScaleTime": st.get("lastScaleTime"), "deployment": d, "generation_before": gen0,
       "conditions": {t: cs(t) for t in ("Ready", "ScalingActive", "TelemetryAvailable", "ForecastAvailable",
                                         "PredictionReady", "ConflictDetected")}}
hard, soft = [], []
if d["uid"] != uid0:
    hard.append(f'the Deployment was replaced (uid {d["uid"]}, before the load {uid0})')
if d["spec_replicas"] != 2:
    hard.append(f'the Deployment has spec.replicas {d["spec_replicas"]}, not 2')
if d["generation"] != gen0:
    hard.append(f'the Deployment generation is {d["generation"]}, before the load {gen0}')
if obs["appliedReplicas"] or obs["lastScaleTime"]:
    hard.append(f'the status records a replica write (appliedReplicas {obs["appliedReplicas"]}, lastScaleTime {obs["lastScaleTime"]})')
if not st:
    soft.append("no status yet")
else:
    if obs["mode"] != "Recommend":
        hard.append(f'status.mode is {obs["mode"]}, not Recommend')
    if cs("ScalingActive") != "False/RecommendMode":
        hard.append(f'ScalingActive is {cs("ScalingActive")}, not False/RecommendMode')
    for t in ("Ready", "TelemetryAvailable"):
        if not cs(t).startswith("True/"):
            soft.append(f"{t} is {cs(t)}")
    if obs["currentReplicas"] != 2:
        soft.append(f'status.currentReplicas is {obs["currentReplicas"]}, not 2')
    if obs["calculatedReplicas"] < want:
        soft.append(f'calculatedReplicas {obs["calculatedReplicas"]} is below {want}')
print(json.dumps({**obs, "hard": hard, "soft": soft}))
sys.exit(1 if hard else 3 if soft else 0)
PY
}

# shadow_ledger_check <first read> <second read> <arm> <namespace> <out.json>: S4's verdict on two copies of the
# operator's ledger (FORECAST_LOG, JSON lines), the second taken later. The second must extend the first (append-only;
# a last line without its newline is an append in progress and is left out); the arm's decision records must have grown
# in number and in their newest "at"; at least one of them must recommend more than the current count; and EVERY
# decision record, of every autoscaler, must have action recommend with applied_replicas equal to current_replicas
# (scale_up, scale_down and scale_error are the write actions; nothing else is expected from Recommend arms either).
# The summary goes to out.json; exit 1 with the reasons on stderr otherwise.
shadow_ledger_check() {
  "$PY" - "$@" <<'PY'
import collections, json, sys
first_p, second_p, arm, ns, out = sys.argv[1:6]
problems = []
def read(path):
    raw = open(path, "rb").read().split(b"\n")
    body, recs = raw[:-1], []
    for i, line in enumerate(body, 1):
        try:
            rec = json.loads(line)
            assert isinstance(rec, dict)
        except (ValueError, AssertionError):
            problems.append(f"{path}: line {i} is not a JSON object")
            continue
        recs.append(rec)
    return body, recs
def summary(recs):
    dec = [r for r in recs if r.get("event") == "decision"]
    mine = [r for r in dec if r.get("application") == arm and r.get("namespace") == ns]
    return {"records": len(recs), "events": dict(collections.Counter(str(r.get("event", "(no event)")) for r in recs)),
            "decisions": len(dec), "decision_actions": dict(collections.Counter(str(r.get("action")) for r in dec)),
            "arm_decisions": len(mine), "arm_newest_at": max((str(r.get("at", "")) for r in mine), default=""),
            "arm_desired_replicas": sorted({r.get("desired_replicas") for r in mine if isinstance(r.get("desired_replicas"), int)}),
            "arm_forecast_status": dict(collections.Counter(str(r.get("forecast_status")) for r in mine))}, dec, mine
(body1, recs1), (body2, recs2) = read(first_p), read(second_p)
(s1, _, _), (s2, dec2, mine2) = summary(recs1), summary(recs2)
if body2[:len(body1)] != body1:
    problems.append("the second read does not extend the first (the ledger is append-only)")
if s1["arm_decisions"] < 1:
    problems.append(f"the first read has no decision record of {ns}/{arm}")
if s2["arm_decisions"] <= s1["arm_decisions"] or s2["arm_newest_at"] <= s1["arm_newest_at"]:
    problems.append(f'the decision records of {ns}/{arm} did not advance: {s1["arm_decisions"]} (newest {s1["arm_newest_at"]}) '
                    f'then {s2["arm_decisions"]} (newest {s2["arm_newest_at"]})')
def ints(r, *keys):
    return all(isinstance(r.get(k), int) and not isinstance(r.get(k), bool) for k in keys)
above = [r for r in mine2 if ints(r, "desired_replicas", "current_replicas") and r["desired_replicas"] > r["current_replicas"]
         and r.get("action") == "recommend"]
if not above:
    problems.append(f"no decision record of {ns}/{arm} recommends more than the current count")
writes = [r for r in dec2 if r.get("action") != "recommend" or not ints(r, "applied_replicas", "current_replicas")
          or r["applied_replicas"] != r["current_replicas"]]
for r in writes[:5]:
    problems.append(f'a decision record is not a pure recommendation: {r.get("at")} {r.get("namespace")}/{r.get("application")} '
                    f'action {r.get("action")} current {r.get("current_replicas")} applied {r.get("applied_replicas")}')
json.dump({"arm": f"{ns}/{arm}", "first": s1, "second": s2, "arm_recommendations_above_current": len(above),
           "arm_highest_desired_replicas": max((r["desired_replicas"] for r in above), default=None),
           "not_pure_recommendations": len(writes), "problems": problems}, open(out, "w"), indent=1)
for m in problems:
    print(m, file=sys.stderr)
sys.exit(1 if problems else 0)
PY
}

# The shadow checks' offline self-test (--render-only --shadow; helm template only, no cluster): the chart rendered with
# the values the smoke installs (c28 release-values and smoke_values, placeholder images) must give the scale rule get
# only with the shadow values and get and update without them
# (scale_rules, which must also refuse each rendering under the other expectation), the shadow rendering alone carries
# the ledger, and S1's verdict and S2's and S4's classifiers give the exit status each synthetic case is named after
# (for S1: the release's own bindings pass; a write grant reaching the identity through its ServiceAccount, its
# username or a group is refused, name-restricted and unlabelled included, whether the answers or only the bindings
# show it; another identity's grant, a read-only grant and an unresolved roleRef are not; a missing or unclear answer
# and a failed control are unproven).
shadow_self_test() {
  local dir bad=0 f want rc kind write="$RELEASE_FULLNAME-operator-write"
  dir="$(mktemp -d)"
  c28 release-values --params "$PARAMS" --out "$dir/release-values.yaml" || bad=1
  for kind in default shadow; do
    smoke_values "{repository: smoke.invalid/operator, tag: self-test}" "{repository: smoke.invalid/forecaster, tag: self-test}" \
      "$([ "$kind" = shadow ] && echo 1 || echo 0)" > "$dir/$kind-values.yaml"
    KUBECONFIG=/dev/null helm template pa-eval "$ROOT/charts/predictive-autoscaler" -n pa-eval-system --kube-version 1.35.0 \
      -f "$dir/release-values.yaml" -f "$dir/$kind-values.yaml" > "$dir/$kind.yaml" || bad=1
    scale_rules "$kind" "$dir/$kind.yaml" "$write" | sed "s/^/render-only: shadow: $kind values: /" || bad=1   # pipefail
    grep -q "serviceAccountName: $RELEASE_FULLNAME-operator\$" "$dir/$kind.yaml" \
      || { echo "shadow self-test: the $kind rendering's operator does not run as $RELEASE_FULLNAME-operator" >&2; bad=1; }
  done
  if scale_rules shadow "$dir/default.yaml" "$write" >/dev/null 2>&1; then
    echo "shadow self-test: the default rendering (scale update allowed) passed as shadow" >&2; bad=1
  fi
  if scale_rules default "$dir/shadow.yaml" "$write" >/dev/null 2>&1; then
    echo "shadow self-test: the shadow rendering passed as the default" >&2; bad=1
  fi
  if ! grep -q 'name: FORECAST_LOG' "$dir/shadow.yaml" || grep -q 'name: FORECAST_LOG' "$dir/default.yaml"; then
    echo "shadow self-test: the ledger must be on with the shadow values and off without them" >&2; bad=1
  fi
  "$PY" - "$dir" <<'PY'
import copy, json, os, sys
d = sys.argv[1]
def cond(**kv):
    return [{"type": t, "status": v.split("/")[0], "reason": v.split("/")[1]} for t, v in kv.items()]
pa = {"metadata": {"name": "arm", "resourceVersion": "7"}, "status": {
    "mode": "Recommend", "calculatedReplicas": 6, "currentReplicas": 2, "conditions": cond(
        Ready="True/ReconcileCompleted", ScalingActive="False/RecommendMode", TelemetryAvailable="True/Measured",
        ForecastAvailable="False/NotTrusted", PredictionReady="False/DataUnavailable", ConflictDetected="False/NoConflict")}}
dep = {"metadata": {"generation": 1, "uid": "u-1"}, "spec": {"replicas": 2}, "status": {"replicas": 2}}
def setc(p, t, v):
    next(c for c in p["status"]["conditions"] if c["type"] == t).update(status=v.split("/")[0], reason=v.split("/")[1])
cases = {
    "0-recommends-more": lambda p, q: None,
    "0-forecast-used": lambda p, q: setc(p, "ForecastAvailable", "True/Used"),
    "3-not-yet": lambda p, q: p["status"].update(calculatedReplicas=2),
    "3-below-minimum": lambda p, q: p["status"].update(calculatedReplicas=3),
    "3-telemetry-hold": lambda p, q: setc(p, "TelemetryAvailable", "False/MetricsUnavailable"),
    "3-no-status": lambda p, q: p.pop("status"),
    "1-scaled": lambda p, q: (q["spec"].update(replicas=6), q["metadata"].update(generation=2)),
    "1-generation-changed": lambda p, q: q["metadata"].update(generation=2),
    "1-replaced": lambda p, q: q["metadata"].update(uid="u-2"),
    "1-active": lambda p, q: (p["status"].update(mode="Active"), setc(p, "ScalingActive", "True/Active")),
    "1-scaling-error": lambda p, q: (setc(p, "Ready", "False/ScalingError"), setc(p, "ScalingActive", "False/ScalingError")),
    "1-applied": lambda p, q: p["status"].update(appliedReplicas=6),
    "1-last-scale-time": lambda p, q: p["status"].update(lastScaleTime="2026-11-10T00:00:00Z"),
}
for name, mutate in cases.items():
    p, q = copy.deepcopy(pa), copy.deepcopy(dep)
    mutate(p, q)
    json.dump(p, open(os.path.join(d, name + ".pa.json"), "w"))
    json.dump(q, open(os.path.join(d, name + ".deploy.json"), "w"))

def dec(at, app, desired, current=2, action="recommend", applied=None):
    return {"event": "decision", "at": f"2026-11-10T00:{at:02d}:00Z", "application": app, "namespace": "demo",
            "forecast_status": "lead_insufficient", "desired_replicas": desired, "current_replicas": current,
            "applied_replicas": current if applied is None else applied, "action": action}
first = [dec(1, "arm", 2), dec(1, "other", 1), dec(2, "arm", 5)]
later = [dec(3, "arm", 8), dec(3, "other", 1, current=3), {"schema": "x", "issued_at": "2026-11-10T00:03:00Z"}]
def lines(recs, tail=b""):
    return b"".join(json.dumps(r).encode() + b"\n" for r in recs) + tail
ledgers = {
    "0-advances": (lines(first), lines(first + later)),
    "0-append-in-progress": (lines(first, b'{"event": "deci'), lines(first + later, b'{"eve')),
    "1-no-advance": (lines(first), lines(first)),
    "1-not-an-extension": (lines(first), lines(first[1:] + later)),
    "1-no-arm-decision": (lines(first[1:2]), lines(first[1:2] + later)),
    "1-never-above-current": (lines([dec(1, "arm", 2)]), lines([dec(1, "arm", 2), dec(2, "arm", 1)])),
    "1-scale-up": (lines(first), lines(first + later + [dec(4, "arm", 8, action="scale_up", applied=8)])),
    "1-scale-error": (lines(first), lines(first + later + [dec(4, "other", 8, action="scale_error")])),
    "1-applied-differs": (lines(first), lines(first + later + [dec(4, "arm", 8, applied=8)])),
    "1-malformed-line": (lines(first), lines(first, b"not json\n") + lines(later)),
}
for name, (a, b) in ledgers.items():
    open(os.path.join(d, name + ".first.jsonl"), "wb").write(a)
    open(os.path.join(d, name + ".second.jsonl"), "wb").write(b)

# S1: the identity is ServiceAccount ctl/op; targets arm-a and arm-b in namespace demo.
def role(kind, name, rules, ns=None):
    return {"kind": kind, "metadata": {"name": name, **({"namespace": ns} if ns else {})}, "rules": rules}
def bind(kind, name, ref_kind, ref, subjects, ns=None):
    return {"kind": kind, "metadata": {"name": name, **({"namespace": ns} if ns else {})},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": ref_kind, "name": ref}, "subjects": subjects}
def scale(verbs, names=None, resources=("deployments/scale",), groups=("apps",)):
    return {"apiGroups": list(groups), "resources": list(resources), "verbs": list(verbs),
            **({"resourceNames": list(names)} if names else {})}
SA = {"kind": "ServiceAccount", "name": "op", "namespace": "ctl"}
base = [role("ClusterRole", "rel-operator", [{"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get", "list", "watch"]}]),
        bind("ClusterRoleBinding", "rel-operator", "ClusterRole", "rel-operator", [SA]),
        role("ClusterRole", "rel-operator-write", [scale(["get"])]),
        bind("RoleBinding", "rel-operator-write", "ClusterRole", "rel-operator-write", [SA], ns="demo"),
        role("ClusterRole", "system:basic-user", [{"apiGroups": ["authorization.k8s.io"], "resources": ["selfsubjectaccessreviews"], "verbs": ["create"]}]),
        bind("ClusterRoleBinding", "system:basic-user", "ClusterRole", "system:basic-user", [{"kind": "Group", "name": "system:authenticated"}]),
        role("ClusterRole", "hpa", [scale(["get", "update"], resources=("*/scale",), groups=("*",))]),
        bind("ClusterRoleBinding", "hpa", "ClusterRole", "hpa", [{"kind": "ServiceAccount", "name": "horizontal-pod-autoscaler", "namespace": "kube-system"}])]
good = {n: {"update": "no", "patch": "no", "get": "yes"} for n in ("-", "arm-a", "arm-b")}
def answered(**change):   # answered(arm_b={"update": "yes"}): the good answers with that target's changed
    a = copy.deepcopy(good)
    for key, v in change.items():
        a[{"none": "-"}.get(key, key.replace("_", "-"))].update(v)
    return a
ext = lambda rule: [role("Role", "ext", [rule], ns="demo"), bind("RoleBinding", "ext", "Role", "ext", [SA], ns="demo")]
perm = {
    "0-release-only": (good, base),
    "1-named-outside-grant": (answered(arm_b={"update": "yes"}), base + ext(scale(["update"], names=["arm-b"]))),
    "1-answer-only": (answered(arm_b={"update": "yes"}), base),
    "1-patch-answer": (answered(arm_a={"patch": "yes"}), base),
    "1-unnamed-answer": (answered(none={"update": "yes"}), base),
    "1-binding-only-other-name": (good, base + ext(scale(["update"], names=["not-a-target"]))),
    "1-patch-grant": (good, base + ext(scale(["patch"], names=["arm-b"]))),
    "1-through-username": (good, base + [role("Role", "ext", [scale(["update"])], ns="demo"), bind(
        "RoleBinding", "ext", "Role", "ext", [{"kind": "User", "name": "system:serviceaccount:ctl:op"}], ns="demo")]),
    "1-through-namespace-group": (good, base + [bind("ClusterRoleBinding", "ext", "ClusterRole", "hpa",
                                                     [{"kind": "Group", "name": "system:serviceaccounts:ctl"}])]),
    "1-through-all-serviceaccounts": (good, base + [bind("RoleBinding", "ext", "ClusterRole", "hpa",
                                                         [{"kind": "Group", "name": "system:serviceaccounts"}], ns="demo")]),
    "1-through-authenticated-wildcard": (good, base + [
        role("ClusterRole", "ext", [{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}]),
        bind("ClusterRoleBinding", "ext", "ClusterRole", "ext", [{"kind": "Group", "name": "system:authenticated"}])]),
    "1-grant-and-unclear-answer": (answered(arm_b={"update": "unproven"}), base + ext(scale(["update"], names=["arm-b"]))),
    "0-other-serviceaccount": (good, base + [role("Role", "ext", [scale(["update"])], ns="demo"), bind(
        "RoleBinding", "ext", "Role", "ext", [{"kind": "ServiceAccount", "name": "op", "namespace": "elsewhere"}], ns="demo")]),
    "0-other-namespace-group": (good, base + [bind("ClusterRoleBinding", "ext", "ClusterRole", "hpa",
                                                   [{"kind": "Group", "name": "system:serviceaccounts:kube-system"}])]),
    "0-named-read-only": (good, base + ext(scale(["get"], names=["arm-b"]))),
    "0-unresolved-role": (good, base + [bind("RoleBinding", "ext", "Role", "ext", [SA], ns="demo")]),
    "0-role-of-another-namespace": (good, base + [role("Role", "ext", [scale(["update"])], ns="elsewhere"),
                                                  bind("RoleBinding", "ext", "Role", "ext", [SA], ns="demo")]),
    "3-unclear-answer": (answered(arm_b={"update": "unproven"}), base),
    "3-missing-answer": ({k: v for k, v in good.items() if k != "arm-b"}, base),
    "3-control-failed": (answered(arm_a={"get": "no"}), base),
}
for name, (ans, rbac) in perm.items():
    with open(os.path.join(d, name + ".answers"), "w") as fh:
        fh.writelines(f"{t} {verb} {a}\n" for t, verbs in ans.items() for verb, a in verbs.items())
    open(os.path.join(d, name + ".targets"), "w").write("arm-a\narm-b\n")
    json.dump({"kind": "List", "items": rbac}, open(os.path.join(d, name + ".rbac.json"), "w"))
open(os.path.join(d, "3-no-target.answers"), "w").write("- update no\n- patch no\n- get yes\n")
open(os.path.join(d, "3-no-target.targets"), "w").write("")
json.dump({"kind": "List", "items": base}, open(os.path.join(d, "3-no-target.rbac.json"), "w"))
PY
  for f in "$dir"/*.answers; do
    want="${f##*/}"; want="${want%%-*}"
    rc=0; shadow_permission_verdict "$f" "${f%.answers}.targets" ctl op "$dir/out.json" "${f%.answers}.rbac.json" 2>/dev/null || rc=$?
    [ "$rc" = "$want" ] || { echo "shadow self-test: S1 ${f##*/} gave exit $rc, want $want" >&2; bad=1; }
  done
  # The named outside grant must be refused for both reasons: the named answer and the binding scan.
  shadow_permission_verdict "$dir/1-named-outside-grant.answers" "$dir/1-named-outside-grant.targets" ctl op "$dir/out.json" \
    "$dir/1-named-outside-grant.rbac.json" 2>/dev/null || true
  "$PY" -c '
import json, sys
d = json.load(open(sys.argv[1]))
assert d["verdict"] == "refused" and len(d["refused"]) == 2 and d["answers"]["-"]["update"] == "no", d
assert [(g["binding"], g["role"], g["resourceNames"]) for g in d["scale_write_grants"]] == [("RoleBinding/demo/ext", "Role/demo/ext", ["arm-b"])], d
' "$dir/out.json" || { echo "shadow self-test: S1 does not report the named outside grant by both its answer and its binding" >&2; bad=1; }
  for f in "$dir"/*.pa.json; do
    want="${f##*/}"; want="${want%%-*}"
    rc=0; shadow_verdict "$f" "${f%.pa.json}.deploy.json" 1 u-1 "$SHADOW_MIN" >/dev/null 2>&1 || rc=$?
    [ "$rc" = "$want" ] || { echo "shadow self-test: S2 ${f##*/} gave exit $rc, want $want" >&2; bad=1; }
  done
  for f in "$dir"/*.first.jsonl; do
    want="${f##*/}"; want="${want%%-*}"
    rc=0; shadow_ledger_check "$f" "${f%.first.jsonl}.second.jsonl" arm demo "$dir/out.json" 2>/dev/null || rc=$?
    [ "$rc" = "$want" ] || { echo "shadow self-test: S4 ${f##*/} gave exit $rc, want $want" >&2; bad=1; }
  done
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
  if [ "$SHADOW" = 1 ]; then
    command -v helm >/dev/null || { echo "render-only: --shadow needs helm (helm template; no cluster is contacted)" >&2; exit 1; }
    shadow_self_test || { echo "render-only: the shadow checks' self-test failed" >&2; exit 1; }
    echo "render-only: shadow: deployments/scale is get only with the shadow values and get, update without them; the ledger is on only with them; S1 refuses a write grant that reaches the operator's identity, a name-restricted unlabelled one included, and calls an unclear answer unproven; S2 and S4 refuse a write, a changed Deployment and a ledger that does not advance"
  fi
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
# sa_can <verb> [Deployment name]: S1's effective-permission question, answered by the API server's authorizer (a
# SubjectAccessReview under impersonation): may the operator's ServiceAccount <verb> the scale subresource of
# Deployments, or of that one Deployment, in the fixture namespace? The subresource is named with --subresource:
# "deployments/scale" as an argument would mean a Deployment NAMED scale, which the operator may never update, so that
# question answers no for any install. The name matters: a rule with resourceNames answers only the question that
# names one of them. Prints yes or no only when kubectl printed exactly that with the matching exit status (0 yes,
# 1 no); an impersonation or API error prints nothing or something else, and is unproven (return 1), never a no.
sa_can() {
  local out rc=0
  out="$(k auth can-i "$1" "deployments.apps${2:+/$2}" --subresource=scale -n "$NS" \
    --as="system:serviceaccount:$CTRL:$OPERATOR_SA" --as-group=system:serviceaccounts \
    --as-group="system:serviceaccounts:$CTRL" --as-group=system:authenticated 2>"$WORK/shadow-can-i.err")" || rc=$?
  case "$rc:$out" in
    0:yes|1:no) echo "$out" ;;
    *) log "can-i $1 deployments/scale ${2:-(no name)} as the operator ServiceAccount: exit $rc, stdout '$out', stderr: $(tr '\n' ' ' < "$WORK/shadow-can-i.err" | cut -c1-300)" >&2
       return 1 ;;
  esac
}
# shadow_permissions <tag>: S1's whole reading, into $WORK/shadow-permissions-<tag>.json (and .answers, .targets, the
# four RBAC lists): update, patch and get asked without a name and for the target Deployment of every live
# PredictiveAutoscaler (an unclear answer is recorded as unproven, never as no), and every ClusterRole, Role,
# ClusterRoleBinding and RoleBinding of the cluster, judged by shadow_permission_verdict (above). Returns its status:
# 0 pass, 1 refused, 3 unproven (also when the cluster could not be read).
shadow_permissions() {
  local base="$WORK/shadow-permissions-$1" name verb answer kind
  k -n "$NS" get predictiveautoscalers.autoscaling.devkuban.com \
    -o jsonpath='{range .items[*]}{.spec.targetDeployment.name}{"\n"}{end}' | sort -u > "$base.targets" || return 3
  if grep -Evq '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$' "$base.targets"; then log "a target name is not a DNS label"; return 3; fi
  : > "$base.answers"
  while read -r name; do
    for verb in update patch get; do
      answer="$(sa_can "$verb" "${name#-}")" || answer=unproven
      echo "$name $verb $answer" >> "$base.answers"
    done
  done < <(echo -; cat "$base.targets")
  for kind in clusterroles roles clusterrolebindings rolebindings; do
    k get "$kind" -A -o json > "$base.$kind.json" || return 3
  done
  shadow_permission_verdict "$base.answers" "$base.targets" "$CTRL" "$OPERATOR_SA" "$base.json" \
    "$base.clusterroles.json" "$base.roles.json" "$base.clusterrolebindings.json" "$base.rolebindings.json"
}
# permissions_line <shadow-permissions json>: its answers on one line, for the log.
permissions_line() {
  "$PY" -c '
import json, sys
d = json.load(open(sys.argv[1]))
row = lambda n: "{}: update={} patch={} get={}".format("(no name)" if n == "-" else n, *(d["answers"].get(n, {}).get(v) for v in ("update", "patch", "get")))
print("; ".join(row(n) for n in ["-"] + d["targets"]) + "; bindings naming the identity: {}; scale write grants among them: {}".format(
    len(d["bindings_of_the_identity"]), len(d["scale_write_grants"])))
' "$1"
}
# operator_pods: "<name> <uid> <node> <restarts>" for every pod of the release's operator.
operator_pods() {
  k -n "$CTRL" get pods -l "app.kubernetes.io/instance=pa-eval,app.kubernetes.io/component=operator" \
    -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.uid} {.spec.nodeName} {.status.containerStatuses[0].restartCount}{"\n"}{end}'
}
# operator_log_read <out.log>: S3's reading of the whole log of every operator pod (a restarted container's previous
# log first) and its counts: LOG_LINES; LOG_FORBIDDEN, the lines with "forbidden" in any case; LOG_QUOTA, those of them
# that are check 6's quota denial as the operator's ScaleOutBlocked event quotes it (the ReplicaSet controller's
# "pods ... is forbidden: exceeded quota: c28-fixture-quota", never an authorization answer, which reads "is forbidden:
# User ... cannot ..."); LOG_SCALE, the lines naming deployments/scale; LOG_RECOMMEND, the shadow arm's "Recommend mode:
# not scaling" lines; OPERATOR_RESTARTS.
operator_log_read() {
  local pods name uid node restarts
  : > "$1"
  pods="$(operator_pods)" || return 1
  OPERATOR_RESTARTS=0
  while read -r name uid node restarts; do
    [ -n "$name" ] || continue
    OPERATOR_RESTARTS=$((OPERATOR_RESTARTS + restarts))
    if [ "$restarts" != 0 ]; then k -n "$CTRL" logs "$name" --previous --tail=-1 >> "$1" || return 1; fi
    k -n "$CTRL" logs "$name" --tail=-1 >> "$1" || return 1
  done <<<"$pods"
  LOG_LINES="$(grep -c '' "$1" || true)"
  LOG_FORBIDDEN="$(grep -ci 'forbidden' "$1" || true)"
  LOG_QUOTA="$(grep -i 'forbidden' "$1" | grep -c 'pods "[^"]*" is forbidden: exceeded quota: c28-fixture-quota' || true)"
  LOG_SCALE="$(grep -c 'deployments/scale' "$1" || true)"
  LOG_RECOMMEND="$(grep 'Recommend mode: not scaling' "$1" | grep -c "\"$SHADOW_ARM\"" || true)"
}
# shadow_ledger_read <out.jsonl>: a copy of the operator's ledger. The operator image is distroless (no shell, no tar),
# so the file is read where the kubelet keeps the pod's emptyDir, inside this run's own kind node. Exactly one operator
# pod, on the support node, is expected.
shadow_ledger_read() {
  local pods name uid node restarts
  pods="$(operator_pods)" || return 1
  [ "$(grep -c . <<<"$pods")" = 1 ] || { log "expected one operator pod, found: $pods"; return 1; }
  read -r name uid node restarts <<<"$pods"
  { [ "$node" = "$S" ] && [[ "$uid" =~ ^[0-9a-f-]{36}$ ]]; } || { log "operator pod $name: node $node, uid $uid"; return 1; }
  docker exec "$S" cat "/var/lib/kubelet/pods/$uid/volumes/kubernetes.io~empty-dir/ledger/forecasts.jsonl" > "$1"
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
smoke_values "$IMG_OPERATOR" "$IMG_FORECASTER" "$SHADOW" > "$WORK/values.yaml"
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

if [ "$SHADOW" = 1 ]; then
  # --- S1. shadow: the operator's ServiceAccount cannot write replicas -------------------------------------------------
  # Now that the autoscalers exist. Three views. The release's live roles (every ClusterRole and Role it labels): the
  # write role's scale rule is get only and no role of the release may write a scale subresource (scale_rules, above).
  # The effective permission, which also covers anything granted outside the release: the authorizer's own answers for
  # the ServiceAccount the operator Deployment runs as, without a name and for every autoscaler's target by name
  # (sa_can, above); the yes to get is the control, granted by the release's own scale rule. And every binding of the
  # cluster that names that identity, resolved to its role (shadow_permissions and shadow_permission_verdict, above).
  guard
  OPERATOR_SA="$(k -n "$CTRL" get deploy "$RELEASE_FULLNAME-operator" -o jsonpath='{.spec.template.spec.serviceAccountName}')" \
    || die "S1. reading the operator Deployment"
  [ "$OPERATOR_SA" = "$RELEASE_FULLNAME-operator" ] || die "S1. the operator runs as '$OPERATOR_SA', not $RELEASE_FULLNAME-operator"
  { k get clusterroles -l app.kubernetes.io/instance=pa-eval -o yaml && echo --- \
      && k get roles -A -l app.kubernetes.io/instance=pa-eval -o yaml; } > "$WORK/shadow-rbac.yaml" || die "S1. reading the release's roles"
  SCALE_RULE="$(scale_rules shadow "$WORK/shadow-rbac.yaml" "$RELEASE_FULLNAME-operator-write")" || die "S1. the live scale rule is not get only"
  rc=0; shadow_permissions real || rc=$?
  [ "$rc" = 0 ] || die "S1. system:serviceaccount:$CTRL:$OPERATOR_SA on deployments/scale in $NS: $([ "$rc" = 1 ] && echo REFUSED || echo "UNPROVEN (exit $rc)") (the reasons are above; $WORK/shadow-permissions-real.json)"
  grep -qx "$SHADOW_SELFTEST_TARGET" "$WORK/shadow-permissions-real.targets" || die "S1. $SHADOW_SELFTEST_TARGET is not an autoscaler target"
  log "S1 saw: $SCALE_RULE"
  log "S1 saw: $(permissions_line "$WORK/shadow-permissions-real.json")"
  pass "S1. shadow: system:serviceaccount:$CTRL:$OPERATOR_SA may not write deployments/scale in $NS: update=no patch=no get=yes without a name and for each of the $(grep -c . "$WORK/shadow-permissions-real.targets") autoscaler targets by name (the authorizer's answers); no binding that names the identity grants a scale write; the live scale rule is get only"

  # --- S1's negative self-test: a name-restricted grant from outside the release must be refused ----------------------
  # What a check without names misses: a Role that carries none of the release's labels and grants update on
  # deployments/scale for ONE named Deployment, bound to the operator's ServiceAccount. S1 must refuse it (the named
  # question answers yes, the unnamed one still no, and the binding scan finds the Role). The grant exists for a few
  # seconds, on an arm whose autoscaler is in Recommend, before any load; it is deleted before the verdict is judged,
  # S1 must pass again, and check 3 then finds every Recommend arm still at 2 replicas.
  guard
  k apply -f - >/dev/null <<EOF || die "S1 self-test. creating the outside grant"
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: $SHADOW_SELFTEST_GRANT, namespace: $NS}
rules:
  - apiGroups: ["apps"]
    resources: ["deployments/scale"]
    resourceNames: ["$SHADOW_SELFTEST_TARGET"]
    verbs: ["update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: $SHADOW_SELFTEST_GRANT, namespace: $NS}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: $SHADOW_SELFTEST_GRANT}
subjects: [{kind: ServiceAccount, name: $OPERATOR_SA, namespace: $CTRL}]
EOF
  attempt=0
  while :; do   # the authorizer follows the new binding within moments; look again for up to 30 s
    attempt=$((attempt + 1))
    rc=0; shadow_permissions negative 2>"$WORK/shadow-permissions-negative.err" || rc=$?
    { [ "$rc" != 1 ] && [ "$attempt" -lt 6 ]; } || break
    sleep 5
  done
  k -n "$NS" delete rolebinding "$SHADOW_SELFTEST_GRANT" >/dev/null || die "S1 self-test. deleting the outside RoleBinding"
  k -n "$NS" delete role "$SHADOW_SELFTEST_GRANT" >/dev/null || die "S1 self-test. deleting the outside Role"
  sed 's/^/S1 self-test (grant present): /' "$WORK/shadow-permissions-negative.err"
  [ "$rc" = 1 ] || die "S1 self-test. the name-restricted outside grant was NOT refused (exit $rc; $WORK/shadow-permissions-negative.json)"
  "$PY" - "$WORK/shadow-permissions-negative.json" "$SHADOW_SELFTEST_TARGET" "$NS" "$SHADOW_SELFTEST_GRANT" <<'PY' || die "S1 self-test. the refusal is not the expected one"
import json, sys
d, target, ns, grant = json.load(open(sys.argv[1])), *sys.argv[2:5]
a = d["answers"]
assert a[target] == {"update": "yes", "patch": "no", "get": "yes"}, f"answers for {target}: {a[target]}"
others = {n: v for n, v in a.items() if n != target}
assert len(others) == len(d["targets"]) and all(v == {"update": "no", "patch": "no", "get": "yes"} for v in others.values()), others
grants = [(g["binding"], g["role"], g["where"], g["verbs"], g["resourceNames"]) for g in d["scale_write_grants"]]
assert grants == [(f"RoleBinding/{ns}/{grant}", f"Role/{ns}/{grant}", ns, ["update"], [target])], grants
assert d["verdict"] == "refused" and len(d["refused"]) == 2 and not d["unproven"], (d["refused"], d["unproven"])
PY
  log "S1 self-test saw (grant present): $(permissions_line "$WORK/shadow-permissions-negative.json")"
  attempt=0
  while :; do   # and forgets the deleted one as quickly
    attempt=$((attempt + 1))
    rc=0; shadow_permissions after || rc=$?
    { [ "$rc" != 0 ] && [ "$attempt" -lt 6 ]; } || break
    sleep 5
  done
  [ "$rc" = 0 ] || die "S1 self-test. after deleting the outside grant S1 does not pass again (exit $rc; $WORK/shadow-permissions-after.json)"
  [ -z "$(k -n "$NS" get roles,rolebindings -o name | grep -F "$SHADOW_SELFTEST_GRANT" || true)" ] || die "S1 self-test. the outside grant is still there"
  log "S1 self-test saw (grant deleted): $(permissions_line "$WORK/shadow-permissions-after.json")"
  pass "S1 self-test. shadow: with Role $NS/$SHADOW_SELFTEST_GRANT (no release label; update on deployments/scale, resourceNames [$SHADOW_SELFTEST_TARGET]) bound to the operator's ServiceAccount, S1 REFUSED: update=yes for $SHADOW_SELFTEST_TARGET by name while the question without a name still answered no, and the binding scan named the Role; the Role and RoleBinding are deleted and S1 passes again"
fi

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
k6_job() {   # k6_job <job name> <ConfigMap with plan.json and c28.js>: the Job that plays that plan through the ingress
  cat <<EOF
apiVersion: batch/v1
kind: Job
metadata: {name: $1, namespace: $NS}
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
      volumes: [{name: plan, configMap: {name: $2}}]
EOF
}
k6_job c28-k6-smoke c28-k6 > "$WORK/k6-job.yaml"
if [ "$SHADOW" = 1 ]; then
  # The shadow arm's own demand, a second Job in the same file (so check 8 examines its pod too): the ordinary program
  # at the same acceleration (12 minutes) and c. It offers 2c for the first 3 minutes, what the arm's 2 replicas carry,
  # then ramps to 8c and stays between 5c and 9c until minute 10: the reactive rule must then recommend well above 2.
  # The Deployment's identity, generation and replica count are taken now, before any of that load.
  c28 plan --kind ordinary --seed 201 --capacity 7 --target "$SHADOW_ARM" --accelerate 120 --out "$WORK/shadow-plan.json"
  k -n "$NS" create configmap c28-k6-shadow --from-file=plan.json="$WORK/shadow-plan.json" --from-file=c28.js="$ROOT/deploy/eval-c28/load/c28.js" >/dev/null
  { echo ---; k6_job c28-k6-shadow c28-k6-shadow; } >> "$WORK/k6-job.yaml"
  guard
  read -r SHADOW_UID SHADOW_GEN SHADOW_REPLICAS <<<"$(k -n "$NS" get deploy "$SHADOW_ARM" -o jsonpath='{.metadata.uid} {.metadata.generation} {.spec.replicas}')"
  { [ -n "$SHADOW_UID" ] && [[ "$SHADOW_GEN" =~ ^[0-9]+$ ]] && [ "$SHADOW_REPLICAS" = 2 ]; } \
    || die "S2. before the load: $SHADOW_ARM uid '$SHADOW_UID' generation '$SHADOW_GEN' replicas '$SHADOW_REPLICAS'"
  SHADOW_BEFORE="calculatedReplicas $(k -n "$NS" get pa "$SHADOW_ARM" -o jsonpath='{.status.calculatedReplicas} at resourceVersion {.metadata.resourceVersion}')" \
    || die "S2. before the load: reading the autoscaler"
fi
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

if [ "$SHADOW" = 1 ]; then
  # --- S2. shadow: a recommendation above the current count, and no write ---------------------------------------------
  # One snapshot of the arm's PredictiveAutoscaler and Deployment every 10 s, judged by shadow_verdict (above), until
  # the recommendation reaches SHADOW_MIN (up to 10 minutes: the demand passes 2c about 3 minutes into the program and
  # falls back at minute 10). A hard finding (a changed Deployment, a recorded write, a mode or ScalingActive other
  # than Recommend's) stops at once. It runs before check 5, whose metrics outage makes every arm hold for a minute.
  deadline=$(( $(date +%s) + 600 )); last=""
  while :; do
    guard
    k -n "$NS" get pa "$SHADOW_ARM" -o json > "$WORK/shadow-pa.json" || die "S2. reading the autoscaler"
    k -n "$NS" get deploy "$SHADOW_ARM" -o json > "$WORK/shadow-deploy.json" || die "S2. reading the Deployment"
    rc=0; shadow_verdict "$WORK/shadow-pa.json" "$WORK/shadow-deploy.json" "$SHADOW_GEN" "$SHADOW_UID" "$SHADOW_MIN" \
      > "$WORK/shadow-recommend.json" || rc=$?
    case "$rc" in 0|1|3) ;; *) die "S2. the verdict itself failed (exit $rc)" ;; esac
    calc="$("$PY" -c 'import json, sys; print(json.load(open(sys.argv[1]))["calculatedReplicas"])' "$WORK/shadow-recommend.json")"
    [ "$calc" = "$last" ] || log "S2 sees $SHADOW_ARM calculatedReplicas $calc (Deployment spec.replicas 2)"
    last="$calc"
    { [ "$rc" = 3 ] && [ "$(date +%s)" -lt "$deadline" ]; } || break
    sleep 10
  done
  [ "$rc" = 0 ] || die "S2. $SHADOW_ARM: $(cat "$WORK/shadow-recommend.json")"
  shadow_ledger_read "$WORK/forecast-ledger-first.jsonl" || die "S4. the first read of the operator's ledger"
  log "S2 saw: $(cat "$WORK/shadow-recommend.json")"
  log "S2 expected, not failures: ForecastAvailable and PredictionReady as reported above (no forecast qualifies in a smoke; the recommendation is the reactive rule's); before the load: $SHADOW_BEFORE"
  pass "S2. shadow: $SHADOW_ARM recommends $calc replicas while its Deployment keeps 2 (generation $SHADOW_GEN, as before the load; uid unchanged); mode Recommend, ScalingActive=False/RecommendMode, no write recorded"

  # --- S3. shadow: the operator's log has no authorization denial ------------------------------------------------------
  # The first reading, while the arm recommends more than it has and before check 6 makes the API server say "forbidden"
  # for its own reason (the quota): a write refused for the missing update verb would be logged with the API server's
  # "forbidden" message naming deployments/scale, and neither string may appear at all (operator_log_read, above).
  operator_log_read "$WORK/operator-first.log" || die "S3. reading the operator log"
  { [ "$LOG_FORBIDDEN" = 0 ] && [ "$LOG_SCALE" = 0 ]; } \
    || die "S3. the operator log has $LOG_FORBIDDEN 'forbidden' lines and $LOG_SCALE 'deployments/scale' lines: $(grep -i -m 3 -e forbidden -e deployments/scale "$WORK/operator-first.log" | cut -c1-400)"
  [ "$LOG_RECOMMEND" -ge 1 ] || die "S3. the operator log ($LOG_LINES lines) has no 'Recommend mode: not scaling' line of $SHADOW_ARM: nothing was proven"
  LOG_FIRST="{\"lines\": $LOG_LINES, \"forbidden_lines\": $LOG_FORBIDDEN, \"deployments_scale_lines\": $LOG_SCALE, \"recommend_mode_lines_of_the_arm\": $LOG_RECOMMEND, \"operator_restarts\": $OPERATOR_RESTARTS}"
  pass "S3. shadow: up to here the operator log ($LOG_LINES lines, $OPERATOR_RESTARTS restarts) has 0 'forbidden' and 0 'deployments/scale' lines, and $LOG_RECOMMEND 'Recommend mode: not scaling' lines of $SHADOW_ARM"
fi

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

if [ "$SHADOW" = 1 ]; then
  # --- S3 again: the whole run's log ----------------------------------------------------------------------------------
  # Since the first reading check 6 denied a fixture pod by quota, and the operator's ScaleOutBlocked event quotes that
  # denial. Those lines are the only "forbidden" ones allowed (LOG_QUOTA); still none may name deployments/scale.
  guard
  operator_log_read "$WORK/operator.log" || die "S3. reading the operator log"
  { [ "$LOG_FORBIDDEN" = "$LOG_QUOTA" ] && [ "$LOG_SCALE" = 0 ]; } \
    || die "S3. the operator log has $LOG_FORBIDDEN 'forbidden' lines, $LOG_QUOTA of them check 6's quota denial, and $LOG_SCALE 'deployments/scale' lines: $(grep -i -e forbidden -e deployments/scale "$WORK/operator.log" | grep -v -m 3 'is forbidden: exceeded quota: c28-fixture-quota' | cut -c1-400)"
  [ "$LOG_RECOMMEND" -ge 1 ] || die "S3. the operator log ($LOG_LINES lines) has no 'Recommend mode: not scaling' line of $SHADOW_ARM: nothing was proven"
  pass "S3. shadow: at the end the operator log ($LOG_LINES lines, $OPERATOR_RESTARTS restarts) has 0 'deployments/scale' lines and $LOG_FORBIDDEN 'forbidden' lines, all $LOG_QUOTA of them check 6's quota denial in a ScaleOutBlocked event (none an authorization denial); $LOG_RECOMMEND 'Recommend mode: not scaling' lines of $SHADOW_ARM"

  # --- S4. shadow: the ledger advances and holds recommendations only --------------------------------------------------
  # The second read, minutes after S2's (checks 5-8 ran in between), against the first (shadow_ledger_check, above).
  shadow_ledger_read "$WORK/forecast-ledger.jsonl" || die "S4. the second read of the operator's ledger"
  shadow_ledger_check "$WORK/forecast-ledger-first.jsonl" "$WORK/forecast-ledger.jsonl" "$SHADOW_ARM" "$NS" "$WORK/shadow-ledger.json" \
    || die "S4. the operator's ledger (see $WORK/shadow-ledger.json)"
  LEDGER="$("$PY" - "$WORK/shadow-ledger.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
a, b = d["first"], d["second"]
print(f'{a["arm_decisions"]} then {b["arm_decisions"]} decision records of the arm (newest {a["arm_newest_at"]} then '
      f'{b["arm_newest_at"]}), {d["arm_recommendations_above_current"]} of them above the current count (highest '
      f'desired_replicas {d["arm_highest_desired_replicas"]}); all {b["decisions"]} decision records of every autoscaler: '
      f'actions {b["decision_actions"]}, {d["not_pure_recommendations"]} with a write action or applied_replicas != current_replicas')
PY
)"
  pass "S4. shadow: the ledger advanced and holds no write: $LEDGER"

  # --- S5. shadow: at the end nothing was written ----------------------------------------------------------------------
  # The arm once more (only hard findings count now: the last metrics outage may still hold its recommendation), and
  # every PredictiveAutoscaler of the run: still Recommend, ScalingActive False/RecommendMode, no write in its status.
  guard
  k -n "$NS" get pa "$SHADOW_ARM" -o json > "$WORK/shadow-pa-final.json" || die "S5. reading the autoscaler"
  k -n "$NS" get deploy "$SHADOW_ARM" -o json > "$WORK/shadow-deploy-final.json" || die "S5. reading the Deployment"
  rc=0; shadow_verdict "$WORK/shadow-pa-final.json" "$WORK/shadow-deploy-final.json" "$SHADOW_GEN" "$SHADOW_UID" 0 \
    > "$WORK/shadow-final.json" || rc=$?
  case "$rc" in 0|3) ;; *) die "S5. $SHADOW_ARM at the end (verdict exit $rc): $(cat "$WORK/shadow-final.json")" ;; esac
  k -n "$NS" get pa -o json > "$WORK/shadow-pas-final.json" || die "S5. reading the autoscalers"
  ARMS_FINAL="$("$PY" - "$WORK/shadow-pas-final.json" <<'PY'
import json, sys
items = json.load(open(sys.argv[1]))["items"]
bad = []
for pa in items:
    st = pa.get("status") or {}
    sa = next((c for c in st.get("conditions") or [] if c.get("type") == "ScalingActive"), {})
    if st.get("mode") != "Recommend" or (sa.get("status"), sa.get("reason")) != ("False", "RecommendMode") \
            or st.get("appliedReplicas") or st.get("lastScaleTime"):
        bad.append(f'{pa["metadata"]["name"]}: mode {st.get("mode")}, ScalingActive {sa.get("status")}/{sa.get("reason")}, '
                   f'appliedReplicas {st.get("appliedReplicas")}, lastScaleTime {st.get("lastScaleTime")}')
if bad or len(items) != 6:
    sys.exit(f"{len(items)} autoscalers; " + "; ".join(bad))
print(len(items))
PY
)" || die "S5. a PredictiveAutoscaler left Recommend or records a write"
  "$PY" - "$WORK" "$SHADOW_ARM" "$SHADOW_GEN" "$SCALE_RULE" "$LOG_LINES" "$LOG_FORBIDDEN" "$LOG_SCALE" "$LOG_RECOMMEND" \
    "$OPERATOR_RESTARTS" "$ARMS_FINAL" "$SHADOW_BEFORE" "$LOG_QUOTA" "$LOG_FIRST" "$SHADOW_SELFTEST_TARGET" <<'PY'
import json, os, sys
work, arm, gen, rule, lines, forbidden, scale, recommend, restarts, arms, before, quota, log_first, selftest_target = sys.argv[1:15]
load = lambda name: json.load(open(os.path.join(work, name)))
rec, final, ledger = load("shadow-recommend.json"), load("shadow-final.json"), load("shadow-ledger.json")
perm, neg, after = (load(f"shadow-permissions-{t}.json") for t in ("real", "negative", "after"))
keep = lambda p: {k: p[k] for k in ("verdict", "answers", "scale_write_grants", "refused", "unproven")}
summary = {
    "arm": arm, "operator_values": {"rbac.scaleWrite": False, "forecastLedger.enabled": True},
    "S1_permissions": {"identity": perm["identity"], "impersonated_groups": perm["groups"],
                       "question": "kubectl auth can-i <verb> deployments.apps[/<name>] --subresource=scale; '-' is the question without a name",
                       "targets": perm["targets"], **keep(perm),
                       "bindings_of_the_identity": perm["bindings_of_the_identity"], "live_scale_rule": rule},
    "S1_negative_self_test": {"outside_grant": f"Role and RoleBinding without the release's labels: update on deployments/scale, resourceNames [{selftest_target}], bound to the operator ServiceAccount",
                              "with_the_grant": keep(neg), "after_deleting_it": keep(after)},
    "S2_recommendation": {"before_the_load": before, "calculatedReplicas": rec["calculatedReplicas"],
                          "currentReplicas": rec["currentReplicas"], "mode": rec["mode"], "conditions": rec["conditions"],
                          "deployment": rec["deployment"], "generation_before_the_load": int(gen),
                          "appliedReplicas": rec["appliedReplicas"], "lastScaleTime": rec["lastScaleTime"],
                          "autoscaler_resourceVersion": rec["resourceVersion"]},
    "S3_operator_log": {"after_S2_before_check_6": json.loads(log_first),
                        "at_the_end": {"lines": int(lines), "forbidden_lines": int(forbidden),
                                       "forbidden_lines_check_6_quota_denial": int(quota),
                                       "forbidden_lines_other": int(forbidden) - int(quota),
                                       "deployments_scale_lines": int(scale),
                                       "recommend_mode_lines_of_the_arm": int(recommend),
                                       "operator_restarts": int(restarts)}},
    "S4_ledger": ledger,
    "S5_final": {"arm_deployment": final["deployment"], "arm_calculatedReplicas": final["calculatedReplicas"],
                 "arm_conditions": final["conditions"], "arm_hard": final["hard"], "arm_soft": final["soft"],
                 "autoscalers_in_recommend_without_a_write": int(arms)},
}
json.dump(summary, open(os.path.join(work, "shadow-summary.json"), "w"), indent=1)
print("shadow-summary " + json.dumps(summary))
PY
  pass "S5. shadow: at the end $SHADOW_ARM still has 2 replicas at generation $SHADOW_GEN, and all $ARMS_FINAL PredictiveAutoscalers are in Recommend with no write recorded ($WORK/shadow-summary.json)"

  # --- S6..S8: the r3 product images (Codex, 2026-11-12) ---------------------------------------------------------------
  # fetch_text <ns> <resource> <local port> <remote port> <path> <scheme>: the body, or nothing (TLS not verified: a
  # read of the smoke's own self-signed forecaster through a port-forward).
  fetch_text() {
    local ns=$1 res=$2 lport=$3 rport=$4 path=$5 scheme=${6:-http} pf
    k -n "$ns" port-forward "$res" "$lport:$rport" >/dev/null 2>&1 & pf=$!
    sleep 2
    "$PY" -c 'import ssl, sys, urllib.request
ctx = ssl._create_unverified_context()
print(urllib.request.urlopen(sys.argv[1], timeout=5, context=ctx).read().decode())' "$scheme://127.0.0.1:$lport$path" 2>/dev/null || true
    kill "$pf" 2>/dev/null || true
  }
  # S6. One Recommend autoscaler at the demonstration's validated profile, rendered by the stage itself (explicit 60/60,
  # minimum lead 600 s, [seasonalNaive], default trust and calibration), on a target of its own. Within a smoke it has
  # no history: a reason of insufficient history or data is expected; a configuration refusal is not.
  VALID=c28-validated
  k -n "$NS" get deploy c28-pa-n-s20 -o json | "$PY" -c '
import json, sys
d = json.load(sys.stdin)
for k in ("uid", "resourceVersion", "creationTimestamp", "generation", "managedFields", "annotations"):
    d["metadata"].pop(k, None)
d.pop("status", None)
name = sys.argv[1]
d["metadata"]["name"] = name
d["metadata"]["labels"]["c28.devkuban.com/target"] = name
d["spec"]["replicas"] = 1
d["spec"]["selector"]["matchLabels"]["c28.devkuban.com/target"] = name
d["spec"]["template"]["metadata"]["labels"]["c28.devkuban.com/target"] = name
print(json.dumps(d))' "$VALID" > "$WORK/validated-deploy.json" || die "S6. rendering the extra target"
  RUN_LABEL_VALUE="$("$PY" -c 'import json, sys; print(json.load(open(sys.argv[1]))["run_id"])' "$jq_params")"
  ( cd "$HARNESS" && "$PY" -m c28 render --params "$ROOT/deploy/eval-c28/params/kind-demo.json" --stage demonstration --part pas ) \
    | "$PY" -c '
import json, sys, yaml
docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
pa = next(d for d in docs if d["metadata"]["labels"].get("c28.devkuban.com/demo-role") == "forecast")
ns, name, run = sys.argv[1], sys.argv[2], sys.argv[3]
pa["metadata"] = {"name": name, "namespace": ns,
                  "labels": {**pa["metadata"]["labels"], "c28.devkuban.com/target": name, "c28.devkuban.com/run": run}}
pa["spec"]["targetDeployment"] = {"name": name, "namespace": ns}
print(json.dumps(pa))' "$NS" "$VALID" "$RUN_LABEL_VALUE" > "$WORK/validated-pa.json" || die "S6. rendering the validated-profile autoscaler"
  k apply -f "$WORK/validated-deploy.json" >/dev/null || die "S6. applying the extra target"
  k apply -f "$WORK/validated-pa.json" >/dev/null || die "S6. applying the validated-profile autoscaler"
  validated_seen() {
    k -n "$NS" get pa "$VALID" -o json > "$WORK/validated-pa-live.json" 2>/dev/null || return 1
    "$PY" - "$WORK/validated-pa-live.json" "$ROOT/ml-engine" <<'PY'
import json, sys
sys.path.insert(0, sys.argv[2])
from forecasting import contracts as c
pa = json.load(open(sys.argv[1]))
st = pa.get("status") or {}
ready = next((x for x in st.get("conditions", []) if x["type"] == "PredictionReady"), None)
lead = ((st.get("prediction") or {}).get("lead") or {})
if ready is None or not lead:
    sys.exit(1)                                            # not decided yet
settings = c.resolve_settings(pa)
sel, why = c.resolve_lead(pa, settings, pa["metadata"]["generation"])
bad = []
if sel is None or sel.lead_seconds != 600:
    bad.append(f"resolved lead {None if sel is None else sel.lead_seconds} ({why})")
if c.validated_profile(settings, 600) is None:
    bad.append("the live object is not the validated profile")
if ready["reason"] in ("ConfigurationNotValidated", "SettingsInvalid", "LeadInsufficient"):
    bad.append(f"PredictionReady {ready['status']}/{ready['reason']}: {ready.get('message')}")
json.dump({"reason": ready["reason"], "status": ready["status"], "lead": lead, "problems": bad},
          open(sys.argv[1] + ".verdict.json", "w"), indent=1)
sys.exit(2 if bad else 0)
PY
  }
  rc=1; for _ in $(seq 1 30); do rc=0; validated_seen || rc=$?; [ "$rc" = 1 ] || break; sleep 10; done
  [ "$rc" = 0 ] || die "S6. the validated-profile autoscaler: $(cat "$WORK/validated-pa-live.json.verdict.json" 2>/dev/null || echo 'no decision within 300 s')"
  pass "S6. a Recommend autoscaler at the demonstration's validated profile resolves to it (lead 600 s) and is not refused for its configuration: PredictionReady $(cut -c1-200 "$WORK/validated-pa-live.json.verdict.json" | tr -d '\n ')"

  # S7. Health and writes: the operator and the forecaster Ready without restarts; the forecaster's journal healthy;
  # no confirmed scale write; the operator's calculated count for the shadow arm is the ledger's last decision.
  for comp in operator forecaster; do
    read -r ready restarts <<<"$(k -n "$CTRL" get pods -l "app.kubernetes.io/instance=pa-eval,app.kubernetes.io/component=$comp" \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready} {.status.containerStatuses[0].restartCount}{"\n"}{end}' | head -1)"
    [ "$ready" = true ] && [ "$restarts" = 0 ] || die "S7. $comp ready '$ready', restarts '$restarts'"
  done
  fetch_text "$CTRL" "deploy/$RELEASE_FULLNAME-operator" 19271 8080 /metrics > "$WORK/operator-metrics.txt"
  fetch_text "$CTRL" "deploy/$RELEASE_FULLNAME-forecaster" 19272 8443 /metrics https > "$WORK/forecaster-metrics.txt"
  shadow_ledger_read "$WORK/forecast-ledger-end.jsonl" || die "S7. reading the operator's ledger"
  "$PY" - "$WORK/operator-metrics.txt" "$WORK/forecaster-metrics.txt" "$WORK/forecast-ledger-end.jsonl" "$SHADOW_ARM" "$NS" \
    > "$WORK/shadow-health.json" <<'PY' || die "S7. $(cat "$WORK/shadow-health.json")"
import json, re, sys
op, fc, ledger, arm, ns = open(sys.argv[1]).read(), open(sys.argv[2]).read(), sys.argv[3], sys.argv[4], sys.argv[5]
def samples(text, name):
    return [(m.group(1), float(m.group(2))) for m in re.finditer(r"^" + name + r"(\{[^}]*\})? ([-0-9.e+]+)$", text, re.M)]
bad = []
if not op.strip() or not fc.strip():
    bad.append("a metrics endpoint could not be read")
applied = [v for lab, v in samples(op, "predictive_autoscaler_scale_writes_total") if 'result="applied"' in (lab or "")]
if not applied or any(v != 0 for v in applied):
    bad.append(f"confirmed scale writes: {applied}")
if samples(fc, "predictive_autoscaler_forecaster_journal_unhealthy_since_seconds"):
    bad.append("the forecaster's journal is unhealthy")
if [v for _, v in samples(fc, "predictive_autoscaler_forecaster_journal_permanent_failure")] != [0.0]:
    bad.append("the forecaster's journal permanent-failure gauge is not exactly one 0")
calc = [v for lab, v in samples(op, "predictive_autoscaler_calculated_replicas")
        if f'autoscaler="{arm}"' in (lab or "") and f'namespace="{ns}"' in (lab or "")]
last = None
for line in open(ledger):
    try:
        d = json.loads(line)
    except ValueError:
        continue
    if d.get("event") == "decision" and d.get("application") == arm and d.get("namespace") == ns:
        last = d
if last is None or len(calc) != 1 or calc[0] != last.get("desired_replicas"):
    bad.append(f"calculated_replicas {calc} vs the ledger's last decision {None if last is None else last.get('desired_replicas')}")
print(json.dumps({"applied_writes": applied, "calculated": calc,
                  "ledger_last": None if last is None else {k: last.get(k) for k in ("at", "desired_replicas", "action", "telemetry_status")},
                  "problems": bad}))
sys.exit(1 if bad else 0)
PY
  pass "S7. operator and forecaster Ready with 0 restarts, journal healthy, 0 confirmed scale writes, and $SHADOW_ARM's calculated_replicas equals the ledger's last decision: $(cut -c1-240 "$WORK/shadow-health.json")"

  # S8. After the metrics outage (check 5) telemetry and recommendations resumed: the shadow arm's ledger has measured
  # decisions after the outage's end, and its TelemetryAvailable condition is True now.
  "$PY" - "$WORK/forecast-ledger-end.jsonl" "$SHADOW_ARM" "$NS" "$end" > "$WORK/shadow-resume.json" <<'PY' || die "S8. $(cat "$WORK/shadow-resume.json")"
import json, sys
from datetime import datetime
ledger, arm, ns, end = sys.argv[1:5]
end_t = datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ")
after = []
for line in open(ledger):
    try:
        d = json.loads(line)
    except ValueError:
        continue
    if d.get("event") == "decision" and d.get("application") == arm and d.get("namespace") == ns:
        t = datetime.strptime(d["at"][:19], "%Y-%m-%dT%H:%M:%S")
        if t > end_t:
            after.append(d)
measured = [d for d in after if d.get("telemetry_status") == "measured" and d.get("action") == "recommend"]
print(json.dumps({"decisions_after_outage": len(after), "measured_recommendations": len(measured)}))
sys.exit(0 if measured else 1)
PY
  [ "$(k -n "$NS" get pa "$SHADOW_ARM" -o jsonpath='{.status.conditions[?(@.type=="TelemetryAvailable")].status}')" = True ] \
    || die "S8. $SHADOW_ARM's TelemetryAvailable is not True after the outage"
  pass "S8. after the metrics outage $SHADOW_ARM measures and recommends again: $(cat "$WORK/shadow-resume.json")"
fi
log "smoke complete (accelerated, engineering only: no C2.8 evidence)"
