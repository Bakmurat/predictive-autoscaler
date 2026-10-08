#!/usr/bin/env bash
# Run the P8 infrastructure-event detector in the cluster (protocol P8 v3.1): raw export at the stop; a short gate-copy
# step once every load-gate hour of the window is finalized; final detection offline on the evidence volume alone.
#
#   p8-job.sh export --run <name> --start <ISO> --stop <ISO>   raw chunked export + preliminary result (one attempt/run)
#   p8-job.sh resume --run <name> --start <ISO> --stop <ISO>   continue that attempt (verified chunks reused)
#   p8-job.sh gates  --run <name> --start <ISO> --stop <ISO>   copy + hash the finalized gate rows (bounded, 5 min)
#   p8-job.sh final  --run <name> --start <ISO> --stop <ISO> [--attributions FILE]   offline final detection
#   p8-job.sh status --run <name>
# Options: --dry-run (print, apply nothing). Needs KUBE_CONTEXT and VM_QUERY_URL (in-cluster vmselect base).
#
# Code identity: the detector, identity file, mask and launcher go into an immutable ConfigMap named after the sha256 of
# all four; an existing ConfigMap of that name is reused only when its contents match. Attributions (final) go into
# their own immutable ConfigMap named after their hash. Jobs: python image by digest, non-root, read-only root fs, no
# service-account token, off the ML node. gates mounts load-evidence-pvc read-only for at most five minutes: it runs only
# at :25–:44 (well clear of the :14 collector), refuses when any load-evidence / p8 Job of the run is active or when any
# pre-flight query fails (fail closed), is pinned to the node
# where either claim is attached (refusing when they are attached to different nodes), and the script waits for the
# copy to finish and for the load volume to be released before returning.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=ml-engine
IMAGE="public.ecr.aws/docker/library/python@sha256:4c47124a8391cb7a9f571164147d154777cf012a4ece5f86097130d7a4478111"
FILES=(infra_events.py infra-identities.json validity-mask.json p8_launcher.py)
MODE="${1:-}"; shift || true
RUN=""; START=""; STOP=""; DRY=""; ATTR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --run) RUN="$2"; shift 2 ;; --start) START="$2"; shift 2 ;; --stop) STOP="$2"; shift 2 ;;
    --attributions) ATTR="$2"; shift 2 ;; --dry-run) DRY=1; shift ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
: "${KUBE_CONTEXT:?set KUBE_CONTEXT}"
k() { kubectl --request-timeout=30s --context "$KUBE_CONTEXT" -n "$NS" "$@"; }
[[ "$RUN" =~ ^[a-z0-9][a-z0-9-]{2,40}$ ]] || { echo "--run must be a short lowercase name" >&2; exit 2; }
if [ "$MODE" = status ]; then
  k get jobs -l "app=p8-detector,p8-run=$RUN" -o wide; k logs -l "app=p8-detector,p8-run=$RUN" --tail=20 --prefix || true; exit 0
fi
case "$MODE" in export|resume|gates|final) ;; *) echo "mode: export|resume|gates|final|status" >&2; exit 2 ;; esac
: "${VM_QUERY_URL:?set VM_QUERY_URL}"; [ -n "$START" ] && [ -n "$STOP" ] || { echo "--start and --stop are required" >&2; exit 2; }
[ -z "$ATTR" ] || [ "$MODE" = final ] || { echo "--attributions only with final" >&2; exit 2; }

ID=$(for f in "${FILES[@]}"; do printf '%s  %s\n' "$(shasum -a 256 "$HERE/$f" | cut -d' ' -f1)" "$f"; done | shasum -a 256 | cut -c1-12)
CM="p8-code-$ID"
cm_json() {   # name, then file=path pairs
  local name="$1"; shift; local args=(); for kv in "$@"; do args+=(--from-file="$kv"); done
  kubectl create configmap "$name" -n "$NS" "${args[@]}" --dry-run=client -o json |
    python3 -c 'import json,sys; d=json.load(sys.stdin); d["metadata"]["labels"]={"app":"p8-detector"}; d["immutable"]=True; print(json.dumps(d))'
}
ensure_cm() {   # create, or verify that the existing one is immutable and holds exactly these bytes
  local name="$1" json="$2"
  if k get configmap "$name" >/dev/null 2>&1; then
    diff <(echo "$json" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["data"], sort_keys=True))') \
         <(k get configmap "$name" -o json | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d.get("immutable") is True, "not immutable"; print(json.dumps(d["data"], sort_keys=True))') \
      >/dev/null || { echo "ConfigMap $name exists mutable or with other contents; refusing" >&2; exit 3; }
  else
    echo "$json" | k create -f - >/dev/null
  fi
}
CODE_JSON=$(cm_json "$CM" $(for f in "${FILES[@]}"; do printf '%s=%s ' "$f" "$HERE/$f"; done))
ATTR_CM=""
if [ -n "$ATTR" ]; then
  ATTR_CM="p8-attr-$(shasum -a 256 "$ATTR" | cut -c1-12)"; ATTR_JSON=$(cm_json "$ATTR_CM" "attributions.json=$ATTR")
fi
JOB="p8-$MODE-$RUN-$(date -u +%m%d%H%M%S)"
pv_of() { k get pvc "$1" -o jsonpath='{.spec.volumeName}'; }
VA_JSON=""
attachments() { VA_JSON=$(kubectl --request-timeout=30s --context "$KUBE_CONTEXT" get volumeattachments -o json) || { echo "volumeattachment query failed" >&2; exit 4; }; }
attached_node() {   # from the last successful attachments() snapshot
  echo "$VA_JSON" | python3 -c 'import json,sys; pv=sys.argv[1]; n=[a["spec"]["nodeName"] for a in json.load(sys.stdin)["items"] if a["spec"]["source"].get("persistentVolumeName")==pv and (a.get("status") or {}).get("attached")]; print(n[0] if n else "")' "$1" ||
    { echo "volumeattachment parse failed" >&2; exit 4; }; }
PIN=""
if [ "$MODE" = gates ]; then
  m=$((10#$(date -u +%M))); if [ "$m" -lt 25 ] || [ "$m" -ge 45 ]; then echo "gates runs only at :25-:44 (now :$m)" >&2; exit 4; fi
  JOBS=$(k get jobs -o json) || { echo "job query failed; refusing" >&2; exit 4; }
  ACTIVE=$(echo "$JOBS" | python3 -c '
import json, sys
run = sys.argv[1]
items = json.load(sys.stdin)["items"]
act = lambda j: bool((j.get("status") or {}).get("active"))
print(" ".join(j["metadata"]["name"] for j in items if act(j) and (j["metadata"]["name"].startswith("load-evidence")
      or (j["metadata"].get("labels") or {}).get("p8-run") == run)))' "$RUN") || { echo "job parse failed; refusing" >&2; exit 4; }
  [ -z "$ACTIVE" ] || { echo "active Jobs: $ACTIVE; try again later" >&2; exit 4; }
  LOAD_PV=$(pv_of load-evidence-pvc) && EVID_PV=$(pv_of p8-evidence-pvc) || { echo "PVC query failed" >&2; exit 4; }
  [ -n "$LOAD_PV" ] && [ -n "$EVID_PV" ] || { echo "load-evidence-pvc or p8-evidence-pvc unbound (apply deploy.sh ml-engine first)" >&2; exit 4; }
  attachments
  LN=$(attached_node "$LOAD_PV"); EN=$(attached_node "$EVID_PV")
  if [ -n "$LN" ] && [ -n "$EN" ] && [ "$LN" != "$EN" ]; then echo "claims attached to different nodes ($LN, $EN); refusing" >&2; exit 4; fi
  PIN="${LN:-$EN}"
fi
python3 - "$JOB" "$CM" "$IMAGE" "$RUN" "$MODE" "$START" "$STOP" "$VM_QUERY_URL" "$ATTR_CM" "$PIN" > /tmp/p8-job-$$.json <<'PY'
import json, sys
job, cm, image, run, mode, start, stop, prom, attr, pin = sys.argv[1:]
mounts = [{"name": "code", "mountPath": "/opt/p8", "readOnly": True}, {"name": "evidence", "mountPath": "/evidence"}]
vols = [{"name": "code", "configMap": {"name": cm}}, {"name": "evidence", "persistentVolumeClaim": {"claimName": "p8-evidence-pvc"}}]
if mode == "gates":
    mounts.append({"name": "gates", "mountPath": "/gates", "readOnly": True})
    vols.append({"name": "gates", "persistentVolumeClaim": {"claimName": "load-evidence-pvc", "readOnly": True}})
if attr:
    mounts.append({"name": "attr", "mountPath": "/opt/p8-attr", "readOnly": True})
    vols.append({"name": "attr", "configMap": {"name": attr}})
deadline = {"export": 4 * 3600, "resume": 4 * 3600, "gates": 300, "final": 3600}[mode]
affinity = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
    {"matchExpressions": [{"key": "predictive-bench/ml-node", "operator": "DoesNotExist"}]}]}}}
if pin:
    affinity["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchExpressions"].append(
        {"key": "kubernetes.io/hostname", "operator": "In", "values": [pin]})
env = [{"name": k, "value": v} for k, v in (("P8_RUN", run), ("P8_START", start), ("P8_STOP", stop), ("P8_PROM", prom))]
print(json.dumps({"apiVersion": "batch/v1", "kind": "Job",
  "metadata": {"name": job, "namespace": "ml-engine", "labels": {"app": "p8-detector", "p8-run": run, "p8-mode": mode}},
  "spec": {"backoffLimit": 0, "activeDeadlineSeconds": deadline, "ttlSecondsAfterFinished": 120 if mode == "gates" else 7 * 86400,
    "template": {"metadata": {"labels": {"app": "p8-detector", "p8-run": run}, "annotations": {"sidecar.istio.io/inject": "false"}},
      "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
        "securityContext": {"runAsNonRoot": True, "runAsUser": 65534, "runAsGroup": 65534, "fsGroup": 65534,
                            "seccompProfile": {"type": "RuntimeDefault"}},
        "affinity": affinity,
        "containers": [{"name": "p8", "image": image, "command": ["python3", "-B", "/opt/p8/p8_launcher.py", mode], "env": env,
          "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
          "resources": {"requests": {"cpu": "250m", "memory": "512Mi"}, "limits": {"cpu": "1", "memory": "2Gi"}},
          "volumeMounts": mounts}],
        "volumes": vols}}}}))
PY
if [ -n "$DRY" ]; then
  echo "$CODE_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("ConfigMap", d["metadata"]["name"], sorted(d["data"]))'
  [ -n "$ATTR_CM" ] && echo "ConfigMap $ATTR_CM [attributions.json]"
  cat /tmp/p8-job-$$.json; rm -f /tmp/p8-job-$$.json; exit 0
fi
ensure_cm "$CM" "$CODE_JSON"; [ -n "$ATTR_CM" ] && ensure_cm "$ATTR_CM" "$ATTR_JSON"
k create -f /tmp/p8-job-$$.json; rm -f /tmp/p8-job-$$.json
echo "job $JOB created (code $CM${ATTR_CM:+, attributions $ATTR_CM}${PIN:+, pinned to $PIN}); follow: $0 status --run $RUN"
if [ "$MODE" = gates ]; then
  rc=0; k wait --for=condition=complete "job/$JOB" --timeout=300s || rc=$?
  k logs "job/$JOB" --tail=20 || true
  [ "$rc" -eq 0 ] || echo "gates Job did not complete (wait rc $rc); checking cleanup anyway" >&2
  # bounded cleanup verification on both paths: the copy pod gone and the load volume released (or held only by a
  # collector that is not ours)
  DEADLINE=$(( $(date +%s) + 180 ))
  while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    PODS=$(k get pods -l "job-name=$JOB" -o name) || PODS="?"
    attachments; LN=$(attached_node "$LOAD_PV")
    if [ -z "$PODS" ] && [ -z "$LN" ]; then echo "copy pod gone, load-evidence volume released"; exit $(( rc ? 5 : 0 )); fi
    [ "$PODS" != "?" ] && [ -n "$PODS" ] && [ "$(k get "${PODS%% *}" -o jsonpath='{.status.phase}' 2>/dev/null)" = Succeeded ] && k delete "${PODS%% *}" --wait=false >/dev/null 2>&1 || true
    sleep 5
  done
  echo "WARNING: copy pod ${PODS:-gone} / load volume attached to ${LN:-nothing}; check before the next :14 collector" >&2; exit 6
fi
