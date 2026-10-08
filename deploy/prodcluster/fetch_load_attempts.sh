#!/usr/bin/env bash
# Fetch the immutable load-gate attempts of given hours from load-evidence-pvc, for the freeze capture's qualification
# gate (D-1093, Codex r59/r60: final evaluations are taken from the attempts, never from log lines, and conflicting final
# rows fail closed). The volume rules are p8-job.sh gates': only at :25–:44 (clear of the :14 collector), never while a
# load-evidence or P8 gates Job is active (fail closed on any query error), pinned to the node the RWO claim is attached
# to (if it is attached). The read-only pod prints a receipt (every attempts/load-<hour>-*.json of the hours and the
# latest projections load-<hour>.json, each with bytes and sha256), one line per file with its exact text, then a
# trailer (bytes and sha256 of everything before it). The local copy is verified against the trailer and published onto
# an exclusive reservation of <out>; a failed transfer is kept under a unique <out>.failed-<UTC>.* name.
# Cleanup is armed before the pod is created and must be PROVEN: a successful query finds no pod with this run's nonce
# and a successful volume-attachment query shows the claim released; anything unconfirmed exits 6, also after a failure.
#   KUBE_CONTEXT=prodcluster fetch_load_attempts.sh --hours 2026-10-07T21:00:00Z,2026-10-07T22:00:00Z --out FILE
# Test hook (tests only): FETCH_CLOCK_MINUTE replaces the current minute for the :25–:44 rule.
set -euo pipefail
HOURS=""; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in --hours) HOURS="$2"; shift 2 ;; --out) OUT="$2"; shift 2 ;; *) echo "unknown option $1" >&2; exit 2 ;; esac
done
: "${KUBE_CONTEXT:?set KUBE_CONTEXT}"; [ -n "$HOURS" ] && [ -n "$OUT" ] || { echo "--hours and --out are required" >&2; exit 2; }
python3 -c 'import re,sys; [sys.exit(f"bad hour {h}") for h in sys.argv[1].split(",") if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:00:00Z", h)]' "$HOURS"
KUBECTL="${KUBECTL:-kubectl}"
k() { "$KUBECTL" --request-timeout=30s --context "$KUBE_CONTEXT" -n ml-engine "$@"; }
IMAGE="public.ecr.aws/docker/library/python@sha256:4c47124a8391cb7a9f571164147d154777cf012a4ece5f86097130d7a4478111"
NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(6))')
POD="load-attempts-fetch-$(date -u +%H%M%S)-$NONCE"

m=$((10#${FETCH_CLOCK_MINUTE:-$(date -u +%M)})); if [ "$m" -lt 25 ] || [ "$m" -ge 45 ]; then echo "runs only at :25-:44 (now :$m)" >&2; exit 4; fi
( set -o noclobber; : > "$OUT" ) 2>/dev/null || { echo "$OUT exists (or cannot be reserved)" >&2; exit 2; }
RESERVED=1
ACTIVE=$(k get jobs -o json | python3 -c '
import json, sys
print(" ".join(j["metadata"]["name"] for j in json.load(sys.stdin)["items"]
               if (j.get("status") or {}).get("active") and (j["metadata"]["name"].startswith("load-evidence")
               or (j["metadata"].get("labels") or {}).get("p8-mode") == "gates")))') || { echo "job query failed; refusing" >&2; rm -f "$OUT"; exit 4; }
[ -z "$ACTIVE" ] || { echo "active Jobs: $ACTIVE; try again later" >&2; rm -f "$OUT"; exit 4; }
LOAD_PV=$(k get pvc load-evidence-pvc -o jsonpath='{.spec.volumeName}') && [ -n "$LOAD_PV" ] || { echo "load-evidence-pvc query failed" >&2; rm -f "$OUT"; exit 4; }
attached_node() {   # exit status of the query is the function's (pipefail); output: the node, empty when released
  "$KUBECTL" --request-timeout=30s --context "$KUBE_CONTEXT" get volumeattachments -o json | python3 -c '
import json, sys
pv = sys.argv[1]
n = [a["spec"]["nodeName"] for a in json.load(sys.stdin)["items"]
     if a["spec"]["source"].get("persistentVolumeName") == pv and (a.get("status") or {}).get("attached")]
print(n[0] if n else "")' "$LOAD_PV"
}
PIN=$(attached_node) || { echo "volumeattachment query failed; refusing" >&2; rm -f "$OUT"; exit 4; }

cleanup() {   # 0 only when a successful query shows no pod of this run AND the volume released
  k delete pod -l "fetch-nonce=$NONCE" --wait=false >/dev/null 2>&1 || true
  local deadline=$(( $(date +%s) + ${FETCH_CLEANUP_SECONDS:-180} )) pods node
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if pods=$(k get pods -l "fetch-nonce=$NONCE" -o name) && [ -z "$pods" ] && node=$(attached_node) && [ -z "$node" ]; then
      echo "reader pod gone, load-evidence volume released"; return 0
    fi
    sleep "${FETCH_POLL_SECONDS:-5}"
  done
  echo "WARNING: could not confirm that the reader pod is gone and the load-evidence volume released; check before the next :14 collector" >&2
  return 6
}
finish() {   # every exit path: unpublished reservation removed; unconfirmed cleanup turns any status into 6
  local rc=$1
  [ "${PUBLISHED:-0}" = 1 ] || { [ "${RESERVED:-0}" = 1 ] && [ ! -s "$OUT" ] && rm -f "$OUT"; }
  if [ "${ARMED:-0}" = 1 ]; then cleanup || rc=6; fi
  exit "$rc"
}
ARMED=1
trap 'finish $?' EXIT

SCRIPT=$(cat <<'PY'
import glob, hashlib, json, os, sys
hours = os.environ["HOURS"].split(","); base = "/gates"
tag = lambda h: "load-" + h[:4] + h[5:7] + h[8:13] + h[14:16] + "Z"
files = []
for h in hours:
    t = tag(h)
    for p in sorted(glob.glob(os.path.join(base, "attempts", t + "-*.json"))) + [os.path.join(base, t + ".json")]:
        if os.path.isfile(p) and not os.path.islink(p):
            b = open(p, "rb").read()
            files.append((h, os.path.relpath(p, base), b))
rec = {"receipt": {"extractor": "fetch_load_attempts.sh v1", "extractor_sha256": os.environ["EXTRACTOR_SHA256"],
                   "hours": hours, "source": {"context": os.environ["SRC_CONTEXT"], "pvc": "load-evidence-pvc",
                                              "pod": os.environ["SRC_POD"], "node": os.environ.get("SRC_NODE", "")},
                   "files": [{"hour": h, "path": p, "bytes": len(b), "sha256": hashlib.sha256(b).hexdigest()}
                             for h, p, b in files]}}
out = [json.dumps(rec)] + [json.dumps({"file": p, "text": b.decode()}) for h, p, b in files]
payload = ("\n".join(out) + "\n").encode()
sys.stdout.write(payload.decode())
print(json.dumps({"trailer": {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}}))
PY
)
SELF_SHA=$(shasum -a 256 "$0" | cut -d' ' -f1)
python3 - "$POD" "$IMAGE" "$HOURS" "$SCRIPT" "$SELF_SHA" "$KUBE_CONTEXT" "$PIN" "$NONCE" <<'PY' | k create -f - >/dev/null
import json, sys
pod, image, hours, script, self_sha, ctx, pin, nonce = sys.argv[1:]
terms = [{"key": "predictive-bench/ml-node", "operator": "DoesNotExist"}]
if pin:
    terms.append({"key": "kubernetes.io/hostname", "operator": "In", "values": [pin]})
print(json.dumps({"apiVersion": "v1", "kind": "Pod",
  "metadata": {"name": pod, "namespace": "ml-engine", "labels": {"app": "load-attempts-fetch", "fetch-nonce": nonce,
                                                                  "app.kubernetes.io/part-of": "predictive-benchmark"},
               "annotations": {"sidecar.istio.io/inject": "false"}},
  "spec": {"restartPolicy": "Never", "activeDeadlineSeconds": 240, "automountServiceAccountToken": False,
    "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{"matchExpressions": terms}]}}},
    "securityContext": {"runAsNonRoot": True, "runAsUser": 65534, "seccompProfile": {"type": "RuntimeDefault"}},
    "containers": [{"name": "x", "image": image, "command": ["python3", "-B", "-c", script],
      "env": [{"name": "HOURS", "value": hours}, {"name": "EXTRACTOR_SHA256", "value": self_sha},
              {"name": "SRC_CONTEXT", "value": ctx}, {"name": "SRC_POD", "value": pod},
              {"name": "SRC_NODE", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}],
      "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
      "resources": {"requests": {"cpu": "20m", "memory": "64Mi"}, "limits": {"cpu": "500m", "memory": "256Mi"}},
      "volumeMounts": [{"name": "gates", "mountPath": "/gates", "readOnly": True}]}],
    "volumes": [{"name": "gates", "persistentVolumeClaim": {"claimName": "load-evidence-pvc", "readOnly": True}}]}}))
PY
k wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout=200s >/dev/null
TMP=$(mktemp "$OUT.part.XXXXXX")
failed_copy() { local f; f=$(mktemp "$OUT.failed-$(date -u +%Y%m%dT%H%M%SZ).XXXXXX"); mv "$TMP" "$f"; echo "$1; kept as $f" >&2; exit 1; }
rc=0; k logs "$POD" > "$TMP" || rc=$?
[ "$rc" -eq 0 ] || failed_copy "kubectl logs failed ($rc)"
python3 -c '
import hashlib, json, sys
data = open(sys.argv[1], "rb").read()
body, _, last = data.rstrip(b"\n").rpartition(b"\n")
t = json.loads(last)["trailer"]
body += b"\n"
sys.exit(0 if len(body) == t["bytes"] and hashlib.sha256(body).hexdigest() == t["sha256"] else 1)
' "$TMP" || failed_copy "transfer did not match the trailer"
mv -f "$TMP" "$OUT"; PUBLISHED=1
echo "$OUT: $(wc -l < "$OUT" | tr -d ' ') lines, verified against the trailer, sha256 $(shasum -a 256 "$OUT" | cut -c1-16)…"
