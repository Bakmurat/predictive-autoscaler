#!/usr/bin/env bash
# Extract the operator's decision and issuance rows from its forecast log, in the cluster (the full log is too large to
# stream through a port-forward). A short read-only pod on the operator's node mounts forecast-log-pvc read-only, hashes
# the WHOLE file (receipt: lines, bytes, sha256, malformed-line count) and prints compact rows since --since:
#   ["D", <decision fields…>, <forecast_lookup fields…>]   one per reconcile ("event":"decision")
#   ["I", <issuance fields…>]   issuance_id, issued_at, application, namespace, inference_input_end, target_anchor,
#                              model_version, model_trained_at, training_cutoff, artifact_sha256, step_minutes,
#                              [[step, target_at, rpm]…] (the receipt lists them; the readers validate the widths)
#   {"trailer": {"bytes": N, "sha256": H}}   the byte count and hash of every line printed before it
# The local copy is verified against the trailer before it is published (atomically); a failed transfer is kept as
# <out>.failed-<UTC>. Used by shortage_events.py (which validates the rows against the receipt and the trailer).
#   KUBE_CONTEXT=prodcluster extract_decisions.sh --since 2026-10-07T00:00:00Z --out rows.jsonl
set -euo pipefail
SINCE=""; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in --since) SINCE="$2"; shift 2 ;; --out) OUT="$2"; shift 2 ;; *) echo "unknown option $1" >&2; exit 2 ;; esac
done
: "${KUBE_CONTEXT:?set KUBE_CONTEXT}"; [ -n "$SINCE" ] && [ -n "$OUT" ] || { echo "--since and --out are required" >&2; exit 2; }
[ -e "$OUT" ] && { echo "$OUT exists" >&2; exit 2; }
k() { kubectl --context "$KUBE_CONTEXT" -n ml-engine "$@"; }
NODE=$(k get pod -l app=predictive-operator -o jsonpath='{.items[0].spec.nodeName}')
POD="decisions-extract-$(date -u +%H%M%S)"
IMAGE="public.ecr.aws/docker/library/python@sha256:4c47124a8391cb7a9f571164147d154777cf012a4ece5f86097130d7a4478111"
SCRIPT=$(cat <<'PY'
import hashlib, json, os, sys
since = os.environ["SINCE"]; p = "/fc/forecasts.jsonl"; h = hashlib.sha256(); n = b = bad = 0; dec = []; iss = []
K = ("at", "application", "forecast_status", "lead_window_peak_rpm", "raw_predicted_replicas", "confidence_adjusted_replicas",
     "predicted_after_clamp", "predicted_replicas", "safeguards", "reactive_replicas", "current_rpm", "desired_replicas",
     "desired_source", "current_replicas", "applied_replicas", "action", "confidence", "forecast_issued_at", "min_replicas",
     "max_replicas")
L = ("resolution", "cache_action", "cache_age_seconds", "returned_issuance_id", "returned_link_status")
I = ("issuance_id", "issued_at", "application", "namespace", "inference_input_end", "target_anchor", "model_version",
     "model_trained_at", "training_cutoff", "artifact_sha256", "step_minutes")
with open(p, "rb") as f:
    for line in f:
        h.update(line); n += 1; b += len(line)
        try:
            r = json.loads(line)
        except Exception:
            bad += 1
            continue
        if r.get("event") == "decision" and r.get("at", "") >= since:
            fl = r.get("forecast_lookup") or {}
            dec.append(["D"] + [r.get(k) for k in K] + [fl.get(k) for k in L])
        elif "forecasts" in r and "issued_at" in r and r.get("issued_at", "") >= since:
            iss.append(["I"] + [r.get(k) for k in I] + [[[x.get("step"), x.get("target_at"), x.get("rpm")]
                                                          for x in r.get("forecasts") or []]])
out = [json.dumps({"receipt": {"path": p, "lines": n, "bytes": b, "sha256": h.hexdigest(), "malformed_lines": bad,
                               "since": since, "decisions": len(dec), "issuances": len(iss), "decision_fields": K,
                               "lookup_fields": L, "issuance_fields": I + ("forecasts",),
                               "extractor": "extract_decisions.sh v3",
                               "extractor_sha256": os.environ["EXTRACTOR_SHA256"],
                               "source": {"context": os.environ["SRC_CONTEXT"], "pvc": "forecast-log-pvc",
                                          "pod": os.environ["SRC_POD"], "node": os.environ["SRC_NODE"]}}})]
out += [json.dumps(row, separators=(",", ":")) for row in dec + iss]
payload = ("\n".join(out) + "\n").encode()
sys.stdout.write(payload.decode())
print(json.dumps({"trailer": {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}}))
PY
)
SELF_SHA=$(shasum -a 256 "$0" | cut -d' ' -f1)
python3 - "$POD" "$NODE" "$IMAGE" "$SINCE" "$SCRIPT" "$SELF_SHA" "$KUBE_CONTEXT" <<'PY' | k create -f - >/dev/null
import json, sys
pod, node, image, since, script, self_sha, ctx = sys.argv[1:]
print(json.dumps({"apiVersion": "v1", "kind": "Pod",
  "metadata": {"name": pod, "namespace": "ml-engine", "labels": {"app": "decisions-extract", "app.kubernetes.io/part-of": "predictive-benchmark"}},
  "spec": {"nodeName": node, "restartPolicy": "Never", "activeDeadlineSeconds": 600, "automountServiceAccountToken": False,
    "securityContext": {"runAsNonRoot": True, "runAsUser": 65534, "seccompProfile": {"type": "RuntimeDefault"}},
    "containers": [{"name": "x", "image": image, "command": ["python3", "-B", "-c", script], "env": [{"name": "SINCE", "value": since}, {"name": "EXTRACTOR_SHA256", "value": self_sha},
              {"name": "SRC_CONTEXT", "value": ctx}, {"name": "SRC_POD", "value": pod}, {"name": "SRC_NODE", "value": node}],
      "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
      "resources": {"requests": {"cpu": "20m", "memory": "64Mi"}, "limits": {"cpu": "500m", "memory": "256Mi"}},
      "volumeMounts": [{"name": "fc", "mountPath": "/fc", "readOnly": True}]}],
    "volumes": [{"name": "fc", "persistentVolumeClaim": {"claimName": "forecast-log-pvc", "readOnly": True}}]}}))
PY
trap 'k delete pod "$POD" --wait=false >/dev/null 2>&1 || true' EXIT
k wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout=300s >/dev/null
TMP="$OUT.part-$$"
rc=0; k logs "$POD" > "$TMP" || rc=$?
if [ "$rc" -ne 0 ]; then
  F="$OUT.failed-$(date -u +%Y%m%dT%H%M%SZ)"; mv "$TMP" "$F"; echo "kubectl logs failed ($rc); partial transfer kept as $F" >&2; exit 1
fi
if python3 -c '
import hashlib, json, sys
data = open(sys.argv[1], "rb").read()
body, _, last = data.rstrip(b"\n").rpartition(b"\n")
t = json.loads(last)["trailer"]
body += b"\n"
sys.exit(0 if len(body) == t["bytes"] and hashlib.sha256(body).hexdigest() == t["sha256"] else 1)
' "$TMP"; then
  mv "$TMP" "$OUT"; echo "$OUT: $(wc -l < "$OUT" | tr -d ' ') lines, verified against the trailer, sha256 $(shasum -a 256 "$OUT" | cut -c1-16)…"
else
  F="$OUT.failed-$(date -u +%Y%m%dT%H%M%SZ)"; mv "$TMP" "$F"; echo "transfer did not match the trailer; kept as $F" >&2; exit 1
fi
