#!/usr/bin/env bash
# Extract the ensemble issuances (E1/E2′ raw forecasts and margin policy) from the evidence archive, in the cluster. A
# short read-only pod next to ml-api (whose node holds the RWO evidence-archive volume) hashes the WHOLE
# logs/api-ensemble.jsonl (receipt: lines, bytes, sha256, malformed lines, records without policy fields = archiver v1)
# and prints compact rows since --since, then a trailer with the byte count and hash of everything printed before it:
#   ["E", <ensemble fields…>]   (the receipt lists them)
# The local copy is verified against the trailer before it is published (atomically); a failed transfer is kept as
# <out>.failed-<UTC>. Used by forecasts.py to attach raw forecasts and policy to operator issuances (matched by
# application, origin and all six served values; unmatched = raw unavailable).
#   KUBE_CONTEXT=prodcluster extract_ensemble.sh --since 2026-10-07T23:35:00Z --out ensemble.jsonl
set -euo pipefail
SINCE=""; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in --since) SINCE="$2"; shift 2 ;; --out) OUT="$2"; shift 2 ;; *) echo "unknown option $1" >&2; exit 2 ;; esac
done
: "${KUBE_CONTEXT:?set KUBE_CONTEXT}"; [ -n "$SINCE" ] && [ -n "$OUT" ] || { echo "--since and --out are required" >&2; exit 2; }
[ -e "$OUT" ] && { echo "$OUT exists" >&2; exit 2; }
KUBECTL="${KUBECTL:-kubectl}"   # deploy.env pins a kubectl within one minor version of the server
k() { "$KUBECTL" --context "$KUBE_CONTEXT" -n ml-engine "$@"; }
POD="ensemble-extract-$(date -u +%H%M%S)"
IMAGE="public.ecr.aws/docker/library/python@sha256:4c47124a8391cb7a9f571164147d154777cf012a4ece5f86097130d7a4478111"
SCRIPT=$(cat <<'PY'
import hashlib, json, os, sys
since = os.environ["SINCE"]; p = "/archive/logs/api-ensemble.jsonl"; h = hashlib.sha256(); n = b = bad = old = 0; rows = []
E = ("ts", "application", "namespace", "origin", "experiment", "margin_mode", "margin_quantile", "partial_rule",
     "fingerprint", "boundary", "margin", "margin_samples", "stale_generation", "line_sha256", "raw", "served",
     "generation")
with open(p, "rb") as f:
    for line in f:
        h.update(line); n += 1; b += len(line)
        try:
            r = json.loads(line)
        except Exception:
            bad += 1
            continue
        if r.get("ts", "") < since:
            continue
        if "experiment" not in r:
            old += 1
            continue
        rows.append(["E"] + [r.get(k) for k in E[:-1]] + [(r.get("body") or {}).get("generation")])
out = [json.dumps({"receipt": {"path": p, "lines": n, "bytes": b, "sha256": h.hexdigest(), "malformed_lines": bad,
                               "records_without_policy": old, "since": since, "ensemble_rows": len(rows),
                               "ensemble_fields": E, "extractor": "extract_ensemble.sh v2",
                               "extractor_sha256": os.environ["EXTRACTOR_SHA256"],
                               "source": {"context": os.environ["SRC_CONTEXT"], "pvc": "evidence-archive-pvc",
                                          "pod": os.environ["SRC_POD"]}}})]
out += [json.dumps(r, separators=(",", ":")) for r in rows]
payload = ("\n".join(out) + "\n").encode()
sys.stdout.write(payload.decode())
print(json.dumps({"trailer": {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}}))
PY
)
SELF_SHA=$(shasum -a 256 "$0" | cut -d' ' -f1)
python3 - "$POD" "$IMAGE" "$SINCE" "$SCRIPT" "$SELF_SHA" "$KUBE_CONTEXT" <<'PY' | k create -f - >/dev/null
import json, sys
pod, image, since, script, self_sha, ctx = sys.argv[1:]
print(json.dumps({"apiVersion": "v1", "kind": "Pod",
  "metadata": {"name": pod, "namespace": "ml-engine", "labels": {"app": "ensemble-extract", "app.kubernetes.io/part-of": "predictive-benchmark"}},
  "spec": {"restartPolicy": "Never", "activeDeadlineSeconds": 600, "automountServiceAccountToken": False,
    "affinity": {"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
        {"labelSelector": {"matchLabels": {"app": "ml-api"}}, "topologyKey": "kubernetes.io/hostname"}]}},
    "securityContext": {"runAsNonRoot": True, "runAsUser": 65534, "seccompProfile": {"type": "RuntimeDefault"}},
    "containers": [{"name": "x", "image": image, "command": ["python3", "-B", "-c", script],
      "env": [{"name": "SINCE", "value": since}, {"name": "EXTRACTOR_SHA256", "value": self_sha},
              {"name": "SRC_CONTEXT", "value": ctx}, {"name": "SRC_POD", "value": pod}],
      "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
      "resources": {"requests": {"cpu": "20m", "memory": "64Mi"}, "limits": {"cpu": "500m", "memory": "256Mi"}},
      "volumeMounts": [{"name": "archive", "mountPath": "/archive", "readOnly": True}]}],
    "volumes": [{"name": "archive", "persistentVolumeClaim": {"claimName": "evidence-archive-pvc", "readOnly": True}}]}}))
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
