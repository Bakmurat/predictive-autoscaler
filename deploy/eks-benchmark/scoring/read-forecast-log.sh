#!/usr/bin/env bash
# Read the operator's persisted forecast record from its PVC (Codex Task 03 C-23/C-25).
#
# The operator image is distroless, so the file is read by a short-lived busybox pod that mounts the
# same claim read-only on the operator's node; content is streamed by exec and verified by length+hash. Collection failure, a missing file and an empty file are three
# distinct outcomes and are never conflated.
#
#   read-forecast-log.sh --all  [--out FILE]   # the COMPLETE file (no truncation)
#   read-forecast-log.sh --last                # the last line only, untruncated
#   read-forecast-log.sh --count               # line count only
#
# Exit: 0 content produced · 1 file missing or empty · 2 collection failed.
set -uo pipefail
CTX="${KUBE_CONTEXT:-predictive-bench}"; NS_ML="${NS_ML:-ml-engine}"
READER_IMAGE="${READER_IMAGE:-public.ecr.aws/docker/library/busybox:1.36}"
HERE="$(cd "$(dirname "$0")" && pwd)"
MODE="--all"; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --all|--last|--count) MODE="$1"; shift ;;
    --out) OUT="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
k() { kubectl --request-timeout=30s --context "$CTX" "$@"; }

if [ -n "$OUT" ] && { [ -e "$OUT" ] || [ -e "$OUT.receipt.json" ]; }; then
  echo "COLLECTION FAILED: output or receipt already exists" >&2; exit 2
fi
if [ -n "$OUT" ]; then
  diagnostics="$OUT.transfer"
  mkdir "$diagnostics" || { echo "COLLECTION FAILED: diagnostics must be new: $diagnostics" >&2; exit 2; }
else
  diagnostics=$(mktemp -d) || exit 2
fi
echo "transfer diagnostics: $diagnostics" >&2
work=$(mktemp -d); pod=""
cleanup() { rm -rf "$work"; if [ -n "$pod" ]; then k delete pod -n "$NS_ML" "$pod" --wait=false >/dev/null 2>&1; fi; }
trap cleanup EXIT
k get pods -n "$NS_ML" -l app=predictive-operator -o json > "$work/pods.json" || exit 2
k get deploy -n "$NS_ML" predictive-operator -o json > "$work/deploy.json" || exit 2
if ! python3 - "$work" <<'IDENTITY'
import json, sys
from pathlib import Path
p=Path(sys.argv[1]); pods=json.loads((p/'pods.json').read_text())['items']
if len(pods)!=1: raise SystemExit('COLLECTION FAILED: exactly one operator pod is required')
pod=pods[0];deploy=json.loads((p/'deploy.json').read_text());spec=deploy['spec']['template']['spec']
file=next(e['value'] for e in spec['containers'][0]['env'] if e['name']=='FORECAST_LOG')
pvc=next(v['persistentVolumeClaim']['claimName'] for v in spec['volumes'] if v['name']=='forecast-log')
source=dict(operator_pod=pod['metadata']['name'],operator_pod_uid=pod['metadata']['uid'],node=pod['spec']['nodeName'],path=file,pvc=pvc)
(p/'source.json').write_text(json.dumps(source))
for key in ('node','path','pvc'): (p/key).write_text(source[key])
IDENTITY
then exit 2; fi
node=$(cat "$work/node"); file=$(cat "$work/path"); pvc=$(cat "$work/pvc")
k get pvc -n "$NS_ML" "$pvc" -o json > "$work/pvc.json" || exit 2
python3 - "$work" "$CTX" "$NS_ML" <<'PVC'
import json,sys
from pathlib import Path
p=Path(sys.argv[1]);s=json.loads((p/'source.json').read_text());v=json.loads((p/'pvc.json').read_text())
s.update(context=sys.argv[2],namespace=sys.argv[3],pvc_uid=v['metadata']['uid'],pv=v['spec']['volumeName'])
(p/'source.json').write_text(json.dumps(s))
PVC
if [ $? -ne 0 ]; then exit 2; fi
dir=$(dirname "$file")
# 2026-09-23: the file is no longer read through `kubectl logs`. That transport returned 161,747 of
# 1,832,147 bytes, cut mid-line, with exit 0. The reader pod now sleeps; the content is streamed
# with `kubectl exec`, and the pod first reports the byte length N and the SHA-256 of the first N
# bytes. The file is append-only, so that prefix is stable; the local copy must match both, or the
# read is a COLLECTION FAILURE.
pod="fc-reader-$$-$RANDOM"
overrides=$(python3 - "$node" "$pvc" "$dir" "$READER_IMAGE" <<'PY2'
import json, sys
node, pvc, dir_, image = sys.argv[1:5]
print(json.dumps({"apiVersion": "v1", "metadata": {"annotations": {"sidecar.istio.io/inject": "false"}}, "spec": {
    "nodeName": node, "restartPolicy": "Never", "activeDeadlineSeconds": 2100,
    "volumes": [{"name": "fc", "persistentVolumeClaim": {"claimName": pvc, "readOnly": True}}],
    "containers": [{"name": "reader", "image": image, "command": ["sleep", "1800"],
                    "resources": {"requests": {"cpu": "10m", "memory": "16Mi"},
                                  "limits": {"cpu": "100m", "memory": "64Mi"}},
                    "volumeMounts": [{"name": "fc", "mountPath": dir_, "readOnly": True}]}]}}))
PY2
)
[ -z "$overrides" ] && { echo "COLLECTION FAILED: could not build the reader pod spec" >&2; exit 2; }
if ! k run "$pod" -n "$NS_ML" --image="$READER_IMAGE" --restart=Never --overrides="$overrides" >/dev/null 2>&1; then
  echo "COLLECTION FAILED: reader pod could not be created" >&2; exit 2
fi
if ! k wait -n "$NS_ML" --for=condition=Ready "pod/$pod" --timeout=180s >/dev/null 2>&1; then
  echo "COLLECTION FAILED: reader pod not Ready — this says nothing about the file" >&2; exit 2
fi
k get pod -n "$NS_ML" "$pod" -o json > "$diagnostics/reader-before.json" || exit 2
probe_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
meta=$(k exec -n "$NS_ML" "$pod" -- sh -c ': __META__; f="$1"; if [ ! -e "$f" ]; then echo __MISSING__; elif [ ! -s "$f" ]; then echo __EMPTY__; else n=$(wc -c < "$f" | tr -d " "); echo "$n $(head -c "$n" "$f" | sha256sum | cut -d" " -f1)"; fi' _ "$file" 2>"$diagnostics/probe.stderr") \
  || { echo "COLLECTION FAILED: remote probe failed" >&2; cat "$diagnostics/probe.stderr" >&2; exit 2; }
printf '%s\n' "$meta" > "$diagnostics/probe.txt"
case "$meta" in
  *__MISSING__*) echo "FILE MISSING: $file has never been created" >&2; exit 1 ;;
  *__EMPTY__*)   echo "FILE EMPTY: $file exists but holds no records" >&2; exit 1 ;;
esac
want_n=${meta%% *}; want_s=${meta##* }
case "$want_n" in ''|*[!0-9]*) echo "COLLECTION FAILED: unusable probe output '$meta'" >&2; exit 2 ;; esac
tmpf="$diagnostics/prefix.partial"
python3 "$HERE/forecast_transfer.py" "$CTX" "$NS_ML" "$pod" "$file" "$want_n" "$want_s" \
  "$tmpf" "$diagnostics" "$diagnostics/reader-before.json" || exit 2
got_n=$(wc -c < "$tmpf" | tr -d ' '); got_s=$(shasum -a 256 "$tmpf" | cut -d' ' -f1)
if [ "$got_n" != "$want_n" ] || [ "$got_s" != "$want_s" ]; then
  echo "COLLECTION FAILED: transport truncated or altered the file (got $got_n bytes, expected $want_n; hash match: $([ "$got_s" = "$want_s" ] && echo yes || echo no))" >&2
  exit 2
fi
# Identity can change while the reader starts; never label new bytes with stale UIDs.
k get pods -n "$NS_ML" -l app=predictive-operator -o json > "$work/pods-after.json" || exit 2
k get pvc -n "$NS_ML" "$pvc" -o json > "$work/pvc-after.json" || exit 2
if ! python3 - "$work" <<'RECHECK'
import json,sys
from pathlib import Path
p=Path(sys.argv[1]);old=json.loads((p/'source.json').read_text())
pods=json.loads((p/'pods-after.json').read_text())['items'];pvc=json.loads((p/'pvc-after.json').read_text())
if len(pods)!=1 or pods[0]['metadata']['uid']!=old['operator_pod_uid'] or pods[0]['spec']['nodeName']!=old['node'] or pvc['metadata']['uid']!=old['pvc_uid'] or pvc['spec']['volumeName']!=old['pv']:
    raise SystemExit('COLLECTION FAILED: operator/PVC identity changed during transfer')
RECHECK
then exit 2; fi
if [ -n "$OUT" ]; then
  if [ "$MODE" = "--all" ]; then
    staged=$(mktemp "$(dirname "$OUT")/.forecast-XXXXXX") || exit 2
    if ! cp "$tmpf" "$staged"; then rm -f "$staged"; exit 2; fi
    # Re-hash the actual saved staging file; publish exclusively on the same filesystem.
    if ! python3 - "$staged" "$OUT" "$want_n" "$want_s" <<'SAVE'
import hashlib,os,sys
from pathlib import Path
stage,out,n,sha=sys.argv[1:];data=Path(stage).read_bytes()
if len(data)!=int(n) or hashlib.sha256(data).hexdigest()!=sha:raise SystemExit('saved bytes differ from remote probe')
os.link(stage,out)
SAVE
    then rm -f "$staged"; exit 2; fi
    rm -f "$staged"
    fingerprint=$(python3 "$HERE/forecast_transfer.py" --fingerprint "$HERE") || exit 2
    python3 "$HERE/forecast_log.py" --log "$OUT" --receipt "$OUT.receipt.json" \
      --remote-bytes "$want_n" --remote-sha256 "$want_s" --probe-at "$probe_at" \
      --source "$work/source.json" --reader-fingerprint "$fingerprint" || exit 2
  else
    # Partial views are diagnostic conveniences; no integrity receipt is emitted.
    if [ "$MODE" = "--last" ]; then tail -n 1 "$tmpf" > "$OUT"; else wc -l < "$tmpf" | tr -d ' ' > "$OUT"; fi
  fi
  echo "wrote verified prefix view to $OUT ($got_n source bytes, sha256 $got_s)"
else
  case "$MODE" in
    --all) cat "$tmpf" ;;
    --last) tail -n 1 "$tmpf" ;;
    --count) wc -l < "$tmpf" | tr -d ' ' ;;
  esac || exit 2
fi
rm -f "$tmpf"
if [ -z "$OUT" ] && ! grep -q '"event": "retry"' "$diagnostics/attempts.jsonl"; then
  rm -rf "$diagnostics"
  echo "temporary diagnostics removed after clean stdout success" >&2
fi
exit 0
