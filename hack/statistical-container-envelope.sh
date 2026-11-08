#!/usr/bin/env bash
# The statistical forecaster's resource envelope IN ITS CONTAINER, with fitting enabled (design C2.6; the release
# measurement before the 384 MiB fit-process limit and the 256Mi/512Mi chart profile are final).
#
# Builds ml-engine/Dockerfile.statistical, then runs hack/statistical-envelope.py inside that image under the chart
# profile's limits (512 MiB memory, 1 CPU, no network, read-only root, a writable /state and /tmp), with the fit
# worker ON and 8 days of history so E1-product fits really run in their own processes. The container's cgroup
# memory peak covers the service and every fit process; an OOM kill or a budget breach fails the run.
#
# Needs docker and network for the build (no cluster, no kubeconfig). Intended for the aspire kind bed host:
#   hack/statistical-container-envelope.sh [--pas 8] [--interval 60] [--hours 12]
# Writes the JSON report to stdout and keeps it as logs/statistical-container-envelope-<utc>.json.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-predictive-autoscaler-forecaster-statistical:envelope}"
PAS=8; INTERVAL=60; HOURS=12
while [ $# -gt 0 ]; do
  case "$1" in
    --pas) PAS="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --hours) HOURS="$2"; shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }
docker build -q -f "$ROOT/ml-engine/Dockerfile.statistical" -t "$IMAGE" "$ROOT/ml-engine" >/dev/null
python3 "$ROOT/hack/sbom-check-statistical.py" --image "$IMAGE"
mkdir -p "$ROOT/logs"
out="$ROOT/logs/statistical-container-envelope-$(date -u +%Y%m%dT%H%M%SZ).json"
name="pa-envelope-$$"
set +e
docker run --name "$name" --memory 512m --memory-swap 512m --cpus 1 --network none --read-only \
  --tmpfs /tmp:size=64m --tmpfs /state:size=2g -v "$ROOT/hack:/hack:ro" -e KUBECONFIG=/dev/null \
  --entrypoint python "$IMAGE" /hack/statistical-envelope.py --pas "$PAS" --interval "$INTERVAL" --hours "$HOURS" \
  --history-days 8 --fit-mode worker --cost-replay --state-dir /state --check --cgroup-peak | tee "$out"
rc=${PIPESTATUS[0]}
set -e
oom="$(docker inspect -f '{{.State.OOMKilled}}' "$name")"
docker rm -f "$name" >/dev/null
if [ "$oom" = "true" ]; then echo "statistical-container-envelope: OOM-killed at 512 MiB" >&2; exit 1; fi
[ "$rc" -eq 0 ] && echo "statistical-container-envelope: PASS ($out)" || echo "statistical-container-envelope: FAIL ($out)" >&2
exit "$rc"
