#!/usr/bin/env bash
# The one checked verification entry point (Codex C-82 / D-103).
#
# Runs the Go and Python suites and PRESERVES their exit status through any logging. The
# failure this exists to prevent: a pytest run piped through `grep`/`tail` for a tidy summary
# swallows pytest's exit code, and a red test ships as green. That happened once (f6d0083);
# it must not be possible again.
#
# Exit status: 0 only if go vet, go test -race and pytest ALL pass. Any other status is a
# failure and publication is gated on this script returning 0 (see README, "Testing").
#
# Usage:
#   scripts/verify.sh                 # full run, log to verify-<utc>.log
#   scripts/verify.sh --python-only   # skip Go (e.g. when Go is unavailable)
#   scripts/verify.sh --go-only
#   PYTEST_ARGS="-k pattern" scripts/verify.sh
#
# Environment:
#   PYTHON   interpreter for the Python suite (default: eval/.venv/bin/python if present,
#            else python3)
#   GO       go binary (default: go on PATH, else /usr/local/go/bin/go)
#   LOG      log path (default: verify-<utc>.log in the repo root; use /dev/null to skip)

set -u
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-}"
if [ -z "$PYTHON" ]; then
  if [ -x "$ROOT/eval/.venv/bin/python" ]; then PYTHON="$ROOT/eval/.venv/bin/python"; else PYTHON="python3"; fi
fi
GO="${GO:-}"
if [ -z "$GO" ]; then
  if command -v go >/dev/null 2>&1; then GO="go"; elif [ -x /usr/local/go/bin/go ]; then GO="/usr/local/go/bin/go"; else GO=""; fi
fi
LOG="${LOG:-$ROOT/verify-$(date -u +%Y%m%dT%H%M%SZ).log}"
PYTEST_ARGS="${PYTEST_ARGS:-}"

run_go=1; run_py=1
for a in "$@"; do
  case "$a" in
    --python-only) run_go=0 ;;
    --go-only)     run_py=0 ;;
    -h|--help)     sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "verify.sh: unknown argument: $a" >&2; exit 2 ;;
  esac
done

# Every command's status is captured in a variable IMMEDIATELY, before any other command
# runs, and the pipeline status is read from PIPESTATUS[0] -- the command, not the tee.
overall=0
status_line() { printf '%s  %-22s exit=%s\n' "$(date -u +%H:%M:%SZ)" "$1" "$2"; }

echo "verify: root=$ROOT log=$LOG" | tee -a "$LOG"

if [ "$run_go" = 1 ]; then
  if [ -z "$GO" ]; then
    echo "verify: go binary not found (set GO= or use --python-only)" | tee -a "$LOG"
    overall=1
  else
    ( cd k8s-operator && "$GO" vet ./... ) 2>&1 | tee -a "$LOG"; rc=${PIPESTATUS[0]}
    status_line "go vet" "$rc" | tee -a "$LOG"; [ "$rc" -eq 0 ] || overall=1
    ( cd k8s-operator && "$GO" test -race -count=1 ./... ) 2>&1 | tee -a "$LOG"; rc=${PIPESTATUS[0]}
    status_line "go test -race" "$rc" | tee -a "$LOG"; [ "$rc" -eq 0 ] || overall=1
  fi
fi

if [ "$run_py" = 1 ]; then
  # shellcheck disable=SC2086
  ( cd ml-engine && "$PYTHON" -m pytest -q $PYTEST_ARGS tests ) 2>&1 | tee -a "$LOG"; rc=${PIPESTATUS[0]}
  status_line "pytest" "$rc" | tee -a "$LOG"; [ "$rc" -eq 0 ] || overall=1
  # The benchmark scorer's own suite (artifact attribution, transition and participation fixtures).
  ( cd deploy/eks-benchmark/scoring && "$PYTHON" -W ignore score_test.py ) 2>&1 | tee -a "$LOG"; rc=${PIPESTATUS[0]}
  status_line "scorer tests" "$rc" | tee -a "$LOG"; [ "$rc" -eq 0 ] || overall=1
  # The in-cluster evidence archive (verified-pair archiving, log preservation, gap detection).
  ( cd deploy/eks-benchmark/instrumentation && "$PYTHON" -W ignore archive_test.py ) 2>&1 | tee -a "$LOG"; rc=${PIPESTATUS[0]}
  status_line "archive tests" "$rc" | tee -a "$LOG"; [ "$rc" -eq 0 ] || overall=1
fi

if [ "$overall" -eq 0 ]; then
  echo "verify: PASS" | tee -a "$LOG"
else
  echo "verify: FAIL (see $LOG)" | tee -a "$LOG"
fi
exit "$overall"
