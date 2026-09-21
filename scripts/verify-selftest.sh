#!/usr/bin/env bash
# Proves that scripts/verify.sh cannot swallow a failing test (Codex C-82 / D-103).
#
# It writes a deliberately failing pytest file into a temporary test directory, points the
# wrapper at it, and asserts the wrapper's exit status is NON-ZERO. Then it runs the wrapper
# against a passing file and asserts ZERO. Both directions, so a wrapper that always fails
# or always passes is caught too.
#
# The wrapper's own logging goes through `tee`; the whole point is that PIPESTATUS[0] -- not
# tee's status -- is what the wrapper returns.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/tests"
cat > "$TMP/tests/test_deliberate_failure.py" <<'PY'
def test_this_must_fail():
    assert 1 == 2, "deliberate failure: the verify wrapper must return non-zero"
PY
cat > "$TMP/tests/test_deliberate_pass.py" <<'PY'
def test_this_must_pass():
    assert 1 == 1
PY

PYTHON="${PYTHON:-}"
if [ -z "$PYTHON" ]; then
  if [ -x "$ROOT/eval/.venv/bin/python" ]; then PYTHON="$ROOT/eval/.venv/bin/python"; else PYTHON="python3"; fi
fi

# Run the REAL wrapper, with pytest pointed at the temp tests via PYTEST_ARGS, and with the
# repository's own tests deselected so only the deliberate files count. --python-only keeps
# Go out of the picture: this self-test is about pytest's status surviving the pipe.
fail_rc=0
LOG=/dev/null PYTHON="$PYTHON" PYTEST_ARGS="--rootdir=$TMP $TMP/tests/test_deliberate_failure.py --ignore=tests" \
  "$ROOT/scripts/verify.sh" --python-only >/dev/null 2>&1 || fail_rc=$?
pass_rc=0
LOG=/dev/null PYTHON="$PYTHON" PYTEST_ARGS="--rootdir=$TMP $TMP/tests/test_deliberate_pass.py --ignore=tests" \
  "$ROOT/scripts/verify.sh" --python-only >/dev/null 2>&1 || pass_rc=$?

echo "verify-selftest: deliberate failure -> wrapper exit $fail_rc (must be non-zero)"
echo "verify-selftest: deliberate pass    -> wrapper exit $pass_rc (must be zero)"
if [ "$fail_rc" -ne 0 ] && [ "$pass_rc" -eq 0 ]; then
  echo "verify-selftest: PASS -- the wrapper preserves pytest's exit status"
  exit 0
fi
echo "verify-selftest: FAIL -- the wrapper does NOT preserve pytest's exit status" >&2
exit 1
