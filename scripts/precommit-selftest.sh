#!/usr/bin/env bash
# Prove the commit gate measures the STAGED tree and nothing else (Codex D-111).
#
# Two cases, both run against the real hook in a throwaway clone so this repository's index
# and working directory are never touched:
#
#   A. staged clean + an UNSTAGED failing test in the working directory  -> commit ALLOWED
#      (the defect this closes: a mixed working directory decided the verdict)
#   B. a staged failing test                                             -> commit REFUSED
#
# Exit status: 0 only if both behave as stated.

set -u
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-$ROOT/eval/.venv/bin/python}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/precommit-selftest.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

fail=0
note() { printf '%s\n' "$*"; }

# A throwaway clone: its index is its own, so nothing here can disturb the real repository.
git clone -q --no-hardlinks "$ROOT" "$WORK/repo" || { note "clone failed"; exit 1; }
cd "$WORK/repo"

# Install the gate AS IT STANDS IN THE WORKING TREE, not as last committed -- otherwise the
# self-test silently measures nothing when the gate is not yet committed, and both cases pass
# for the wrong reason. (That is exactly what happened the first time this was run.)
mkdir -p .githooks scripts
cp "$ROOT/.githooks/pre-commit" .githooks/pre-commit
cp "$ROOT/scripts/precommit-verify.sh" scripts/precommit-verify.sh
cp "$ROOT/scripts/verify.sh" scripts/verify.sh
chmod +x .githooks/pre-commit scripts/precommit-verify.sh scripts/verify.sh
git config core.hooksPath .githooks
[ -x .githooks/pre-commit ] || { note "the gate was not installed into the clone"; exit 1; }
git config user.name "selftest"
git config user.email "selftest@example.invalid"

# The venv is untracked, so the clone has none; point the gate at the real one.
export PYTHON

FAILING_TEST='def test_deliberate_failure_for_the_selftest():
    assert False, "deliberately failing test"
'

note "=== case A: staged tree is clean, an UNSTAGED failing test sits in the working dir ==="
printf '%s' "$FAILING_TEST" > ml-engine/tests/test_zz_unstaged_failure.py   # NOT staged
printf '\n# staged, harmless\n' >> README.md
git add README.md
if git commit -q -m "selftest: staged-clean commit with an unstaged failing test"; then
  note "  ALLOWED  <- correct: the unstaged failure did not decide the verdict"
else
  note "  REFUSED  <- WRONG: the gate read the working directory, not the index"
  fail=1
fi
rm -f ml-engine/tests/test_zz_unstaged_failure.py

note "=== case B: a FAILING test is staged ==="
printf '%s' "$FAILING_TEST" > ml-engine/tests/test_zz_staged_failure.py
git add ml-engine/tests/test_zz_staged_failure.py
if git commit -q -m "selftest: staged failing test must be refused" 2>/dev/null; then
  note "  ALLOWED  <- WRONG: a red suite was committed"
  fail=1
else
  note "  REFUSED  <- correct"
fi

if [ "$fail" -eq 0 ]; then
  note "PASS -- the gate verifies the staged tree and only the staged tree"
else
  note "FAIL -- see above"
fi
exit "$fail"
