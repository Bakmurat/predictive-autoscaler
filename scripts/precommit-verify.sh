#!/usr/bin/env bash
# Verify the EXACT TREE BEING COMMITTED, not the working directory (Codex D-111).
#
# The failure this exists to prevent, which happened once: a commit chain ran the suite over a
# working directory holding a LATER change's tests against an EARLIER change's source, saw 8
# failures, and committed anyway. The committed tree was in fact green, but nothing had
# established that -- the run had measured a tree that was never committed.
#
# So this script measures the index and nothing else:
#   * exports the staged tree to a temporary directory with `git checkout-index`
#     -- never `git stash`, never `git reset`, never touching another agent's working files;
#   * binds the pass to the staged TREE HASH plus the hashes of the verification script, the
#     Makefile and the dependency manifests, so a pass cannot be reused for a different tree
#     or a different verifier;
#   * refuses any partial-suite flag or inherited test selection (PYTEST_ARGS, --python-only,
#     --go-only): a publication gate runs the whole suite or it is not a gate;
#   * RE-CHECKS the staged tree hash immediately before allowing the commit, so a stage that
#     changed while the suite ran does not inherit the pass.
#
# Exit status: 0 only when the staged tree verified clean and is still the tree being
# committed. Anything else refuses the commit.
#
# Note: a local hook is bypassable (`git commit --no-verify`). It narrows the window; it does
# not close it. Protected CI on the remote is the stronger guarantee -- see README, "Testing".

set -u
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

say() { printf 'precommit: %s\n' "$*"; }
die() { say "REFUSING THE COMMIT -- $*"; exit 1; }

# --- 1. A gate runs the whole suite ---------------------------------------------------------
# An inherited selection would let a green subset stand in for the suite.
for var in PYTEST_ARGS; do
  if [ -n "${!var:-}" ]; then
    die "$var is set ('${!var}'); a publication gate runs the complete suite"
  fi
done
for a in "$@"; do
  case "$a" in
    --python-only|--go-only|-k|--deselect|--last-failed|--lf)
      die "partial-suite flag '$a' is not allowed in the commit gate" ;;
  esac
done

# --- 2. Identify the tree actually being committed ------------------------------------------
if ! git rev-parse --git-dir >/dev/null 2>&1; then
  die "not a git repository"
fi
STAGED_TREE="$(git write-tree)" || die "could not write the index to a tree"
if [ -z "$STAGED_TREE" ]; then
  die "empty staged tree"
fi

# Nothing staged at all means nothing to verify -- let git report that itself.
if git diff --cached --quiet 2>/dev/null; then
  say "nothing staged; leaving the decision to git"
  exit 0
fi

# --- 3. Bind the pass to the verifier as well as the tree -----------------------------------
verifier_fingerprint() {
  local f
  for f in scripts/verify.sh scripts/precommit-verify.sh Makefile \
           ml-engine/requirements.txt ml-engine/requirements-dev.txt k8s-operator/go.mod; do
    if [ -f "$f" ]; then
      printf '%s  %s\n' "$(git hash-object "$f" 2>/dev/null || echo missing)" "$f"
    fi
  done
}
FINGERPRINT="$(verifier_fingerprint | shasum -a 256 | awk '{print $1}')"

say "staged tree   $STAGED_TREE"
say "verifier      $FINGERPRINT"

# --- 4. Export the staged tree in isolation and verify THAT ---------------------------------
WORK="$(mktemp -d "${TMPDIR:-/tmp}/precommit-verify.XXXXXX")" || die "could not create a temp dir"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

# checkout-index writes only what is staged. The working directory is not read and not touched.
if ! git checkout-index -a -f --prefix="$WORK/"; then
  die "could not export the staged tree"
fi

# The venv and any other untracked tooling are not in the index; point the verifier at the
# real one rather than copying it.
export PYTHON="${PYTHON:-$ROOT/eval/.venv/bin/python}"
export LOG="$WORK/verify.log"

say "verifying the staged tree in $WORK"
( cd "$WORK" && bash scripts/verify.sh ) 2>&1 | sed 's/^/  | /'
rc=${PIPESTATUS[0]}
if [ "$rc" -ne 0 ]; then
  cp -f "$WORK/verify.log" "$ROOT/precommit-verify-failed.log" 2>/dev/null || true
  die "verify failed on the staged tree (exit $rc); log: precommit-verify-failed.log"
fi

# --- 5. Re-check that the tree we verified is still the tree being committed -----------------
STAGED_TREE_NOW="$(git write-tree)" || die "could not re-read the index"
if [ "$STAGED_TREE_NOW" != "$STAGED_TREE" ]; then
  die "the index changed while the suite ran ($STAGED_TREE -> $STAGED_TREE_NOW); re-stage and commit again"
fi
FINGERPRINT_NOW="$(verifier_fingerprint | shasum -a 256 | awk '{print $1}')"
if [ "$FINGERPRINT_NOW" != "$FINGERPRINT" ]; then
  die "the verifier changed while the suite ran; re-run the commit"
fi

say "PASS -- staged tree $STAGED_TREE verified clean"
exit 0
