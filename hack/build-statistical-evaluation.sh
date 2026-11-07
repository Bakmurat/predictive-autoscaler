#!/usr/bin/env bash
# The C2.8 lab EVALUATION image of the statistical forecaster, built only from a pushed commit, never a working tree.
#
#   hack/build-statistical-evaluation.sh [--push] <commit> <image-ref>
#   hack/build-statistical-evaluation.sh --context-only <dir> <commit>     (prepare and verify the context; no docker)
#
# The build context is `git archive <commit> ml-engine` plus source-manifest.txt (`git ls-tree -r <commit>`, headed by
# the commit); Dockerfile.statistical's verify stage (ml-engine/build_record.py) checks every copied file against it and
# fails on anything dirty or mismatched, so the record's source tree is the image's content. With --push it prints the
# pushed image digest: that digest, with the commit, goes into the lab-qualification/v1 file (image_digest,
# source_tree), and the chart must run the image by that digest. Needs git; docker for the build.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="${EVAL_BUILD_REPO:-$ROOT}"          # tests point this at a scratch repository
push=0 context_only=""
while [ $# -gt 0 ]; do
  case "$1" in
    --push) push=1; shift ;;
    --context-only) context_only="$2"; shift 2 ;;
    *) break ;;
  esac
done
[ $# -ge 1 ] || { echo "usage: $0 [--push] <commit> <image-ref> | --context-only <dir> <commit>" >&2; exit 2; }
commit="$(git -C "$REPO" rev-parse --verify --quiet --end-of-options "$1^{commit}")" \
  || { echo "refused: $1 is not a commit" >&2; exit 1; }

prepare() {   # $1: an empty directory; leaves the context in $1/ml-engine
  git -C "$REPO" archive --format=tar "$commit" ml-engine | tar -x -C "$1"
  { echo "# commit $commit"
    git -C "$REPO" ls-tree -r --full-tree "$commit" -- ml-engine | sed $'s#\tml-engine/#\t#'
  } > "$1/ml-engine/source-manifest.txt"
}

if [ -n "$context_only" ]; then
  mkdir -p "$context_only"
  [ -z "$(ls -A "$context_only")" ] || { echo "refused: $context_only is not empty" >&2; exit 1; }
  prepare "$context_only"
  python3 "$context_only/ml-engine/build_record.py" "$context_only/ml-engine" "$commit" true \
    "$context_only/build-info.json"
  echo "context verified for $commit: $context_only/ml-engine"
  exit 0
fi

[ $# -eq 2 ] || { echo "usage: $0 [--push] <commit> <image-ref>" >&2; exit 2; }
ref="$2"
git -C "$REPO" branch -r --contains "$commit" | grep -q . \
  || { echo "refused: $commit is on no pushed branch (reviewers must be able to read it)" >&2; exit 1; }
command -v docker >/dev/null || { echo "docker not found" >&2; exit 2; }
ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT
prepare "$ctx"
docker build -f "$ctx/ml-engine/Dockerfile.statistical" --build-arg SOURCE_TREE="$commit" \
  --build-arg EVALUATION_BUILD=true -t "$ref" "$ctx/ml-engine"
echo "built $ref from $commit (evaluation build)"
if [ "$push" = 1 ]; then
  docker push "$ref" >/dev/null
  digest="$(docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$ref" | head -n1 | sed 's/.*@//')"
  echo "image_digest: $digest"
  echo "source_tree:  $commit"
fi
