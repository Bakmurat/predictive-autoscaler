#!/usr/bin/env bash
# Compile EXACTLY the files the fixture Dockerfile's build stage copies (Codex review of PR #51: the image once copied
# only main.go and could not build). Reads every COPY of the build stage (not COPY --from), expands its sources against
# this directory with the context's .dockerignore applied, copies them to a scratch module and runs `go build` there,
# and requires the copied Go files to be exactly the package's non-test files (go list), so a source file that the
# image would miss, or an extra one it would take, fails verify. No docker needed; nothing outside a temp dir is written.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GO="${GO:-go}"
cd "$HERE"
[ -f Dockerfile ] || { echo "check-build-context: no Dockerfile" >&2; exit 1; }
# The sources of every build-stage COPY: the lines between "FROM ... AS build" and the next FROM, minus --from copies.
mapfile -t patterns < <(awk '
  toupper($1) == "FROM" { build = (tolower($NF) == "build" && toupper($(NF-1)) == "AS"); next }
  build && toupper($1) == "COPY" { if ($2 ~ /^--from/) next; for (i = 2; i < NF; i++) if ($i !~ /^--/) print $i }
' Dockerfile)
[ "${#patterns[@]}" -gt 0 ] || { echo "check-build-context: the build stage copies nothing" >&2; exit 1; }
ignored() {   # .dockerignore patterns (simple globs, one per line, # comments)
  local f="$1" p
  [ -f .dockerignore ] || return 1
  while IFS= read -r p; do
    case "$p" in ''|'#'*) continue ;; esac
    # shellcheck disable=SC2053
    [[ "$f" == $p ]] && return 0
  done < .dockerignore
  return 1
}
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
copied=()
shopt -s nullglob
for pat in "${patterns[@]}"; do
  matches=($pat)
  [ "${#matches[@]}" -gt 0 ] || { echo "check-build-context: COPY source $pat matches nothing" >&2; exit 1; }
  for f in "${matches[@]}"; do
    ignored "$f" && continue
    cp "$f" "$WORK/"; copied+=("$f")
  done
done
want="$("$GO" list -f '{{join .GoFiles "\n"}}' . | sort)"
got="$(printf '%s\n' "${copied[@]}" | grep '\.go$' | sort -u)"
if [ "$want" != "$got" ]; then
  echo "check-build-context: the image copies [$(echo $got)] but the package's non-test files are [$(echo $want)]" >&2
  exit 1
fi
(cd "$WORK" && CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=mod "$GO" build -trimpath -o /dev/null .) \
  || { echo "check-build-context: the Dockerfile's sources do not build" >&2; exit 1; }
echo "check-build-context: the image's build stage copies ${copied[*]} and they build"
