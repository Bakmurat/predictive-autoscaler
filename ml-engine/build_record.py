#!/usr/bin/env python3
"""The statistical image's build record (/app/build-info.json), written in Dockerfile.statistical's `verify` stage.

Usage (inside the build): python build_record.py <context dir> <SOURCE_TREE> <EVALUATION_BUILD true|false> <output>

A release build (EVALUATION_BUILD=false) records its claimed source tree and evaluation_build false: it never honours a
lab qualification, so nothing rests on that claim. An EVALUATION build (C2.8 lab qualification) must prove that every
file the final stage copies is exactly the named commit's: the context must carry `source-manifest.txt` (written by
hack/build-statistical-evaluation.sh from `git ls-tree -r <commit> -- ml-engine`, the context itself being
`git archive <commit> ml-engine`), headed by that commit; every copied file's git blob hash must equal the manifest's,
every manifest file under a copied directory must be present, nothing extra may be (a dirty or mismatched context
fails the build), and the Dockerfile and this script are checked the same way. The record then holds the commit, the
number of verified files and the manifest's SHA256; the image digest is bound later, in the qualification file.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys

_TREE = re.compile(r"^[0-9a-f]{40}$")
DOCKERFILE = "Dockerfile.statistical"
MANIFEST = "source-manifest.txt"


class Refused(Exception):
    pass


def blob_sha1(path: str) -> str:
    with open(path, "rb") as fh:
        data = fh.read()
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def final_stage_copies(dockerfile_text: str) -> list:
    """The sources the final stage COPYs from the context (not --from another stage)."""
    stages, current = [], []
    for raw in dockerfile_text.splitlines():
        line = raw.strip()
        if line.startswith("FROM "):
            current = []
            stages.append(current)
        elif line.startswith("COPY ") and "--from=" not in line:
            current.extend(line.split()[1:-1])
    return stages[-1] if stages else []


def read_manifest(path: str, tree: str) -> dict:
    try:
        with open(path) as fh:
            lines = fh.read().splitlines()
    except OSError as e:
        raise Refused(f"an evaluation build needs {MANIFEST} (build with hack/build-statistical-evaluation.sh): {e}")
    if not lines or lines[0] != f"# commit {tree}":
        raise Refused(f"{MANIFEST} does not belong to commit {tree}")
    entries = {}
    for line in lines[1:]:
        meta, _, name = line.partition("\t")
        parts = meta.split()
        if len(parts) != 3 or parts[1] != "blob" or not name:
            raise Refused(f"{MANIFEST}: malformed line {line!r}")
        entries[name] = parts[2]
    return entries


def verify(ctx: str, tree: str) -> dict:
    manifest_path = os.path.join(ctx, MANIFEST)
    entries = read_manifest(manifest_path, tree)
    with open(os.path.join(ctx, DOCKERFILE)) as fh:
        sources = final_stage_copies(fh.read())
    wanted = {DOCKERFILE, os.path.basename(__file__)}
    for src in sources:
        if src.endswith("/"):
            prefix = src
            present = {os.path.relpath(os.path.join(d, f), ctx) for d, _, fs in os.walk(os.path.join(ctx, src))
                       for f in fs}
            listed = {n for n in entries if n.startswith(prefix)}
            if present != listed:
                raise Refused(f"{src} differs from the commit: extra {sorted(present - listed)}, "
                              f"missing {sorted(listed - present)}")
            wanted |= present
        else:
            wanted.add(src)
    for name in sorted(wanted):
        if name not in entries:
            raise Refused(f"{name} is not in commit {tree}")
        path = os.path.join(ctx, name)
        if not os.path.isfile(path) or blob_sha1(path) != entries[name]:
            raise Refused(f"{name} differs from commit {tree}")
    with open(manifest_path, "rb") as fh:
        manifest_sha = hashlib.sha256(fh.read()).hexdigest()
    return {"verified_files": len(wanted), "source_manifest_sha256": manifest_sha}


def record(ctx: str, tree: str, flag: str) -> dict:
    if flag not in ("true", "false"):
        raise Refused("EVALUATION_BUILD must be true or false")
    if flag == "false":
        return {"source_tree": tree, "evaluation_build": False}
    if not _TREE.match(tree):
        raise Refused("an evaluation build needs SOURCE_TREE=<40-hex commit>")
    return {"source_tree": tree, "evaluation_build": True, **verify(ctx, tree)}


def main(argv) -> int:
    if len(argv) != 5:
        print(__doc__.splitlines()[2], file=sys.stderr)
        return 2
    try:
        rec = record(argv[1], argv[2], argv[3])
    except Refused as e:
        print(f"build record refused: {e}", file=sys.stderr)
        return 1
    with open(argv[4], "w") as fh:
        json.dump(rec, fh, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
