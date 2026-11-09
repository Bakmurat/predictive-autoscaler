#!/usr/bin/env python3
"""S1: build and publish the evaluation images on aspire, immutably (STAGING-C28-PROD.md, S1).

  build_images.py --record R [plan]                    read-only: clean pinned checkout, tags absent (local AND
                                                       remote), credential helper configured, generator resolvable;
                                                       writes the plan file and prints its digest
  build_images.py --record R apply --plan F --plan-digest D   build, push, verify the registry digests independently, pull
                                                       and inspect by digest, SBOM-check the statistical image, and
                                                       write images.json (a new file)
  build_images.py --record R verify                    read-only: re-resolve every images.json digest in the registry

Images: <harbor>/<project>/operator, statistical-evaluation and c28-fixture, each tagged c28-<run id>, or
c28-<run id>-<attempt> when the record sets build.attempt (a tag that already exists locally or in the registry is
refused: never reused, never overwritten), plus the upstream generator pinned by digest. The operator and fixture
build from `git archive <commit>` of a clean checkout at exactly the record's source.commit (never a working tree); the
statistical image is built by hack/build-statistical-evaluation.sh, whose Dockerfile verify stage checks every copied
file against the commit's source manifest. A pushed digest is accepted only when the registry's own manifest bytes
hash to it (docker buildx imagetools inspect --raw) and a pull by digest yields an image carrying exactly that
RepoDigest; a stale local tag or a first RepoDigest is never trusted. Credentials come only from the existing docker
credential helper: a plaintext auth entry for the registry is refused, and nothing here passes or prints a credential.
No image, tag or container that existed before is removed; no cluster is contacted.

The record's "build" section:
  {"checkout": "<clean checkout of source.commit>", "harbor": "<registry host>",
   "project": "autoscaler", "build_dir": "<new empty directory>", "generator_image": "<repo>@sha256:...",
   "docker_config": "<dir, default ~/.docker>", "attempt": "<optional: r2, r3, ...>"}

A rebuild of the same run (build.attempt): the run's first build has no attempt and keeps its tags (c28-<run id>);
every later build of that run sets build.attempt to a new slug matching ^r[0-9]{1,3}$ (e.g. "r2"), which gives new
tags c28-<run id>-<attempt>, so nothing that exists is reused or overwritten. The run id stays the run's
(images.json's run_id must equal the record's for calibrate.py and the other consumers, which read refs by digest and
never rebuild a tag from the run id). Each attempt needs its own record with a fresh build_dir (empty or absent, as
for every build) and a fresh evidence_dir: plan refuses an evidence_dir that already holds an images.json or a build
receipt chain (any earlier build's, with or without an attempt). The plan and images.json carry the attempt (only when
set: a record without one gives exactly the documents it always gave) and the tags; verify checks that images.json is
this record's run, attempt and tags.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from typing import Callable, Dict, List, Optional

import common_records as C
import execlib as X

HELPER = "build"
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_HOST = re.compile(r"^[a-z0-9]([a-z0-9.-]*[a-z0-9])?(:[0-9]+)?$")
_DIGEST_REF = re.compile(r"^([a-z0-9][a-z0-9./:-]*)@(sha256:[0-9a-f]{64})$")
_PUSH_DIGEST = re.compile(r"digest: (sha256:[0-9a-f]{64})")
_ATTEMPT = re.compile(r"^r[0-9]{1,3}$")
NAMES = {"operator": "operator", "statistical": "statistical-evaluation", "fixture": "c28-fixture"}


class Tools:
    """Subprocess access for git, docker and tar. Output is captured and never logged; only digests are recorded."""

    def __init__(self, run: Callable = subprocess.run, docker_config: str = ""):
        self.run = run
        self.env = dict(os.environ)
        self.env.pop("KUBECONFIG", None)
        self.env["KUBECONFIG"] = "/dev/null"          # nothing here may reach a cluster
        if docker_config:
            self.env["DOCKER_CONFIG"] = docker_config

    def __call__(self, argv: List[str], check: bool = True, timeout: int = 3600, stdin: Optional[str] = None):
        for a in argv:
            if re.search(r"(?i)(--password|-p=|--password-stdin|token=)", a):
                raise X.StagingError("a credential argument was about to be passed; refusing")
        res = self.run(argv, capture_output=True, text=True, timeout=timeout, env=self.env, input=stdin)
        if check and res.returncode != 0:
            err = (res.stderr or "").strip().splitlines()
            raise X.StagingError(f"{argv[0]} {argv[1] if len(argv) > 1 else ''} failed: "
                                 f"{_scrub(err[-1]) if err else res.returncode}")
        return res


def _scrub(line: str) -> str:
    return re.sub(r"(?i)(authorization|bearer|basic|token|password)\S*\s*\S+", r"\1 <redacted>", line)[:200]


def _section(record: X.Record) -> Dict:
    s = dict(record.section("build"))
    if "commit" in s and s["commit"] != record.commit:
        raise X.StagingError("build.commit differs from the record's source.commit")
    s["commit"] = record.commit
    if not _HOST.match(str(s.get("harbor", ""))) or not X._DNS.match(str(s.get("project", ""))):
        raise X.StagingError("build.harbor (registry host) and build.project are required")
    if not _DIGEST_REF.match(str(s.get("generator_image", ""))):
        raise X.StagingError("build.generator_image must be pinned by digest")
    for k in ("checkout", "build_dir"):
        if not s.get(k):
            raise X.StagingError(f"build.{k} is required")
    if "attempt" in s and not (isinstance(s["attempt"], str) and _ATTEMPT.fullmatch(s["attempt"])):
        raise X.StagingError("build.attempt, when set, must be a rebuild slug matching ^r[0-9]{1,3}$ (e.g. \"r2\"); "
                             "leave it out for the run's first build")
    return s


def tag_suffix(record: X.Record, s: Dict) -> str:
    """c28-<run id>, or c28-<run id>-<attempt> for a later build of the same run."""
    return f"c28-{record.run_id}" + (f"-{s['attempt']}" if s.get("attempt") else "")


def tags(record: X.Record, s: Dict) -> Dict[str, str]:
    return {k: f"{s['harbor']}/{s['project']}/{n}:{tag_suffix(record, s)}" for k, n in NAMES.items()}


def evidence_problems(record: X.Record) -> List[str]:
    """A fresh evidence_dir per build: one that already holds an images.json or a build receipt chain belongs to an
    earlier build (another attempt, or this one already applied), whose evidence is never mixed with or overwritten."""
    out = []
    images = os.path.join(record.evidence_dir, "images.json")
    if os.path.lexists(images):
        try:
            with open(images) as fh:
                doc = json.load(fh)
            held = f"run {doc.get('run_id')!r}, attempt {doc.get('attempt') or 'none'}"
        except (OSError, ValueError):
            held = "unreadable"
        out.append(f"{images} already exists ({held}): each build attempt needs a fresh evidence_dir")
    chain = C.receipts_path(record.doc, HELPER)
    if os.path.lexists(chain) and (not os.path.isfile(chain) or os.path.getsize(chain) > 0):
        out.append(f"{chain} already holds a build receipt chain: each build attempt needs a fresh evidence_dir")
    return out


def docker_config_dir(s: Dict) -> str:
    return os.path.expanduser(s.get("docker_config") or os.environ.get("DOCKER_CONFIG") or "~/.docker")


def credential_problems(s: Dict) -> List[str]:
    """The registry's credentials must come from a credential helper (credHelpers or credsStore), never a plaintext
    auth entry in config.json. Values are never read into the plan."""
    path = os.path.join(docker_config_dir(s), "config.json")
    try:
        with open(path) as fh:
            cfg = json.load(fh)
    except (OSError, ValueError):
        return [f"no docker config at {path}: configure the registry's credential helper first"]
    out = []
    host = s["harbor"]
    entry = (cfg.get("auths") or {}).get(host) or (cfg.get("auths") or {}).get(f"https://{host}") or {}
    if any(k in entry for k in ("auth", "identitytoken", "password", "registrytoken")):
        out.append(f"config.json holds a plaintext credential for {host}: use the credential helper only")
    if not ((cfg.get("credHelpers") or {}).get(host) or cfg.get("credsStore")):
        out.append(f"no credential helper is configured for {host}")
    return out


def remote_manifest(t: Tools, ref: str) -> Optional[bytes]:
    """The registry's raw manifest bytes for ref; None when the registry says it does not exist. Any other failure
    (auth, network) is a refusal: absence must be proven, not assumed."""
    res = t(["docker", "buildx", "imagetools", "inspect", "--raw", ref], check=False, timeout=120)
    if res.returncode == 0:
        return res.stdout.encode()
    err = (res.stderr or "").lower()
    if "not found" in err or "manifest unknown" in err or "no such manifest" in err:
        return None
    raise X.StagingError(f"cannot prove whether {ref} exists in the registry: {_scrub((res.stderr or '').strip()[-200:])}")


def local_exists(t: Tools, ref: str) -> bool:
    return t(["docker", "image", "inspect", ref], check=False, timeout=60).returncode == 0


def plan(record: X.Record, t: Tools) -> Dict:
    s = _section(record)
    problems: List[str] = []
    co = record.resolve(s["checkout"])
    head = t(["git", "-C", co, "rev-parse", "HEAD"], check=False).stdout.strip()
    if head != s["commit"]:
        problems.append(f"the checkout is at {head or 'nothing'}, not the reviewed commit")
    if t(["git", "-C", co, "status", "--porcelain"], check=False).stdout.strip():
        problems.append("the checkout is not clean")
    if not t(["git", "-C", co, "branch", "-r", "--contains", s["commit"]], check=False).stdout.strip():
        problems.append("the commit is on no pushed branch")
    bd = record.resolve(s["build_dir"])
    if os.path.exists(bd) and os.listdir(bd):
        problems.append(f"{bd} is not empty (a new build directory per run)")
    problems += evidence_problems(record)
    problems += credential_problems(s)
    tg = tags(record, s)
    state = {}
    for k, ref in tg.items():
        if local_exists(t, ref):
            problems.append(f"{ref} exists locally (tags are never reused)")
        try:
            if remote_manifest(t, ref) is not None:
                problems.append(f"{ref} exists in the registry (tags are never reused)")
            state[k] = "absent"
        except X.StagingError as e:
            problems.append(str(e))
    gen = s["generator_image"]
    try:
        raw = remote_manifest(t, gen)
        if raw is None:
            problems.append(f"the generator {gen} does not resolve")
        elif "sha256:" + X.sha256_bytes(raw) != _DIGEST_REF.match(gen).group(2):
            problems.append(f"the registry's manifest for {gen} does not hash to its digest")
    except X.StagingError as e:
        problems.append(str(e))
    versions = {"docker": t(["docker", "version", "--format", "{{.Server.Version}}"], check=False).stdout.strip(),
                "git": t(["git", "--version"], check=False).stdout.strip()}
    actions = [{"build": k, "tag": ref} for k, ref in sorted(tg.items())] + [{"push": ref} for _, ref in sorted(tg.items())]
    extra = {"attempt": s["attempt"]} if s.get("attempt") else {}      # absent: the plan stays exactly as before
    return X.new_plan(HELPER, record, actions, commit=s["commit"], tags=tg, remote_state=state, generator=gen,
                      build_dir=bd, tools=versions, problems=problems,
                      never=["remove a pre-existing image or tag", "pass a credential", "contact a cluster"], **extra)


def _verify_pushed(t: Tools, tag: str, digest: str) -> str:
    repo = tag.rsplit(":", 1)[0]
    ref = f"{repo}@{digest}"
    raw = remote_manifest(t, ref)
    if raw is None or "sha256:" + X.sha256_bytes(raw) != digest:
        raise X.StagingError(f"the registry's manifest bytes for {ref} do not hash to the pushed digest")
    raw_tag = remote_manifest(t, tag)
    if raw_tag is None or "sha256:" + X.sha256_bytes(raw_tag) != digest:
        raise X.StagingError(f"{tag} does not resolve to the pushed digest in the registry")
    t(["docker", "pull", "-q", ref], timeout=1800)
    rd = t(["docker", "image", "inspect", "--format", "{{json .RepoDigests}}", ref]).stdout.strip()
    if ref not in json.loads(rd or "[]"):
        raise X.StagingError(f"the image pulled by digest does not carry {ref}")
    return ref


def apply(record: X.Record, t: Tools, reviewed: Dict, reviewed_digest: str) -> Dict:
    p = plan(record, t)
    X.require_live(reviewed, reviewed_digest, HELPER, record, p)
    if p["problems"]:
        raise X.StagingError("the build plan has problems: " + "; ".join(p["problems"]))
    s = _section(record)
    co, bd, tg = record.resolve(s["checkout"]), p["build_dir"], p["tags"]
    os.makedirs(bd, exist_ok=True)
    tar = os.path.join(bd, "source.tar")
    t(["git", "-C", co, "archive", "--format=tar", "-o", tar, s["commit"], "k8s-operator", "deploy/eval-c28/fixture"])
    t(["tar", "-x", "-f", tar, "-C", bd])
    X.receipt(record, HELPER, "context", {"plan_sha256": reviewed_digest, "source_tar_sha256": X.sha256_file(tar)})
    t(["docker", "build", "-q", "-t", tg["operator"], os.path.join(bd, "k8s-operator")], timeout=3600)
    t(["docker", "build", "-q", "-t", tg["fixture"], os.path.join(bd, "deploy", "eval-c28", "fixture")], timeout=3600)
    t(["bash", os.path.join(co, "hack", "build-statistical-evaluation.sh"), s["commit"], tg["statistical"]], timeout=3600)
    images = {}
    for k in ("operator", "fixture", "statistical"):
        if remote_manifest(t, tg[k]) is not None:            # re-checked right before the push (a race is refused)
            raise X.StagingError(f"{tg[k]} appeared in the registry during the build; refusing to overwrite")
        out = t(["docker", "push", tg[k]], timeout=3600).stdout
        m = [d for line in out.splitlines() if line.startswith(tg[k].rsplit(":", 1)[1] + ":")
             for d in _PUSH_DIGEST.findall(line)] or _PUSH_DIGEST.findall(out)
        if len(set(m)) != 1:
            raise X.StagingError(f"the push of {tg[k]} reported no single digest")
        ref = _verify_pushed(t, tg[k], m[0])
        images[k] = {"tag": tg[k], "ref": ref}
        X.receipt(record, HELPER, "pushed", {"plan_sha256": reviewed_digest, "image": k, "ref": ref})
    gen = s["generator_image"]
    t(["docker", "pull", "-q", gen], timeout=1800)
    images["generator"] = {"ref": gen}
    sbom = t([sys.executable, os.path.join(co, "hack", "sbom-check-statistical.py"), "--image", images["statistical"]["ref"]],
             check=False, timeout=1800)
    if sbom.returncode != 0:
        raise X.StagingError("the statistical image failed its SBOM check; no fallback to an older tag")
    doc = {"schema": "c28-images/v1", "run_id": record.run_id, "source_commit": s["commit"], "verified": True,
           "images": images, "sbom": {"statistical": "pass"}, "tools": p["tools"], "plan_sha256": reviewed_digest,
           "at": X.utcnow()}
    if s.get("attempt"):
        doc["attempt"] = s["attempt"]
    path = os.path.join(record.evidence_dir, "images.json")
    sha = X.write_new(path, json.dumps(doc, indent=1, sort_keys=True) + "\n")
    data = {"plan_sha256": reviewed_digest, "file": path, "sha256": sha,
            "images": {k: v["ref"] for k, v in images.items()}}
    if s.get("attempt"):
        data["attempt"] = s["attempt"]
    return X.receipt(record, HELPER, "images-recorded", data)


def verify(record: X.Record, t: Tools) -> Dict:
    path = os.path.join(record.evidence_dir, "images.json")
    with open(path) as fh:
        doc = json.load(fh)
    problems = []
    s = _section(record)
    if doc.get("run_id") != record.run_id:
        problems.append(f"images.json is run {doc.get('run_id')!r}, not this record's run {record.run_id!r}")
    if doc.get("attempt") != s.get("attempt"):
        problems.append(f"images.json is attempt {doc.get('attempt') or 'none'}, not this record's "
                        f"{s.get('attempt') or 'none'}")
    want = tags(record, s)
    for k in sorted(want):
        if ((doc.get("images") or {}).get(k) or {}).get("tag") != want[k]:
            problems.append(f"{k}: images.json's tag is not this record's {want[k]}")
    for k, v in sorted(doc["images"].items()):
        m = _DIGEST_REF.match(v["ref"])
        try:
            raw = remote_manifest(t, v["ref"])
        except X.StagingError as e:
            problems.append(str(e)); continue
        if raw is None or "sha256:" + X.sha256_bytes(raw) != m.group(2):
            problems.append(f"{k}: {v['ref']} no longer resolves to its digest")
        if v.get("tag"):
            raw_tag = remote_manifest(t, v["tag"])
            if raw_tag is None or "sha256:" + X.sha256_bytes(raw_tag) != m.group(2):
                problems.append(f"{k}: the tag {v['tag']} moved or vanished (the digest stays authoritative)")
    return {"helper": HELPER, "action": "verify", "images": {k: v["ref"] for k, v in doc["images"].items()},
            "problems": problems, "result": "PASS" if not problems else "FAIL"}


def main(argv=None, run=subprocess.run) -> int:
    ap = argparse.ArgumentParser(prog="build_images.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", required=True)
    ap.add_argument("command", nargs="?", default="plan", choices=["plan", "apply", "verify"])
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    args = ap.parse_args(argv)
    try:
        record = X.load_record(args.record, ("build",))
        t = Tools(run, docker_config_dir(record.section("build")))
        if args.command == "plan":
            X.emit(X.cli_plan(record, HELPER, plan(record, t)))
        elif args.command == "apply":
            if not args.plan:
                raise X.StagingError("apply needs --plan <the reviewed plan file> and --plan-digest")
            X.emit(apply(record, t, X.load_plan(args.plan), args.plan_digest))
        else:
            out = verify(record, t)
            X.emit(out)
            return 0 if out["result"] == "PASS" else 1
    except X.StagingError as e:
        print(f"build_images: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
