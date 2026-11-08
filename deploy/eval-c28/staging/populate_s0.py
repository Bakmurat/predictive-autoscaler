#!/usr/bin/env python3
"""S0 export populator: fill archive.evidence.export_dir with every mandatory artifact (STAGING-C28-PROD.md S0; the
S0 record proposal's content_sources, pvc_content_requirements and export_contract).

  populate_s0.py --record R probe-plan                        read-only: what the size/file-count probe would run
  populate_s0.py --record R probe --plan F --plan-digest D    run the read-only probe pods, record sizes and selections
  populate_s0.py --record R [plan]                            read-only: the export plan (from the probe receipts)
  populate_s0.py --record R export --plan F --plan-digest D   populate export_dir exactly as reviewed
  populate_s0.py --record R verify                            read-only: export_dir == mandatory, every chain verifies
  populate_s0.py --record R recover-plan | recover --plan F --plan-digest D   reconcile an unknown pod outcome

What it produces, beneath export_dir (never overwriting a different file; an identical one is kept):
  * campaign/, protocol/, inventory/: the allowlisted files copied byte-for-byte from content_sources, each secret-
    scanned (a hit blocks; no value is printed) and hashed at source and destination; every checksum manifest's chain
    is verified at the source base and again at the destination.
  * source/legacy-source-manifest.json (git blob IDs of the legacy revision's manifests, the observed operator image
    digest, the terminations file's revision and blob), source/helper-source-manifest.json (the helper revision's
    blob IDs), source/terminations.json (the exact bytes of the final campaign terminations file at its revision).
  * restoration/legacy-selective-restore.md: the exact UID-bound restore order of the legacy objects (from the
    inventory), the retained claims, no namespace-wide restore, no PVC replacement, a foreign-writer check before the
    generators restart.
  * pvc-exports/<pvc>/campaign-export.tar, manifest.json, SHA256SUMS for forecast-log-pvc, evidence-archive-pvc,
    load-evidence-pvc and ml-models-pvc: only campaign files selected by their OWN names or record timestamps against
    an immutable cutoff (never "whatever is there now"), streamed out of a read-only reader pod as a tar with a per-file
    sha256 receipt and a trailer; every member is verified locally against the receipt and the reviewed selection.
  * storage/pvc-retention-and-export-coverage.json: all five claims (UID, PV, reclaim policy, consumers), what is
    protected by a verified export (or, for p8-evidence-pvc, the existing verified fetch) and what stays retained but
    NOT backed up.
  * S0-SHA256SUMS over every mandatory file except itself, written last.

Reader pods (one per claim, per probe or export): namespace ml-engine, the claim mounted read-only, no service-account
token, non-root, read-only root filesystem, pinned to the node the RWO volume is attached to (VolumeAttachment), a
deadline. They are refused while any load-evidence, ml-training, evidence-archive or p8 Job is active (a query error
refuses too). Each pod is created and deleted through common_records.mutation_step under the "s0-export" lock (the run
also refuses while the legacy lock is held), and cleanup is PROVEN: the pod is gone, no pod with the run's nonce
remains, and a volume that was not attached before is released again; otherwise the run fails and says so.

Selection rules (populate.pvcs.<pvc>.rules; the plan shows the effective rules; each rule may set its own cutoff):
  {"type": "jsonl_prefix", "path": P, "ts_fields": [...]}         the longest prefix of an append-only JSONL file
        whose records are all at or before the cutoff (each record must carry one of ts_fields; a later record before
        an earlier one, or a record without a timestamp, fails the rule); exported as the prefix bytes
  {"type": "named", "glob": G, "ts_regex": R, "ts_format": F}     files whose NAME carries a UTC time at or before the
        cutoff (R's first group parsed with F)
  {"type": "bundles", "manifest_glob": G, "ts_field": T}          whole directories whose manifest's T is at or
        before the cutoff (the manifest and every regular file beside it)
  {"type": "pairs", "sidecar_glob": G, "artifact_from": S, "artifact_to": A, "ts_field": T, "sha_field": H}
        a sidecar whose T is at or before the cutoff, with its artifact (name S replaced by A); with H, the artifact's
        sha256 must equal the sidecar's H, or the pair is inconsistent (in progress) and excluded
Empty files are never exported (listed as excluded). A rule that matches nothing, or any rule problem, blocks the plan.

The record's "populate" section:
  {"cutoff": "2026-11-02T00:00:00Z",
   "content_sources": {"campaign/": DIR, "protocol/": DIR, "inventory/": DIR},
   "legacy_source": {"repo_dir": DIR, "commit": "0472829", "paths": [...], "operator_image": "<repo>@sha256:..."},
   "terminations": {"repo_dir": DIR, "commit": REV, "path": "deploy/prodcluster/load-gate/terminations.json"},
   "helper_source": {"repo_dir": DIR, "commit": REV, "paths": [...]},
   "inventory": "<inventory file name under content_sources inventory/>",
   "retained_pvcs": [{"name", "uid"}, ...],                                   all five claims
   "p8": {"export": "campaign/final-20261102/p8-fetch-final-20261102.jsonl", "sha256": HEX},
   "requirements": {"<pvc>": "<what Report 2 / rollback needs from it>"},
   "reader": {"image": "<repo>@sha256:...", "deadline_seconds": 1800, "exec_timeout_seconds": 1800},
   "budget": {"max_total_bytes": N},
   "pvcs": {"<pvc>": {"uid": UID, "mount": "/data", "max_bytes": N, "rules": [...]}, ...}}
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import tarfile
import time
from typing import Callable, Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common_records as C  # noqa: E402
import execlib as X  # noqa: E402
import legacy_allowlist as L  # noqa: E402

HELPER = "populate-s0"
PROBE = "populate-s0-probe"
LOCK_SCOPE = "s0-export"
NS = "ml-engine"
READER_LABEL = "c28.devkuban.com/s0-reader"
NONCE_LABEL = "c28.devkuban.com/s0-nonce"
PVC_EXPORTS = ("forecast-log-pvc", "evidence-archive-pvc", "load-evidence-pvc", "ml-models-pvc")
LOCAL_TOPS = ("campaign/", "protocol/", "inventory/")
BUSY_JOB_PREFIXES = ("load-evidence", "ml-training", "evidence-archive", "p8")
_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$")
_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_COMMITISH = re.compile(r"^[0-9a-f]{7,40}$")

# --- the in-pod reader (stdlib only; read-only; run with the selection on stdin) -------------------------------------

READER = r'''
import datetime, fnmatch, glob, hashlib, io, json, os, re, sys, tarfile

def utc(s):
    if not isinstance(s, str):
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            t = datetime.datetime.strptime(s, fmt)
            if t.tzinfo is None:
                t = t.replace(tzinfo=datetime.timezone.utc)
            return t.astimezone(datetime.timezone.utc)
        except ValueError:
            pass
    return None

def safe(mount, rel):
    p = os.path.normpath(os.path.join(mount, rel))
    if not p.startswith(mount.rstrip("/") + "/") or os.path.islink(p):
        raise ValueError("unsafe path " + rel)
    return p

def files_under(mount, pattern):
    out = []
    for p in sorted(glob.glob(os.path.join(mount, pattern))):
        if os.path.isfile(p) and not os.path.islink(p):
            out.append(os.path.relpath(p, mount))
    return out

def jsonl_prefix(mount, rule, cutoff):
    p = safe(mount, rule["path"])
    if not os.path.isfile(p):
        return [], {"missing": rule["path"]}, ["the file " + rule["path"] + " does not exist"]
    probs, offset, lines, later, size, seen_later = [], 0, 0, 0, 0, False
    last_ts = None
    with open(p, "rb") as fh:
        for raw in fh:
            size += len(raw)
            if not raw.endswith(b"\n"):
                break                                 # a partially written last line is never exported
            try:
                rec = json.loads(raw)
            except ValueError:
                probs.append("malformed JSON at line %d" % (lines + later + 1))
                break
            ts = None
            for f in rule["ts_fields"]:
                ts = utc(rec.get(f)) if isinstance(rec, dict) else None
                if ts:
                    break
            if ts is None:
                probs.append("a record without any of %s at line %d" % (rule["ts_fields"], lines + later + 1))
                break
            if ts <= cutoff:
                if seen_later:
                    probs.append("a record at or before the cutoff after a later one (not append-ordered)")
                    break
                offset += len(raw); lines += 1
            else:
                seen_later = True; later += 1
    if lines == 0 and not probs:
        probs.append("no record of " + rule["path"] + " is at or before the cutoff")
    sel = [{"path": rule["path"], "kind": "prefix", "bytes": offset, "lines": lines}] if lines and not probs else []
    return sel, {"lines_after_cutoff": later, "file_bytes_read": size}, probs

def named(mount, rule, cutoff):
    sel, excl, probs = [], {"after_cutoff": 0, "empty": 0, "unnamed": 0}, []
    rx = re.compile(rule["ts_regex"])
    for rel in files_under(mount, rule["glob"]):
        m = rx.search(os.path.basename(rel))
        if not m:
            excl["unnamed"] += 1; continue
        try:
            ts = datetime.datetime.strptime(m.group(1), rule["ts_format"]).replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            excl["unnamed"] += 1; continue
        size = os.path.getsize(os.path.join(mount, rel))
        if ts > cutoff:
            excl["after_cutoff"] += 1
        elif size == 0:
            excl["empty"] += 1
        else:
            sel.append({"path": rel, "kind": "file", "bytes": size, "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")})
    return sel, excl, probs

def bundles(mount, rule, cutoff):
    sel, excl, probs = [], {"after_cutoff": 0, "empty": 0, "no_timestamp": 0}, []
    for rel in files_under(mount, rule["manifest_glob"]):
        try:
            man = json.load(open(os.path.join(mount, rel)))
        except ValueError:
            probs.append("unreadable manifest " + rel); continue
        ts = utc(man.get(rule["ts_field"])) if isinstance(man, dict) else None
        if ts is None:
            excl["no_timestamp"] += 1; continue
        if ts > cutoff:
            excl["after_cutoff"] += 1; continue
        d = os.path.dirname(rel)
        for f in files_under(mount, os.path.join(d, "*")):
            size = os.path.getsize(os.path.join(mount, f))
            if size == 0:
                excl["empty"] += 1
            else:
                sel.append({"path": f, "kind": "file", "bytes": size, "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")})
    return sel, excl, probs

def sha(path, limit=None):
    h, n = hashlib.sha256(), 0
    with open(path, "rb") as fh:
        while True:
            want = 1 << 20 if limit is None else min(1 << 20, limit - n)
            if want <= 0:
                break
            b = fh.read(want)
            if not b:
                break
            h.update(b); n += len(b)
    return h.hexdigest(), n

def pairs(mount, rule, cutoff):
    sel, excl, probs = [], {"after_cutoff": 0, "empty": 0, "no_timestamp": 0, "inconsistent": 0, "no_artifact": 0}, []
    for rel in files_under(mount, rule["sidecar_glob"]):
        try:
            meta = json.load(open(os.path.join(mount, rel)))
        except ValueError:
            probs.append("unreadable sidecar " + rel); continue
        ts = utc(meta.get(rule["ts_field"])) if isinstance(meta, dict) else None
        if ts is None:
            excl["no_timestamp"] += 1; continue
        if ts > cutoff:
            excl["after_cutoff"] += 1; continue
        art = rel[: -len(rule["artifact_from"])] + rule["artifact_to"] if rel.endswith(rule["artifact_from"]) else None
        ap = os.path.join(mount, art) if art else None
        if not ap or not os.path.isfile(ap) or os.path.islink(ap):
            excl["no_artifact"] += 1; continue
        if rule.get("sha_field"):
            if sha(ap)[0] != meta.get(rule["sha_field"]):
                excl["inconsistent"] += 1; continue
        for f in (rel, art):
            size = os.path.getsize(os.path.join(mount, f))
            if size == 0:
                excl["empty"] += 1
            else:
                sel.append({"path": f, "kind": "file", "bytes": size, "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")})
    return sel, excl, probs

RULES = {"jsonl_prefix": jsonl_prefix, "named": named, "bundles": bundles, "pairs": pairs}

class Hashing:
    def __init__(self, out):
        self.out, self.h, self.n = out, hashlib.sha256(), 0
    def write(self, b):
        self.h.update(b); self.n += len(b); self.out.write(b)
        return len(b)
    def flush(self):
        self.out.flush()

class Limited(io.RawIOBase):
    def __init__(self, fh, limit, h):
        self.fh, self.left, self.h = fh, limit, h
    def readable(self):
        return True
    def read(self, n=-1):
        if self.left <= 0:
            return b""
        n = self.left if n is None or n < 0 else min(n, self.left)
        b = self.fh.read(n)
        self.left -= len(b); self.h.update(b)
        return b

def main():
    job = json.load(sys.stdin)
    mount, mode = job["mount"], job["mode"]
    if mode == "probe":
        out = {"rules": [], "volume": {"files": 0, "bytes": 0}}
        for d, _, names in os.walk(mount):
            for n in names:
                p = os.path.join(d, n)
                if os.path.isfile(p) and not os.path.islink(p):
                    out["volume"]["files"] += 1; out["volume"]["bytes"] += os.path.getsize(p)
        for rule in job["rules"]:
            cutoff = utc(rule["cutoff"])
            try:
                sel, excl, probs = RULES[rule["type"]](mount, rule, cutoff)
            except Exception as e:
                sel, excl, probs = [], {}, ["%s: %s" % (type(e).__name__, e)]
            if not sel and not probs:
                probs.append("the rule selects nothing")
            out["rules"].append({"rule": rule, "selected": sel, "excluded": excl, "problems": probs})
        sys.stdout.write(json.dumps(out, sort_keys=True))
        return 0
    # export: exactly the reviewed selection, in order; a file whose size moved is refused (not immutable)
    out = Hashing(sys.stdout.buffer)
    files, probs = [], []
    with tarfile.open(fileobj=out, mode="w|", format=tarfile.PAX_FORMAT) as tar:
        for item in job["selection"]:
            p = safe(mount, item["path"])
            size = os.path.getsize(p)
            if item["kind"] == "file" and size != item["bytes"]:
                probs.append(item["path"] + " changed size since the probe"); break
            if item["kind"] == "prefix" and size < item["bytes"]:
                probs.append(item["path"] + " shrank below its reviewed prefix"); break
            h = hashlib.sha256()
            info = tarfile.TarInfo(item["path"])
            info.size, info.mode, info.mtime = item["bytes"], 0o444, 0
            with open(p, "rb") as fh:
                tar.addfile(info, Limited(fh, item["bytes"], h))
            if item["kind"] == "prefix":
                with open(p, "rb") as fh:
                    fh.seek(item["bytes"] - 1)
                    if fh.read(1) != b"\n":
                        probs.append(item["path"] + ": the reviewed prefix does not end at a record boundary"); break
            files.append({"path": item["path"], "bytes": item["bytes"], "sha256": h.hexdigest()})
    out.flush()
    sys.stderr.write(json.dumps({"trailer": {"bytes": out.n, "sha256": out.h.hexdigest()}, "files": files,
                                 "problems": probs}) + "\n")
    return 1 if probs else 0

sys.exit(main())
'''
READER_SHA256 = hashlib.sha256(READER.encode()).hexdigest()

DEFAULT_RULES = {
    "forecast-log-pvc": [{"type": "jsonl_prefix", "path": "forecasts.jsonl", "ts_fields": ["at", "issued_at"]}],
    "evidence-archive-pvc": [
        {"type": "bundles", "manifest_glob": "archive/*/manifest.json", "ts_field": "copied_at"},
        {"type": "jsonl_prefix", "path": "INDEX.jsonl", "ts_fields": ["copied_at", "at", "recovered_at"]},
        {"type": "jsonl_prefix", "path": "logs/api-ensemble.jsonl", "ts_fields": ["ts"]},
        {"type": "jsonl_prefix", "path": "logs/trainer-index.jsonl", "ts_fields": ["at", "finished_at", "started_at"]},
    ],
    "load-evidence-pvc": [
        {"type": "named", "glob": "attempts/load-*.json", "ts_regex": r"^load-(\d{8}T\d{4})Z", "ts_format": "%Y%m%dT%H%M"},
        {"type": "named", "glob": "load-*.json", "ts_regex": r"^load-(\d{8}T\d{4})Z\.json$", "ts_format": "%Y%m%dT%H%M"},
    ],
    "ml-models-pvc": [{"type": "pairs", "sidecar_glob": "*.meta.json", "artifact_from": ".meta.json",
                       "artifact_to": ".pkl", "ts_field": "trained_at"}],
}
RULE_KEYS = {"jsonl_prefix": {"type", "path", "ts_fields", "cutoff"},
             "named": {"type", "glob", "ts_regex", "ts_format", "cutoff"},
             "bundles": {"type", "manifest_glob", "ts_field", "cutoff"},
             "pairs": {"type", "sidecar_glob", "artifact_from", "artifact_to", "ts_field", "sha_field", "cutoff"}}


# --- the record -------------------------------------------------------------------------------------------------------

def section(record: X.Record) -> Dict:
    s = record.section("populate")
    probs = []
    if not _TS.match(str(s.get("cutoff", ""))):
        probs.append("populate.cutoff must be an RFC 3339 UTC time")
    cs = s.get("content_sources") or {}
    for top in LOCAL_TOPS:
        if not os.path.isdir(record.resolve(str(cs.get(top, "")))):
            probs.append(f"populate.content_sources[{top!r}] must be an existing directory")
    for k in ("legacy_source", "helper_source", "terminations"):
        v = s.get(k) or {}
        if not (os.path.isdir(record.resolve(str(v.get("repo_dir", "")))) and _COMMITISH.match(str(v.get("commit", "")))):
            probs.append(f"populate.{k} needs repo_dir and commit")
    if not _DIGEST_IMAGE.match(str((s.get("legacy_source") or {}).get("operator_image", ""))):
        probs.append("populate.legacy_source.operator_image must be the observed digest-pinned image")
    if not _DIGEST_IMAGE.match(str((s.get("reader") or {}).get("image", ""))):
        probs.append("populate.reader.image must be pinned by digest")
    if not isinstance((s.get("budget") or {}).get("max_total_bytes"), int):
        probs.append("populate.budget.max_total_bytes is required")
    pvcs = s.get("pvcs") or {}
    if sorted(pvcs) != sorted(PVC_EXPORTS):
        probs.append(f"populate.pvcs must configure exactly {list(PVC_EXPORTS)}")
    for name, p in pvcs.items():
        if not (X._UUID.match(str(p.get("uid", ""))) and str(p.get("mount", "")).startswith("/")
                and isinstance(p.get("max_bytes"), int) and p["max_bytes"] > 0):
            probs.append(f"populate.pvcs.{name} needs uid, an absolute mount and max_bytes")
    retained = s.get("retained_pvcs") or []
    if len(retained) != 5 or not all(X._UUID.match(str(r.get("uid", ""))) for r in retained):
        probs.append("populate.retained_pvcs must list all five claims with UIDs")
    if not re.match(r"^[0-9a-f]{64}$", str((s.get("p8") or {}).get("sha256", ""))):
        probs.append("populate.p8 needs the existing export and its sha256")
    if probs:
        raise X.StagingError("the populate section is not usable: " + "; ".join(probs))
    return s


def effective_rules(s: Dict, pvc: str) -> List[Dict]:
    rules = s["pvcs"][pvc].get("rules", "default")
    rules = copy.deepcopy(DEFAULT_RULES[pvc] if rules == "default" else rules)
    out = []
    for r in rules:
        if r.get("type") not in RULE_KEYS or not set(r) <= RULE_KEYS[r["type"]]:
            raise X.StagingError(f"{pvc}: unknown rule or field in {r}")
        r.setdefault("cutoff", s["cutoff"])
        if not _TS.match(r["cutoff"]):
            raise X.StagingError(f"{pvc}: rule cutoff {r['cutoff']!r} is not an RFC 3339 UTC time")
        out.append(r)
    return out


def evidence(record: X.Record) -> Dict:
    ev = (record.doc.get("archive") or {}).get("evidence") or {}
    if not ev.get("export_dir") or not ev.get("mandatory") or set(ev["mandatory"]) != set(ev.get("allowlist") or []):
        raise X.StagingError("archive.evidence needs export_dir and mandatory == allowlist")
    return ev


# --- local sources ----------------------------------------------------------------------------------------------------

def local_sources(record: X.Record, s: Dict) -> Tuple[List[Dict], List[str]]:
    ev = evidence(record)
    out, probs = [], []
    for rel in sorted(ev["mandatory"]):
        top = next((t for t in LOCAL_TOPS if rel.startswith(t)), None)
        if not top:
            continue
        src = os.path.join(record.resolve(s["content_sources"][top]), rel[len(top):])
        if not os.path.isfile(src) or os.path.islink(src):
            probs.append(f"{rel}: the source {src} is missing")
            continue
        data = open(src, "rb").read()
        try:
            C.assert_clean(data.decode("utf-8", errors="ignore"))
        except C.StagingError:
            probs.append(f"{rel}: the secret scan hit (no value printed); a hit blocks the export")
        out.append({"path": rel, "source": src, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)})
    return out, probs


def chain_problems(base_for: Callable[[str], str], manifests: List[str]) -> List[str]:
    """Each manifest verified against the files beside it in its own base (a source tree or the export)."""
    out = []
    for m in manifests:
        mpath = base_for(m)
        try:
            lines = [ln for ln in open(mpath).read().splitlines() if ln.strip()]
        except OSError:
            out.append(f"{m} is unreadable")
            continue
        if not lines:
            out.append(f"{m} lists no file")
        for ln in lines:
            parts = ln.split(None, 1)
            if len(parts) != 2 or len(parts[0]) != 64:
                out.append(f"{m}: malformed line")
                continue
            target = os.path.normpath(os.path.join(os.path.dirname(mpath), parts[1].lstrip("*").strip()))
            if not os.path.isfile(target):
                out.append(f"{m}: {parts[1]} is missing")
            elif C.sha256_file(target) != parts[0].lower():
                out.append(f"{m}: {parts[1]} does not match its recorded checksum")
    return out


def source_chain_problems(record: X.Record, s: Dict) -> List[str]:
    ev = evidence(record)
    local = [m for m in ev.get("checksum_manifests") or [] if m.startswith(LOCAL_TOPS)]

    def base(m):
        top = next(t for t in LOCAL_TOPS if m.startswith(t))
        return os.path.join(record.resolve(s["content_sources"][top]), m[len(top):])
    return chain_problems(base, local)


# --- generated provenance -----------------------------------------------------------------------------------------

def git_env() -> Dict[str, str]:
    """The environment for git: every ambient GIT_* variable removed (inside a git hook GIT_DIR and GIT_INDEX_FILE
    point at ANOTHER repository and would silently redirect -C)."""
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def _git(repo: str, *args: str, run=subprocess.run) -> bytes:
    res = run(["git", "-C", repo, *args], capture_output=True, timeout=120, env=git_env())
    if res.returncode != 0:
        raise X.StagingError(f"git {args[0]} failed in {repo}")
    return res.stdout


def _blobs(repo: str, commit: str, paths: List[str], run=subprocess.run) -> Tuple[str, List[Dict]]:
    full = _git(repo, "rev-parse", "--verify", f"{commit}^{{commit}}", run=run).decode().strip()
    out = []
    for line in _git(repo, "ls-tree", "-r", "--full-tree", full, "--", *paths, run=run).decode().splitlines():
        meta, path_ = line.split("\t", 1)
        mode, kind, blob = meta.split()
        out.append({"path": path_, "mode": mode, "type": kind, "blob": blob})
    if not out:
        raise X.StagingError(f"{commit} has none of {paths}")
    return full, out


def generated_sources(record: X.Record, s: Dict, run=subprocess.run) -> Dict[str, bytes]:
    lg, hp, tm = s["legacy_source"], s["helper_source"], s["terminations"]
    lcommit, lblobs = _blobs(record.resolve(lg["repo_dir"]), lg["commit"], lg["paths"], run)
    tcommit = _git(record.resolve(tm["repo_dir"]), "rev-parse", "--verify", f"{tm['commit']}^{{commit}}",
                   run=run).decode().strip()
    tblob = _git(record.resolve(tm["repo_dir"]), "rev-parse", f"{tcommit}:{tm['path']}", run=run).decode().strip()
    terminations = _git(record.resolve(tm["repo_dir"]), "cat-file", "blob", tblob, run=run)
    try:
        json.loads(terminations)
    except ValueError as e:
        raise X.StagingError("the terminations file at its revision is not JSON") from e
    hcommit, hblobs = _blobs(record.resolve(hp["repo_dir"]), hp["commit"], hp["paths"], run)
    legacy = {"schema": "c28-legacy-source-manifest/v1", "commit": lcommit, "paths": lg["paths"], "blobs": lblobs,
              "observed_operator_image": lg["operator_image"],
              "terminations": {"path": tm["path"], "commit": tcommit, "blob": tblob,
                               "sha256": hashlib.sha256(terminations).hexdigest()},
              "note": "git tree entries only; no environment, Secret or credential content"}
    helper = {"schema": "c28-helper-source-manifest/v1", "commit": hcommit, "paths": hp["paths"], "blobs": hblobs,
              "populator": {"path": "deploy/eval-c28/staging/populate_s0.py",
                            "sha256": C.sha256_file(os.path.abspath(__file__)), "reader_sha256": READER_SHA256}}
    out = {"source/legacy-source-manifest.json": (json.dumps(legacy, indent=1, sort_keys=True) + "\n").encode(),
           "source/helper-source-manifest.json": (json.dumps(helper, indent=1, sort_keys=True) + "\n").encode(),
           "source/terminations.json": terminations}
    for k, v in out.items():
        C.assert_clean(v.decode("utf-8", errors="ignore"))
    return out


def _inventory(record: X.Record, s: Dict) -> Dict:
    p = os.path.join(record.resolve(s["content_sources"]["inventory/"]), s["inventory"])
    with open(p) as fh:
        return json.load(fh)


def restoration_md(record: X.Record, s: Dict) -> bytes:
    inv = _inventory(record, s)
    found = {(f["kind"], f["namespace"], f["name"]): f for f in inv["legacy"]["found"]}
    lines = ["# Legacy benchmark: selective restoration (UID-bound)", "",
             f"Cluster `{inv['cluster_uid']}`, inventory `{s['inventory']}`, run `{record.run_id}`. Executed only "
             "through `legacy.py restore-plan` / `restore` (reviewed plan digest), never by hand and never as a "
             "namespace-wide restore. Restore undoes exactly what quiesce did, newest first, each object bound to "
             "its UID and post-quiesce state; anything that drifted is refused, not overwritten.", "",
             "## Order (reverse of the quiesce order)", ""]
    for n, role in enumerate(reversed(L.ROLE_ORDER), 1):
        lines.append(f"{n}. **{role}**")
        for k, ns, name, r in L.ALLOWLIST:
            if r != role:
                continue
            f = found.get((k, ns, name))
            uid = f["uid"] if f else "absent at inventory"
            lines.append(f"   - `{k}/{ns}/{name}` UID `{uid}`")
        if role == "generator":
            lines.append("   - before restarting arrivals: verify that no foreign writer owns any arm target and "
                         "that each target is ready with exactly one writer")
    lines += ["", "## Never restored or replaced", ""]
    for r in s["retained_pvcs"]:
        lines.append(f"- PersistentVolumeClaim `ml-engine/{r['name']}` UID `{r['uid']}`: retained as is; its data is "
                     "never replaced, restored over, or deleted")
    lines += ["- the legacy CRD, the archived legacy PredictiveAutoscalers and the namespaces", "",
              "## Missing at inventory (reviewed)", ""]
    for m in (record.doc.get("archive") or {}).get("unaccounted_reviewed") or []:
        lines.append(f"- `{m['kind']}/{m['namespace']}/{m['name']}` ({m['role']}): {m['reason']}")
    text = "\n".join(lines) + "\n"
    C.assert_clean(text)
    return text.encode()


# --- the cluster ------------------------------------------------------------------------------------------------------

def busy_jobs(kube: X.Kube) -> List[str]:
    """Active load-evidence, ml-training, evidence-archive or p8 Jobs (a query error raises: fail closed)."""
    out = []
    for j in kube.list("Job", NS):
        m = j["metadata"]
        if (j.get("status") or {}).get("active") and (m["name"].startswith(BUSY_JOB_PREFIXES)
                                                      or (m.get("labels") or {}).get("p8-mode")):
            out.append(m["name"])
    return sorted(out)


def claim_binding(kube: X.Kube, name: str, uid: str) -> Dict:
    pvc = kube.get("PersistentVolumeClaim", name, NS)
    if pvc is None or pvc["metadata"]["uid"] != uid:
        raise X.StagingError(f"{name} is missing or its UID differs from the record")
    pv_name = (pvc.get("spec") or {}).get("volumeName")
    pv = kube.get("PersistentVolume", pv_name) if pv_name else None
    if pv is None:
        raise X.StagingError(f"{name} is not bound to a volume")
    node = attached_node(kube, pv_name)
    return {"name": name, "uid": uid, "pv": pv_name, "pv_uid": pv["metadata"]["uid"],
            "reclaim_policy": (pv.get("spec") or {}).get("persistentVolumeReclaimPolicy"),
            "storage_class": (pvc.get("spec") or {}).get("storageClassName"),
            "access_modes": (pvc.get("spec") or {}).get("accessModes"), "attached_node": node}


def attached_node(kube: X.Kube, pv_name: str) -> str:
    nodes = [a["spec"]["nodeName"] for a in kube.list("VolumeAttachment")
             if (a.get("spec") or {}).get("source", {}).get("persistentVolumeName") == pv_name
             and (a.get("status") or {}).get("attached")]
    if len(nodes) > 1:
        raise X.StagingError(f"{pv_name} is attached to several nodes {nodes}")
    return nodes[0] if nodes else ""


def consumers(kube: X.Kube) -> Dict[str, List[Dict]]:
    out: Dict[str, List[Dict]] = {}
    for p in kube.list("Pod", NS):
        for v in (p.get("spec") or {}).get("volumes") or []:
            c = (v.get("persistentVolumeClaim") or {}).get("claimName")
            if c:
                out.setdefault(c, []).append({"pod": p["metadata"]["name"], "uid": p["metadata"].get("uid"),
                                              "node": (p.get("spec") or {}).get("nodeName"),
                                              "read_only": bool(v["persistentVolumeClaim"].get("readOnly"))})
    return out


def leftover_readers(kube: X.Kube, record: X.Record) -> List[str]:
    return sorted(p["metadata"]["name"] for p in kube.list("Pod", NS, labels={READER_LABEL: record.run_id}))


def reader_pod(record: X.Record, s: Dict, pvc: str, binding: Dict, nonce: str) -> Dict:
    r = s["reader"]
    terms = [{"key": "kubernetes.io/hostname", "operator": "In", "values": [binding["attached_node"]]}] \
        if binding["attached_node"] else []
    spec = {"restartPolicy": "Never", "activeDeadlineSeconds": int(r.get("deadline_seconds", 1800)),
            "automountServiceAccountToken": False, "enableServiceLinks": False,
            "securityContext": {"runAsNonRoot": True, "runAsUser": 65534, "runAsGroup": 65534,
                                "seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [{"name": "reader", "image": r["image"],
                            "command": ["python3", "-B", "-c",
                                        f"import time; time.sleep({int(r.get('deadline_seconds', 1800))})"],
                            "stdin": False,
                            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                                "capabilities": {"drop": ["ALL"]}},
                            "resources": {"requests": {"cpu": "50m", "memory": "64Mi"},
                                          "limits": {"cpu": "1", "memory": "256Mi"}},
                            "volumeMounts": [{"name": "data", "mountPath": s["pvcs"][pvc]["mount"],
                                              "readOnly": True}]}],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": pvc, "readOnly": True}}]}
    if terms:
        spec["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchExpressions": terms}]}}}
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": f"c28-s0-{pvc[:-4]}-{nonce}", "namespace": NS,
                         "labels": {READER_LABEL: record.run_id, NONCE_LABEL: nonce,
                                    "app.kubernetes.io/part-of": "c28-evaluation"},
                         "annotations": {"sidecar.istio.io/inject": "false"}},
            "spec": spec}


class Exec:
    """kubectl exec into a reader pod with an explicit kubeconfig and context; the job JSON on stdin, stdout to a
    file, stderr captured (the trailer). No credential is passed or printed."""

    def __init__(self, record: X.Record, timeout: int, run=subprocess.run):
        kc = record.doc["kubeconfig"]
        self.base = ["kubectl", "--kubeconfig", kc["path"], "--context", kc["context"], "-n", NS]
        self.timeout, self.run = timeout, run

    def __call__(self, pod: str, job: Dict, out_path: str) -> Tuple[int, str]:
        argv = self.base + ["exec", "-i", pod, "-c", "reader", "--", "python3", "-B", "-c", READER]
        env = {k: v for k, v in os.environ.items() if k != "KUBECONFIG"}
        env["KUBECONFIG"] = self.base[2]
        with open(out_path, "wb") as fh:
            res = self.run(argv, input=json.dumps(job).encode(), stdout=fh, stderr=subprocess.PIPE,
                           timeout=self.timeout, env=env)
        return res.returncode, (res.stderr or b"").decode("utf-8", errors="replace")


class Reader:
    """One reader pod's lifecycle under the s0-export lock: created and deleted through mutation_step, cleanup proven."""

    def __init__(self, record: X.Record, kube: X.Kube, lock, helper: str, s: Dict, pvc: str, binding: Dict,
                 sleep=time.sleep, now=time.monotonic):
        self.record, self.kube, self.lock, self.helper, self.s = record, kube, lock, helper, s
        self.pvc, self.binding, self.sleep, self.now = pvc, binding, sleep, now
        self.nonce = secrets.token_hex(5)
        self.body = reader_pod(record, s, pvc, binding, self.nonce)
        self.name, self.uid = self.body["metadata"]["name"], None

    def _step(self, op: str, call: Callable[[], Dict], extra: Dict) -> Dict:
        entries = X.receipts(self.record, self.helper)
        n = str(max([int(e["event"].split("-", 1)[1]) for e in entries
                     if e["event"].startswith(("intent-", "step-", "reconciled-"))] or [0]) + 1)
        intent = {"op": op, "kind": "Pod", "namespace": NS, "name": self.name, "pvc": self.pvc, "nonce": self.nonce,
                  "pre": {"attached_node": self.binding["attached_node"]}, **extra}

        def do():
            out = call()
            return "applied", {"uid": ((out or {}).get("metadata") or {}).get("uid") or extra.get("uid")}
        return C.mutation_step(self.record.doc, self.helper, self.lock, n, intent, do)

    def __enter__(self):
        C.require_cluster(self.kube.api, self.record.cluster_uid)
        r = self._step("create", lambda: self.kube.api.create(X.path("Pod", NS), self.body),
                       {"body_sha256": C.digest(self.body)})
        self.uid = r["data"]["post"]["uid"]
        deadline = self.now() + 300
        while True:
            p = self.kube.get("Pod", self.name, NS)
            phase = ((p or {}).get("status") or {}).get("phase")
            if p is not None and phase == "Running":
                return self
            if phase in ("Failed", "Succeeded") or self.now() > deadline:
                raise X.StagingError(f"reader pod {self.name} did not start (phase {phase})")
            self.sleep(2)

    def __exit__(self, exc_type, exc, tb):
        self.cleanup()
        return False

    def cleanup(self) -> None:
        """Delete by UID and PROVE: no pod of this nonce remains and a volume not attached before is released."""
        if self.uid and self.kube.get("Pod", self.name, NS) is not None:
            C.require_cluster(self.kube.api, self.record.cluster_uid)
            self._step("delete", lambda: self.kube.api.delete(X.path("Pod", NS, self.name), self.uid) or {},
                       {"uid": self.uid})
        deadline = self.now() + 300
        while True:
            left = self.kube.list("Pod", NS, labels={NONCE_LABEL: self.nonce})
            node = attached_node(self.kube, self.binding["pv"])
            released = bool(self.binding["attached_node"]) or not node
            if not left and released:
                X.receipt(self.record, self.helper, "reader-cleanup-proven",
                          {"pvc": self.pvc, "pod": self.name, "uid": self.uid, "nonce": self.nonce,
                           "volume_attached_after": node, "volume_attached_before": self.binding["attached_node"]})
                return
            if self.now() > deadline:
                raise X.StagingError(f"cleanup of {self.name} is NOT proven (pods {[p['metadata']['name'] for p in left]}"
                                     f", volume attached to {node!r}); check before anything else runs")
            self.sleep(2)


# --- probe ------------------------------------------------------------------------------------------------------------

def _pre_checks(record: X.Record, kube: X.Kube, s: Dict) -> Tuple[Dict, List[str]]:
    C.require_cluster(kube.api, record.cluster_uid)
    probs = []
    if os.path.exists(C.lock_path(record.doc, "legacy")):
        probs.append("the legacy mutation lock is held")
    busy = busy_jobs(kube)
    if busy:
        probs.append(f"active Jobs {busy}: no reader while they run")
    left = leftover_readers(kube, record)
    if left:
        probs.append(f"leftover reader pods {left}: clean them first (recover)")
    bindings = {p: claim_binding(kube, p, s["pvcs"][p]["uid"]) for p in PVC_EXPORTS}
    return bindings, probs


def probe_plan(record: X.Record, kube: X.Kube) -> Dict:
    s = section(record)
    bindings, probs = _pre_checks(record, kube, s)
    actions = [{"pvc": p, "binding": bindings[p], "mount": s["pvcs"][p]["mount"], "rules": effective_rules(s, p)}
               for p in PVC_EXPORTS]
    return X.new_plan(PROBE, record, actions, reader_sha256=READER_SHA256, reader_image=s["reader"]["image"],
                      problems=probs, note="read-only reader pods: sizes, file counts and the rule selections")


def _run_reader(record, kube, lock, helper, s, pvc, binding, job, exe, sleep, now) -> Tuple[int, str, str]:
    tmp = os.path.join(record.evidence_dir, "populate", f"{helper}-{pvc}-{secrets.token_hex(4)}.out")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with Reader(record, kube, lock, helper, s, pvc, binding, sleep, now) as rd:
        rc, err = exe(rd.name, job, tmp)
    return rc, err, tmp


def probe(record: X.Record, kube: X.Kube, reviewed: Dict, digest: str, exe=None, sleep=time.sleep,
          now=time.monotonic) -> Dict:
    s = section(record)
    exe = exe or Exec(record, int(s["reader"].get("exec_timeout_seconds", 1800)))
    with C.exclusive_lock(record.doc, LOCK_SCOPE, holder=PROBE) as lock:
        live = probe_plan(record, kube)
        X.require_live(reviewed, digest, PROBE, record, live)
        if live["problems"]:
            raise X.StagingError("the probe plan has problems: " + "; ".join(live["problems"]))
        results = {}
        for a in live["actions"]:
            job = {"mode": "probe", "mount": a["mount"], "rules": a["rules"]}
            rc, err, tmp = _run_reader(record, kube, lock, PROBE, s, a["pvc"], a["binding"], job, exe, sleep, now)
            try:
                out = json.load(open(tmp))
            except ValueError as e:
                raise X.StagingError(f"{a['pvc']}: the probe output is not JSON (exit {rc})") from e
            finally:
                os.unlink(tmp)
            results[a["pvc"]] = out
            X.receipt(record, PROBE, "probed", {"pvc": a["pvc"], "plan_sha256": digest, "binding": a["binding"],
                                                "rules_sha256": C.digest(a["rules"]), "result": out})
        return X.receipt(record, PROBE, "probe-complete", {"plan_sha256": digest, "pvcs": sorted(results)})


def probe_results(record: X.Record) -> Dict[str, Dict]:
    out = {}
    for e in X.receipts(record, PROBE):
        if e["event"] == "probed":
            out[e["data"]["pvc"]] = e["data"]
    return out


# --- the export plan --------------------------------------------------------------------------------------------------

def plan(record: X.Record, kube: X.Kube) -> Dict:
    s = section(record)
    ev = evidence(record)
    bindings, probs = _pre_checks(record, kube, s)
    local, lprobs = local_sources(record, s)
    probs += lprobs + source_chain_problems(record, s)
    try:
        gen = generated_sources(record, s)
        gen["restoration/legacy-selective-restore.md"] = restoration_md(record, s)
    except (X.StagingError, OSError, KeyError) as e:
        gen, probs = {}, probs + [f"generated provenance: {e}"]
    probed = probe_results(record)
    pvcs, total = {}, 0
    for p in PVC_EXPORTS:
        rules = effective_rules(s, p)
        r = probed.get(p)
        if r is None:
            probs.append(f"{p}: not probed (run probe first)")
            continue
        if r["rules_sha256"] != C.digest(rules) or r["binding"]["uid"] != bindings[p]["uid"] \
                or r["binding"]["pv"] != bindings[p]["pv"]:
            probs.append(f"{p}: the probe was for other rules or another binding: probe again")
            continue
        selection = [it for rr in r["result"]["rules"] for it in rr["selected"]]
        for rr in r["result"]["rules"]:
            probs += [f"{p}: rule {rr['rule']['type']} ({rr['rule'].get('path') or rr['rule'].get('glob') or rr['rule'].get('manifest_glob') or rr['rule'].get('sidecar_glob')}): {x}"
                      for x in rr["problems"]]
        paths = [it["path"] for it in selection]
        if len(set(paths)) != len(paths):
            probs.append(f"{p}: two rules select the same file")
        size = sum(it["bytes"] for it in selection)
        total += size
        if size > s["pvcs"][p]["max_bytes"]:
            probs.append(f"{p}: the selection is {size} bytes, over its budget {s['pvcs'][p]['max_bytes']}")
        if not selection:
            probs.append(f"{p}: nothing selected (an empty export is never produced)")
        pvcs[p] = {"binding": bindings[p], "rules": rules, "selection": selection, "bytes": size,
                   "volume": r["result"]["volume"], "excluded": [rr["excluded"] for rr in r["result"]["rules"]]}
    if total > s["budget"]["max_total_bytes"]:
        probs.append(f"the PVC exports total {total} bytes, over the budget {s['budget']['max_total_bytes']}")
    export_dir = ev["export_dir"]
    for f in local:
        dst = os.path.join(export_dir, f["path"])
        if os.path.exists(dst) and C.sha256_file(dst) != f["sha256"]:
            probs.append(f"{f['path']} exists in export_dir with other content (never overwritten)")
    expected = {f["path"] for f in local} | set(gen) | {f"pvc-exports/{p}/{n}" for p in PVC_EXPORTS
                                                       for n in ("campaign-export.tar", "manifest.json", "SHA256SUMS")}
    expected |= {"storage/pvc-retention-and-export-coverage.json", "S0-SHA256SUMS"}
    if expected != set(ev["mandatory"]):
        probs.append(f"what this populator produces differs from archive.evidence.mandatory: missing "
                     f"{sorted(set(ev['mandatory']) - expected)[:5]}, extra {sorted(expected - set(ev['mandatory']))[:5]}")
    p8 = s["p8"]
    p8_path = next((f for f in local if f["path"] == p8["export"]), None)
    if p8_path is None or p8_path["sha256"] != p8["sha256"]:
        probs.append("the existing p8-evidence-pvc fetch is not among the copied files with its recorded sha256")
    actions = [{"copy": f["path"], "sha256": f["sha256"], "bytes": f["bytes"]} for f in local]
    actions += [{"generate": k, "sha256": hashlib.sha256(v).hexdigest()} for k, v in sorted(gen.items())]
    actions += [{"export": p, "files": len(v["selection"]), "bytes": v["bytes"]} for p, v in pvcs.items()]
    return X.new_plan(HELPER, record, actions, export_dir=export_dir, local=local, pvcs=pvcs,
                      consumers=consumers(kube), total_pvc_bytes=total, reader_sha256=READER_SHA256,
                      problems=probs)


# --- export -----------------------------------------------------------------------------------------------------------

def _write_identical(path: str, data: bytes) -> str:
    """A new file; an existing identical one is kept; an existing different one is refused (never overwritten)."""
    if os.path.exists(path):
        if C.sha256_file(path) != hashlib.sha256(data).hexdigest():
            raise X.StagingError(f"{path} exists with other content (never overwritten)")
        return "kept"
    C.write_once(path, data)
    return "written"


def verify_tar(path: str, selection: List[Dict], trailer: Dict) -> List[str]:
    out = []
    if C.sha256_file(path) != trailer["trailer"]["sha256"] or os.path.getsize(path) != trailer["trailer"]["bytes"]:
        return ["the received tar does not match the reader's trailer"]
    want = [(it["path"], it["bytes"]) for it in selection]
    got_files = trailer["files"]
    if [(f["path"], f["bytes"]) for f in got_files] != want:
        return ["the reader's receipt is not exactly the reviewed selection"]
    with tarfile.open(path, "r:") as tar:
        members = tar.getmembers()
        if [(m.name, m.size) for m in members] != want:
            return ["the tar's members are not exactly the reviewed selection"]
        for m, f in zip(members, got_files):
            if not m.isreg() or m.name.startswith("/") or ".." in m.name.split("/"):
                out.append(f"unsafe tar member {m.name}")
                continue
            h = hashlib.sha256(tar.extractfile(m).read()).hexdigest()
            if h != f["sha256"]:
                out.append(f"{m.name}: the member's sha256 differs from the reader's receipt")
    return out


def export_pvc(record, kube, lock, s, pvc, info, exe, sleep, now, digest) -> Dict:
    d = os.path.join(evidence(record)["export_dir"], "pvc-exports", pvc)
    os.makedirs(d, exist_ok=True)
    tar_path = os.path.join(d, "campaign-export.tar")
    if os.path.exists(tar_path):
        raise X.StagingError(f"{tar_path} exists (a PVC export is produced once)")
    job = {"mode": "export", "mount": s["pvcs"][pvc]["mount"], "selection": info["selection"]}
    rc, err, tmp = _run_reader(record, kube, lock, HELPER, s, pvc, info["binding"], job, exe, sleep, now)
    try:
        trailer = json.loads(err.strip().splitlines()[-1]) if err.strip() else {}
    except ValueError:
        trailer = {}
    if rc != 0 or not trailer or trailer.get("problems"):
        os.unlink(tmp)
        raise X.StagingError(f"{pvc}: the reader failed (exit {rc}): {(trailer.get('problems') or ['no trailer'])}")
    probs = verify_tar(tmp, info["selection"], trailer)
    if probs:
        os.rename(tmp, tmp + ".rejected")
        raise X.StagingError(f"{pvc}: " + "; ".join(probs))
    os.rename(tmp, tar_path)
    tar_sha = C.sha256_file(tar_path)
    sel = {it["path"]: it for it in info["selection"]}
    manifest = {"schema": "c28-pvc-export/v1", "run_id": record.run_id, "cluster_uid": record.cluster_uid,
                "pvc": pvc, "pvc_uid": info["binding"]["uid"], "pv": info["binding"]["pv"],
                "pv_uid": info["binding"]["pv_uid"], "rules": info["rules"],
                "cutoffs": sorted({r["cutoff"] for r in info["rules"]}), "reader_sha256": READER_SHA256,
                "populator_sha256": C.sha256_file(os.path.abspath(__file__)), "plan_sha256": digest,
                "files": [{"source_path": f["path"], "member": f["path"], "bytes": f["bytes"], "sha256": f["sha256"],
                           "selection": {k: sel[f["path"]].get(k) for k in ("kind", "ts", "lines")
                                         if sel[f["path"]].get(k) is not None}} for f in trailer["files"]],
                "excluded": info["excluded"], "volume_totals_at_probe": info["volume"],
                "tar": {"file": "campaign-export.tar", "bytes": os.path.getsize(tar_path), "sha256": tar_sha},
                "note": "campaign artifacts selected by their own names or record timestamps against the cutoff; "
                        "the rest of the volume is retained but not exported"}
    mtext = (json.dumps(manifest, indent=1, sort_keys=True) + "\n").encode()
    C.assert_clean(mtext.decode())
    _write_identical(os.path.join(d, "manifest.json"), mtext)
    sums = f"{tar_sha}  campaign-export.tar\n{hashlib.sha256(mtext).hexdigest()}  manifest.json\n".encode()
    _write_identical(os.path.join(d, "SHA256SUMS"), sums)
    X.receipt(record, HELPER, "pvc-exported", {"pvc": pvc, "files": len(trailer["files"]), "tar_sha256": tar_sha,
                                               "manifest_sha256": hashlib.sha256(mtext).hexdigest()})
    return manifest


def coverage(record: X.Record, s: Dict, p: Dict, manifests: Dict[str, Dict]) -> bytes:
    inv = _inventory(record, s)
    pvs = {v["claim"]["uid"]: v for v in inv["objects"].get("persistentvolumes", []) if v.get("claim")}
    claims = []
    for r in s["retained_pvcs"]:
        pv = pvs.get(r["uid"], {})
        entry = {"claim": r["name"], "namespace": NS, "uid": r["uid"], "pv": pv.get("name"),
                 "pv_uid": pv.get("uid"), "reclaim_policy": pv.get("reclaim_policy"),
                 "storage_class": pv.get("storage_class"), "consumers_at_plan": p["consumers"].get(r["name"], []),
                 "required": (s.get("requirements") or {}).get(r["name"], "")}
        if r["name"] in manifests:
            m = manifests[r["name"]]
            entry["protected"] = {"export": f"pvc-exports/{r['name']}/campaign-export.tar",
                                  "manifest": f"pvc-exports/{r['name']}/manifest.json", "files": len(m["files"]),
                                  "bytes": sum(f["bytes"] for f in m["files"]), "tar_sha256": m["tar"]["sha256"],
                                  "cutoffs": m["cutoffs"], "verified": "every member against the reader receipt"}
            entry["residual_not_backed_up"] = {
                "description": "every other file of the volume (and any later appends) is RETAINED on the claim but "
                               "NOT backed up by S0", "volume_files_at_probe": m["volume_totals_at_probe"]["files"],
                "volume_bytes_at_probe": m["volume_totals_at_probe"]["bytes"]}
        elif r["name"] == "p8-evidence-pvc":
            entry["protected"] = {"export": s["p8"]["export"], "sha256": s["p8"]["sha256"],
                                  "verified": "the existing campaign fetch, copied byte-for-byte and hash-checked"}
            entry["residual_not_backed_up"] = {"description": "every file the fetch did not include is RETAINED "
                                                              "but NOT backed up by S0"}
        if entry["reclaim_policy"] != "Retain":
            entry["warning"] = (f"the bound PV reclaims with {entry['reclaim_policy']}: deleting the claim would "
                                "delete the residual contents; the claim must not be deleted")
        claims.append(entry)
    doc = {"schema": "c28-pvc-retention-and-export-coverage/v1", "run_id": record.run_id,
           "cluster_uid": record.cluster_uid, "claims": claims,
           "statement": "S0 protects the listed campaign artifacts by verified exports; it is not a full-volume "
                        "backup of any claim. All five claims, their PVs and data are retained (never deleted)."}
    text = json.dumps(doc, indent=1, sort_keys=True) + "\n"
    C.assert_clean(text)
    return text.encode()


def export(record: X.Record, kube: X.Kube, reviewed: Dict, digest: str, exe=None, sleep=time.sleep,
           now=time.monotonic) -> Dict:
    s = section(record)
    ev = evidence(record)
    exe = exe or Exec(record, int(s["reader"].get("exec_timeout_seconds", 1800)))
    with C.exclusive_lock(record.doc, LOCK_SCOPE, holder=HELPER) as lock:
        live = plan(record, kube)
        X.require_live(reviewed, digest, HELPER, record, live)
        if live["problems"]:
            raise X.StagingError("the export plan has problems: " + "; ".join(live["problems"]))
        root = ev["export_dir"]
        os.makedirs(root, exist_ok=True)
        X.receipt(record, HELPER, "export-start", {"plan_sha256": digest, "export_dir": root})
        for f in live["local"]:
            data = open(f["source"], "rb").read()
            if hashlib.sha256(data).hexdigest() != f["sha256"]:
                raise X.StagingError(f"{f['path']}: the source changed since the plan")
            os.makedirs(os.path.dirname(os.path.join(root, f["path"])), exist_ok=True)
            _write_identical(os.path.join(root, f["path"]), data)
            if C.sha256_file(os.path.join(root, f["path"])) != f["sha256"]:
                raise X.StagingError(f"{f['path']}: the destination copy differs")
        gen = generated_sources(record, s)
        gen["restoration/legacy-selective-restore.md"] = restoration_md(record, s)
        want = {a["generate"]: a["sha256"] for a in live["actions"] if "generate" in a}
        for k, v in gen.items():
            if hashlib.sha256(v).hexdigest() != want.get(k):
                raise X.StagingError(f"{k} differs from the reviewed plan")
            os.makedirs(os.path.dirname(os.path.join(root, k)), exist_ok=True)
            _write_identical(os.path.join(root, k), v)
        manifests = {}
        for pvc in PVC_EXPORTS:
            manifests[pvc] = export_pvc(record, kube, lock, s, pvc, live["pvcs"][pvc], exe, sleep, now, digest)
        os.makedirs(os.path.join(root, "storage"), exist_ok=True)
        _write_identical(os.path.join(root, "storage", "pvc-retention-and-export-coverage.json"),
                         coverage(record, s, live, manifests))
        sums = "".join(f"{C.sha256_file(os.path.join(root, m))}  {m}\n" for m in sorted(ev["mandatory"])
                       if m != "S0-SHA256SUMS")
        _write_identical(os.path.join(root, "S0-SHA256SUMS"), sums.encode())
        v = verify(record)
        if v["problems"]:
            raise X.StagingError("the populated export does not verify: " + "; ".join(v["problems"][:10]))
        return X.receipt(record, HELPER, "export-complete", {"plan_sha256": digest, "files": v["files"],
                                                             "s0_sha256sums": C.sha256_file(os.path.join(root,
                                                                                                         "S0-SHA256SUMS"))})


def verify(record: X.Record) -> Dict:
    """Read-only: export_dir holds exactly the mandatory files, every checksum manifest verifies at the destination,
    and every PVC export's tar matches its manifest member by member."""
    ev = evidence(record)
    root, probs = ev["export_dir"], []
    have = set()
    for d, _, names in os.walk(root):
        for n in names:
            have.add(os.path.relpath(os.path.join(d, n), root))
    missing, extra = sorted(set(ev["mandatory"]) - have), sorted(have - set(ev["mandatory"]))
    if missing:
        probs.append(f"missing {missing[:10]}{' ...' if len(missing) > 10 else ''}")
    if extra:
        probs.append(f"not mandatory {extra[:10]}")
    if not missing:
        probs += chain_problems(lambda m: os.path.join(root, m), ev.get("checksum_manifests") or [])
        for pvc in PVC_EXPORTS:
            d = os.path.join(root, "pvc-exports", pvc)
            m = json.load(open(os.path.join(d, "manifest.json")))
            files = [{"path": f["member"], "bytes": f["bytes"], "sha256": f["sha256"]} for f in m["files"]]
            probs += [f"{pvc}: {x}" for x in verify_tar(os.path.join(d, "campaign-export.tar"),
                                                        [{"path": f["path"], "bytes": f["bytes"]} for f in files],
                                                        {"trailer": {"bytes": m["tar"]["bytes"],
                                                                     "sha256": m["tar"]["sha256"]}, "files": files})]
            if not files or any(f["bytes"] <= 0 for f in files):
                probs.append(f"{pvc}: the export lists no or empty payloads")
    return {"helper": HELPER, "action": "verify", "export_dir": root, "files": len(have), "problems": probs,
            "result": "PASS" if not probs else "FAIL"}


# --- recovery ---------------------------------------------------------------------------------------------------------

def _unresolved(record: X.Record) -> List[Tuple[str, str, Dict]]:
    out = []
    for helper in (HELPER, PROBE):
        steps: Dict[str, Dict] = {}
        for e in X.receipts(record, helper):
            for pre in ("intent-", "step-", "reconciled-"):
                if e["event"].startswith(pre):
                    steps.setdefault(e["event"][len(pre):], {})[pre] = e["data"]
        for n, st in sorted(steps.items(), key=lambda kv: int(kv[0])):
            if "intent-" in st and "step-" not in st and "reconciled-" not in st:
                out.append((helper, n, st["intent-"]))
    return out


def recover_plan(record: X.Record, kube: X.Kube) -> Dict:
    actions = []
    for helper, n, it in _unresolved(record):
        live = kube.get("Pod", it["name"], NS)
        nonce_ok = live is not None and (live["metadata"].get("labels") or {}).get(NONCE_LABEL) == it["nonce"]
        if it["op"] == "create":
            observed = "done" if nonce_ok else ("not-done" if live is None else "unresolved")
        else:
            observed = "done" if live is None or live["metadata"].get("uid") != it.get("uid") else "not-done"
        actions.append({"helper": helper, "step": n, "op": it["op"], "pod": it["name"], "observed": observed,
                        "uid": (live or {}).get("metadata", {}).get("uid")})
    lp = C.lock_path(record.doc, LOCK_SCOPE)
    return X.new_plan("populate-s0-recover", record, actions, lock=lp,
                      lock_sha256=C.sha256_file(lp) if os.path.exists(lp) else "",
                      leftover_readers=leftover_readers(kube, record))


def recover(record: X.Record, kube: X.Kube, reviewed: Dict, digest: str) -> Dict:
    """recovery_guard (owner proven gone), reconciled-<n> for decided outcomes, then delete every leftover reader pod of
    this run by UID, and release the stale lock only when nothing stays unresolved."""
    live = recover_plan(record, kube)
    X.require_live(reviewed, digest, "populate-s0-recover", record, live)
    with C.recovery_guard(record.doc, LOCK_SCOPE, live["lock_sha256"]) as guard:
        C.require_cluster(kube.api, record.cluster_uid)
        bad = [a for a in live["actions"] if a["observed"] == "unresolved"]
        for a in live["actions"]:
            if a["observed"] != "unresolved":
                C.append_receipt(record.doc, a["helper"], f"reconciled-{a['step']}",
                                 {"observed": a["observed"], "uid": a["uid"]})
        if bad:
            raise X.StagingError(f"unresolved outcomes {bad}: the lock stays")
        for name in live["leftover_readers"]:
            p = kube.get("Pod", name, NS)
            if p is not None:
                kube.api.delete(X.path("Pod", NS, name), p["metadata"]["uid"])
        if leftover_readers(kube, record):
            raise X.StagingError("leftover reader pods remain: the lock stays")
        guard.release_stale()
        return X.receipt(record, HELPER, "recovered", {"plan_sha256": digest, "reconciled": len(live["actions"])})


# --- CLI --------------------------------------------------------------------------------------------------------------

def main(argv=None, api=None, exe=None) -> int:
    ap = argparse.ArgumentParser(prog="populate_s0.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", required=True)
    ap.add_argument("command", nargs="?", default="plan",
                    choices=["probe-plan", "probe", "plan", "export", "verify", "recover-plan", "recover"])
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    args = ap.parse_args(argv)
    try:
        record = X.load_record(args.record, ("populate", "archive"))
        if args.command == "verify":
            out = verify(record)
            X.emit(out)
            return 0 if out["result"] == "PASS" else 1
        kube = record.kube(api)
        if args.command in ("probe", "export", "recover") and not args.plan:
            raise X.StagingError("a mutation needs --plan <the reviewed plan file> and --plan-digest")
        if args.command == "probe-plan":
            X.emit(X.cli_plan(record, PROBE, probe_plan(record, kube), "probe-plan"))
        elif args.command == "probe":
            X.emit(probe(record, kube, X.load_plan(args.plan), args.plan_digest, exe))
        elif args.command == "plan":
            p = plan(record, kube)
            saved = X.save_plan(record, HELPER, p)
            X.emit({**saved, "problems": p["problems"], "total_pvc_bytes": p["total_pvc_bytes"],
                    "pvcs": {k: {"files": len(v["selection"]), "bytes": v["bytes"]} for k, v in p["pvcs"].items()}})
        elif args.command == "export":
            X.emit(export(record, kube, X.load_plan(args.plan), args.plan_digest, exe))
        elif args.command == "recover-plan":
            X.emit(X.cli_plan(record, "populate-s0-recover", recover_plan(record, kube), "recover-plan"))
        else:
            X.emit(recover(record, kube, X.load_plan(args.plan), args.plan_digest))
    except X.StagingError as e:
        print(f"populate_s0: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
