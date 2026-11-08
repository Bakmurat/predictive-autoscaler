#!/usr/bin/env python3
"""S0 export populator: fill archive.evidence.export_dir with every mandatory artifact (STAGING-C28-PROD.md S0; the
S0 record proposal's content_sources, pvc_content_requirements and export_contract).

  populate_s0.py --record R probe-plan                        read-only: what the probe would run
  populate_s0.py --record R probe --plan F --plan-digest D    read-only reader pods: sizes, selections and digests
  populate_s0.py --record R [plan]                            read-only: the export plan (from the probe receipts)
  populate_s0.py --record R export --plan F --plan-digest D   populate export_dir exactly as reviewed
  populate_s0.py --record R verify                            read-only: export_dir == mandatory, every chain verifies
  populate_s0.py --record R recover-plan | recover --plan F --plan-digest D   finish an unresolved reader lifecycle

What it produces, beneath export_dir (never overwriting a different file; an identical one is kept):
  * campaign/, protocol/, inventory/: the allowlisted files copied byte-for-byte from content_sources, each secret-
    scanned (a hit blocks; no value is printed) and hashed at source and destination; every checksum manifest's chain
    is verified at the source base and again at the destination.
  * source/legacy-source-manifest.json, source/helper-source-manifest.json, source/terminations.json (provenance from
    git, run without any ambient GIT_* environment), restoration/legacy-selective-restore.md.
  * pvc-exports/<pvc>/campaign-export.tar, manifest.json, SHA256SUMS for forecast-log-pvc, evidence-archive-pvc,
    load-evidence-pvc and ml-models-pvc.
  * storage/pvc-retention-and-export-coverage.json, then S0-SHA256SUMS over every mandatory file except itself.

PVC selection is BYTE-BOUND: the probe records every selected file's (or prefix's) sha256, and the export streams
exactly those bytes; a member whose sha256 differs from the probe's is refused. The probe also verifies the ORIGINAL
hashes the campaign writers recorded (an archive bundle's artifact_sha256/sidecar_sha256, a model sidecar's
artifact_sha256/artifact_bytes, a trainer log's log_sha256); transport hashes never replace them. Every file of a
claim is either selected, excluded by a rule with its reason, or matched by a reviewed exclusion
(populate.pvcs.<pvc>.excluded_reviewed); anything else blocks the plan. A model pair is exported only when the
evidence archive selection holds its immutable archived copy (same artifact and sidecar hashes).

Rules (populate.pvcs.<pvc>.rules; cutoff "campaign" = populate.cutoff, "collection" = populate.collection_cutoff for
the collectors' own post-stop files, or an explicit UTC time):
  {"type": "jsonl_prefix", "path": P, "ts_fields": [...] | "ts_by": {"field": F, "types": {T: [...]}},
   "known_keys": [...], "ref_classes": {...}, "order_tolerance_seconds": N, "order_tolerance_review": "..."}
        the prefix of an append-only JSONL file up to its LAST record at or before the cutoff; a record's time is the
        first present field of ts_fields, or of its own type's list (types keyed by the discriminator F; "" = without
        F). An unknown type or a record without a usable time fails the rule and is reported by its SHAPE only: the
        ALLOWLISTED key names it has (known_keys, the timestamp fields, F), a count and digest of any other keys, and
        F only when it is a known type (else its JSON type and a digest). Ordering is STRICT by default; a tolerance
        for interleaved writers is a per-rule exception that needs order_tolerance_review, and the plan and manifest
        list it (order_tolerances); its overhang lines are counted apart (overshoot_lines), never campaign records.
        ref_classes {field, class_by, artifact_prefixes, fingerprint_prefixes} sorts the field's sha256 values in the
        records up to the cutoff by the record's class_by (model_name): persisted artifacts vs generation fingerprints;
        a value whose record lacks class_by must match a classified one, any other class fails
  {"type": "named", "glob": G, "ts_regex": R, "ts_format": F}  files whose NAME carries a time at or before the cutoff
  {"type": "bundles", "manifest_glob": G, "ts_field": T}       archive bundles by their manifest's T, every original
        hash verified; a sidecar variant's artifact_key must name a selected bundle
  {"type": "pairs", "sidecar_glob": G, "artifact_from": S, "artifact_to": A, "ts_field": T, "sha_field": H}
        model sidecar + artifact; EVERY pair is verified first (the artifact's sha256 and size equal the sidecar's, else
        it is excluded as not matching), and only then is its time used
  {"type": "trainer_logs", "index": I, "dir": D, "reviewed": [{"job", "pod_uid", "decision", "reason"}]}
        every trainer record of index I; EACH log classified by its own container's EXECUTION times (never
        collected_at) against the cutoff: finished at or before it = campaign evidence, selected (log_sha256
        verified) even when collected later; started after it = excluded as "post-campaign training (executed after
        the cutoff)"; a .previous.log is selected when the current instance started at or before the cutoff and is
        otherwise ambiguous (the writer records no previous-instance times). Ambiguous logs block unless the record is
        reviewed (decision select | exclude). <job>-<pod uid[:8]>.json goes with its logs. Unreferenced files block
A reviewed "empty_by_design" reason is accepted for ml-models-pvc ONLY, and only when nothing is selected, nothing lies
outside its rules and every model pair present is VERIFIED and was trained after the cutoff (its trained_at), and
the reviewed campaign artifact inventory (populate.pvcs.ml-models-pvc.campaign_artifacts: [{artifact_sha256,
reason}]) covers every artifact the campaign forecast records and training bundles (up to the cutoff) reference,
each resolving to verified evidence-archive members (bundle manifest, artifact, sidecar). The export is then an
empty tar whose manifest, like the coverage file, carries those member references (with the evidence-archive export's
tar and manifest digests) and the current pairs' hashes and byte counts; verify re-resolves them.

Reader pods (namespace ml-engine): the claim mounted read-only, no service-account token, non-root, read-only root
filesystem, pinned to the node the RWO volume is attached to (VolumeAttachment), a deadline. A reviewed plan binds
only the claim's storage IDENTITY (UID, PV, reclaim policy, class, access modes); the LIVE preconditions are
re-validated before EACH reader and recorded in its reader-preconditions receipt, never digested: no active
load-evidence, ml-training, evidence-archive or p8 Job (a query error refuses), every pod using the claim is an
allowlisted writer kind (WRITER_KINDS: the campaign CronJobs' Job pods, the ml-api / predictive-operator ReplicaSet
pods), and the node the volume is attached to now. The whole lifecycle of a reader (create, work,
delete, proof) is ONE common_records.mutation_step: its intent is durable before the pod is created, a delete intent
is durable before every delete, and the outcome is written only once cleanup is PROVEN (no pod of the run's nonce
remains and a volume that was not attached before is released). An unproven cleanup leaves the intent unresolved and
the lock held until `recover` proves it. The lock is the legacy scope's (common_records.exclusive_lock(rec,
"legacy")): S0 readers and legacy quiesce/restore exclude each other atomically, and recovery here never releases the
lock while a legacy intent is unresolved.

The record's "populate" section:
  {"cutoff": "2026-11-02T00:00:00Z", "collection_cutoff": "2026-11-02T08:44:00Z",
   "content_sources": {"campaign/": DIR, "protocol/": DIR, "inventory/": DIR},
   "legacy_source": {"repo_dir": DIR, "commit": "0472829", "paths": [...], "operator_image": "<repo>@sha256:..."},
   "terminations": {"repo_dir": DIR, "commit": REV, "path": "deploy/prodcluster/load-gate/terminations.json"},
   "helper_source": {"repo_dir": DIR, "commit": REV, "paths": [...]},
   "inventory": "<inventory file name>", "retained_pvcs": [{"name", "uid"} x5],
   "p8": {"export": "campaign/final-20261102/p8-fetch-final-20261102.jsonl", "sha256": HEX},
   "requirements": {"<pvc>": "..."}, "reader": {"image": "<repo>@sha256:...", "deadline_seconds": N,
   "exec_timeout_seconds": N, "read_access": "nonroot" | "dac-read-search", "supplemental_groups": [GID]},
   "budget": {"max_total_bytes": N},
   "pvcs": {"<pvc>": {"uid": UID, "mount": "/data", "max_bytes": N, "rules": "default" | [...],
                      "excluded_reviewed": "default" | [{"glob": G, "reason": "..."}],
                      "empty_by_design": "<reason>" (ml-models-pvc only)}}}
"""

from __future__ import annotations

import argparse
import calendar
import copy
import datetime
import fnmatch
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import tarfile
import time
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common_records as C  # noqa: E402
import execlib as X  # noqa: E402
import legacy_allowlist as L  # noqa: E402

HELPER = "populate-s0"
PROBE = "populate-s0-probe"
LOCK_SCOPE = "legacy"                     # the legacy scope's ONE lock: S0 readers and legacy mutations exclude
LEGACY_HELPERS = ("legacy", "legacy-restore")
NS = "ml-engine"
READER_LABEL = "c28.devkuban.com/s0-reader"
NONCE_LABEL = "c28.devkuban.com/s0-nonce"
PVC_EXPORTS = ("forecast-log-pvc", "evidence-archive-pvc", "load-evidence-pvc", "ml-models-pvc")
LOCAL_TOPS = ("campaign/", "protocol/", "inventory/")
BUSY_JOB_PREFIXES = ("load-evidence", "ml-training", "evidence-archive", "p8")
_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}$")
_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_RFC3339 = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$")


def ts_ns(s) -> Optional[int]:
    """The host twin of the reader's utc(): an RFC 3339 / RFC3339Nano time as integer nanoseconds since the epoch,
    or None. Host-side cutoff checks compare these integers, never strings."""
    m = _RFC3339.match(s) if isinstance(s, str) else None
    if not m:
        return None
    y, mo, d, h, mi, se, frac, tz = m.groups()
    try:
        t = datetime.datetime(int(y), int(mo), int(d), int(h), int(mi), int(se), tzinfo=datetime.timezone.utc)
    except ValueError:
        return None
    off = 0
    if tz != "Z":
        if int(tz[1:3]) > 23 or int(tz[4:6]) > 59:
            return None
        off = (int(tz[1:3]) * 3600 + int(tz[4:6]) * 60) * (1 if tz[0] == "+" else -1)
    return (calendar.timegm(t.utctimetuple()) - off) * 10 ** 9 + int((frac or "").ljust(9, "0"))
_COMMITISH = re.compile(r"^[0-9a-f]{7,40}$")

# --- the in-pod reader (stdlib only; read-only; the job on stdin) ----------------------------------------------------

READER = r'''
import datetime, glob, hashlib, io, json, os, re, sys, tarfile

RFC3339 = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$")
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
NS = 10 ** 9

def utc(s):
    """An RFC 3339 / Go time.RFC3339Nano time (fractional seconds of 1-9 digits; Z or a +-hh:mm offset) as INTEGER
    NANOSECONDS since the epoch (UTC), or None for anything else. Every comparison uses these integers; nothing is
    rounded or truncated."""
    m = RFC3339.match(s.strip()) if isinstance(s, str) else None
    if not m:
        return None
    y, mo, d, h, mi, se, frac, tz = m.groups()
    try:
        t = datetime.datetime(int(y), int(mo), int(d), int(h), int(mi), int(se), tzinfo=datetime.timezone.utc)
    except ValueError:
        return None
    off = 0
    if tz != "Z":
        oh, om = int(tz[1:3]), int(tz[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 3600 + om * 60) * (1 if tz[0] == "+" else -1)
    secs = (t - EPOCH).days * 86400 + (t - EPOCH).seconds - off
    return secs * NS + int((frac or "").ljust(9, "0"))

def iso(t):
    """Integer nanoseconds as RFC 3339 UTC with the FULL fraction (trailing zeros dropped; none when whole)."""
    sec, frac = divmod(t, NS)
    out = (EPOCH + datetime.timedelta(seconds=sec)).strftime("%Y-%m-%dT%H:%M:%S")
    return out + (("." + ("%09d" % frac).rstrip("0")) if frac else "") + "Z"

def category(e):
    """A fixed category for an exception: never its message (paths, contents or values stay inside the pod)."""
    for cls, cat in ((PermissionError, "permission"), (FileNotFoundError, "missing"), (NotADirectoryError, "missing"),
                     (IsADirectoryError, "not-a-file"), (UnicodeDecodeError, "decode"), (ValueError, "malformed"),
                     (MemoryError, "memory"), (OSError, "io")):
        if isinstance(e, cls):
            return cat
    return "internal"

def sanitized(e):
    line, tb = 0, e.__traceback__
    while tb is not None:
        if tb.tb_frame.f_code.co_filename == "reader":
            line = tb.tb_lineno
        tb = tb.tb_next
    return {"class": type(e).__name__, "line": line, "category": category(e)}

def absent_why(p):
    """None when p is a regular file; otherwise why not: missing or permission (an unsearchable directory)."""
    try:
        os.stat(p)
    except PermissionError:
        return "not readable (permission)"
    except OSError:
        return "missing"
    return None if os.path.isfile(p) else "not a regular file"

def safe(mount, rel):
    p = os.path.normpath(os.path.join(mount, rel))
    if not p.startswith(mount.rstrip("/") + "/") or os.path.islink(p):
        raise ValueError("unsafe path " + rel)
    return p

def files_under(mount, pattern):
    return [os.path.relpath(p, mount) for p in sorted(glob.glob(os.path.join(mount, pattern)))
            if os.path.isfile(p) and not os.path.islink(p)]

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

def item(mount, rel, kind="file", limit=None, **extra):
    h, n = sha(os.path.join(mount, rel), limit)
    d = {"path": rel, "kind": kind, "bytes": n, "sha256": h}
    d.update(extra)
    return d

def digest12(v):
    return hashlib.sha256(json.dumps(v, sort_keys=True).encode()).hexdigest()[:12]

def shape(rec, rule):
    """A record's SHAPE for a diagnosis, disclosing only ALLOWLISTED names: the known key names it has (the rule's
    known_keys and timestamp fields), the count and a digest of any other keys, and its discriminator only when it is
    a known type (otherwise its JSON type and a digest). Never a value, never an unknown name."""
    if not isinstance(rec, dict):
        return "not a JSON object (%s)" % type(rec).__name__
    by = rule.get("ts_by") or {}
    field = by.get("field", "event")
    known = set(rule.get("known_keys") or []) | set(rule.get("ts_fields") or []) | {field}
    known |= {f for fs in (by.get("types") or {}).values() for f in fs}
    have = sorted(k for k in rec if k in known)
    other = sorted(k for k in rec if k not in known)
    out = "keys %s" % have + (", %d other keys (sha256 %s)" % (len(other), digest12(other)) if other else "")
    if field not in rec:
        ev = "<absent>"
    elif isinstance(rec[field], str) and rec[field] in (by.get("types") or {}):
        ev = rec[field]
    else:
        ev = "unknown (%s, sha256 %s)" % (type(rec[field]).__name__, digest12(rec[field]))
    return "%s, %s %s" % (out, field, ev)

def record_ts(rec, rule):
    """(timestamp, None) or (None, why): per-record-type fields when the rule has ts_by, else ts_fields."""
    if not isinstance(rec, dict):
        return None, "not a JSON object"
    by = rule.get("ts_by")
    if by:
        kind = rec.get(by["field"], "")
        fields = by["types"].get(kind) if isinstance(kind, str) else None
        if fields is None:
            return None, "an unknown record type"
    else:
        fields = rule["ts_fields"]
    for f in fields:
        t = utc(rec.get(f))
        if t:
            return t, None
    return None, "no timestamp for its type (%s)" % fields

HEX64 = re.compile(r"^[0-9a-f]{64}$")

NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")

def names_ok(*vs):
    return all(isinstance(v, str) and NAME.match(v) for v in vs)

def classify_ref(rec, rc, out):
    """One forecast-log record's digest: an issuance (it has class_by, the model_name) is an ARTIFACT (its identity
    model_name/application/namespace kept) or an ensemble GENERATION (model_name, application, namespace, experiment =
    model_version before "@", boundary = training_cutoff, fingerprint; model_version must end in @fingerprint[:12]);
    a record without class_by (a decision) must repeat the digest of an issuance OF ITS OWN WORKLOAD
    (application, namespace, forecast_issued_at). Returns why it fails, or None."""
    v = rec.get(rc["field"])
    if v in (None, ""):
        return None
    if not (isinstance(v, str) and HEX64.match(v)):
        return "has a %s that is not a sha256" % rc["field"]
    app, ns = rec.get("application"), rec.get("namespace")
    if rc["class_by"] not in rec:
        if out["issuances"].get((app, ns, utc(rec.get("forecast_issued_at")))) != v:
            return "repeats no earlier issuance of its own workload with that %s" % rc["field"]
        return None
    name = rec.get(rc["class_by"])
    issued = utc(rec.get("issued_at"))
    if not names_ok(name, app, ns) or issued is None:
        return "is an issuance without a valid model_name, application, namespace and issued_at"
    if (app, ns, issued) in out["issuances"] and out["issuances"][(app, ns, issued)] != v:
        return "conflicts with an earlier issuance of its workload at the same issued_at"
    out["issuances"][(app, ns, issued)] = v
    if name.startswith(tuple(rc["artifact_prefixes"])):
        out["artifacts"].add(v)
        out["artifact_ids"].add((name, app, ns))
        return None
    if name.startswith(tuple(rc["fingerprint_prefixes"])):
        mv, b = rec.get("model_version"), utc(rec.get("training_cutoff"))
        if not (isinstance(mv, str) and mv.endswith("@" + v[:12]) and names_ok(mv.split("@")[0]) and b is not None):
            return "is a generation issuance whose model_version / training_cutoff do not bind its fingerprint"
        out["generations"].add((name, app, ns, mv.split("@")[0], iso(b), v))
        return None
    return "carries a %s but its %s is of no declared class" % (rc["field"], rc["class_by"])

def archived_generation(rec, out):
    """An archived ENSEMBLE_ISSUANCE record (archive.py: application, namespace, boundary, fingerprint; the raw line is
    bound by line_sha256): kept as (application, namespace, boundary, fingerprint). A parse_error record counts as
    unparsed."""
    if "parse_error" in rec:
        out["unparsed"] += 1
        return None
    fp, b = rec.get("fingerprint"), utc(rec.get("boundary"))
    if not (isinstance(fp, str) and HEX64.match(fp) and b is not None
            and names_ok(rec.get("application"), rec.get("namespace"))):
        return "is not an ENSEMBLE_ISSUANCE record with application, namespace, boundary and fingerprint"
    out["ensemble_lines"].add((rec["application"], rec["namespace"], iso(b), fp))
    return None

def scan(mount, rel, rule, cutoff, collect=False):
    """The prefix of an append-only JSONL file that ends at its LAST record at or before the cutoff. Records inside
    it but after the cutoff (interleaved writers) are allowed only within order_tolerance_seconds; a record at or
    before the cutoff that follows a later record by more is not append-ordered and fails. A record without a usable
    timestamp fails, reported by its shape only. Returns {offset, lines, overshoot, after, records?} and problems."""
    tol = int(rule.get("order_tolerance_seconds", 0)) * NS                            # strict unless reviewed
    out = {"offset": 0, "lines": 0, "overshoot": 0, "after": 0, "records": [], "after_records": [],
           "artifacts": set(), "artifact_ids": set(), "generations": set(), "issuances": {},
           "ensemble_lines": set(), "unparsed": 0}
    rc = rule.get("ref_classes")
    pend_n, pend_max, pend_recs, pend_bytes, probs = 0, None, [], 0, []
    with open(safe(mount, rel), "rb") as fh:
        for n, raw in enumerate(fh, 1):
            if not raw.endswith(b"\n"):
                break                                  # a partially written last line is never exported
            try:
                rec = json.loads(raw)
            except ValueError:
                probs.append("malformed JSON at line %d of %s" % (n, rel)); break
            ts, why = record_ts(rec, rule)
            if ts is None:
                probs.append("a record of %s at line %d has %s: %s" % (rel, n, why, shape(rec, rule))); break
            if ts <= cutoff:
                if pend_n and pend_max > cutoff + tol:
                    probs.append("%s: a record at or before the cutoff (line %d) follows a record more than %d s "
                                 "after it (not append-ordered)" % (rel, n, tol // NS)); break
                why = classify_ref(rec, rc, out) if rc else None
                if why is None and rule.get("collect_generations"):
                    why = archived_generation(rec, out)
                if why:
                    probs.append("a record of %s at line %d %s: %s" % (rel, n, why, shape(rec, rule))); break
                out["offset"] += pend_bytes + len(raw)
                out["lines"] += pend_n + 1
                out["overshoot"] += pend_n
                if collect:
                    out["after_records"] += pend_recs
                    out["records"].append((rec, ts))
                pend_n, pend_max, pend_recs, pend_bytes = 0, None, [], 0
            else:
                pend_n += 1; pend_bytes += len(raw)
                pend_max = ts if pend_max is None or ts > pend_max else pend_max
                if collect:
                    pend_recs.append((rec, ts))
    out["after"] = pend_n
    if collect:
        out["after_records"] += pend_recs
    return out, probs

def jsonl_prefix(mount, rule, cutoff):
    why = absent_why(safe(mount, rule["path"]))
    if why:
        return [], [], ["the file " + rule["path"] + " is " + why]
    res, probs = scan(mount, rule["path"], rule, cutoff)
    if not res["lines"] and not probs:
        probs.append("no record of " + rule["path"] + " is at or before the cutoff")
    if probs:
        return [], [], probs
    extra = {"overshoot_lines": res["overshoot"]}
    if rule.get("ref_classes"):
        extra["referenced_artifact_sha256"] = sorted(res["artifacts"])
        extra["artifact_identities"] = [list(x) for x in sorted(res["artifact_ids"])]
        extra["referenced_generations"] = [dict(zip(("model_name", "application", "namespace", "experiment",
                                                     "boundary", "fingerprint"), g)) for g in sorted(res["generations"])]
    if rule.get("collect_generations"):
        extra["archived_generations"] = [dict(zip(("application", "namespace", "boundary", "fingerprint"), g))
                                         for g in sorted(res["ensemble_lines"])]
        extra["unparsed_lines"] = res["unparsed"]
    if res["overshoot"]:
        extra["overshoot_note"] = ("lines after the cutoff inside the prefix under the reviewed order tolerance: an "
                                   "overhang of interleaved writers, not campaign records")
    return [item(mount, rule["path"], "prefix", res["offset"], lines=res["lines"], lines_after_cutoff=res["after"],
                 **extra)], [], []

def named(mount, rule, cutoff):
    sel, excl, rx = [], [], re.compile(rule["ts_regex"])
    for rel in files_under(mount, rule["glob"]):
        m = rx.search(os.path.basename(rel))
        try:
            dt = datetime.datetime.strptime(m.group(1), rule["ts_format"]).replace(tzinfo=datetime.timezone.utc)
            ts = utc(dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
        except (AttributeError, ValueError):
            excl.append({"path": rel, "reason": "no time in its name"}); continue
        if ts > cutoff:
            excl.append({"path": rel, "reason": "named after the cutoff"})
        elif os.path.getsize(os.path.join(mount, rel)) == 0:
            excl.append({"path": rel, "reason": "empty"})
        else:
            sel.append(item(mount, rel, ts=iso(ts)))
    return sel, excl, []

def bundles(mount, rule, cutoff):
    sel, excl, probs = [], [], []
    for rel in files_under(mount, rule["manifest_glob"]):
        d = os.path.dirname(rel)
        members = files_under(mount, os.path.join(d, "*"))
        try:
            man = json.load(open(os.path.join(mount, rel)))
        except ValueError:
            probs.append("unreadable manifest " + rel); continue
        ts = utc(man.get(rule["ts_field"])) if isinstance(man, dict) else None
        if ts is None or ts > cutoff:
            excl += [{"path": f, "reason": "bundle without %s" % rule["ts_field"] if ts is None
                      else "bundle copied after the cutoff"} for f in members]
            continue
        art = os.path.join(d, man.get("artifact") or "")
        sides = [f for f in members if f not in (rel, art)]
        ok = len(sides) == 1
        if os.path.isfile(os.path.join(mount, art)):
            ok = ok and sha(os.path.join(mount, art))[0] == man.get("artifact_sha256")
        elif not man.get("artifact_key"):
            ok = False                                 # neither the artifact nor a variant reference
        if ok:
            ok = sha(os.path.join(mount, sides[0]))[0] == man.get("sidecar_sha256")
            try:
                ok = ok and json.load(open(os.path.join(mount, sides[0]))).get("artifact_sha256") == man.get("artifact_sha256")
            except ValueError:
                ok = False
        if not ok:
            probs.append("bundle %s: its files do not match the manifest's original hashes" % d); continue
        ref = {"bundle": man.get("key"), "artifact_sha256": man.get("artifact_sha256"),
               "sidecar_sha256": man.get("sidecar_sha256"), "artifact_key": man.get("artifact_key"),
               "verified": "artifact_sha256/sidecar_sha256 of the bundle manifest"}
        for f in members:
            if os.path.getsize(os.path.join(mount, f)) == 0:
                excl.append({"path": f, "reason": "empty"})
            else:
                sel.append(item(mount, f, ts=iso(ts), **(ref if f == rel else {"bundle": man.get("key")})))
    return sel, excl, probs

def pairs(mount, rule, cutoff):
    sel, excl = [], []
    for rel in files_under(mount, rule["sidecar_glob"]):
        art = rel[: -len(rule["artifact_from"])] + rule["artifact_to"] if rel.endswith(rule["artifact_from"]) else None
        both = [rel] + ([art] if art and os.path.isfile(os.path.join(mount, art)) else [])
        try:
            meta = json.load(open(os.path.join(mount, rel)))
        except ValueError:
            meta = {}
        ts = utc(meta.get(rule["ts_field"])) if isinstance(meta, dict) else None
        # every pair is verified (artifact sha256 and size against its sidecar) BEFORE its timestamp is used
        if len(both) < 2:
            reason = "sidecar without its artifact"
        else:
            h, n = sha(os.path.join(mount, art))
            if h != meta.get(rule["sha_field"]) or meta.get("artifact_bytes") not in (None, n):
                reason = "the artifact does not match its sidecar (in progress or rewritten)"
            elif ts is None:
                reason = "sidecar without " + rule["ts_field"]
            elif ts > cutoff:
                reason = "trained after the cutoff"
            else:
                reason = None
        if reason == "trained after the cutoff":
            sh, sn = sha(os.path.join(mount, rel))
            excl.append({"path": rel, "reason": reason, "ts": iso(ts), "pair_verified": True,
                         "pair": {"sidecar": rel, "sidecar_bytes": sn, "sidecar_sha256": sh, "artifact": art,
                                  "artifact_bytes": n, "artifact_sha256": h,
                                  "sidecar_artifact_sha256": meta.get(rule["sha_field"]),
                                  "sidecar_artifact_bytes": meta.get("artifact_bytes")}})
            excl.append({"path": art, "reason": reason, "ts": iso(ts), "pair_verified": True, "pair_sidecar": rel})
            continue
        if reason:
            excl += [{"path": f, "reason": reason} for f in both]
            continue
        sel.append(item(mount, rel, ts=iso(ts), artifact_sha256=meta.get(rule["sha_field"])))
        sel.append(item(mount, art, ts=iso(ts), verified="equals the sidecar's " + rule["sha_field"]))
    return sel, excl, []

FAR_FUTURE = utc("9999-01-01T00:00:00Z")

def log_class(started, finished, cutoff):
    """One log's class by ITS OWN execution times: select (finished at or before the cutoff), exclude (started after
    it), or the reason it is ambiguous."""
    if started is None or finished is None:
        return "its execution times are missing"
    if finished <= cutoff:
        return "select"
    if started > cutoff:
        return "exclude"
    return "its execution straddles the cutoff"

def trainer_logs(mount, rule, cutoff):
    """Trainer logs classified by EXECUTION time against the (campaign) cutoff, whatever their collected_at; each log
    by its OWN container's times: a container's current log by its started_at/finished_at; its .previous.log (the
    instance before a restart, whose times the writer does not record) is campaign evidence when the current instance
    started at or before the cutoff (the previous one ended before that), otherwise ambiguous. A record's .json goes
    with its logs (selected when any log is). Ambiguous logs block unless the record is reviewed (rule "reviewed":
    decision select | exclude); a file no record references blocks."""
    sel, excl = [], []
    why = absent_why(safe(mount, rule["index"]))
    if why:
        return [], [], ["the trainer index " + rule["index"] + " is " + why]
    res, probs = scan(mount, rule["index"], {"ts_fields": ["collected_at"]}, FAR_FUTURE, collect=True)
    if probs:
        return [], [], probs
    reviewed = {(r["job"], r["pod_uid"]): r for r in rule.get("reviewed") or []}
    used = set()
    for rec, collected in res["records"]:
        base = "%s-%s" % (rec.get("job"), str(rec.get("pod_uid", ""))[:8])
        key = (rec.get("job"), rec.get("pod_uid"))
        cs = [c for c in rec.get("containers") or [] if isinstance(c, dict)]
        logs = []                                    # (path, log_sha256 or None, class, extra)
        for c in cs:
            st, fi = utc(c.get("started_at")), utc(c.get("finished_at"))
            own = {"container": c.get("name"), "started_at": iso(st) if st else None,
                   "finished_at": iso(fi) if fi else None}
            logs.append((os.path.join(rule["dir"], "%s.%s.log" % (base, c.get("name"))), c.get("log_sha256"),
                         log_class(st, fi, cutoff), own))
            if c.get("previous_log_saved"):
                prev = {"container": c.get("name"), "instance": "previous",
                        "execution": "ended before the current instance started at %s" % (iso(st) if st else "?")}
                logs.append((os.path.join(rule["dir"], "%s.%s.previous.log" % (base, c.get("name"))), None,
                             "select" if st is not None and st <= cutoff
                             else "its previous instance's execution times are not recorded", prev))
        if not cs:
            logs.append((None, None, "its execution times are missing", {}))
        ambiguous = [x for x in logs if x[2] not in ("select", "exclude")]
        if ambiguous and key not in reviewed:
            for f, _, why, _ in ambiguous:
                probs.append("trainer record %s: %s%s; needs an explicit review" % (
                    base, (os.path.basename(f) + ": ") if f else "", why))
            continue
        if not ambiguous and key in reviewed:
            probs.append("trainer record %s is reviewed but its execution is unambiguous" % base); continue
        rv = None
        if ambiguous:
            used.add(key)
            rv = reviewed[key]
        resolved = [(f, want, rv["decision"] if c not in ("select", "exclude") else c,
                     dict(extra, reviewed="reviewed: " + rv["reason"]) if c not in ("select", "exclude") else extra)
                    for f, want, c, extra in logs if f]
        meta = os.path.join(rule["dir"], base + ".json")
        keep = [x for x in resolved if x[2] == "select"]
        late = {"collected_after_cutoff": True} if collected > cutoff else {}
        for f, _, c, extra in resolved:
            if c == "exclude" and absent_why(safe(mount, f)) is None:
                excl.append(dict({"path": f, "reason": extra.get("reviewed")
                                  or "post-campaign training (executed after the cutoff)"}, **extra))
        if not keep:
            if absent_why(safe(mount, meta)) is None:
                excl.append({"path": meta, "reason": rv and "reviewed: " + rv["reason"]
                             or "post-campaign training (executed after the cutoff)"})
            continue
        refs = [(meta, None, {"describes": [os.path.basename(f) for f, _, _, _ in keep]})] + \
               [(f, want, extra) for f, want, _, extra in keep]
        for f, want, extra in refs:
            p = safe(mount, f)
            why = absent_why(p)
            if why:
                probs.append("trainer record %s: %s is %s" % (base, f, why)); continue
            h, n = sha(p)
            if want is not None and h != want:
                probs.append("trainer record %s: %s does not match its log_sha256" % (base, f)); continue
            if n == 0:
                excl.append({"path": f, "reason": "empty (hash verified)"})
            else:
                sel.append(item(mount, f, ts=extra.get("finished_at"), trainer_record=base, **dict(extra, **late),
                                **({"verified": "equals the index's log_sha256"} if want else {})))
    for key in set(reviewed) - used:
        if not any((r.get("job"), r.get("pod_uid")) == key for r, _ in res["records"]):
            probs.append("a reviewed trainer record (job %s) is not in the index" % key[0])
    return sel, excl, probs

RULES = {"jsonl_prefix": jsonl_prefix, "named": named, "bundles": bundles, "pairs": pairs,
         "trainer_logs": trainer_logs}

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
    job = globals().get("JOB") or json.load(sys.stdin)
    mount, mode = job["mount"], job["mode"]
    if mode == "probe":
        out = {"rules": [], "volume": {"files": 0, "bytes": 0}, "unconsidered": []}
        considered = set()
        for rule in job["rules"]:
            try:
                sel, excl, probs = RULES[rule["type"]](mount, rule, utc(rule["cutoff"]))
            except Exception as e:
                x = sanitized(e)
                sel, excl, probs = [], [], ["%(class)s (%(category)s) at reader line %(line)d" % x]
            if not sel and not probs:
                probs.append("the rule selects nothing")
            considered |= {s["path"] for s in sel} | {x["path"] for x in excl}
            out["rules"].append({"rule": rule, "selected": sel, "excluded": excl, "problems": probs})
        unreadable = []
        for d, _, names in os.walk(mount, onerror=lambda e: unreadable.append(category(e))):
            for n in names:
                p = os.path.join(d, n)
                if os.path.isfile(p) and not os.path.islink(p):
                    rel, size = os.path.relpath(p, mount), os.path.getsize(p)
                    out["volume"]["files"] += 1; out["volume"]["bytes"] += size
                    if rel not in considered:
                        out["unconsidered"].append({"path": rel, "bytes": size})
        out["unreadable_directories"] = {"count": len(unreadable), "categories": sorted(set(unreadable))}
        sys.stdout.write(json.dumps(out, sort_keys=True))
        return 0
    out = Hashing(sys.stdout.buffer)
    files, probs = [], []
    with tarfile.open(fileobj=out, mode="w|", format=tarfile.PAX_FORMAT) as tar:
        for it in job["selection"]:
            p = safe(mount, it["path"])
            size = os.path.getsize(p)
            if (it["kind"] == "file" and size != it["bytes"]) or (it["kind"] == "prefix" and size < it["bytes"]):
                probs.append(it["path"] + " changed since the probe"); break
            h = hashlib.sha256()
            info = tarfile.TarInfo(it["path"])
            info.size, info.mode, info.mtime = it["bytes"], 0o444, 0
            with open(p, "rb") as fh:
                tar.addfile(info, Limited(fh, it["bytes"], h))
            if h.hexdigest() != it["sha256"]:
                probs.append(it["path"] + ": its bytes differ from the probe's sha256"); break
            files.append({"path": it["path"], "bytes": it["bytes"], "sha256": h.hexdigest()})
    out.flush()
    sys.stderr.write(json.dumps({"trailer": {"bytes": out.n, "sha256": out.h.hexdigest()}, "files": files,
                                 "problems": probs}) + "\n")
    return 1 if probs else 0

def run():
    try:
        return main()
    except Exception as e:      # a sanitized failure, never a traceback with paths or values
        err = json.dumps({"reader_error": sanitized(e)})
        stream = sys.stderr if (globals().get("JOB") or {}).get("mode") == "export" else sys.stdout
        stream.write(err + "\n")
        return 3

sys.exit(run())
'''
READER_SHA256 = hashlib.sha256(READER.encode()).hexdigest()
# The exec command line stays short: kubectl puts the command in the request URL, and a proxy in front of the API
# (Rancher's ingress) refuses a long one (the 12 KB script made a 23 KB URL). The script travels on stdin and runs
# only if it hashes to the reviewed READER_SHA256 given on the command line.
BOOT = ("import hashlib,json,sys\n"
        "d=json.loads(sys.stdin.readline())\n"
        "j=json.dumps(d['job'],sort_keys=True,separators=(',',':'))\n"
        "if hashlib.sha256(d['script'].encode()).hexdigest()!=sys.argv[1] or "
        "hashlib.sha256(j.encode()).hexdigest()!=sys.argv[2]:\n"
        " sys.stdout.write(json.dumps({'reader_error':{'class':'DigestMismatch','line':0,'category':'integrity'}}))\n"
        " sys.exit(3)\n"
        "exec(compile(d['script'],'reader','exec'),{'__name__':'reader','JOB':json.loads(j)})\n")
READ_ACCESS = ("nonroot", "dac-read-search")
READER_LINES = READER.count("\n") + 1
# What a reader error may say (anything else is reported as one fixed "unclassified" error, never echoed).
ERROR_CATEGORIES = ("permission", "missing", "not-a-file", "decode", "malformed", "memory", "io", "internal", "integrity")
ERROR_CLASSES = ("PermissionError", "FileNotFoundError", "NotADirectoryError", "IsADirectoryError",
                 "UnicodeDecodeError", "JSONDecodeError", "ValueError", "MemoryError", "OSError", "KeyError",
                 "TypeError", "IndexError", "AttributeError", "RuntimeError", "AssertionError", "TarError",
                 "StreamError", "DigestMismatch", "Exception")


def job_digest(job: Dict) -> str:
    """The sha256 of the canonical job JSON: the mode, mount, rules and selection the reviewed plan authorised."""
    return hashlib.sha256(json.dumps(job, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def exec_argv(pod: str, job: Dict) -> List[str]:
    """The short exec command line: the bootstrap, the reviewed script's digest and the job's digest (the bootstrap
    refuses a script or a job on stdin that does not hash to them)."""
    return ["exec", "-i", pod, "-c", "reader", "--", "python3", "-B", "-c", BOOT, READER_SHA256, job_digest(job)]


def exec_stdin(job: Dict) -> bytes:
    return (json.dumps({"script": READER, "job": job}) + "\n").encode()


def exec_failure(rc: int, stdout_text: str, stderr_text: str) -> str:
    """A SANITIZED description of a failed reader run: the reader's own error (class, line, category) when it
    reported one, else a fixed category of the kubectl failure. Neither stream's text is ever echoed."""
    for text in (stdout_text, stderr_text):
        for line in reversed([ln for ln in text.strip().splitlines() if ln.strip()][-3:]):
            try:
                e = json.loads(line).get("reader_error")
            except (ValueError, AttributeError):
                continue
            if isinstance(e, dict):
                ok = (set(e) == {"class", "line", "category"} and e["class"] in ERROR_CLASSES
                      and e["category"] in ERROR_CATEGORIES and isinstance(e["line"], int)
                      and not isinstance(e["line"], bool) and 0 <= e["line"] <= READER_LINES)
                if not ok:
                    return f"the reader failed: unclassified error (exit {rc})"
                return f"the reader failed: {e['class']} ({e['category']}) at reader line {e['line']} (exit {rc})"
    err = stderr_text or ""
    for pat, cat in ((r"414|URI Too|too large|too long", "request-too-large"), (r"Forbidden|forbidden|403", "forbidden"),
                     (r"Unauthorized|401", "unauthorized"), (r"upgrade|websocket|SPDY", "exec-upgrade-failed"),
                     (r"not found|NotFound|404", "not-found"), (r"timeout|Timeout|deadline", "timeout"),
                     (r"executable file not found|exec format", "no-python")):
        if re.search(pat, err):
            return f"kubectl exec failed ({cat}, exit {rc})"
    return f"kubectl exec failed (unclassified, exit {rc})"

COLLECTOR_LOGS = (("INDEX.jsonl", ["recovered_at", "copied_at", "at"]), ("REJECTS.jsonl", ["at"]),
                  ("RUNS.jsonl", ["finished_at"]), ("logs/api-ensemble.jsonl", ["collected_at"]),
                  ("logs/api-reload.jsonl", ["collected_at"]), ("logs/api-coverage.jsonl", ["collected_at"]),
                  ("logs/trainer-index.jsonl", ["collected_at"]))
# The JSON key names the operator writes at 0472829: the only names a shape diagnosis may print for forecasts.jsonl.
FORECAST_KEYS_0472829 = sorted(
    "action application applied_replicas artifact_sha256 at attempt_id autoscaler_name autoscaler_namespace "
    "autoscaler_uid cache_action cache_age_seconds components confidence confidence_adjusted_replicas "
    "current_replicas current_rpm data desired_replicas desired_source elapsed_seconds error_class error_stage event "
    "event_seq forecasting forecast_issued_at forecast_lookup forecasts forecast_status fresh_attempt_id "
    "horizon_minutes http_status inference_input_end instrumentation_status issuance_id issued_at "
    "lead_window_peak_rpm lookup_id mape mape_measured mape_scored max_replicas max_sane_rpm metric_type min_replicas "
    "model_name model_trained_at model_version namespace near_prediction_rpm operator_run_id outcome "
    "predicted_after_clamp predicted_replicas predictions prior_issuance_id raw_predicted_replicas reactive_replicas "
    "rejected_at request_body_sha256 request_namespace resolution result returned_issuance_id returned_link_status "
    "rpm safeguards schema sequence_length served_predictions_status served_step_count served_step_status started_at "
    "status step step_minutes target_anchor target_at timestamp training_cutoff value".split())
DEFAULT_RULES = {
    # The operator's record types at 0472829 (decision_record.go, forecast_attempt_record.go, the issuance and
    # sanity-rejection writers in predictiveautoscaler_controller.go), keyed by "event"; any other type fails closed.
    "forecast-log-pvc": [{"type": "jsonl_prefix", "path": "forecasts.jsonl", "cutoff": "campaign",
                          "ts_by": {"field": "event", "types": {
                              "": ["issued_at"], "decision": ["at"], "sanity_rejected": ["rejected_at"],
                              "forecast_ledger_session": ["at"], "forecast_attempt_started": ["at"],
                              "forecast_attempt_completed": ["at"]}},
                          "known_keys": FORECAST_KEYS_0472829,
                          # issuances name their model: lstm_* serve a persisted trainer artifact (artifact_sha256 =
                          # the .pkl's sha256); ensemble_* serve an in-memory generation whose artifact_sha256 is the
                          # generation's data fingerprint (ml-engine/models/seasonal_ensemble.py, api/main.py), never
                          # a file; decisions carry no model_name and repeat an issuance's digest
                          "ref_classes": {"field": "artifact_sha256", "class_by": "model_name",
                                          "artifact_prefixes": ["lstm_"], "fingerprint_prefixes": ["ensemble_"]}}],
    "evidence-archive-pvc": [{"type": "bundles", "manifest_glob": "archive/*/manifest.json", "ts_field": "copied_at",
                              "cutoff": "collection"}]
    + [dict({"type": "jsonl_prefix", "path": p, "ts_fields": f, "cutoff": "collection"},
            **({"collect_generations": True} if p == "logs/api-ensemble.jsonl" else {})) for p, f in COLLECTOR_LOGS]
    + [{"type": "trainer_logs", "index": "logs/trainer-index.jsonl", "dir": "logs/trainer", "cutoff": "campaign"}],
    "load-evidence-pvc": [
        {"type": "named", "glob": "attempts/load-*.json", "ts_regex": r"^load-(\d{8}T\d{4})Z", "ts_format": "%Y%m%dT%H%M",
         "cutoff": "campaign"},
        {"type": "named", "glob": "load-*.json", "ts_regex": r"^load-(\d{8}T\d{4})Z\.json$", "ts_format": "%Y%m%dT%H%M",
         "cutoff": "campaign"}],
    "ml-models-pvc": [{"type": "pairs", "sidecar_glob": "lstm_*.meta.json", "artifact_from": ".meta.json",
                       "artifact_to": ".pkl", "ts_field": "trained_at", "sha_field": "artifact_sha256",
                       "cutoff": "campaign"}],
}
DEFAULT_EXCLUDED = {
    "forecast-log-pvc": [],
    "evidence-archive-pvc": [{"glob": "logs/api-state.json", "reason": "the collector's mutable cursor, not evidence"},
                             {"glob": ".staging/*", "reason": "the archiver's in-progress staging, never complete"},
                             {"glob": ".staging/*/*", "reason": "the archiver's in-progress staging, never complete"}],
    "load-evidence-pvc": [{"glob": ".lock", "reason": "the collector's empty lock file"}],
    "ml-models-pvc": [],
}
RULE_KEYS = {"jsonl_prefix": {"type", "path", "ts_fields", "ts_by", "known_keys", "ref_classes",
                              "collect_generations", "order_tolerance_seconds", "order_tolerance_review", "cutoff"},
             "named": {"type", "glob", "ts_regex", "ts_format", "cutoff"},
             "bundles": {"type", "manifest_glob", "ts_field", "cutoff"},
             "pairs": {"type", "sidecar_glob", "artifact_from", "artifact_to", "ts_field", "sha_field", "cutoff"},
             "trainer_logs": {"type", "index", "dir", "cutoff", "reviewed"}}


# --- the record -------------------------------------------------------------------------------------------------------

_IDENT_KEYS = {"artifact_arms": ("model_name", "application", "namespace"),
               "ensemble_arms": ("model_name", "application", "namespace", "experiment")}


def forecast_identity_problems(fi) -> List[str]:
    """The reviewed forecast identities (required with empty_by_design): the exact artifact arms
    {model_name, application, namespace}, the exact ensemble arms {..., experiment} and the reviewed generation
    fingerprint map [{sha256, boundary, model_names, source}], each entry bound to its source evidence."""
    where = "populate.pvcs.ml-models-pvc.forecast_identities"
    if not isinstance(fi, dict) or set(fi) != {"artifact_arms", "ensemble_arms", "generation_fingerprints"}:
        return [f"{where} must hold artifact_arms, ensemble_arms and generation_fingerprints"]
    probs = []
    for k, keys in _IDENT_KEYS.items():
        arms = fi[k]
        if not (isinstance(arms, list) and all(isinstance(a, dict) and set(a) == set(keys) and all(
                isinstance(a[x], str) and a[x] for x in keys) for a in arms)
                and len({tuple(a[x] for x in keys) for a in arms}) == len(arms)):
            probs.append(f"{where}.{k} must be unique {{{', '.join(keys)}}} entries")
    names = {a.get("model_name") for a in fi["ensemble_arms"] if isinstance(a, dict)}
    gf = fi["generation_fingerprints"]
    if not (isinstance(gf, list) and all(
            isinstance(g, dict) and set(g) == {"sha256", "boundary", "model_names", "source"}
            and re.match(r"^[0-9a-f]{64}$", str(g["sha256"])) and _TS.match(str(g["boundary"]))
            and isinstance(g["model_names"], list) and g["model_names"] and set(g["model_names"]) <= names
            and str(g["source"]).strip() for g in gf) and len({g["sha256"] for g in gf}) == len(gf)):
        probs.append(f"{where}.generation_fingerprints must be unique {{sha256, boundary, model_names (reviewed "
                     f"ensemble arms), source}} entries")
    return probs


def section(record: X.Record) -> Dict:
    s = record.section("populate")
    probs = []
    for k in ("cutoff",) + (("collection_cutoff",) if "collection_cutoff" in s else ()):
        if not _TS.match(str(s.get(k, ""))):
            probs.append(f"populate.{k} must be an RFC 3339 UTC time")
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
    rd = s.get("reader") or {}
    if rd.get("read_access", "nonroot") not in READ_ACCESS:
        probs.append(f"populate.reader.read_access must be one of {READ_ACCESS}")
    if not all(isinstance(g, int) and not isinstance(g, bool) and g > 0 for g in rd.get("supplemental_groups") or []):
        probs.append("populate.reader.supplemental_groups must be positive integer GIDs")
    if not isinstance((s.get("budget") or {}).get("max_total_bytes"), int):
        probs.append("populate.budget.max_total_bytes is required")
    pvcs = s.get("pvcs") or {}
    if sorted(pvcs) != sorted(PVC_EXPORTS):
        probs.append(f"populate.pvcs must configure exactly {list(PVC_EXPORTS)}")
    for name, p in pvcs.items():
        if not (X._UUID.match(str(p.get("uid", ""))) and str(p.get("mount", "")).startswith("/")
                and isinstance(p.get("max_bytes"), int) and p["max_bytes"] > 0):
            probs.append(f"populate.pvcs.{name} needs uid, an absolute mount and max_bytes")
    for name, p in pvcs.items():
        if "empty_by_design" in p and (name != "ml-models-pvc" or not str(p["empty_by_design"]).strip()):
            probs.append(f"populate.pvcs.{name}.empty_by_design is allowed only for ml-models-pvc, with a reason")
        ca = p.get("campaign_artifacts")
        if "campaign_artifacts" in p and name != "ml-models-pvc":
            probs.append(f"populate.pvcs.{name}.campaign_artifacts is for ml-models-pvc only")
        if p.get("empty_by_design") and name == "ml-models-pvc":
            ds = [a.get("artifact_sha256") for a in ca if isinstance(a, dict)] if isinstance(ca, list) else []
            if not (ca and len(ds) == len(ca) == len(set(ds)) and all(
                    set(a) == {"artifact_sha256", "reason"} and re.match(r"^[0-9a-f]{64}$", str(a["artifact_sha256"]))
                    and str(a["reason"]).strip() for a in ca)):
                probs.append("populate.pvcs.ml-models-pvc.campaign_artifacts must be the reviewed campaign artifact "
                             "inventory: unique {artifact_sha256, reason} entries (required with empty_by_design)")
            probs += forecast_identity_problems(p.get("forecast_identities"))
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
    named_cutoffs = {"campaign": s["cutoff"], "collection": s.get("collection_cutoff", s["cutoff"])}
    out = []
    for r in rules:
        if r.get("type") not in RULE_KEYS or not set(r) <= RULE_KEYS[r["type"]]:
            raise X.StagingError(f"{pvc}: unknown rule or field in {r}")
        if r["type"] == "jsonl_prefix":
            by = r.get("ts_by")
            ok_by = by is None or (isinstance(by, dict) and set(by) == {"field", "types"} and isinstance(by["field"], str)
                                   and isinstance(by["types"], dict) and by["types"] and all(
                                       isinstance(v, list) and v and all(isinstance(f, str) for f in v)
                                       for v in by["types"].values()))
            if not ok_by or (by is None and not r.get("ts_fields")):
                raise X.StagingError(f"{pvc}: jsonl_prefix needs ts_fields or ts_by {{field, types: {{type: [fields]}}}}")
            if not all(isinstance(k, str) for k in r.get("known_keys") or []):
                raise X.StagingError(f"{pvc}: known_keys must be key names")
            rc = r.get("ref_classes")
            if rc is not None:
                rc = rc if isinstance(rc, dict) else {}
                pre = lambda k: rc.get(k) if isinstance(rc.get(k), list) and rc.get(k) and all(
                    isinstance(x, str) and x for x in rc[k]) else None
                ap, fp = pre("artifact_prefixes"), pre("fingerprint_prefixes")
                known = r.get("known_keys") or []
                if not (set(rc) == {"field", "class_by", "artifact_prefixes",
                                                             "fingerprint_prefixes"}
                        and rc["field"] in known and rc["class_by"] in known and ap and fp
                        and not any(a.startswith(f) or f.startswith(a) for a in ap for f in fp)):
                    raise X.StagingError(f"{pvc}: ref_classes needs field and class_by among known_keys and disjoint "
                                         f"non-empty artifact_prefixes / fingerprint_prefixes")
            tol = r.get("order_tolerance_seconds", 0)
            if not isinstance(tol, int) or isinstance(tol, bool) or tol < 0:
                raise X.StagingError(f"{pvc}: order_tolerance_seconds must be a non-negative integer")
            # strict by default; a tolerance is a reviewed, recorded exception for THIS rule only
            if bool(tol) != bool(str(r.get("order_tolerance_review", "")).strip()):
                raise X.StagingError(f"{pvc}: a non-zero order_tolerance_seconds needs an order_tolerance_review "
                                     f"(and a review needs a tolerance) for {r['path']}")
        if r["type"] == "trainer_logs":
            rv = r.get("reviewed") or []
            keys = [(x.get("job"), x.get("pod_uid")) for x in rv if isinstance(x, dict)]
            if not (isinstance(rv, list) and len(keys) == len(rv) == len(set(keys)) and all(
                    set(x) == {"job", "pod_uid", "decision", "reason"} and x["decision"] in ("select", "exclude")
                    and isinstance(x["job"], str) and isinstance(x["pod_uid"], str) and str(x["reason"]).strip()
                    for x in rv)):
                raise X.StagingError(f"{pvc}: trainer_logs reviewed entries need a unique job and pod_uid, a decision "
                                     f"(select | exclude) and a reason")
        r["cutoff"] = named_cutoffs.get(r.get("cutoff", "campaign"), r.get("cutoff"))
        if not _TS.match(str(r["cutoff"])):
            raise X.StagingError(f"{pvc}: rule cutoff {r['cutoff']!r} is not an RFC 3339 UTC time")
        out.append(r)
    return out


def order_tolerances(rules: List[Dict]) -> List[Dict]:
    """The reviewed per-rule order tolerances (strict = none), stated in the plan and the manifest."""
    return [{"path": r["path"], "seconds": r["order_tolerance_seconds"], "review": r["order_tolerance_review"],
             "overhang": "lines after the cutoff inside the prefix; not campaign records"}
            for r in rules if r.get("order_tolerance_seconds")]


def reviewed_exclusions(s: Dict, pvc: str) -> List[Dict]:
    ex = s["pvcs"][pvc].get("excluded_reviewed", "default")
    ex = copy.deepcopy(DEFAULT_EXCLUDED[pvc] if ex == "default" else ex)
    if not all(isinstance(e, dict) and e.get("glob") and str(e.get("reason", "")).strip() for e in ex):
        raise X.StagingError(f"{pvc}: every reviewed exclusion needs a glob and a reason")
    return ex


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


def attached_node(kube: X.Kube, pv_name: str) -> str:
    nodes = [a["spec"]["nodeName"] for a in kube.list("VolumeAttachment")
             if (a.get("spec") or {}).get("source", {}).get("persistentVolumeName") == pv_name
             and (a.get("status") or {}).get("attached")]
    if len(nodes) > 1:
        raise X.StagingError(f"{pv_name} is attached to several nodes {nodes}")
    return nodes[0] if nodes else ""


def claim_binding(kube: X.Kube, name: str, uid: str) -> Dict:
    pvc = kube.get("PersistentVolumeClaim", name, NS)
    if pvc is None or pvc["metadata"]["uid"] != uid:
        raise X.StagingError(f"{name} is missing or its UID differs from the record")
    pv_name = (pvc.get("spec") or {}).get("volumeName")
    pv = kube.get("PersistentVolume", pv_name) if pv_name else None
    if pv is None:
        raise X.StagingError(f"{name} is not bound to a volume")
    return {"name": name, "uid": uid, "pv": pv_name, "pv_uid": pv["metadata"]["uid"],
            "reclaim_policy": (pv.get("spec") or {}).get("persistentVolumeReclaimPolicy"),
            "storage_class": (pvc.get("spec") or {}).get("storageClassName"),
            "access_modes": (pvc.get("spec") or {}).get("accessModes"), "attached_node": attached_node(kube, pv_name)}


# The binding fields a reviewed plan binds: the claim's storage IDENTITY. Where the volume is attached right now, and
# which pods use it, are LIVE preconditions (re-validated before each reader, recorded in the receipts), never part
# of a plan digest: the campaign CronJobs create a pod every 15 minutes.
STABLE_BINDING = ("name", "uid", "pv", "pv_uid", "reclaim_policy", "storage_class", "access_modes")
# The writer kinds allowed to use a claim while S0 reads it, by the pod's controller (kind, name pattern): the
# campaign CronJobs' Job pods (<cronjob>-<scheduled minute>) and the Deployments' ReplicaSet pods. Anything else
# blocks the reader.
WRITER_KINDS = {
    "evidence-archive-pvc": [("Job", r"evidence-archive-\d+")],
    "forecast-log-pvc": [("ReplicaSet", r"predictive-operator-[a-z0-9]+")],
    "load-evidence-pvc": [("Job", r"load-evidence-\d+")],
    "ml-models-pvc": [("Job", r"evidence-archive-\d+"), ("Job", r"ml-training-\d+"), ("ReplicaSet", r"ml-api-[a-z0-9]+")],
}


def stable(binding: Dict) -> Dict:
    return {k: binding.get(k) for k in STABLE_BINDING}


def consumers(kube: X.Kube) -> Dict[str, List[Dict]]:
    out: Dict[str, List[Dict]] = {}
    for p in kube.list("Pod", NS):
        if READER_LABEL in (p["metadata"].get("labels") or {}):
            continue
        refs = p["metadata"].get("ownerReferences") or []
        ctl = next((r for r in refs if r.get("controller")), refs[0] if refs else {})
        for v in (p.get("spec") or {}).get("volumes") or []:
            c = (v.get("persistentVolumeClaim") or {}).get("claimName")
            if c:
                out.setdefault(c, []).append({"pod": p["metadata"]["name"], "uid": p["metadata"].get("uid"),
                                              "node": (p.get("spec") or {}).get("nodeName"),
                                              "owner": {"kind": ctl.get("kind"), "name": ctl.get("name")},
                                              "phase": (p.get("status") or {}).get("phase"),
                                              "read_only": bool(v["persistentVolumeClaim"].get("readOnly"))})
    return out


def consumer_problems(pvc: str, cons: List[Dict]) -> List[str]:
    """Every pod using the claim is an allowlisted writer kind (its controller's kind and name pattern); an unknown
    one is reported by a digest of its identity, never its names."""
    out = []
    for c in cons:
        o = c.get("owner") or {}
        if not any(o.get("kind") == k and isinstance(o.get("name"), str) and re.fullmatch(rx, o["name"])
                   for k, rx in WRITER_KINDS.get(pvc, [])):
            out.append(f"{pvc}: a pod of an unknown writer kind uses the claim (sha256 "
                       f"{hashlib.sha256(json.dumps([c.get('pod'), o.get('kind'), o.get('name')]).encode()).hexdigest()[:12]}"
                       f"); only {[k + ' ' + rx for k, rx in WRITER_KINDS.get(pvc, [])]} may")
    return out


def leftover_readers(kube: X.Kube, record: X.Record) -> List[str]:
    return sorted(p["metadata"]["name"] for p in kube.list("Pod", NS, labels={READER_LABEL: record.run_id}))


def reader_security(r: Dict) -> Tuple[Dict, Dict]:
    """(pod securityContext, container capabilities) for populate.reader.read_access:
      nonroot          UID/GID 65534, every capability dropped; reads what is world-readable, or group-readable with
                       populate.reader.supplemental_groups (the files' group). fsGroup is never used: on many drivers
                       it re-owns the volume (a write to evidence) and it cannot open owner-only (0600) files anyway.
      dac-read-search  UID 0 with every capability dropped EXCEPT CAP_DAC_READ_SEARCH (needed for root-owned 0600
                       files and 0700 directories). The capability bypasses read and directory-search permission checks
                       and also permits open_by_handle_at(2) and some linkat(2) uses, so it is NOT what protects the
                       evidence: write protection comes from the READ-ONLY claim mount (and the read-only root
                       filesystem); seccomp RuntimeDefault, no privilege escalation, no other capability, no
                       service-account token and the proven cleanup bound the rest. Select it explicitly, only for the
                       evidence reader."""
    mode = r.get("read_access", "nonroot")
    groups = [int(g) for g in r.get("supplemental_groups") or []]
    base = {"seccompProfile": {"type": "RuntimeDefault"}}
    if groups:
        base["supplementalGroups"] = groups
    if mode == "dac-read-search":
        return dict(base, runAsNonRoot=False, runAsUser=0, runAsGroup=0), {"drop": ["ALL"], "add": ["DAC_READ_SEARCH"]}
    return dict(base, runAsNonRoot=True, runAsUser=65534, runAsGroup=65534), {"drop": ["ALL"]}


def reader_pod(record: X.Record, s: Dict, pvc: str, binding: Dict, nonce: str) -> Dict:
    r = s["reader"]
    deadline = int(r.get("deadline_seconds", 1800))
    pod_sc, caps = reader_security(r)
    spec = {"restartPolicy": "Never", "activeDeadlineSeconds": deadline,
            "automountServiceAccountToken": False, "enableServiceLinks": False,
            "securityContext": pod_sc,
            "containers": [{"name": "reader", "image": r["image"],
                            "command": ["python3", "-B", "-c", f"import time; time.sleep({deadline})"],
                            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                                "capabilities": caps},
                            "resources": {"requests": {"cpu": "50m", "memory": "64Mi"},
                                          "limits": {"cpu": "1", "memory": "256Mi"}},
                            "volumeMounts": [{"name": "data", "mountPath": s["pvcs"][pvc]["mount"],
                                              "readOnly": True}]}],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": pvc, "readOnly": True}}]}
    if binding["attached_node"]:
        spec["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In",
                                   "values": [binding["attached_node"]]}]}]}}}
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
        argv = self.base + exec_argv(pod, job)
        env = {k: v for k, v in os.environ.items() if k != "KUBECONFIG"}
        env["KUBECONFIG"] = self.base[2]
        with open(out_path, "wb") as fh:
            res = self.run(argv, input=exec_stdin(job), stdout=fh, stderr=subprocess.PIPE,
                           timeout=self.timeout, env=env)
        return res.returncode, (res.stderr or b"").decode("utf-8", errors="replace")


def _next_step(record: X.Record, helper: str) -> str:
    return str(max([int(e["event"].split("-", 1)[1]) for e in X.receipts(record, helper)
                    if e["event"].startswith(("intent-", "step-", "reconciled-"))] or [0]) + 1)


def prove_cleanup(record: X.Record, kube: X.Kube, helper: str, step: str, pvc: str, pv: str, nonce: str,
                  attached_before: str, sleep, now, timeout: float = 300) -> Dict:
    """Delete every pod of the nonce (each delete intent durable first, UID-bound) and PROVE: no pod of the nonce
    remains and a volume that was not attached before is released. Raises when unproven."""
    deadline = now() + timeout
    deleted = set()
    while True:
        left = kube.list("Pod", NS, labels={NONCE_LABEL: nonce})
        for p in left:
            uid = p["metadata"].get("uid")
            if uid in deleted:
                continue
            C.require_cluster(kube.api, record.cluster_uid)
            X.receipt(record, helper, f"delete-intent-{step}", {"pod": p["metadata"]["name"], "uid": uid,
                                                                "nonce": nonce})
            try:
                kube.api.delete(X.path("Pod", NS, p["metadata"]["name"]), uid)
            except C.NotFound:
                pass
            deleted.add(uid)
        node = attached_node(kube, pv)
        if not left and (attached_before or not node):
            return {"pods_left": 0, "attached_before": attached_before, "attached_after": node,
                    "deleted": sorted(deleted)}
        if now() > deadline:
            raise X.StagingError(f"cleanup of the {pvc} reader (nonce {nonce}) is NOT proven: pods "
                                 f"{[p['metadata']['name'] for p in left]}, volume attached to {node!r}; the "
                                 "operation stays unresolved and the lock held (recover)")
        sleep(2)


def revalidate(record: X.Record, kube: X.Kube, s: Dict, pvc: str, planned: Dict) -> Dict:
    """Right before EACH reader, the LIVE preconditions (not part of any plan digest): the claim's storage identity
    is the planned one, no campaign Job runs, every pod using the claim is an allowlisted writer kind, and the node
    the volume is attached to now (the reader pod is pinned there). Returns them for the receipts."""
    C.require_cluster(kube.api, record.cluster_uid)
    busy = busy_jobs(kube)
    if busy:
        raise X.StagingError(f"active Jobs {busy}: no reader while they run")
    live = claim_binding(kube, pvc, s["pvcs"][pvc]["uid"])
    if stable(live) != stable(planned):
        raise X.StagingError(f"{pvc}'s storage identity changed since the plan ({stable(live)} vs {stable(planned)})")
    cons = consumers(kube).get(pvc, [])
    probs = consumer_problems(pvc, cons)
    if probs:
        raise X.StagingError("; ".join(probs))
    return {"attached_node": live["attached_node"], "busy_jobs": [],
            "consumers": [{k: c[k] for k in ("pod", "uid", "owner", "phase", "read_only")} for c in cons]}


def run_reader(record: X.Record, kube: X.Kube, lock, helper: str, s: Dict, pvc: str, binding: Dict, job: Dict,
               exe, sleep, now) -> Tuple[int, str, str]:
    """ONE reader lifecycle as ONE mutation_step: intent (pod, nonce, claim, attachment) before the create; the pod
    created, used and deleted; the outcome only after cleanup is PROVEN. A work failure after a proven cleanup is
    re-raised with the lock safe; an unproven cleanup leaves the intent unresolved and the lock held."""
    pre = revalidate(record, kube, s, pvc, binding)
    binding = dict(stable(binding), attached_node=pre["attached_node"])     # the attachment resolved live
    nonce = secrets.token_hex(5)
    body = reader_pod(record, s, pvc, binding, nonce)
    name = body["metadata"]["name"]
    tmp = os.path.join(record.evidence_dir, "populate", f"{helper}-{pvc}-{nonce}.out")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    step = _next_step(record, helper)
    intent = {"op": "reader", "pvc": pvc, "pv": binding["pv"], "pod": name, "nonce": nonce,
              "attached_before": binding["attached_node"], "body_sha256": C.digest(body), "mode": job["mode"]}
    failure: List[BaseException] = []
    result: Dict = {"rc": None, "stderr": "", "uid": None}

    def lifecycle():
        try:
            C.require_cluster(kube.api, record.cluster_uid)
            out = kube.api.create(X.path("Pod", NS), body)
            result["uid"] = (out.get("metadata") or {}).get("uid")
            deadline = now() + 300
            while True:
                p = kube.get("Pod", name, NS)
                phase = ((p or {}).get("status") or {}).get("phase")
                if phase == "Running":
                    break
                if phase in ("Failed", "Succeeded") or now() > deadline:
                    raise X.StagingError(f"reader pod {name} did not start (phase {phase})")
                sleep(2)
            result["rc"], result["stderr"] = exe(name, job, tmp)
        except Exception as e:      # noqa: BLE001 - kept, re-raised after the proven cleanup
            failure.append(e)
        proof = prove_cleanup(record, kube, helper, step, pvc, binding["pv"], nonce, binding["attached_node"],
                              sleep, now)            # raises when unproven: no outcome, the lock stays
        return ("cleaned" if not failure else "cleaned-after-failure"),             {"uid": result["uid"], "proof": proof, "error": type(failure[0]).__name__ if failure else None}
    X.receipt(record, helper, "reader-preconditions", {"pvc": pvc, "step": step, "nonce": nonce, **pre})
    C.mutation_step(record.doc, helper, lock, step, intent, lifecycle)
    X.receipt(record, helper, "reader-cleanup-proven", {"pvc": pvc, "pod": name, "nonce": nonce, "step": step})
    if failure:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise failure[0]
    return result["rc"], result["stderr"], tmp


def _pre_checks(record: X.Record, kube: X.Kube, s: Dict) -> Tuple[Dict, List[str]]:
    C.require_cluster(kube.api, record.cluster_uid)
    probs = []
    lp = C.lock_path(record.doc, LOCK_SCOPE)
    if os.path.exists(lp):
        probs.append(f"the shared legacy-scope lock is held ({lp})")
    busy = busy_jobs(kube)
    if busy:
        probs.append(f"active Jobs {busy}: no reader while they run")
    left = leftover_readers(kube, record)
    if left:
        probs.append(f"leftover reader pods {left}: run recover")
    for h in (HELPER, PROBE) + LEGACY_HELPERS:
        if _unresolved_steps(record, h):
            probs.append(f"unresolved intents in the {h} journal: run recover")
    bindings = {p: stable(claim_binding(kube, p, s["pvcs"][p]["uid"])) for p in PVC_EXPORTS}
    cons = consumers(kube)
    for p in PVC_EXPORTS:
        probs += consumer_problems(p, cons.get(p, []))
    return bindings, probs


def _steps(record: X.Record, helper: str) -> Dict[str, Dict]:
    steps: Dict[str, Dict] = {}
    for e in X.receipts(record, helper):
        for pre in ("intent-", "step-", "reconciled-", "delete-intent-"):
            if e["event"].startswith(pre):
                steps.setdefault(e["event"][len(pre):], {}).setdefault(pre, []).append(e["data"])
                break
    return steps


def _unresolved_steps(record: X.Record, helper: str) -> List[Tuple[str, Dict]]:
    return [(n, st["intent-"][0]) for n, st in sorted(_steps(record, helper).items(), key=lambda kv: int(kv[0]))
            if "intent-" in st and "step-" not in st and "reconciled-" not in st]


# --- probe ------------------------------------------------------------------------------------------------------------

def probe_plan(record: X.Record, kube: X.Kube) -> Dict:
    s = section(record)
    bindings, probs = _pre_checks(record, kube, s)
    actions = [{"pvc": p, "binding": bindings[p], "mount": s["pvcs"][p]["mount"], "rules": effective_rules(s, p)}
               for p in PVC_EXPORTS]
    pod_sc, caps = reader_security(s["reader"])
    return X.new_plan(PROBE, record, actions, reader_sha256=READER_SHA256, reader_image=s["reader"]["image"],
                      reader_security={"pod": pod_sc, "capabilities": caps, "read_only_root_filesystem": True,
                                       "service_account_token": False},
                      problems=probs, note="read-only reader pods: sizes, selections, original-hash checks, digests")


def probe(record: X.Record, kube: X.Kube, reviewed: Dict, digest: str, exe=None, sleep=time.sleep,
          now=time.monotonic) -> Dict:
    s = section(record)
    exe = exe or Exec(record, int(s["reader"].get("exec_timeout_seconds", 1800)))
    with C.exclusive_lock(record.doc, LOCK_SCOPE, holder=PROBE) as lock:
        live = probe_plan(record, kube)
        live["problems"] = [p for p in live["problems"] if not p.startswith("the shared legacy-scope lock")]
        C.require_reviewed(reviewed, digest, PROBE, record.doc)
        if live["problems"]:
            raise X.StagingError("the probe plan has problems: " + "; ".join(live["problems"]))
        X.require_live(reviewed, digest, PROBE, record, live)
        for a in live["actions"]:
            job = {"mode": "probe", "mount": a["mount"], "rules": a["rules"]}
            rc, err, tmp = run_reader(record, kube, lock, PROBE, s, a["pvc"], a["binding"], job, exe, sleep, now)
            text = open(tmp, encoding="utf-8", errors="replace").read()
            os.unlink(tmp)
            try:
                out = json.loads(text)
            except ValueError:
                out = None
            if rc != 0 or not isinstance(out, dict) or "reader_error" in out:
                raise X.StagingError(f"{a['pvc']}: {exec_failure(rc, text, err)}")
            X.receipt(record, PROBE, "probed", {"pvc": a["pvc"], "plan_sha256": digest, "binding": a["binding"],
                                                "rules_sha256": C.digest(a["rules"]), "result": out})
        return X.receipt(record, PROBE, "probe-complete", {"plan_sha256": digest})


def probe_results(record: X.Record) -> Dict[str, Dict]:
    return {e["data"]["pvc"]: e["data"] for e in X.receipts(record, PROBE) if e["event"] == "probed"}


# --- the export plan --------------------------------------------------------------------------------------------------

def _rule_label(r: Dict) -> str:
    return f"{r['type']} {r.get('path') or r.get('glob') or r.get('manifest_glob') or r.get('sidecar_glob') or r.get('index')}"


def archive_closure_problems(pvcs: Dict[str, Dict]) -> List[str]:
    """Every exported model pair has its immutable archived copy in the evidence-archive selection (same artifact and
    sidecar hashes); every sidecar-variant bundle's artifact_key names a selected bundle."""
    out = []
    ea = pvcs.get("evidence-archive-pvc", {}).get("selection", [])
    manifests = [it for it in ea if "artifact_sha256" in it and "sidecar_sha256" in it]
    archived = {(m["artifact_sha256"], m["sidecar_sha256"]) for m in manifests}
    keys = {m.get("bundle") for m in manifests}
    for m in manifests:
        if m.get("artifact_key") and m["artifact_key"] not in keys:
            out.append(f"evidence-archive bundle {m['bundle']}: its artifact_key {m['artifact_key']} is not selected")
    sel = pvcs.get("ml-models-pvc", {}).get("selection", [])
    for sc in (it for it in sel if it.get("artifact_sha256")):
        if (sc["artifact_sha256"], sc["sha256"]) not in archived:
            out.append(f"ml-models {sc['path']}: no immutable archived copy (same artifact and sidecar hashes) in the "
                       "evidence-archive selection; the mutable model directory is not historical storage")
    return out


def empty_by_design(pvc: str, reason: str, selection: List[Dict], rule_excluded: List[Dict],
                    unconsidered: List[Dict], rules: List[Dict]) -> Tuple[Dict, List[str]]:
    """A reviewed EMPTY export (ml-models-pvc only): accepted only when nothing is selected, the volume holds nothing
    outside the rules, and EVERY model pair present was excluded as trained after the cutoff (its trained_at checked
    here against the cutoff), so nothing campaign-relevant is skipped. The campaign's artifacts are the archived
    bundles of the evidence archive."""
    probs = []
    cutoff = max(r["cutoff"] for r in rules)
    if selection:
        probs.append(f"{pvc}: declared empty by design but the rules select {len(selection)} files")
    if unconsidered:
        probs.append(f"{pvc}: declared empty by design but {len(unconsidered)} files lie outside its rules")
    pairs_after = []
    for x in rule_excluded:
        # only a VERIFIED pair (artifact sha256 and size equal its sidecar's) may justify the empty export by its time
        if (x.get("reason") != "trained after the cutoff" or x.get("pair_verified") is not True
                or ts_ns(x.get("ts")) is None or not ts_ns(x["ts"]) > ts_ns(cutoff)):
            probs.append(f"{pvc}: declared empty by design but {x.get('path')} is excluded as {x.get('reason')!r} "
                         f"(only pairs trained after the cutoff may be skipped)")
        elif "pair" in x:
            pairs_after.append(dict(x["pair"], trained_at=x["ts"]))
    sidecars = {pr["sidecar"] for pr in pairs_after}
    for x in rule_excluded:
        if "pair" not in x and x.get("pair_sidecar") not in sidecars:
            probs.append(f"{pvc}: declared empty by design but {x.get('path')} is not part of a verified pair")
    return {"reason": reason, "cutoff": cutoff, "current_pairs_trained_after_cutoff": pairs_after,
            "campaign_artifacts": []}, probs


def resolve_artifact(d: str, ea: List[Dict]) -> Optional[Dict]:
    """A campaign artifact digest resolved to VERIFIED evidence-archive members: a selected bundle manifest whose
    original artifact_sha256 is d (its artifact and sidecar bytes verified by the reader against the manifest's
    artifact_sha256/sidecar_sha256), the artifact member (in its own bundle, or in the bundle its artifact_key names)
    with sha256 d, and the sidecar member with the manifest's sidecar_sha256. None when any piece is missing."""
    by_dir: Dict[str, List[Dict]] = {}
    for it in ea:
        by_dir.setdefault(os.path.dirname(it["path"]), []).append(it)
    mans = [m for m in ea if "sidecar_sha256" in m and m.get("artifact_sha256") == d]
    for m in sorted(mans, key=lambda m: (bool(m.get("artifact_key")), m["path"])):
        own = by_dir.get(os.path.dirname(m["path"]), [])
        side = next((x for x in own if x is not m and x["sha256"] == m.get("sidecar_sha256")), None)
        home = own
        if m.get("artifact_key"):
            tgt = next((t for t in ea if "sidecar_sha256" in t and t.get("bundle") == m["artifact_key"]), None)
            home = by_dir.get(os.path.dirname(tgt["path"]), []) if tgt else []
        art = next((x for x in home if "sidecar_sha256" not in x and x["sha256"] == d), None)
        if side and art:
            return {"artifact_sha256": d, "bundle": m.get("bundle"), "manifest_member": m["path"],
                    "manifest_sha256": m["sha256"], "artifact_member": art["path"], "artifact_bytes": art["bytes"],
                    "sidecar_member": side["path"], "sidecar_sha256": side["sha256"], "sidecar_bytes": side["bytes"],
                    "verified": "artifact and sidecar bytes equal the original bundle manifest's artifact_sha256 / "
                                "sidecar_sha256 and the sidecar's artifact_sha256 (reader); members re-verified in "
                                "the evidence-archive export tar"}
    return None


def campaign_artifact_problems(s: Dict, pvcs: Dict[str, Dict]) -> List[str]:
    """For an empty-by-design ml-models export: the reviewed campaign artifact inventory covers every artifact the
    campaign forecast issuances/decisions reference and every training bundle copied up to the campaign cutoff, and
    each inventory digest resolves to verified evidence-archive members (stored as references in the plan)."""
    mm = pvcs.get("ml-models-pvc") or {}
    if not mm.get("empty_by_design"):
        return []
    out, cutoff = [], s["cutoff"]
    inv = [a["artifact_sha256"] for a in s["pvcs"]["ml-models-pvc"]["campaign_artifacts"]]
    fl = pvcs.get("forecast-log-pvc", {}).get("selection", [])
    referenced = {d for it in fl for d in it.get("referenced_artifact_sha256") or []}
    if not any("referenced_artifact_sha256" in it for it in fl):
        out.append("ml-models-pvc: empty by design needs the forecast log's referenced artifacts (ref_classes)")
    gens, gprobs = ensemble_generation_check(s, pvcs, set(inv))
    out += gprobs
    ea = pvcs.get("evidence-archive-pvc", {}).get("selection", [])
    trained = {m["artifact_sha256"] for m in ea if "sidecar_sha256" in m and m.get("artifact_sha256")
               and ts_ns(m.get("ts")) is not None and ts_ns(m["ts"]) <= ts_ns(cutoff)}
    for d in sorted(referenced - set(inv)):
        out.append(f"ml-models-pvc: the campaign forecast log references artifact {d} outside the reviewed campaign "
                   "artifact inventory")
    for d in sorted(trained - set(inv)):
        out.append(f"ml-models-pvc: the campaign training bundle artifact {d} is outside the reviewed campaign "
                   "artifact inventory")
    refs = []
    for d in inv:
        r = resolve_artifact(d, ea)
        if r is None:
            out.append(f"ml-models-pvc: campaign artifact {d} does not resolve to a verified evidence-archive bundle "
                       "member (artifact and sidecar)")
        else:
            refs.append(dict(r, referenced_by=sorted(
                (["forecast log"] if d in referenced else []) + (["training bundle"] if d in trained else []))))
    mm["empty_by_design"]["campaign_artifacts"] = refs
    mm["empty_by_design"]["ensemble_generations"] = gens
    return out


def identity_digest(t) -> str:
    """An UNREVIEWED identity in a diagnosis: a digest of its values, never the values (they may be anything)."""
    return hashlib.sha256(json.dumps(list(t)).encode()).hexdigest()[:12]


def ensemble_generation_check(s: Dict, pvcs: Dict[str, Dict], inventory: set) -> Tuple[Dict, List[str]]:
    """The forecast log's identities against the REVIEWED ones: every artifact issuance is a reviewed artifact arm;
    every generation issuance is a reviewed ensemble arm (model_name, application, namespace, experiment) and its
    fingerprint is in the reviewed map with the same boundary and arm; the map holds nothing unreferenced; and each
    referenced generation is corroborated by an archived ENSEMBLE_ISSUANCE record of the evidence archive with the same
    application, namespace, boundary and fingerprint (that record keeps no experiment; the issuance's model_version
    binds it). The inputs of a generation are not retained, so it cannot be replayed."""
    fi = s["pvcs"]["ml-models-pvc"]["forecast_identities"]
    out = []
    fl = pvcs.get("forecast-log-pvc", {}).get("selection", [])
    ids = {tuple(x) for it in fl for x in it.get("artifact_identities") or []}
    arms_a = {tuple(a[k] for k in _IDENT_KEYS["artifact_arms"]) for a in fi["artifact_arms"]}
    for x in sorted(ids - arms_a):
        out.append(f"ml-models-pvc: an artifact issuance identity (model_name, application, namespace; sha256 "
                   f"{identity_digest(x)}) is not a reviewed artifact arm")
    arms_e = {tuple(a[k] for k in _IDENT_KEYS["ensemble_arms"]): a for a in fi["ensemble_arms"]}
    by_name = {a["model_name"]: [] for a in fi["ensemble_arms"]}
    for a in fi["ensemble_arms"]:
        by_name[a["model_name"]].append(a)
    reviewed = {g["sha256"]: g for g in fi["generation_fingerprints"]}
    archived = {(g["application"], g["namespace"], g["boundary"], g["fingerprint"])
                for it in pvcs.get("evidence-archive-pvc", {}).get("selection", [])
                for g in it.get("archived_generations") or []}
    seen: Dict[str, Dict] = {}
    for g in (g for it in fl for g in it.get("referenced_generations") or []):
        fp = g["fingerprint"]
        arm = (g["model_name"], g["application"], g["namespace"], g["experiment"])
        if arm not in arms_e:
            out.append(f"ml-models-pvc: generation {fp} was issued by an identity (model_name, application, namespace, "
                       f"experiment; sha256 {identity_digest(arm)}) that is not a reviewed ensemble arm")
            continue
        r = reviewed.get(fp)
        if r is None:
            out.append(f"ml-models-pvc: generation {fp} is not in the reviewed generation fingerprint map")
            continue
        if r["boundary"] != g["boundary"] or g["model_name"] not in r["model_names"]:
            out.append(f"ml-models-pvc: generation {fp} conflicts with its reviewed boundary/arms")
            continue
        if fp in inventory:
            out.append(f"ml-models-pvc: {fp} is both a generation fingerprint and a reviewed campaign artifact")
        if (g["application"], g["namespace"], g["boundary"], fp) not in archived:
            out.append(f"ml-models-pvc: generation {fp} ({g['application']}, {g['boundary']}) is not corroborated by "
                       "an archived ENSEMBLE_ISSUANCE record")
            continue
        e = seen.setdefault(fp, {"sha256": fp, "boundary": g["boundary"], "arms": [], "source": r["source"],
                                 "corroborated_by": "logs/api-ensemble.jsonl (evidence-archive export)"})
        if list(arm) not in e["arms"]:
            e["arms"].append(list(arm))
    for fp in sorted(set(reviewed) - {g["fingerprint"] for it in fl for g in it.get("referenced_generations") or []}):
        out.append(f"ml-models-pvc: the reviewed generation fingerprint {fp} is referenced by no campaign issuance")
    entries = [seen[k] for k in sorted(seen)]
    for e in entries:
        e["arms"].sort()
    return {"count": len(entries), "entries": entries,
            "experiment_note": "the archived ENSEMBLE_ISSUANCE record keeps application, namespace, boundary and "
                               "fingerprint, not the experiment (archive.py at 0472829); the issuance's model_version "
                               "binds the experiment",
            "replay": "ensemble replay verification unavailable; fingerprints preserved, inputs not",
            "why": "ensemble_* issuances carry the in-memory generation's data fingerprint as artifact_sha256 "
                   "(ml-engine/models/seasonal_ensemble.py, api/main.py at 0472829); no model file exists"}, out


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
        if r["rules_sha256"] != C.digest(rules) or {k: r["binding"][k] for k in ("uid", "pv", "pv_uid")} != \
                {k: bindings[p][k] for k in ("uid", "pv", "pv_uid")}:
            probs.append(f"{p}: the probe was for other rules or another binding: probe again")
            continue
        selection = [it for rr in r["result"]["rules"] for it in rr["selected"]]
        ebd = s["pvcs"][p].get("empty_by_design")
        for rr in r["result"]["rules"]:
            rprobs = [x for x in rr["problems"] if not (ebd and x == "the rule selects nothing")]
            probs += [f"{p}: rule {_rule_label(rr['rule'])}: {x}" for x in rprobs]
        ud = r["result"].get("unreadable_directories") or {}
        if ud.get("count"):
            probs.append(f"{p}: {ud['count']} directories are unreadable ({', '.join(ud['categories'])}): the claim "
                         "cannot be accounted for (see populate.reader.read_access)")
        paths = [it["path"] for it in selection]
        if len(set(paths)) != len(paths):
            probs.append(f"{p}: two rules select the same file")
        rule_excluded = [x for rr in r["result"]["rules"] for x in rr["excluded"]]
        reviewed_ex = reviewed_exclusions(s, p)
        exclusions = []
        for f in r["result"]["unconsidered"]:
            why = next((e["reason"] for e in reviewed_ex if fnmatch.fnmatch(f["path"], e["glob"])), None)
            if why is None:
                probs.append(f"{p}: {f['path']} is neither selected, excluded by a rule, nor a reviewed exclusion")
            else:
                exclusions.append({"path": f["path"], "bytes": f["bytes"], "reason": why})
        size = sum(it["bytes"] for it in selection)
        total += size
        if size > s["pvcs"][p]["max_bytes"]:
            probs.append(f"{p}: the selection is {size} bytes, over its budget {s['pvcs'][p]['max_bytes']}")
        empty = None
        if ebd:
            empty, eprobs = empty_by_design(p, ebd, selection, rule_excluded, r["result"]["unconsidered"], rules)
            probs += eprobs
        elif not selection:
            probs.append(f"{p}: nothing selected (an empty export is never produced)")
        pvcs[p] = {"binding": bindings[p], "rules": rules, "order_tolerances": order_tolerances(rules),
                   "selection": selection, "bytes": size,
                   "volume": r["result"]["volume"], "rule_excluded": rule_excluded, "reviewed_exclusions": exclusions,
                   "empty_by_design": empty}
    probs += archive_closure_problems(pvcs)
    probs += campaign_artifact_problems(s, pvcs)
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
    p8f = next((f for f in local if f["path"] == p8["export"]), None)
    if p8f is None or p8f["sha256"] != p8["sha256"]:
        probs.append("the existing p8-evidence-pvc fetch is not among the copied files with its recorded sha256")
    actions = [{"copy": f["path"], "sha256": f["sha256"], "bytes": f["bytes"]} for f in local]
    actions += [{"generate": k, "sha256": hashlib.sha256(v).hexdigest()} for k, v in sorted(gen.items())]
    actions += [{"export": p, "files": len(v["selection"]), "bytes": v["bytes"],
                 "selection_sha256": C.digest(v["selection"])} for p, v in pvcs.items()]
    return X.new_plan(HELPER, record, actions, export_dir=export_dir, local=local, pvcs=pvcs,
                      total_pvc_bytes=total, reader_sha256=READER_SHA256,
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
    """The received tar equals the reader's trailer, its receipt equals the reviewed selection (paths, sizes AND the
    probe's sha256), and every member's bytes hash to it."""
    if C.sha256_file(path) != trailer["trailer"]["sha256"] or os.path.getsize(path) != trailer["trailer"]["bytes"]:
        return ["the received tar does not match the reader's trailer"]
    want = [(it["path"], it["bytes"], it["sha256"]) for it in selection]
    if [(f["path"], f["bytes"], f["sha256"]) for f in trailer["files"]] != want:
        return ["the reader's receipt is not exactly the reviewed selection (paths, sizes and sha256)"]
    out = []
    with tarfile.open(path, "r:") as tar:
        members = tar.getmembers()
        if [(m.name, m.size) for m in members] != [(p, b) for p, b, _ in want]:
            return ["the tar's members are not exactly the reviewed selection"]
        for m, (_, _, h) in zip(members, want):
            if not m.isreg() or m.name.startswith("/") or ".." in m.name.split("/"):
                out.append(f"unsafe tar member {m.name}")
            elif hashlib.sha256(tar.extractfile(m).read()).hexdigest() != h:
                out.append(f"{m.name}: the member's bytes differ from the reviewed sha256")
    return out


def export_pvc(record, kube, lock, s, pvc, info, exe, sleep, now, digest, done=None) -> Dict:
    d = os.path.join(evidence(record)["export_dir"], "pvc-exports", pvc)
    os.makedirs(d, exist_ok=True)
    tar_path = os.path.join(d, "campaign-export.tar")
    if os.path.exists(tar_path):
        raise X.StagingError(f"{tar_path} exists (a PVC export is produced once)")
    if info.get("empty_by_design"):
        return export_empty(record, d, pvc, info, digest, done or {})
    job = {"mode": "export", "mount": s["pvcs"][pvc]["mount"], "selection": info["selection"]}
    rc, err, tmp = run_reader(record, kube, lock, HELPER, s, pvc, info["binding"], job, exe, sleep, now)
    try:
        trailer = json.loads(err.strip().splitlines()[-1]) if err.strip() else {}
    except ValueError:
        trailer = {}
    if rc != 0 or not trailer or trailer.get("problems") or "reader_error" in trailer:
        os.unlink(tmp)
        why = trailer.get("problems") if trailer.get("problems") else exec_failure(rc, "", err)
        raise X.StagingError(f"{pvc}: {why}")
    probs = verify_tar(tmp, info["selection"], trailer)
    if probs:
        os.rename(tmp, tmp + ".rejected")
        raise X.StagingError(f"{pvc}: " + "; ".join(probs))
    os.rename(tmp, tar_path)
    tar_sha = C.sha256_file(tar_path)
    manifest = {"schema": "c28-pvc-export/v1", "run_id": record.run_id, "cluster_uid": record.cluster_uid,
                "pvc": pvc, "pvc_uid": info["binding"]["uid"], "pv": info["binding"]["pv"],
                "pv_uid": info["binding"]["pv_uid"], "rules": info["rules"],
                "cutoffs": sorted({r["cutoff"] for r in info["rules"]}), "reader_sha256": READER_SHA256,
                "order_tolerances": order_tolerances(info["rules"]),
                "populator_sha256": C.sha256_file(os.path.abspath(__file__)), "plan_sha256": digest,
                "files": [dict({k: v for k, v in it.items() if k not in ("path",)}, source_path=it["path"],
                               member=it["path"]) for it in info["selection"]],
                "rule_excluded": info["rule_excluded"], "reviewed_exclusions": info["reviewed_exclusions"],
                "volume_totals_at_probe": info["volume"],
                "tar": {"file": "campaign-export.tar", "bytes": os.path.getsize(tar_path), "sha256": tar_sha},
                "note": "campaign artifacts selected by their own names or record timestamps against the cutoff, "
                        "byte-bound to the probe and with the writers' original hashes verified; the rest of the "
                        "volume is retained but not exported"}
    mtext = (json.dumps(manifest, indent=1, sort_keys=True) + "\n").encode()
    C.assert_clean(mtext.decode())
    _write_identical(os.path.join(d, "manifest.json"), mtext)
    sums = f"{tar_sha}  campaign-export.tar\n{hashlib.sha256(mtext).hexdigest()}  manifest.json\n".encode()
    _write_identical(os.path.join(d, "SHA256SUMS"), sums)
    X.receipt(record, HELPER, "pvc-exported", {"pvc": pvc, "files": len(info["selection"]), "tar_sha256": tar_sha,
                                               "manifest_sha256": hashlib.sha256(mtext).hexdigest()})
    return manifest


EA_DIR = "pvc-exports/evidence-archive-pvc"

def archive_reference_problems(pvc: str, refs: List[Dict], root: str) -> List[str]:
    """Every campaign artifact reference resolves in the EXPORTED evidence-archive manifest (member, sha256, bytes)
    and names that export's tar and manifest digests; the tar's members are verified against the manifest
    separately (verify_tar)."""
    out = []
    mpath = os.path.join(root, EA_DIR, "manifest.json")
    if not os.path.isfile(mpath):
        return [f"{pvc}: the evidence-archive export is missing, so no campaign artifact reference can resolve"]
    m = json.load(open(mpath))
    msha = C.sha256_file(mpath)
    files = {f["member"]: f for f in m.get("files") or []}
    if not refs:
        out.append(f"{pvc}: empty by design without any verified campaign artifact reference")
    for r in refs:
        ex = r.get("export") or {}
        if ex.get("tar_sha256") != m["tar"]["sha256"] or ex.get("manifest_sha256") != msha:
            out.append(f"{pvc}: campaign artifact {r.get('artifact_sha256')} names another evidence-archive export")
        for k, want, nbytes in (("artifact_member", r.get("artifact_sha256"), r.get("artifact_bytes")),
                                ("sidecar_member", r.get("sidecar_sha256"), r.get("sidecar_bytes")),
                                ("manifest_member", r.get("manifest_sha256"), None)):
            f = files.get(r.get(k))
            if not f or f["sha256"] != want or (nbytes is not None and f["bytes"] != nbytes):
                out.append(f"{pvc}: campaign artifact {r.get('artifact_sha256')}: its {k} is not a verified member "
                           "of the evidence-archive export")
        man = files.get(r.get("manifest_member")) or {}
        if man.get("artifact_sha256") != r.get("artifact_sha256") or \
                man.get("sidecar_sha256") != r.get("sidecar_sha256"):
            out.append(f"{pvc}: campaign artifact {r.get('artifact_sha256')}: the exported bundle manifest does not "
                       "carry its original artifact/sidecar hashes")
    return out


def export_empty(record: X.Record, d: str, pvc: str, info: Dict, digest: str, done: Dict) -> Dict:
    """The reviewed empty-by-design export: an empty tar (no reader pod) and a manifest with the reason, the current
    (post-cutoff) pairs' hashes and byte counts, and every campaign artifact's verified evidence-archive member
    references, checked against the evidence-archive export before sealing."""
    import io
    root = evidence(record)["export_dir"]
    if "evidence-archive-pvc" not in done:
        raise X.StagingError(f"{pvc}: the evidence-archive export must precede an empty-by-design export")
    ea_m = os.path.join(root, EA_DIR, "manifest.json")
    ex = {"tar": f"{EA_DIR}/campaign-export.tar", "tar_sha256": done["evidence-archive-pvc"]["tar"]["sha256"],
          "manifest": f"{EA_DIR}/manifest.json", "manifest_sha256": C.sha256_file(ea_m),
          "source_pvc_uid": done["evidence-archive-pvc"]["pvc_uid"]}
    ebd = copy.deepcopy(info["empty_by_design"])
    ebd["campaign_artifacts"] = [dict(r, export=ex) for r in ebd["campaign_artifacts"]]
    probs = archive_reference_problems(pvc, ebd["campaign_artifacts"], root)
    if probs:
        raise X.StagingError("; ".join(probs))
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT):
        pass
    data = buf.getvalue()
    tar_path = os.path.join(d, "campaign-export.tar")
    _write_identical(tar_path, data)
    manifest = {"schema": "c28-pvc-export/v1", "run_id": record.run_id, "cluster_uid": record.cluster_uid,
                "pvc": pvc, "pvc_uid": info["binding"]["uid"], "pv": info["binding"]["pv"],
                "pv_uid": info["binding"]["pv_uid"], "rules": info["rules"],
                "cutoffs": sorted({r["cutoff"] for r in info["rules"]}), "reader_sha256": READER_SHA256,
                "order_tolerances": order_tolerances(info["rules"]),
                "populator_sha256": C.sha256_file(os.path.abspath(__file__)), "plan_sha256": digest, "files": [],
                "empty_by_design": ebd, "rule_excluded": info["rule_excluded"],
                "reviewed_exclusions": info["reviewed_exclusions"], "volume_totals_at_probe": info["volume"],
                "tar": {"file": "campaign-export.tar", "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}}
    mtext = (json.dumps(manifest, indent=1, sort_keys=True) + "\n").encode()
    C.assert_clean(mtext.decode())
    _write_identical(os.path.join(d, "manifest.json"), mtext)
    sums = (f"{manifest['tar']['sha256']}  campaign-export.tar\n"
            f"{hashlib.sha256(mtext).hexdigest()}  manifest.json\n").encode()
    _write_identical(os.path.join(d, "SHA256SUMS"), sums)
    X.receipt(record, HELPER, "pvc-exported", {"pvc": pvc, "files": 0, "empty_by_design": True,
                                               "tar_sha256": manifest["tar"]["sha256"]})
    return manifest


def coverage(record: X.Record, s: Dict, p: Dict, manifests: Dict[str, Dict], cons: Dict[str, List[Dict]]) -> bytes:
    inv = _inventory(record, s)
    pvs = {v["claim"]["uid"]: v for v in inv["objects"].get("persistentvolumes", []) if v.get("claim")}
    ea = manifests.get("evidence-archive-pvc", {}).get("files", [])
    archived = {(f["artifact_sha256"], f["sidecar_sha256"]): f for f in ea if "sidecar_sha256" in f}
    claims = []
    for r in s["retained_pvcs"]:
        pv = pvs.get(r["uid"], {})
        entry = {"claim": r["name"], "namespace": NS, "uid": r["uid"], "pv": pv.get("name"),
                 "pv_uid": pv.get("uid"), "reclaim_policy": pv.get("reclaim_policy"),
                 "storage_class": pv.get("storage_class"), "consumers_at_export": [
                     {k: c[k] for k in ("pod", "uid", "owner", "read_only")} for c in cons.get(r["name"], [])],
                 "required": (s.get("requirements") or {}).get(r["name"], "")}
        if r["name"] in manifests and manifests[r["name"]].get("empty_by_design"):
            m = manifests[r["name"]]
            e = m["empty_by_design"]
            mpath = os.path.join(evidence(record)["export_dir"], "pvc-exports", r["name"], "manifest.json")
            entry["protected"] = {
                "empty_by_design": True, "reason": e["reason"], "cutoff": e["cutoff"], "files": 0,
                "export": f"pvc-exports/{r['name']}/campaign-export.tar", "tar_sha256": m["tar"]["sha256"],
                "manifest": f"pvc-exports/{r['name']}/manifest.json", "manifest_sha256": C.sha256_file(mpath),
                "plan_sha256": m["plan_sha256"],
                "current_pairs_trained_after_cutoff": e["current_pairs_trained_after_cutoff"],
                "campaign_artifacts": e["campaign_artifacts"],
                "ensemble_generations": e.get("ensemble_generations"),
                "statement": "the campaign model artifacts are protected by the listed VERIFIED evidence-archive "
                             "members, not by this empty tar"}
            entry["residual_not_backed_up"] = {
                "description": "the current (post-campaign) models are RETAINED on the claim but NOT backed up by S0; "
                               "no PVC/PV data deletion, overwrite or live storage change is authorised",
                "volume_files_at_probe": m["volume_totals_at_probe"]["files"],
                "volume_bytes_at_probe": m["volume_totals_at_probe"]["bytes"]}
        elif r["name"] in manifests:
            m = manifests[r["name"]]
            entry["protected"] = {"export": f"pvc-exports/{r['name']}/campaign-export.tar",
                                  "manifest": f"pvc-exports/{r['name']}/manifest.json", "files": len(m["files"]),
                                  "bytes": sum(f["bytes"] for f in m["files"]), "tar_sha256": m["tar"]["sha256"],
                                  "cutoffs": m["cutoffs"],
                                  "verified": "every member against the probe's sha256 and the writers' original "
                                              "hashes"}
            if r["name"] == "ml-models-pvc":
                entry["protected"]["archived_copies"] = [
                    {"model_sidecar": f["member"], "archived_bundle": archived[(f["artifact_sha256"], f["sha256"])]["bundle"]}
                    for f in m["files"] if f.get("artifact_sha256") and (f["artifact_sha256"], f["sha256"]) in archived]
            entry["residual_not_backed_up"] = {
                "description": "every other file of the volume (and any later appends) is RETAINED on the claim but "
                               "NOT backed up by S0", "reviewed_exclusions": m["reviewed_exclusions"],
                "rule_excluded": len(m["rule_excluded"]),
                "volume_files_at_probe": m["volume_totals_at_probe"]["files"],
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
        live["problems"] = [p for p in live["problems"] if not p.startswith("the shared legacy-scope lock")]
        C.require_reviewed(reviewed, digest, HELPER, record.doc)
        if live["problems"]:
            raise X.StagingError("the export plan has problems: " + "; ".join(live["problems"]))
        X.require_live(reviewed, digest, HELPER, record, live)
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
            manifests[pvc] = export_pvc(record, kube, lock, s, pvc, live["pvcs"][pvc], exe, sleep, now, digest,
                                        manifests)
        os.makedirs(os.path.join(root, "storage"), exist_ok=True)
        _write_identical(os.path.join(root, "storage", "pvc-retention-and-export-coverage.json"),
                         coverage(record, s, live, manifests, consumers(kube)))
        sums = "".join(f"{C.sha256_file(os.path.join(root, m))}  {m}\n" for m in sorted(ev["mandatory"])
                       if m != "S0-SHA256SUMS")
        _write_identical(os.path.join(root, "S0-SHA256SUMS"), sums.encode())
        v = verify(record)
        if v["problems"]:
            raise X.StagingError("the populated export does not verify: " + "; ".join(v["problems"][:10]))
        return X.receipt(record, HELPER, "export-complete", {
            "plan_sha256": digest, "files": v["files"],
            "s0_sha256sums": C.sha256_file(os.path.join(root, "S0-SHA256SUMS"))})


def verify(record: X.Record) -> Dict:
    """Read-only: export_dir holds exactly the mandatory files, every checksum manifest verifies at the destination,
    and every PVC export's tar matches its manifest member by member (sha256)."""
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
            probs += [f"{pvc}: {x}" for x in verify_tar(os.path.join(d, "campaign-export.tar"), files,
                                                        {"trailer": {"bytes": m["tar"]["bytes"],
                                                                     "sha256": m["tar"]["sha256"]}, "files": files})]
            if m.get("empty_by_design"):
                if files:
                    probs.append(f"{pvc}: declared empty by design but lists payloads")
                probs += archive_reference_problems(pvc, m["empty_by_design"].get("campaign_artifacts") or [], root)
                cov = json.load(open(os.path.join(root, "storage", "pvc-retention-and-export-coverage.json")))
                c = next((c for c in cov.get("claims", []) if c.get("claim") == pvc), {}).get("protected") or {}
                if c.get("campaign_artifacts") != m["empty_by_design"]["campaign_artifacts"] or \
                        c.get("current_pairs_trained_after_cutoff") != \
                        m["empty_by_design"]["current_pairs_trained_after_cutoff"] or \
                        c.get("manifest_sha256") != C.sha256_file(os.path.join(d, "manifest.json")):
                    probs.append(f"{pvc}: the coverage file does not carry the manifest's verified references")
            elif not files or any(f["bytes"] <= 0 for f in files):
                probs.append(f"{pvc}: the export lists no or empty payloads")
    return {"helper": HELPER, "action": "verify", "export_dir": root, "files": len(have), "problems": probs,
            "result": "PASS" if not probs else "FAIL"}


# --- recovery ---------------------------------------------------------------------------------------------------------

def recover_plan(record: X.Record, kube: X.Kube) -> Dict:
    """Read-only: every unresolved reader lifecycle with the pods of its nonce still present and its volume state."""
    actions = []
    for helper in (HELPER, PROBE):
        for n, it in _unresolved_steps(record, helper):
            pods = kube.list("Pod", NS, labels={NONCE_LABEL: it["nonce"]})
            node = attached_node(kube, it["pv"])
            actions.append({"helper": helper, "step": n, "pvc": it["pvc"], "nonce": it["nonce"],
                            "pods": sorted(p["metadata"]["name"] for p in pods), "attached_now": node,
                            "attached_before": it["attached_before"]})
    legacy = [f"{h}/{n}" for h in LEGACY_HELPERS for n, _ in _unresolved_steps(record, h)]
    lp = C.lock_path(record.doc, LOCK_SCOPE)
    return X.new_plan("populate-s0-recover", record, actions, lock=lp,
                      lock_sha256=C.sha256_file(lp) if os.path.exists(lp) else "",
                      legacy_unresolved=legacy, leftover_readers=leftover_readers(kube, record))


def recover(record: X.Record, kube: X.Kube, reviewed: Dict, digest: str, sleep=time.sleep,
            now=time.monotonic) -> Dict:
    """recovery_guard (the lock's owner proven gone); for each unresolved reader lifecycle: delete the pods of its
    nonce (durable delete intents) and PROVE cleanup, then reconciled-<n>; release the shared lock only when nothing of
    S0 OR legacy stays unresolved and no reader pod of the run remains."""
    live = recover_plan(record, kube)
    X.require_live(reviewed, digest, "populate-s0-recover", record, live)
    with C.recovery_guard(record.doc, LOCK_SCOPE, live["lock_sha256"]) as guard:
        C.require_cluster(kube.api, record.cluster_uid)
        for a in live["actions"]:
            proof = prove_cleanup(record, kube, a["helper"], a["step"], a["pvc"],
                                  _unresolved_steps_map(record, a["helper"])[a["step"]]["pv"], a["nonce"],
                                  a["attached_before"], sleep, now)
            C.append_receipt(record.doc, a["helper"], f"reconciled-{a['step']}",
                             {"observed": "done", "cleanup": proof})
        if leftover_readers(kube, record):
            raise X.StagingError("reader pods of the run remain: the lock stays")
        if live["legacy_unresolved"]:
            raise X.StagingError(f"legacy intents {live['legacy_unresolved']} are unresolved: the shared lock stays "
                                 "(legacy.py recover)")
        guard.release_stale()
        return X.receipt(record, HELPER, "recovered", {"plan_sha256": digest, "reconciled": len(live["actions"])})


def _unresolved_steps_map(record: X.Record, helper: str) -> Dict[str, Dict]:
    return dict(_unresolved_steps(record, helper))


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
