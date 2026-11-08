#!/usr/bin/env python3
"""Archive the legacy benchmark before anything stops (STAGING-C28-PROD.md S0; "Helpers to implement": archive.py).

  archive.py --record R plan                                   read-only (default): what will be archived
  archive.py --record R snapshot --plan P --plan-digest D      sanitized object snapshots (reads the cluster only)
  archive.py --record R export --plan P --plan-digest D        copy the allowlisted evidence files off-cluster
  archive.py --record R velero-submit --plan P --plan-digest D create the reviewed Velero Backup (the one mutation)
  archive.py --record R velero-receipt --plan P                read-only: that submitted Backup's phase, errors,
                                                               warnings (bound to its UID, run and reviewed spec)
  archive.py --record R verify --plan P                        the archive-complete receipt, or a refusal

The plan binds: the sanitized snapshots of every allowlisted legacy object and every writer of a legacy target (with
the claims' PVs), the campaign source revisions (git ls-tree of archive.source_revision.commit, the reviewed
benchmark revision, never the helper commit source.commit nor the working tree: blob ids of deploy/prodcluster,
deploy/eks-benchmark and k8s-manifests/base, each required), the populate-s0 export-complete receipt the evidence
comes from (archive.evidence.export_receipt, with the S0-SHA256SUMS digest it certified; every file S0-SHA256SUMS
certifies is re-hashed here and at the destination, and the archived evidence is exactly those files plus the
manifest, whatever checksum_manifests lists), the evidence files the approved export procedure staged locally (record archive.evidence.export_dir; only files matching
archive.evidence.allowlist, never a live
database file or WAL; every file of the explicit archive.evidence.mandatory manifest present; every checksum manifest
in archive.evidence.checksum_manifests verified file by file, here and again at the destination), and the Velero
Backup specification (unique name, explicit storage location, scope, TTL;
labelled metadata-only when it does not back up volume data). Snapshots never contain a Secret value, ConfigMap
contents, a literal credential env value or a last-applied configuration; no Secret or ConfigMap is archived at all.
`verify` recomputes every hash: the snapshots, the received copies at the off-cluster destination, the source list;
it requires the Backup Completed with zero errors and each warning assessed in the record; any gap refuses the
archive-complete receipt that legacy.py requires before S4.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shutil
import subprocess
import sys
from typing import Dict, List, Mapping, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common_records as C  # noqa: E402
import inventory as I  # noqa: E402
import legacy_allowlist as L  # noqa: E402

HELPER = "archive"
POPULATE = "populate-s0"
S0_SUMS = "S0-SHA256SUMS"
SOURCE_PATHS = ("deploy/prodcluster", "deploy/eks-benchmark", "k8s-manifests/base")
LIVE_DB = ("*-wal", "*-shm", "*-journal", "*.sqlite", "*.sqlite3", "*.db")


def _campaign_source(rec: Mapping, run=subprocess.run) -> Dict:
    """The campaign's source trees at archive.source_revision: an explicit, reviewed commit (the benchmark's), never
    the helper commit in source.commit, so an archive cannot silently record whatever the executor has checked out."""
    sr = (rec.get("archive") or {}).get("source_revision")
    if not isinstance(sr, dict) or set(sr) != {"repo_dir", "commit"} or not sr["repo_dir"]:
        raise C.StagingError("archive.source_revision = {repo_dir, commit} is required: the campaign's source revision "
                             "(source.commit is the helper's and is never used here)")
    commit = str(sr["commit"])
    if not C._COMMIT.match(commit):
        raise C.StagingError("archive.source_revision.commit must be a full git commit")
    repo = os.path.expanduser(str(sr["repo_dir"]))
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    v = run(["git", "-C", repo, "rev-parse", "--verify", "--quiet", f"{commit}^{{commit}}"], capture_output=True,
            text=True, env=env)
    if v.returncode != 0 or v.stdout.strip() != commit:
        raise C.StagingError(f"archive.source_revision.commit {commit} is not a commit in {repo}")
    p = run(["git", "-C", repo, "ls-tree", "-r", commit, "--", *SOURCE_PATHS], capture_output=True, text=True, env=env)
    if p.returncode != 0 or not p.stdout.strip():
        raise C.StagingError(f"git ls-tree of {commit} failed or is empty")
    blobs = {}
    for line in p.stdout.splitlines():
        meta, _, path = line.partition("\t")
        blobs[path] = meta.split()[2]
    missing = [d for d in SOURCE_PATHS if not any(b.startswith(d + "/") for b in blobs)]
    if missing:
        raise C.StagingError(f"the campaign source revision {commit} lacks {', '.join(missing)}")
    return {"commit": commit, "paths": list(SOURCE_PATHS), "blobs": blobs}


def _s0_entries(root: str) -> Dict[str, str]:
    """S0-SHA256SUMS as {path: sha256}, every entry verified against the file it certifies (always, whatever
    archive.evidence.checksum_manifests lists)."""
    problems = checksum_chain_problems(root, [S0_SUMS])
    if problems:
        raise C.StagingError(f"{S0_SUMS} does not verify: " + "; ".join(problems))
    out = {}
    for ln in open(os.path.join(root, S0_SUMS)).read().splitlines():
        if ln.strip():
            want, rel = ln.split(None, 1)
            out[os.path.normpath(rel.lstrip("*").strip())] = want.lower()
    return out


def _export_binding(rec: Mapping) -> Dict:
    """The completed populate-s0 export this archive takes its evidence from: its export-complete receipt (by hash, in
    the verified chain), the S0-SHA256SUMS digest it certified, which must still match the staged file, and every
    file that manifest certifies, re-hashed."""
    ev = rec["archive"].get("evidence") or {}
    ref = ev.get("export_receipt")
    if not isinstance(ref, dict) or set(ref) != {"sha256", "s0_sha256sums"}:
        raise C.StagingError("archive.evidence.export_receipt = {sha256, s0_sha256sums} is required: the populate-s0 "
                             "export-complete receipt the evidence comes from")
    done = [r for r in C.read_receipts(C.receipts_path(rec, POPULATE)) if r["sha256"] == ref["sha256"]]
    if not done or done[0]["event"] != "export-complete" or done[0]["run_id"] != rec["run_id"]:
        raise C.StagingError("archive.evidence.export_receipt names no populate-s0 export-complete receipt of this run")
    root = ev.get("export_dir") or ""
    sums = os.path.join(root, S0_SUMS)
    if done[0]["data"].get("s0_sha256sums") != ref["s0_sha256sums"] or not os.path.isfile(sums) \
            or C.sha256_file(sums) != ref["s0_sha256sums"]:
        raise C.StagingError(f"{S0_SUMS} does not match the export-complete receipt: the staged export changed")
    entries = _s0_entries(root)
    return {"receipt_sha256": ref["sha256"], "s0_sha256sums": ref["s0_sha256sums"],
            "plan_sha256": done[0]["data"].get("plan_sha256"), "entries": len(entries),
            "entries_sha256": C.digest(entries)}


def _inventory_problems(evidence: List[Mapping], entries: Mapping[str, str]) -> List[str]:
    """The archived evidence is exactly what S0-SHA256SUMS certifies, plus that manifest itself."""
    have = {f["path"]: f["sha256"] for f in evidence if f["path"] != S0_SUMS}
    out = [f"{p} is archived but not certified by {S0_SUMS}" for p in sorted(set(have) - set(entries))]
    out += [f"{p} is certified by {S0_SUMS} but not archived" for p in sorted(set(entries) - set(have))]
    out += [f"{p} differs from its {S0_SUMS} entry" for p in sorted(set(have) & set(entries)) if have[p] != entries[p]]
    if not any(f["path"] == S0_SUMS for f in evidence):
        out.append(f"{S0_SUMS} itself is not archived")
    return out


def _evidence_files(rec: Mapping) -> List[Dict]:
    ev = rec["archive"].get("evidence") or {}
    root, allow = ev.get("export_dir"), ev.get("allowlist") or []
    if not root or not allow:
        raise C.StagingError("archive.evidence needs export_dir and a non-empty allowlist")
    files, refused = [], []
    for d, _, names in os.walk(root):
        for n in sorted(names):
            rel = os.path.relpath(os.path.join(d, n), root)
            if any(fnmatch.fnmatch(n, pat) for pat in LIVE_DB):
                refused.append(f"{rel} (a live database file: export it with its supported snapshot procedure)")
            elif not any(fnmatch.fnmatch(rel, pat) for pat in allow):
                refused.append(f"{rel} (not on the evidence allowlist)")
            else:
                files.append({"path": rel, "sha256": C.sha256_file(os.path.join(root, rel)),
                              "bytes": os.path.getsize(os.path.join(root, rel))})
    if refused:
        raise C.StagingError("export directory holds files that are not archived as evidence: " + "; ".join(refused))
    if not files:
        raise C.StagingError("no evidence file matches the allowlist")
    mandatory = ev.get("mandatory") or []
    if not mandatory:
        raise C.StagingError("archive.evidence.mandatory must list every required artifact explicitly (allowlisting "
                             "alone cannot show that nothing is missing)")
    have = {f["path"] for f in files}
    missing = sorted(set(mandatory) - have)
    if missing:
        raise C.StagingError(f"mandatory artifacts are missing from the export: {missing}")
    chains = ev.get("checksum_manifests") or []
    if not set(chains) <= set(mandatory):
        raise C.StagingError("every checksum manifest must itself be a mandatory artifact")
    problems = checksum_chain_problems(root, chains)
    if problems:
        raise C.StagingError("checksum chains do not verify: " + "; ".join(problems))
    return sorted(files, key=lambda f: f["path"])


def checksum_chain_problems(root: str, manifests) -> List[str]:
    """Each checksum manifest (sha256sum format: "<hex>  <path relative to the manifest's directory>") must list at
    least one file, and every listed file must exist under root with that hash (a chain is verified, not copied)."""
    out = []
    for m in manifests:
        mpath = os.path.join(root, m)
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
            want, rel = parts[0].lower(), parts[1].lstrip("*").strip()
            target = os.path.abspath(os.path.join(os.path.dirname(mpath), rel))
            if not target.startswith(os.path.abspath(root) + os.sep):
                out.append(f"{m}: {rel} lies outside the export")
            elif not os.path.isfile(target):
                out.append(f"{m}: {rel} is missing")
            elif C.sha256_file(target) != want:
                out.append(f"{m}: {rel} does not match its recorded checksum")
    return out


def velero_backup(rec: Mapping) -> Dict:
    v = rec["archive"].get("velero") or {}
    need = ("namespace", "backup_name", "storage_location", "ttl", "metadata_only", "included_namespaces")
    missing = [k for k in need if k not in v]
    if missing:
        raise C.StagingError(f"archive.velero needs {missing}")
    if not isinstance(v["metadata_only"], bool):
        raise C.StagingError("archive.velero.metadata_only must be true or false")
    labels = {"c28.devkuban.com/run": rec["run_id"], "c28.devkuban.com/metadata-only": str(v["metadata_only"]).lower()}
    spec = {"storageLocation": v["storage_location"], "ttl": v["ttl"],
            "includedNamespaces": list(v["included_namespaces"]),
            "includedClusterScopedResources": list(v.get("included_cluster_resources") or []),
            "excludedResources": ["secrets"] if v.get("exclude_secrets", False) else [],
            "snapshotVolumes": not v["metadata_only"], "defaultVolumesToFsBackup": False}
    return {"apiVersion": "velero.io/v1", "kind": "Backup",
            "metadata": {"name": v["backup_name"], "namespace": v["namespace"], "labels": labels}, "spec": spec}


def reviewed_missing_problems(rec: Mapping, missing: List[Mapping]) -> List[str]:
    """archive.unaccounted_reviewed must name EXACTLY the allowlisted objects found missing, each once, by identity
    (kind, namespace, name, role) with a non-empty reason; no blanket boolean, no extra or absent entry."""
    reviewed = rec["archive"].get("unaccounted_reviewed", [])
    if not isinstance(reviewed, list):
        return ["archive.unaccounted_reviewed must be a list of {kind, namespace, name, role, reason} entries"]
    keys, out = [], []
    for e in reviewed:
        if not isinstance(e, dict) or set(e) != {"kind", "namespace", "name", "role", "reason"} or \
                not isinstance(e.get("reason"), str) or not e["reason"].strip():
            out.append(f"unaccounted_reviewed entry {e!r} needs exactly kind, namespace, name, role and a reason")
            continue
        keys.append((e["kind"], e["namespace"], e["name"], e["role"]))
    if len(keys) != len(set(keys)):
        out.append("unaccounted_reviewed lists an object twice")
    want = {(m["kind"], m["namespace"], m["name"], m["role"]) for m in missing}
    if set(keys) != want:
        out.append(f"the reviewed missing set differs from the missing set: unreviewed "
                   f"{sorted(want - set(keys))}, reviewed but present {sorted(set(keys) - want)}")
    return out


def build_plan(api, rec: Mapping, run=subprocess.run) -> Dict:
    inv = I.collect(api, rec)
    errs = reviewed_missing_problems(rec, inv["legacy"]["missing"])
    if errs:
        raise C.StagingError("plan refused: " + "; ".join(errs))
    objs = []
    for e in inv["legacy"]["found"]:
        objs.append({k: e[k] for k in ("kind", "namespace", "name", "uid")})
    targets = {("demo", a) for a in L.ARMS}
    for kind in ("horizontalpodautoscalers", "scaledobjects", "pa-legacy", "pa-new"):
        for w in inv["writers"].get(kind) or []:
            t = w.get("target") or {}
            if (t.get("namespace") or w["namespace"], t.get("name")) in targets and \
                    not any(o["uid"] == w["uid"] for o in objs):
                objs.append({"kind": kind, "namespace": w["namespace"], "name": w["name"], "uid": w["uid"]})
    for pv in inv["objects"]["persistentvolumes"]:
        objs.append({"kind": "persistentvolumes", "namespace": None, "name": pv["name"], "uid": pv["uid"]})
    backup = velero_backup(rec)
    vpath = C.path_of("backups", backup["metadata"]["namespace"], backup["metadata"]["name"])
    if "velero.io" not in inv["api_groups"]:
        raise C.StagingError("the velero.io API group is not served: no Velero backup is possible")
    if api.get(vpath) is not None:
        raise C.StagingError(f"a Velero Backup named {backup['metadata']['name']} exists: choose a unique name")
    evidence = _evidence_files(rec)                  # its own chains first, then the populate export binding
    binding = _export_binding(rec)
    gaps = _inventory_problems(evidence, _s0_entries(rec["archive"]["evidence"]["export_dir"]))
    if gaps:
        raise C.StagingError("the evidence is not the certified populate-s0 export: " + "; ".join(gaps))
    return C.make_plan(HELPER, rec, [], inventory_sha256=C.digest(inv), snapshots=objs,
                       unaccounted=inv["legacy"]["missing"], blockers=inv["legacy"]["blockers"],
                       campaign_source=_campaign_source(rec, run), export=binding,
                       evidence=evidence, velero=backup,
                       mandatory=sorted(rec["archive"]["evidence"]["mandatory"]),
                       checksum_manifests=sorted(rec["archive"]["evidence"].get("checksum_manifests") or []),
                       destination=rec["archive"]["evidence"].get("destination"))


def _dir(rec: Mapping, *parts) -> str:
    return os.path.join(rec["evidence_dir"], "archive", *parts)


def snapshot(api, rec: Mapping, plan: Mapping, reviewed: str, clock=None) -> Dict:
    C.require_reviewed(plan, reviewed, HELPER, rec)
    C.require_cluster(api, rec["cluster_uid"])
    manifest = []
    for s in plan["snapshots"]:
        o = api.get(C.path_of(s["kind"], s["namespace"], s["name"]))
        if o is None or (o.get("metadata") or {}).get("uid") != s["uid"]:
            raise C.StagingError(f"{s['kind']} {s['namespace']}/{s['name']} is gone or has another UID; refusing")
        rel = os.path.join("objects", s["kind"], s["namespace"] or "_cluster", f"{s['name']}.json")
        sha = C.write_json_once(_dir(rec, rel), C.sanitize_object(o))
        manifest.append(dict(s, file=rel, sha256=sha))
    mpath = _dir(rec, "snapshots-manifest.json")
    msha = C.write_json_once(mpath, {"plan_sha256": reviewed, "snapshots": manifest})
    C.append_receipt(rec, HELPER, "snapshot", {"plan_sha256": reviewed, "manifest_sha256": msha,
                                               "objects": len(manifest)}, clock)
    return {"manifest": mpath, "sha256": msha}


def export(rec: Mapping, plan: Mapping, reviewed: str, clock=None) -> Dict:
    """Copy the planned evidence files to the off-cluster destination and verify what arrived (never overwrites)."""
    C.require_reviewed(plan, reviewed, HELPER, rec)
    src, dst = rec["archive"]["evidence"]["export_dir"], plan["destination"]
    if not dst:
        raise C.StagingError("archive.evidence.destination (the off-cluster directory) is required")
    for f in plan["evidence"]:
        s, d = os.path.join(src, f["path"]), os.path.join(dst, f["path"])
        if C.sha256_file(s) != f["sha256"]:
            raise C.StagingError(f"{f['path']} changed since the plan; refusing")
        if os.path.exists(d):
            raise C.StagingError(f"{d} exists at the destination: evidence is never overwritten")
        os.makedirs(os.path.dirname(d), exist_ok=True)
        shutil.copyfile(s, d)
    received = verify_destination(plan)
    C.append_receipt(rec, HELPER, "export", {"plan_sha256": reviewed, "files": len(plan["evidence"]),
                                             "received_ok": not received}, clock)
    if received:
        raise C.StagingError("the destination does not hold the exported files intact: " + "; ".join(received))
    return {"files": len(plan["evidence"])}


def verify_destination(plan: Mapping) -> List[str]:
    out = []
    planned = {f["path"] for f in plan["evidence"]}
    out += [f"mandatory {m} was not planned" for m in plan.get("mandatory", []) if m not in planned]
    for f in plan["evidence"]:
        d = os.path.join(plan["destination"] or "", f["path"])
        if not os.path.isfile(d):
            out.append(f"{f['path']} missing at the destination")
        elif C.sha256_file(d) != f["sha256"]:
            out.append(f"{f['path']} differs at the destination")
    if not out:
        chains = sorted(set(plan.get("checksum_manifests") or []) | {S0_SUMS})
        out += [f"at the destination: {p}" for p in checksum_chain_problems(plan["destination"], chains)]
    return out


def velero_submit(api, rec: Mapping, plan: Mapping, reviewed: str, clock=None) -> Dict:
    C.require_reviewed(plan, reviewed, HELPER, rec)
    C.require_cluster(api, rec["cluster_uid"])
    b = plan["velero"]
    path = C.path_of("backups", b["metadata"]["namespace"], b["metadata"]["name"])
    if api.get(path) is not None:
        raise C.StagingError("the Backup exists already; never resubmitted under the same name")
    created = api.create(C.path_of("backups", b["metadata"]["namespace"]), b)
    r = C.append_receipt(rec, HELPER, "velero-submitted", {"plan_sha256": reviewed, "name": b["metadata"]["name"],
                                                           "uid": (created.get("metadata") or {}).get("uid")}, clock)
    return r


def _submitted(rec: Mapping, pdig: str) -> Optional[Dict]:
    subs = [r["data"] for r in C.read_receipts(C.receipts_path(rec, HELPER))
            if r["event"] == "velero-submitted" and r["data"]["plan_sha256"] == pdig]
    return subs[-1] if subs else None


def velero_receipt(api, rec: Mapping, plan: Mapping, clock=None) -> Dict:
    """The status of exactly the Backup this plan submitted: same UID, run label and reviewed spec."""
    C.require_cluster(api, rec["cluster_uid"])
    pdig = C.plan_digest(plan)
    sub = _submitted(rec, pdig)
    if sub is None:
        raise C.StagingError("this plan submitted no Backup")
    b = plan["velero"]
    o = api.get(C.path_of("backups", b["metadata"]["namespace"], b["metadata"]["name"]))
    if o is None:
        raise C.StagingError("the Backup does not exist")
    m, spec = o.get("metadata") or {}, o.get("spec") or {}
    if m.get("uid") != sub["uid"]:
        raise C.StagingError("the Backup has another UID than the one this plan submitted (replaced); refusing")
    if (m.get("labels") or {}).get("c28.devkuban.com/run") != rec["run_id"] or \
            any(spec.get(k) != v for k, v in b["spec"].items()):
        raise C.StagingError("the Backup's labels or spec differ from the reviewed specification; refusing")
    st = o.get("status") or {}
    data = {"plan_sha256": pdig, "name": b["metadata"]["name"], "uid": m.get("uid"), "phase": st.get("phase"),
            "errors": st.get("errors", 0), "warnings": st.get("warnings", 0),
            "metadata_only": b["spec"]["snapshotVolumes"] is False,
            "storage_location": (o.get("spec") or {}).get("storageLocation"),
            "started": st.get("startTimestamp"), "completed": st.get("completionTimestamp")}
    C.append_receipt(rec, HELPER, "velero-status", data, clock)
    return data


def verify(rec: Mapping, plan: Mapping, clock=None) -> Dict:
    """Every hash recomputed; Velero Completed with zero errors and assessed warnings; nothing unaccounted."""
    pdig = C.plan_digest(plan)
    problems = []
    receipts = C.read_receipts(C.receipts_path(rec, HELPER))
    snaps = [r for r in receipts if r["event"] == "snapshot" and r["data"]["plan_sha256"] == pdig]
    mpath = _dir(rec, "snapshots-manifest.json")
    if not snaps or not os.path.exists(mpath) or C.sha256_file(mpath) != snaps[-1]["data"]["manifest_sha256"]:
        problems.append("no intact snapshot manifest for this plan")
    else:
        with open(mpath) as fh:
            man = json.load(fh)
        if len(man["snapshots"]) != len(plan["snapshots"]):
            problems.append("the snapshot manifest does not cover every planned object")
        for s in man["snapshots"]:
            p = _dir(rec, s["file"])
            if not os.path.exists(p) or C.sha256_file(p) != s["sha256"]:
                problems.append(f"snapshot {s['file']} is missing or changed")
            else:
                with open(p) as fh:
                    try:
                        C.assert_clean(fh.read())
                    except C.StagingError:
                        problems.append(f"snapshot {s['file']} is not sanitized")
    problems += verify_destination(plan)
    if not plan["campaign_source"]["blobs"]:
        problems.append("no campaign source revisions recorded")
    try:
        if _export_binding(rec) != plan["export"]:
            problems.append("the populate-s0 export binding differs from the plan")
    except C.StagingError as e:
        problems.append(str(e))
    sub = _submitted(rec, pdig)
    status = [r["data"] for r in receipts if r["event"] == "velero-status" and sub is not None
              and r["data"].get("plan_sha256") == pdig and r["data"]["uid"] == sub["uid"]]
    if not status:
        problems.append("no Velero status receipt")
    else:
        v = status[-1]
        assessed = (rec["archive"].get("velero") or {}).get("warnings_assessed") or []
        if v["phase"] != "Completed" or v["errors"]:
            problems.append(f"Velero backup is {v['phase']} with {v['errors']} errors")
        if v["warnings"] and len(assessed) != v["warnings"]:
            problems.append(f"{v['warnings']} Velero warnings, {len(assessed)} assessed in "
                            f"archive.velero.warnings_assessed")
    problems += reviewed_missing_problems(rec, plan["unaccounted"])
    if plan["blockers"]:
        problems.append("inventory blockers: " + "; ".join(plan["blockers"]))
    if problems:
        C.append_receipt(rec, HELPER, "archive-refused", {"plan_sha256": pdig, "problems": problems}, clock)
        raise C.StagingError("archive incomplete: " + "; ".join(problems))
    r = C.append_receipt(rec, HELPER, "archive-complete", {"plan_sha256": pdig, "velero": status[-1],
                                                          "snapshots": len(plan["snapshots"]),
                                                          "evidence_files": len(plan["evidence"]),
                                                          "helper_commit": plan["source_commit"],
                                                          "campaign_commit": plan["campaign_source"]["commit"],
                                                          "export_receipt_sha256": plan["export"]["receipt_sha256"]},
                              clock)
    return r


def main(argv=None, api=None, run=subprocess.run, clock=None) -> int:
    ap = argparse.ArgumentParser(prog="archive.py", description=__doc__.splitlines()[0])
    C.add_common_args(ap)
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("plan")
    for name in ("snapshot", "export", "velero-submit"):
        p = sub.add_parser(name)
        p.add_argument("--plan", required=True)
        p.add_argument("--plan-digest", required=True)
    vr = sub.add_parser("velero-receipt")
    vr.add_argument("--plan", required=True)
    v = sub.add_parser("verify")
    v.add_argument("--plan", required=True)
    a = ap.parse_args(argv)
    cmd = a.cmd or "plan"
    try:
        rec = C.load_record(a.record, ("archive",))
        api = api or C.api_for(rec)
        if cmd == "plan":
            plan = build_plan(api, rec, run)
            path = _dir(rec, f"plan-{C.digest(plan)[:12]}.json")
            C.write_json_once(path, plan)
            vpath = _dir(rec, f"velero-backup-reviewed-{C.digest(plan)[:12]}.json")
            C.write_json_once(vpath, plan["velero"])
            print(f"plan: {path}\nvelero spec: {vpath}\nplan digest (review, then pass --plan-digest): "
                  f"{C.plan_digest(plan)}")
        elif cmd == "snapshot":
            print(json.dumps(snapshot(api, rec, C.load_plan(a.plan), a.plan_digest, clock)))
        elif cmd == "export":
            print(json.dumps(export(rec, C.load_plan(a.plan), a.plan_digest, clock)))
        elif cmd == "velero-submit":
            print(json.dumps(velero_submit(api, rec, C.load_plan(a.plan), a.plan_digest, clock)["data"]))
        elif cmd == "velero-receipt":
            print(json.dumps(velero_receipt(api, rec, C.load_plan(a.plan), clock), sort_keys=True))
        else:
            r = verify(rec, C.load_plan(a.plan), clock)
            print(f"archive complete: receipt {r['sha256']} (set legacy.archive_receipt_sha256 to it)")
    except C.StagingError as e:
        print(f"archive: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
