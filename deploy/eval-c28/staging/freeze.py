#!/usr/bin/env python3
"""S5c: assemble the run-lock and check separately signed reviewer receipts (STAGING-C28-PROD.md, S5c).

  freeze.py --record R [plan]                       read-only: the assembled run-lock, its problems and digest
  freeze.py --record R assemble --plan F --plan-digest D   write the run-lock (new file), lock it (<file>.sha256), and the
                                                    confirmation k6 plans it names (new files)
  freeze.py --record R receipt-body --reviewer NAME read-only: the canonical receipt a reviewer signs themselves
  freeze.py --record R verify                       read-only: run-lock verified, >= 2 distinct signed reviewer
                                                    receipts attesting its canonical digest before the schedule start,
                                                    and the status of every gate as its signed receipts record it
  freeze.py --record R plan-qualification           read-only: the lab-qualification/v1 document IF every gate G0-G4
                                                    has a signed "pass" from two distinct reviewers
  freeze.py --record R qualification --plan F --plan-digest D   write that document (a new file), validated by qualification.py

No automatic gate approval: this helper never writes, infers or defaults a gate outcome, and never signs. A gate is
"pass" only when two distinct allowlisted reviewers each signed a gate receipt saying so for this run-lock digest;
anything else is reported as it is (pending, fail, unsigned) and the qualification is refused. The harness checksum
(<runlock>.sha256) is an integrity lock, not a signature: reviewer receipts are verified with `ssh-keygen -Y verify`
against the record's allowed-signers file (namespaces c28-reviewer-receipt and c28-gate-receipt).

What the run-lock takes from where:
  derived  schema, run_id, kubeconfig (path, context, cluster UID), infrastructure (reserve section + its applied
           receipt), slo/operating_point/seeds/session_order/applicability (frozen), capacity.c_rps (0.875 c0), the
           rendered sha256 and families of every arm (from the params), confirmation plans and programs, scorer hash,
           chart and CRD hashes (from the clean source), images and source commit (images.json), activation scope
           (targets.json) and fault scope (the quota prepare.py created, by UID)
  inputs   the reviewed measurements and decisions in the freeze section's inputs file: manifest path, capacity
           c0_rps, cpu_tuning, startup, queries, collection, schedule, admission, data_licenses, retention,
           acknowledgments
The record's "freeze" section:
  {"inputs": "<file>", "source_dir": "<clean checkout of the reviewed commit>", "images": "<images.json>",
   "manifest_root": "<dir>", "runlock": "<new run-lock path>", "allowed_signers": "<ssh allowed signers file>",
   "reviewers": ["<principal>", ...], "receipts_dir": "<dir of <name>.json + <name>.json.sig>",
   "gates_dir": "<dir of gate receipts + .sig>", "prerequisites": {"<label>": {"path": "...", "sha256": "..."}},
   "qualification": {"not_before": "...", "expires": "...", "out": "<new file>"}}
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
from typing import Callable, Dict, List, Tuple

import execlib as X

HELPER = "freeze"
QUALIFY = "freeze-qualification"
RECEIPT_SCHEMA = "c28-reviewer-receipt/v1"
GATE_SCHEMA = "c28-gate-receipt/v1"
RECEIPT_NS = "c28-reviewer-receipt"
GATE_NS = "c28-gate-receipt"
QUAL_GATES = ("G0", "G1", "G2", "G3", "G4")
INPUT_KEYS = ("manifest", "capacity", "cpu_tuning", "startup", "queries", "collection", "schedule", "admission",
              "data_licenses", "retention", "acknowledgments")


def _section(record: X.Record) -> Dict:
    s = record.section("freeze")
    for k in ("inputs", "source_dir", "images", "runlock", "allowed_signers", "receipts_dir"):
        if not s.get(k):
            raise X.StagingError(f"freeze.{k} is required")
    if len(set(s.get("reviewers") or [])) < 2:
        raise X.StagingError("freeze.reviewers must name at least two distinct reviewers")
    return s


def tree_sha256(root: str) -> str:
    """A deterministic hash of a directory: every file's relative path and content hash, sorted."""
    h = []
    for dirpath, dirnames, files in os.walk(root):
        dirnames.sort()
        for f in sorted(files):
            p = os.path.join(dirpath, f)
            h.append(f"{os.path.relpath(p, root)}\0{X.sha256_file(p)}")
    if not h:
        raise X.StagingError(f"{root} is empty or missing")
    return X.sha256_bytes("\n".join(sorted(h)).encode())


def _load(path: str):
    with open(path) as fh:
        return json.load(fh)


def _images(record: X.Record, s: Dict) -> Tuple[Dict, str]:
    doc = _load(record.resolve(s["images"]))
    if doc.get("schema") != "c28-images/v1" or doc.get("verified") is not True:
        raise X.StagingError("images.json must be a verified c28-images/v1 record (build_images.py apply)")
    if doc.get("source_commit") != record.commit:
        raise X.StagingError("images.json was built from another commit than the record's source.commit")
    return {k: v["ref"] for k, v in sorted(doc["images"].items())}, doc["source_commit"]


def _prepared(record: X.Record) -> Dict:
    import prepare
    entries = X.receipts(record, "prepare")
    created = {(k, n): ref for (k, _, n), ref in prepare.created_objects(record).items()}
    phases = {e["data"]["phase"] for e in entries if e["event"] == "phase-complete"}
    return {"created": created, "phases": phases}


def _infrastructure(record: X.Record) -> Tuple[Dict, List[str]]:
    s = record.section("reserve")
    import reserve
    entries = X.receipts(record, "reserve")
    held = reserve.reserved_nodes(record)
    nodes, probs = [], []
    if X.open_intents(entries):
        probs.append("the reserve journal has intents with unknown outcomes: run recover first")
    for role in ("workload", "support"):
        n = s["nodes"][role]
        if n["uid"] not in held:
            probs.append(f"the {role} node's reservation is not applied in the reserve receipts")
        nodes.append({"role": role, "name": n["name"], "uid": n["uid"]})
    return {"nodes": nodes, "reservation_receipts_sha256": entries[-1]["sha256"] if entries else ""}, probs


def assemble_doc(record: X.Record) -> Tuple[Dict, Dict, List[str]]:
    """(run-lock document, confirmation plans by key, problems)."""
    c28 = X.import_harness()
    F, R, S = c28.frozen, c28.render, c28.schedule
    s = _section(record)
    params = X.load_params(record)
    inputs = _load(record.resolve(s["inputs"]))
    problems: List[str] = []
    missing = [k for k in INPUT_KEYS if k not in inputs]
    if missing:
        problems.append(f"inputs lack {missing}")
        return {}, {}, problems
    root = record.resolve(s.get("manifest_root", "."))
    mpath = os.path.join(root, inputs["manifest"]["path"])
    manifest = {"path": inputs["manifest"]["path"],
                "sha256": X.sha256_file(mpath) if os.path.isfile(mpath) else ""}
    if not manifest["sha256"]:
        problems.append(f"the manifest {mpath} is not available")
    images, commit = _images(record, s)
    src = record.resolve(s["source_dir"])
    c0 = inputs["capacity"].get("c0_rps")
    if c0 != params["c0_rps"]:
        problems.append("capacity.c0_rps differs from the params the targets were rendered with")
    try:
        c_rps = R.capacity_rps(c0)
    except (R.ParamsError, TypeError, ValueError) as e:
        problems.append(f"capacity: {e}")
        return {}, {}, problems
    targets_path = os.path.join(record.evidence_dir, "targets.json")
    if not os.path.exists(targets_path):
        problems.append("targets.json is missing (prepare.py phase targets)")
        return {}, {}, problems
    targets = _load(targets_path)
    infra, p = _infrastructure(record)
    problems += p
    prep = _prepared(record)
    for ph in ("common", "targets", "pas"):
        if ph not in prep["phases"]:
            problems.append(f"prepare phase {ph} is not complete in the receipts")
    quota = prep["created"].get(("ResourceQuota", params["quota"]["name"]))
    if quota is None:
        problems.append("the fixture quota prepare.py created is not in the receipts")
    arms, plans = {}, {}
    for profile in F.PROFILES:
        for arm in F.ARMS:
            objs = R.render_arm(params, arm, profile)
            entry = {"rendered_sha256": X.sha256_bytes(X.canonical(objs))}
            if arm in F.PA_ARMS:
                entry.update({"families": list(F.PA_FAMILIES[arm]), "effective_candidates": list(F.PA_CANDIDATES[arm])})
            arms[f"{arm}/{profile}"] = entry
            t = R.target_name(arm, profile)
            for seed in F.CONFIRMATION_SEEDS:
                plans[f"scored/{seed}/{t}"] = S.k6_plan("scored", seed, float(c_rps), t)
    programs = {f"{k}/{sd}": S.program(k, sd) for k in ("ordinary", "scored")
                for sd in F.CONFIRMATION_SEEDS + F.TUNING_SEEDS}
    doc = {
        "schema": c28.runlock.SCHEMA, "run_id": record.run_id, "manifest": manifest,
        "source": {"commit": commit, "dir_tree_sha256": tree_sha256(os.path.join(src, "deploy", "eval-c28", "harness", "c28"))},
        "images": images,
        "chart_sha256": tree_sha256(os.path.join(src, "charts", "predictive-autoscaler")),
        "crd_sha256": tree_sha256(os.path.join(src, "charts", "predictive-autoscaler", "crds")),
        "kubeconfig": {"path": record.doc["kubeconfig"]["path"], "context": record.doc["kubeconfig"]["context"],
                       "cluster_uid": record.cluster_uid},
        "infrastructure": infra,
        "slo": {"p95_seconds": F.SLO_P95_S, "p99_seconds": F.SLO_P99_S, "error_fraction": F.SLO_ERROR_FRACTION,
                "timeout_seconds": F.REQUEST_TIMEOUT_S},
        "workload": {"plans": {k: v["digest"] for k, v in sorted(plans.items())},
                     "programs_sha256": X.sha256_bytes(X.canonical(programs))},
        "capacity": {"c0_rps": c0, "c_rps": c_rps},
        "cpu_tuning": dict(inputs["cpu_tuning"], selected_target_percent=params["hpa_cpu_target_percent"]),
        "arms": arms,
        "operating_point": {"capacity_ratio": F.CAPACITY_RATIO, "nominal_coverage_bps": F.NOMINAL_COVERAGE_BPS,
                            "extra_max_replicas": F.EXTRA_MAX_REPLICAS, "extra_max_percent": F.EXTRA_MAX_PERCENT,
                            "pa_headroom_percent": F.PA_HEADROOM_PERCENT},
        "seeds": {"confirmation": list(F.CONFIRMATION_SEEDS), "tuning": list(F.TUNING_SEEDS), "order": F.ORDER_SEED,
                  "bootstrap": F.BOOTSTRAP_SEED, "bootstrap_resamples": F.BOOTSTRAP_RESAMPLES},
        "session_order": S.session_order(),
        "startup": inputs["startup"], "queries": inputs["queries"], "collection": inputs["collection"],
        "schedule": inputs["schedule"], "admission": inputs["admission"], "data_licenses": inputs["data_licenses"],
        "retention": inputs["retention"],
        "scorer": {"sha256": X.sha256_file(os.path.join(src, "deploy", "eval-c28", "harness", "c28", "score.py")),
                   "bootstrap_seed": F.BOOTSTRAP_SEED},
        "activation_scope": {"cluster_uid": record.cluster_uid, "namespaces": [params["namespace"]],
                             "target_uids": sorted(targets["deployments"].values()),
                             "targets": dict(sorted(targets["deployments"].items()))},
        "fault_scope": {"namespace": params["namespace"], "quota_name": params["quota"]["name"],
                        "quota_uid": (quota or {}).get("uid", ""), "run_label": f"{X.RUN_LABEL}={record.run_id}",
                        "priority_class": params["priority_class"], "pods_normal": params["quota"]["pods_normal"]},
        "acknowledgments": inputs["acknowledgments"],
        "applicability": {"declaration": F.APPLICABILITY, "manifest_sha256": manifest["sha256"]},
    }
    if targets.get("cluster_uid") != record.cluster_uid or len(targets.get("deployments", {})) != 10:
        problems.append("targets.json is not the ten targets of this cluster")
    if images.get("fixture") != params["image"]:
        problems.append("the params' fixture image is not the verified images.json fixture")
    sch = inputs["schedule"]
    qd = sch.get("qualification_deadline_utc")
    if not (isinstance(qd, str) and c28.runlock._TS.match(qd) and sch.get("start_utc", "") <= qd < sch.get("end_utc", "")):
        problems.append("schedule.qualification_deadline_utc (the frozen 14-day qualification deadline) is required "
                        "inside the schedule")
    lat = sch.get("max_release_lateness_s")
    if not (isinstance(lat, int) and not isinstance(lat, bool) and 0 < lat <= 900):
        problems.append("schedule.max_release_lateness_s (the predeclared block lateness limit, 1-900 s) is required")
    for label, pr in sorted((s.get("prerequisites") or {}).items()):
        path = record.resolve(pr.get("path", ""))
        if not os.path.isfile(path) or X.sha256_file(path) != pr.get("sha256"):
            problems.append(f"prerequisite {label}: {pr.get('path')} is missing or its hash differs")
    problems += c28.runlock.problems(doc)
    return doc, plans, problems


def plan(record: X.Record, kube: X.Kube = None, live: bool = False, _plans: Dict = None) -> Dict:
    doc, plans, problems = assemble_doc(record)
    if _plans is not None:
        _plans.update(plans)
    s = _section(record)
    out_path = record.resolve(s["runlock"])
    if os.path.exists(out_path) or os.path.exists(out_path + ".sha256"):
        problems.append(f"{out_path} exists: a run-lock is written and locked once")
    if live and kube is not None and doc:
        problems += live_identity_problems(record, kube, doc)
    c28 = X.import_harness()
    digest = X.sha256_bytes(c28.runlock.canonical(doc)) if doc else ""
    return X.new_plan(HELPER, record, [{"write": out_path, "canonical_sha256": digest},
                                       {"write": "plans/", "files": len(plans)}],
                      runlock_path=out_path, runlock_canonical_sha256=digest, runlock=doc,
                      plan_files=sorted(plans), problems=problems)


def live_identity_problems(record: X.Record, kube: X.Kube, doc: Dict) -> List[str]:
    out = []
    params = X.load_params(record)
    deployed = {d["metadata"]["uid"] for d in kube.list("Deployment", params["namespace"],
                                                         labels={X.RUN_LABEL: record.run_id})}
    missing = sorted(set(doc["activation_scope"]["target_uids"]) - deployed)
    if missing:
        out.append(f"target UIDs not live: {missing}")
    q = kube.get("ResourceQuota", doc["fault_scope"]["quota_name"], doc["fault_scope"]["namespace"])
    if q is None or q["metadata"]["uid"] != doc["fault_scope"]["quota_uid"]:
        out.append("the fixture quota's live UID differs")
    for n in doc["infrastructure"]["nodes"]:
        live = kube.get("Node", n["name"])
        if live is None or live["metadata"]["uid"] != n["uid"]:
            out.append(f"node {n['name']}'s live UID differs")
    if kube.identity() != doc["kubeconfig"]["cluster_uid"]:
        out.append("the cluster identity differs")
    return out


def assemble(record: X.Record, reviewed: Dict, reviewed_digest: str, kube: X.Kube = None, live: bool = False) -> Dict:
    plans: Dict = {}
    p = plan(record, kube, live, plans)
    X.require_live(reviewed, reviewed_digest, HELPER, record, p)
    if p["problems"]:
        raise X.StagingError("the run-lock has problems: " + "; ".join(p["problems"]))
    c28 = X.import_harness()
    plan_dir = os.path.join(os.path.dirname(p["runlock_path"]), "plans")
    for key, body in sorted(plans.items()):
        X.write_new(os.path.join(plan_dir, key.replace("/", "-") + ".json"), json.dumps(body, indent=1, sort_keys=True))
    X.write_new(p["runlock_path"], json.dumps(p["runlock"], indent=1, sort_keys=True) + "\n")
    digest = c28.runlock.lock(p["runlock_path"])
    if digest != p["runlock_canonical_sha256"]:
        raise X.StagingError("the locked digest differs from the reviewed canonical digest")
    return X.receipt(record, HELPER, "runlock-locked", {"plan_sha256": reviewed_digest, "runlock": p["runlock_path"],
                                                        "runlock_sha256": digest, "plans": len(plans)})


# --- signed receipts ------------------------------------------------------------------------------------------------

def _verified_lock(record: X.Record) -> Tuple[str, Dict]:
    c28 = X.import_harness()
    s = _section(record)
    path = record.resolve(s["runlock"])
    root = record.resolve(s.get("manifest_root", os.path.dirname(path)))
    try:
        return c28.runlock.load_verified(path, root)
    except (c28.runlock.RunLockError, OSError) as e:
        raise X.StagingError(f"the run-lock does not verify: {e}") from e


def receipt_body(record: X.Record, reviewer: str, now: Callable[[], str] = X.utcnow) -> Dict:
    s = _section(record)
    if reviewer not in s["reviewers"]:
        raise X.StagingError(f"{reviewer} is not an allowlisted reviewer")
    digest, _ = _verified_lock(record)
    prereq = {k: v.get("sha256") for k, v in sorted((s.get("prerequisites") or {}).items())}
    return {"schema": RECEIPT_SCHEMA, "run_id": record.run_id, "runlock_sha256": digest, "prerequisites": prereq,
            "reviewer": reviewer, "at": now(),
            "statement": "I reviewed this canonical run-lock digest and the listed prerequisite receipts."}


def ssh_verify(allowed: str, principal: str, namespace: str, data: bytes, sig_path: str,
               run: Callable = subprocess.run) -> bool:
    res = run(["ssh-keygen", "-Y", "verify", "-f", allowed, "-I", principal, "-n", namespace, "-s", sig_path],
              input=data, capture_output=True, timeout=30)
    return res.returncode == 0


def _signed_docs(record: X.Record, directory: str, namespace: str, run: Callable) -> List[Tuple[Dict, str]]:
    """(document, problem) for every <file>.json in the directory; problem empty when its signature verifies for the
    reviewer it names, with the record's allowed-signers file."""
    s = _section(record)
    allowed = record.resolve(s["allowed_signers"])
    out = []
    for path in sorted(glob.glob(os.path.join(record.resolve(directory), "*.json"))):
        with open(path, "rb") as fh:
            data = fh.read()
        try:
            doc = json.loads(data)
        except ValueError:
            out.append(({}, f"{os.path.basename(path)} is not JSON"))
            continue
        who = doc.get("reviewer")
        if who not in s["reviewers"]:
            out.append((doc, f"{os.path.basename(path)}: reviewer {who!r} is not allowlisted"))
        elif not os.path.exists(path + ".sig"):
            out.append((doc, f"{os.path.basename(path)} is unsigned"))
        elif not ssh_verify(allowed, who, namespace, data, path + ".sig", run):
            out.append((doc, f"{os.path.basename(path)}: the signature does not verify for {who}"))
        else:
            out.append((doc, ""))
    return out


def _valid_receipts(record: X.Record, runlock_digest: str, run: Callable) -> Tuple[Dict[str, Dict], List[str]]:
    """{reviewer: receipt} of the valid, signed reviewer receipts for this canonical run-lock digest (signed before
    the schedule's start), and why the others do not count."""
    s = _section(record)
    _, lock = _verified_lock(record)
    start = lock["schedule"]["start_utc"]
    good, notes = {}, []
    for doc, prob in _signed_docs(record, s["receipts_dir"], RECEIPT_NS, run):
        if prob:
            notes.append(prob)
        elif doc.get("schema") != RECEIPT_SCHEMA or doc.get("run_id") != record.run_id:
            notes.append(f"{doc.get('reviewer')}: not a reviewer receipt of this run")
        elif doc.get("runlock_sha256") != runlock_digest:
            notes.append(f"{doc.get('reviewer')}: attests another run-lock digest")
        elif not (isinstance(doc.get("at"), str) and doc["at"] < start):
            notes.append(f"{doc.get('reviewer')}: signed at {doc.get('at')}, not before the schedule start {start}")
        else:
            good[doc["reviewer"]] = doc
    return good, notes


def reviewer_status(record: X.Record, runlock_digest: str, run: Callable = subprocess.run) -> Tuple[bool, str]:
    """Two distinct allowlisted reviewers signed receipts for exactly this canonical run-lock digest, before the
    schedule's start."""
    try:
        good, notes = _valid_receipts(record, runlock_digest, run)
    except X.StagingError as e:
        return False, str(e)
    if len(good) < 2:
        return False, f"{len(good)} valid reviewer receipt(s), two distinct required" + (
            f" ({'; '.join(notes)})" if notes else "")
    return True, f"signed by {sorted(good)}"


def signed_prerequisite(record: X.Record, runlock_digest: str, label: str, run: Callable = subprocess.run) -> bool:
    """The prerequisite `label` (freeze.prerequisites) exists with its recorded hash, and at least two distinct valid
    reviewer receipts attest exactly that hash."""
    pr = (_section(record).get("prerequisites") or {}).get(label)
    if not pr:
        return False
    path = record.resolve(pr.get("path", ""))
    if not os.path.isfile(path) or X.sha256_file(path) != pr.get("sha256"):
        return False
    try:
        good, _ = _valid_receipts(record, runlock_digest, run)
    except X.StagingError:
        return False
    return len([r for r in good.values() if (r.get("prerequisites") or {}).get(label) == pr["sha256"]]) >= 2


def gate_status(record: X.Record, runlock_digest: str, run: Callable = subprocess.run) -> Dict[str, Dict]:
    """Each gate's outcome exactly as its signed receipts record it. 'pass' needs two distinct reviewers saying pass
    and nobody saying otherwise; nothing is inferred."""
    s = _section(record)
    per = {g: {"pass": set(), "other": [], "problems": []} for g in QUAL_GATES}
    if s.get("gates_dir"):
        for doc, prob in _signed_docs(record, s["gates_dir"], GATE_NS, run):
            g = doc.get("gate")
            if g not in per:
                continue
            if prob:
                per[g]["problems"].append(prob)
            elif doc.get("schema") != GATE_SCHEMA or doc.get("run_id") != record.run_id or \
                    doc.get("runlock_sha256") != runlock_digest:
                per[g]["problems"].append(f"{doc.get('reviewer')}: not a gate receipt for this run-lock")
            elif doc.get("outcome") == "pass" and doc.get("evidence"):
                per[g]["pass"].add(doc["reviewer"])
            else:
                per[g]["other"].append(f"{doc.get('reviewer')}: {doc.get('outcome')}")
    out = {}
    for g, v in per.items():
        if len(v["pass"]) >= 2 and not v["other"]:
            outcome = "pass"
        elif v["other"]:
            outcome = "not pass (" + "; ".join(v["other"]) + ")"
        else:
            outcome = "pending"
        out[g] = {"outcome": outcome, "signed_pass_by": sorted(v["pass"]), "problems": v["problems"]}
    return out


def verify(record: X.Record, run: Callable = subprocess.run) -> Dict:
    digest, _ = _verified_lock(record)
    ok, detail = reviewer_status(record, digest, run)
    gates = gate_status(record, digest, run)
    return {"helper": HELPER, "action": "verify", "runlock_sha256": digest, "reviewers_ok": ok,
            "reviewers": detail, "gates": gates, "result": "PASS" if ok else "FAIL",
            "note": "gate outcomes are reported, never approved by this helper"}


def plan_qualification(record: X.Record, run: Callable = subprocess.run) -> Dict:
    s = _section(record)
    q = s.get("qualification") or {}
    digest, lock = _verified_lock(record)
    problems = []
    ok, detail = reviewer_status(record, digest, run)
    if not ok:
        problems.append(f"reviewer receipts: {detail}")
    gates = gate_status(record, digest, run)
    for g, v in gates.items():
        if v["outcome"] != "pass":
            problems.append(f"{g} is {v['outcome']}: no qualification without two signed passes")
    # Source, image and scope come from the SIGNED run-lock only; the working files must agree with it.
    commit = lock["source"]["commit"]
    st_ref = lock["images"].get("statistical", "")
    scope = lock["activation_scope"]
    ns = lock["collection"]["namespace"]
    lock_targets = scope.get("targets") or {}
    pa_targets = sorted(n for n in lock_targets if n.startswith("c28-pa-"))
    if sorted(lock_targets.values()) != sorted(scope["target_uids"]) or len(pa_targets) != 6:
        problems.append("the run-lock's activation scope does not name the six PA targets by UID")
    images, img_commit = _images(record, s)
    if images.get("statistical") != st_ref or img_commit != commit:
        problems.append("images.json differs from the signed run-lock (statistical image or source commit)")
    targets = _load(os.path.join(record.evidence_dir, "targets.json"))
    if targets.get("deployments") != lock_targets or targets.get("cluster_uid") != scope["cluster_uid"]:
        problems.append("targets.json differs from the signed run-lock's target UIDs")
    if scope["cluster_uid"] != record.cluster_uid:
        problems.append("the signed run-lock is for another cluster than the record")
    reviewers = sorted({r for v in gates.values() for r in v["signed_pass_by"]})
    doc = {"schema": "lab-qualification/v1", "source_tree": commit, "image_digest": st_ref.partition("@")[2],
           "run_lock_sha256": digest, "gates": {g: "pass" for g in QUAL_GATES}, "reviewers": reviewers,
           "not_before": q.get("not_before", ""), "expires": q.get("expires", ""),
           "scope": {"cluster_uid": scope["cluster_uid"], "namespaces": [ns],
                     "targets": [{"namespace": ns, "name": n, "uid": lock_targets[n]} for n in pa_targets]}}
    if not problems:
        problems += _validate_qualification(doc, commit)
    successor = None
    if q.get("expires", "") and q["expires"] < lock["schedule"]["end_utc"]:
        successor = (f"expires {q['expires']}, before the schedule ends {lock['schedule']['end_utc']}: schedule and "
                     "review a successor qualification in advance (this one is never extended)")
    out = record.resolve(q.get("out", "lab-qualification.json"))
    if os.path.exists(out):
        problems.append(f"{out} exists (a qualification is never rewritten)")
    return X.new_plan(QUALIFY, record, [{"write": out}], file=out, document=doc if not problems else None,
                      gates=gates, successor_required=successor, problems=problems)


def _validate_qualification(doc: Dict, commit: str) -> List[str]:
    ml = os.path.join(os.path.dirname(X.harness_path()), "..", "..", "ml-engine")
    sys.path.insert(0, os.path.abspath(ml))
    try:
        from forecasting import qualification as Q
    except ImportError as e:                       # pragma: no cover - the tree always has it
        return [f"qualification.py is not importable: {e}"]
    try:
        nb = Q._ts(doc["not_before"], "not_before")
        Q.parse(json.dumps(doc).encode(), Q.BuildInfo(commit, True), nb, doc["image_digest"])
    except Q.QualificationRefused as e:
        return [f"qualification.py refuses it: {e}"]
    return []


def qualification(record: X.Record, reviewed: Dict, reviewed_digest: str, run: Callable = subprocess.run) -> Dict:
    p = plan_qualification(record, run)
    X.require_live(reviewed, reviewed_digest, QUALIFY, record, p)
    if p["problems"]:
        raise X.StagingError("no qualification: " + "; ".join(p["problems"]))
    text = json.dumps(p["document"], indent=1, sort_keys=True) + "\n"
    sha = X.write_new(p["file"], text)
    return X.receipt(record, HELPER, "qualification-written", {"plan_sha256": reviewed_digest, "file": p["file"],
                                                               "sha256": sha})


def main(argv=None, api=None, ssh_run=subprocess.run) -> int:
    ap = argparse.ArgumentParser(prog="freeze.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", required=True)
    ap.add_argument("command", nargs="?", default="plan",
                    choices=["plan", "assemble", "receipt-body", "verify", "plan-qualification", "qualification"])
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    ap.add_argument("--reviewer", default="")
    ap.add_argument("--live", action="store_true", help="also check target, quota and node UIDs live (read-only)")
    args = ap.parse_args(argv)
    try:
        record = X.load_record(args.record, ("freeze",))
        kube = record.kube(api) if args.live else None
        if args.command in ("assemble", "qualification") and not args.plan:
            raise X.StagingError("a write needs --plan <the reviewed plan file> and --plan-digest")
        if args.command == "plan":
            X.emit(X.cli_plan(record, HELPER, plan(record, kube, args.live)))
        elif args.command == "assemble":
            X.emit(assemble(record, X.load_plan(args.plan), args.plan_digest, kube, args.live))
        elif args.command == "receipt-body":
            X.emit(receipt_body(record, args.reviewer))
        elif args.command == "verify":
            out = verify(record, ssh_run)
            X.emit(out)
            return 0 if out["result"] == "PASS" else 1
        elif args.command == "plan-qualification":
            X.emit(X.cli_plan(record, QUALIFY, plan_qualification(record, ssh_run), "qualification-plan"))
        else:
            X.emit(qualification(record, X.load_plan(args.plan), args.plan_digest, ssh_run))
    except X.StagingError as e:
        print(f"freeze: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
