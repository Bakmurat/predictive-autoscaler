"""Command line of the C2.8 harness.

  python -m c28 render   --params P [--out FILE]               all fixture/arm manifests (multi-document YAML)
  python -m c28 release-values --params P [--out FILE]         the evaluation release's chart values (routes, TLS name)
  python -m c28 plan     --kind ordinary|scored --seed S --capacity C --target T [--accelerate N] [--out FILE]
  python -m c28 faults   --params P --block-start 2026-11-20T00:00:00Z --targets T1,T2 [--out FILE]
  python -m c28 order                                          the frozen arm-session order (JSON)
  python -m c28 runlock  lock|verify FILE [--manifest-root DIR]
  python -m c28 collect  --runlock FILE --targets T1,T2 --start S --end E --out DIR [--file LABEL=PATH ...]
                         (query API, namespace and query definitions come from the run-lock)
  python -m c28 verify-bundle DIR
  python -m c28 reduce   --runlock FILE --bundle DIR --target T --seed S --block-start D [--out FILE]
                         (verifies the bundle's hashes, run-lock, scope, queries, the seed's plan and a permitted
                         date; capacity from the run-lock)
  python -m c28 score    --runlock FILE --study FILE [--out FILE]
                         (every block names its bundle and is re-derived from it; reused or edited evidence is refused)
  Commands that take --runlock also take --manifest-root DIR (default: the run-lock's directory).
  python -m c28 audit    --params P --live FILE                exclusive-writer check of live writer objects (JSON list)
  python -m c28 quota    --runlock FILE --params P deny_new_pods|restore [--apply]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

import yaml

from . import collect, inject, reduce, render, runlock, schedule, score


def _write(text: str, out: str) -> None:
    if out:
        with open(out, "w") as fh:
            fh.write(text)
    else:
        sys.stdout.write(text)


def _json(obj) -> str:
    return json.dumps(obj, indent=1, sort_keys=True) + "\n"


def _load(path: str):
    with open(path) as fh:
        return json.load(fh)


def _utc(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


class BindingError(ValueError):
    pass


def _verified(args):
    root = args.manifest_root or os.path.dirname(os.path.abspath(args.runlock))
    return runlock.load_verified(args.runlock, root)


def _within(lock, start: int, end: int) -> None:
    lo, hi = (int(_utc(lock["schedule"][k]).timestamp()) for k in ("start_utc", "end_utc"))
    if not lo <= start < end <= hi:
        raise BindingError("the time range lies outside the run-lock's schedule")


def _arm_profile(lock: dict, target: str):
    for key in lock["arms"]:
        arm, profile = key.split("/")
        if render.target_name(arm, profile) == target:
            return arm, profile
    raise BindingError(f"{target} is not a target of the run-lock")


def reduce_bound(lock: dict, digest: str, bundle: str, target: str, block_start: int, seed: int) -> dict:
    """Reduce one scored block only from an intact bundle collected under this run-lock, with its locked queries and
    scope, the locked plan of exactly this seed and target, on one of the block's permitted dates; the result carries
    its binding."""
    arm, profile = _arm_profile(lock, target)
    start_text = datetime.fromtimestamp(block_start, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if start_text not in lock["schedule"]["blocks"].get(f"{arm}/{profile}/{seed}", []):
        raise BindingError(f"{start_text} is not a permitted date for {arm}/{profile}/{seed}")
    probs = collect.verify_bundle(bundle)
    if probs:
        raise BindingError("the bundle's hashes do not verify: " + "; ".join(probs[:5]))
    with open(os.path.join(bundle, "index.json")) as fh:
        index = json.load(fh)
    col = lock["collection"]
    if index.get("runlock_sha256") != digest:
        raise BindingError("the bundle was collected under another run-lock")
    if index.get("namespace") != col["namespace"] or str(index.get("query_api", "")).rstrip("/") != col["query_api"].rstrip("/"):
        raise BindingError("the bundle's namespace or query API differs from the run-lock")
    entries = {q["name"]: q for q in index["queries"] if q["target"] == target}
    for q in lock["queries"]:
        e = entries.get(q["name"])
        if e is None or e["expr"] != q["expr"].format(target=target, namespace=col["namespace"]) or e["step"] != q["step"]:
            raise BindingError(f"query {q['name']} is not the locked definition")
    if not int(_utc(index["start"]).timestamp()) <= block_start < block_start + 86_400 <= int(_utc(index["end"]).timestamp()):
        raise BindingError("the block lies outside the bundle's range")
    _within(lock, block_start, block_start + 86_400)
    capacity = lock["capacity"]["c_rps"]
    want = lock["workload"]["plans"].get(f"scored/{seed}/{target}")
    plan = None
    for f in index["files"]:
        if f["label"] != "plan":
            continue
        with open(os.path.join(bundle, f["file"])) as fh:
            cand = json.load(fh)
        if cand.get("digest") == want and schedule.digest(cand) == want:
            plan = cand
    if want is None or plan is None or plan.get("target") != target or plan.get("seed") != seed or \
            plan.get("accelerate") != 1 or plan.get("kind") != "scored" or float(plan.get("capacity_rps", -1)) != float(capacity):
        raise BindingError(f"the bundle lacks the locked confirmation plan of seed {seed} for {target} (label plan)")
    out = reduce.block_metrics(bundle, target, block_start, float(capacity), plan["scheduled_per_minute"])
    with open(os.path.join(bundle, "SHA256SUMS"), "rb") as fh:
        sums = hashlib.sha256(fh.read()).hexdigest()
    out["binding"] = {"runlock_sha256": digest, "bundle_sums_sha256": sums, "target": target, "seed": seed,
                      "block_start": start_text, "capacity_rps": capacity, "plan_digest": want}
    return out


_MEASURED = ("recorder_coverage", "dispatch_fraction_min", "reconciled")


def check_bindings(study: dict, lock: dict, digest: str) -> None:
    """Every scored block is re-derived from its own bundle under this run-lock and must equal it: its seed's locked
    plan, a permitted date, the same binding, windows, day and measured validity. No evidence serves two blocks."""
    seen_evidence, seen_slots = set(), set()
    for b in study.get("blocks", []):
        where = f"{b.get('arm')}/{b.get('profile')}/{b.get('seed')}"
        bind, bundle = b.get("binding") or {}, b.get("bundle")
        if not bundle or not bind:
            raise BindingError(f"block {where} has no bundle or binding")
        target = render.target_name(b.get("arm"), b.get("profile"))
        if bind.get("target") != target or bind.get("seed") != b.get("seed"):
            raise BindingError(f"block {where} is bound to another target or seed")
        start = int(_utc(bind["block_start"]).timestamp())
        evidence = (bind.get("bundle_sums_sha256"), target, bind["block_start"])
        if evidence in seen_evidence or (target, bind["block_start"]) in seen_slots:
            raise BindingError(f"block {where} reuses evidence of another block")
        seen_evidence.add(evidence)
        seen_slots.add((target, bind["block_start"]))
        fresh = reduce_bound(lock, digest, bundle, target, start, b.get("seed"))
        if fresh["binding"] != bind:
            raise BindingError(f"block {where}: the binding differs from its re-derived reduction")
        if fresh["windows"] != b.get("windows") or fresh["day"] != b.get("day") or \
                any(fresh[k] != (b.get("validity") or {}).get(k) for k in _MEASURED):
            raise BindingError(f"block {where}: the metrics differ from its re-derived reduction")


def check_applicability(study: dict, lock: dict) -> None:
    """Section 12: the study scores under exactly the run-lock's applicability declaration (the frozen one, bound to
    the same manifest digest), and every PA block reports the family field the run-lock recorded for its arm."""
    if study.get("applicability") != lock.get("applicability"):
        raise BindingError("the study's applicability declaration is not the run-lock's")
    for b in study.get("blocks", []):
        rec = lock["arms"].get(f"{b.get('arm')}/{b.get('profile')}", {})
        if b.get("families") is not None and b["families"] != rec.get("families"):
            raise BindingError(f"block {b.get('arm')}/{b.get('profile')}/{b.get('seed')}: families differ from the "
                               f"run-lock's")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="c28", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("render"); r.add_argument("--params", required=True); r.add_argument("--out", default="")
    rv = sub.add_parser("release-values"); rv.add_argument("--params", required=True); rv.add_argument("--out", default="")
    p = sub.add_parser("plan")
    p.add_argument("--kind", required=True, choices=["ordinary", "scored"]); p.add_argument("--seed", type=int, required=True)
    p.add_argument("--capacity", type=float, required=True); p.add_argument("--target", required=True)
    p.add_argument("--accelerate", type=int, default=1); p.add_argument("--out", default="")
    f = sub.add_parser("faults"); f.add_argument("--params", required=True); f.add_argument("--block-start", required=True)
    f.add_argument("--targets", required=True); f.add_argument("--out", default="")
    sub.add_parser("order")
    rl = sub.add_parser("runlock"); rl.add_argument("action", choices=["lock", "verify"]); rl.add_argument("file")
    rl.add_argument("--manifest-root", default="")
    c = sub.add_parser("collect")
    for k in ("--runlock", "--targets", "--out"):
        c.add_argument(k, required=True)
    c.add_argument("--start", required=True); c.add_argument("--end", required=True)
    c.add_argument("--file", action="append", default=[], help="LABEL=PATH, repeatable")
    vb = sub.add_parser("verify-bundle"); vb.add_argument("dir")
    rd = sub.add_parser("reduce")
    rd.add_argument("--runlock", required=True); rd.add_argument("--bundle", required=True)
    rd.add_argument("--target", required=True); rd.add_argument("--block-start", required=True)
    rd.add_argument("--seed", type=int, required=True)
    rd.add_argument("--out", default="")
    s = sub.add_parser("score"); s.add_argument("--runlock", required=True); s.add_argument("--study", required=True)
    s.add_argument("--out", default="")
    for sp in (c, rd, s):
        sp.add_argument("--manifest-root", default="")
    a = sub.add_parser("audit"); a.add_argument("--params", required=True); a.add_argument("--live", required=True)
    q = sub.add_parser("quota"); q.add_argument("--runlock", required=True); q.add_argument("--params", required=True)
    q.add_argument("--manifest-root", default="")
    q.add_argument("action", choices=["deny_new_pods", "restore"]); q.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)

    if args.cmd == "render":
        _write(yaml.safe_dump_all(render.render_all(_load(args.params)), sort_keys=True), args.out)
    elif args.cmd == "release-values":
        _write(yaml.safe_dump(render.eval_release_values(_load(args.params)), sort_keys=True), args.out)
    elif args.cmd == "plan":
        _write(_json(schedule.k6_plan(args.kind, args.seed, args.capacity, args.target, args.accelerate)), args.out)
    elif args.cmd == "faults":
        params = _load(args.params)
        cms = render.with_faults(render.render_all(params), schedule.fault_timetable(_utc(args.block_start)),
                                 args.targets.split(","))
        _write(yaml.safe_dump_all(cms, sort_keys=True), args.out)
    elif args.cmd == "order":
        _write(_json(schedule.session_order()), "")
    elif args.cmd == "runlock":
        fn = runlock.lock if args.action == "lock" else (lambda path: runlock.verify(path, args.manifest_root))
        print(fn(args.file))
    elif args.cmd == "collect":
        digest, lock = _verified(args)
        targets = args.targets.split(",")
        unknown = sorted(set(targets) - set(runlock.locked_targets(lock)))
        if unknown:
            raise BindingError(f"not targets of the run-lock: {unknown}")
        start, end = int(_utc(args.start).timestamp()), int(_utc(args.end).timestamp())
        _within(lock, start, end)
        files = {}
        for spec in args.file:
            label, _, path = spec.partition("=")
            files.setdefault(label, []).append(path)
        col = lock["collection"]
        idx = collect.collect(args.out, col["query_api"], targets, col["namespace"], start, end, digest, files,
                              queries=lock["queries"])
        print(_json({"gaps": idx["gaps"]}), end="")
        return 1 if idx["gaps"] else 0
    elif args.cmd == "verify-bundle":
        probs = collect.verify_bundle(args.dir)
        print("\n".join(probs) or "bundle intact")
        return 1 if probs else 0
    elif args.cmd == "reduce":
        digest, lock = _verified(args)
        _write(_json(reduce_bound(lock, digest, args.bundle, args.target, int(_utc(args.block_start).timestamp()),
                                  args.seed)), args.out)
    elif args.cmd == "score":
        digest, lock = _verified(args)
        study = _load(args.study)
        check_bindings(study, lock, digest)
        check_applicability(study, lock)
        _write(_json(score.score(study)), args.out)
    elif args.cmd == "audit":
        params = _load(args.params)
        probs = render.audit_live(render.expected_writers(params), _load(args.live))
        print("\n".join(probs) or "every fixture target has exactly its one writer")
        return 1 if probs else 0
    elif args.cmd == "quota":
        _, lock = _verified(args)
        for cmd in inject.apply(lock, _load(args.params), args.action, dry_run=not args.apply):
            print(" ".join(cmd))
    return 0


if __name__ == "__main__":
    sys.exit(main())
