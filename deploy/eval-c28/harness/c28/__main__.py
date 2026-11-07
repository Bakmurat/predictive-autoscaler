"""Command line of the C2.8 harness.

  python -m c28 render   --params P [--out FILE]               all fixture/arm manifests (multi-document YAML)
  python -m c28 plan     --kind ordinary|scored --seed S --capacity C --target T [--accelerate N] [--out FILE]
  python -m c28 faults   --params P --block-start 2026-11-20T00:00:00Z --targets T1,T2 [--out FILE]
  python -m c28 order                                          the frozen arm-session order (JSON)
  python -m c28 runlock  lock|verify FILE [--manifest-root DIR]
  python -m c28 collect  --runlock FILE --query-api URL --namespace NS --targets T1,T2 --start S --end E --out DIR
                         [--file LABEL=PATH ...]
  python -m c28 verify-bundle DIR
  python -m c28 reduce   --bundle DIR --target T --block-start S --capacity C [--out FILE]
  python -m c28 score    --study FILE [--out FILE]
  python -m c28 audit    --params P --live FILE                exclusive-writer check of live writer objects (JSON list)
  python -m c28 quota    --runlock FILE --params P deny_new_pods|restore [--apply]
"""

from __future__ import annotations

import argparse
import json
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="c28", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("render"); r.add_argument("--params", required=True); r.add_argument("--out", default="")
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
    for k in ("--runlock", "--query-api", "--namespace", "--targets", "--out"):
        c.add_argument(k, required=True)
    c.add_argument("--start", required=True); c.add_argument("--end", required=True)
    c.add_argument("--file", action="append", default=[], help="LABEL=PATH, repeatable")
    vb = sub.add_parser("verify-bundle"); vb.add_argument("dir")
    rd = sub.add_parser("reduce")
    rd.add_argument("--bundle", required=True); rd.add_argument("--target", required=True)
    rd.add_argument("--block-start", required=True); rd.add_argument("--capacity", type=float, required=True)
    rd.add_argument("--out", default="")
    s = sub.add_parser("score"); s.add_argument("--study", required=True); s.add_argument("--out", default="")
    a = sub.add_parser("audit"); a.add_argument("--params", required=True); a.add_argument("--live", required=True)
    q = sub.add_parser("quota"); q.add_argument("--runlock", required=True); q.add_argument("--params", required=True)
    q.add_argument("action", choices=["deny_new_pods", "restore"]); q.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)

    if args.cmd == "render":
        _write(yaml.safe_dump_all(render.render_all(_load(args.params)), sort_keys=True), args.out)
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
        digest = runlock.verify(args.runlock)
        files = {}
        for spec in args.file:
            label, _, path = spec.partition("=")
            files.setdefault(label, []).append(path)
        idx = collect.collect(args.out, args.query_api, args.targets.split(","), args.namespace,
                              int(_utc(args.start).timestamp()), int(_utc(args.end).timestamp()), digest, files)
        print(_json({"gaps": idx["gaps"]}), end="")
        return 1 if idx["gaps"] else 0
    elif args.cmd == "verify-bundle":
        probs = collect.verify_bundle(args.dir)
        print("\n".join(probs) or "bundle intact")
        return 1 if probs else 0
    elif args.cmd == "reduce":
        _write(_json(reduce.block_metrics(args.bundle, args.target, int(_utc(args.block_start).timestamp()),
                                          args.capacity)), args.out)
    elif args.cmd == "score":
        result = score.score(_load(args.study))
        _write(_json(result), args.out)
    elif args.cmd == "audit":
        params = _load(args.params)
        probs = render.audit_live(render.expected_writers(params), _load(args.live))
        print("\n".join(probs) or "every fixture target has exactly its one writer")
        return 1 if probs else 0
    elif args.cmd == "quota":
        lock = _load(args.runlock)
        if args.apply:
            runlock.verify(args.runlock)
        for cmd in inject.apply(lock, _load(args.params), args.action, dry_run=not args.apply):
            print(" ".join(cmd))
    return 0


if __name__ == "__main__":
    sys.exit(main())
