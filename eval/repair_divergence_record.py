#!/usr/bin/env python3
"""Rebuild the divergence record that `reproduce_divergence.py` threw away (Codex C-95).

The 2026-09-21T22:53Z run wrote every unseeded repetition under the single JSON key "None",
so `eval/divergence.json` kept ONE outcome per arm out of five. The console log
(`eval/divergence.log`) recorded all forty, and is the surviving primary record of that run.

This script parses that log and emits the record the run should have written: one entry per
repetition with a unique id, an explicit failure type, failures counted separately from
threshold exceedances, and spread over SUCCESSFUL repetitions only.

It re-derives nothing by re-training. The unseeded repetitions are not reproducible by
construction, so re-running would produce a DIFFERENT experiment, not this one. What cannot
be recovered is stated as null rather than invented: `worst_abs_error` was written only for
the surviving (last) repetition of each arm, so it is null for the other four.

    eval/.venv/bin/python eval/repair_divergence_record.py \
        --log eval/divergence.log --old eval/divergence.json \
        --out eval/divergence-repaired.json
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(HERE))

HEADER = re.compile(r"^=== (?P<scenario>\w+) \(data seed (?P<seed>-?\d+), "
                    r"(?P<history>\w+) history, (?P<epochs>\d+) epochs, unseeded\)")
REP = re.compile(r"^\s{2}(?P<arm>\S+)\s+seed\s+(?P<seed>\S+): MAE (?P<value>.+?)"
                 r"(?P<div>\s+DIVERGED)?\s+\[(?P<secs>[\d.]+)s\]\s*$")

# The log's only failure wording. Anything else that is not a number is refused rather than
# silently classified.
FAILURE_WORDS = {"non-finite forecast": "non_finite_forecast",
                 "no scored origins": "no_scored_origins"}


def parse_log(text: str) -> list:
    """-> [{scenario, data_seed, history, epochs, arms: {arm: [rep, ...]}}] in log order."""
    scenarios, current = [], None
    counters: dict = {}
    for line in text.splitlines():
        h = HEADER.match(line)
        if h:
            current = {"scenario": h["scenario"], "data_seed": int(h["seed"]),
                       "history": h["history"], "epochs": int(h["epochs"]), "arms": {}}
            scenarios.append(current)
            counters = {}
            continue
        m = REP.match(line)
        if not m or current is None:
            continue
        arm = m["arm"]
        counters[arm] = counters.get(arm, 0) + 1
        rec = {"rep_id": f"rep{counters[arm]:02d}",
               "model_seed": None if m["seed"] == "None" else int(m["seed"]),
               "train_seconds": float(m["secs"]),
               "worst_abs_error": None,
               "source": "eval/divergence.log (2026-09-21T22:53Z run)"}
        raw = m["value"].strip()
        try:
            rec.update({"failure_type": None, "mae": float(raw)})
        except ValueError:
            if raw not in FAILURE_WORDS:
                raise SystemExit(f"unrecognised outcome in the log, refusing to guess: {raw!r}")
            rec.update({"failure_type": FAILURE_WORDS[raw], "failure_detail": raw})
        current["arms"].setdefault(arm, []).append(rec)
    return scenarios


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default=str(HERE / "divergence.log"))
    ap.add_argument("--old", default=str(HERE / "divergence.json"))
    ap.add_argument("--out", default=str(HERE / "divergence-repaired.json"))
    args = ap.parse_args()

    from reproduce_divergence import (ARMS, DIVERGENCE_FACTOR, ORIGINAL, PERMITTED_CLAIM,
                                      WITHHELD_CLAIM, summarise_arm)

    parsed = parse_log(Path(args.log).read_text())
    old = json.loads(Path(args.old).read_text())
    old_by_scenario = {r["scenario"]: r for r in old["results"]}

    results = []
    for sc in parsed:
        prior = old_by_scenario[sc["scenario"]]
        ref = prior["seasonal_baseline_mae"]
        threshold = prior["divergence_threshold"]
        entry = {k: sc[k] for k in ("scenario", "data_seed", "history", "epochs")}
        entry.update({
            "train_points": prior["train_points"],
            "scored_origins": prior["scored_origins"],
            "seasonal_baseline_mae": ref,
            "divergence_threshold": threshold,
            "trainings_per_repetition": 1,
            "trainings_per_run_in_the_withdrawn_sweep":
                "~11 (rolling retrain) -- NOT matched here",
            "arms": {},
        })
        for arm, reps in sc["arms"].items():
            # The one field the overwrite destroyed: only the LAST repetition's
            # worst_abs_error survived into the old JSON. Restore it where it belongs and
            # leave the rest null rather than fabricate.
            survivor = (prior["arms"].get(arm, {}).get("seeds", {}) or {}).get("None")
            if survivor and survivor.get("mae") is not None:
                for r in reps:
                    if r.get("mae") == survivor["mae"] and r["worst_abs_error"] is None:
                        r["worst_abs_error"] = survivor.get("worst_abs_error")
                        r["worst_abs_error_note"] = "the only repetition the old record kept"
                        break
            summary = summarise_arm(reps, threshold=threshold)
            summary["config"] = ARMS[arm]
            entry["arms"][arm] = summary
        results.append(entry)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "kind": "divergence_reproduction",
        "record_version": 2,
        "rebuilt_from": {
            "log": args.log, "defective_json": args.old,
            "why": "reproduce_divergence.py keyed repetitions by model seed; every unseeded "
                   "repetition had the seed None, so four of five were overwritten per arm "
                   "(Codex C-95 / D-120). The log is the surviving complete record.",
            "not_recoverable": "per-repetition worst_abs_error, except the one repetition "
                               "the defective record happened to keep",
        },
        "original_conditions": ORIGINAL,
        "divergence_factor": DIVERGENCE_FACTOR,
        "permitted_claim": PERMITTED_CLAIM,
        "withheld_claim": WITHHELD_CLAIM,
        "arms": ARMS,
        "results": results,
    }
    Path(args.out).write_text(json.dumps(payload, indent=2, default=str))

    for e in results:
        print(f"\n{e['scenario']} (baseline {e['seasonal_baseline_mae']}, "
              f"threshold {e['divergence_threshold']})")
        for arm, a in e["arms"].items():
            print(f"  {arm:14s} failed {a['failed_runs']}/{a['runs']}  "
                  f"over-threshold {a['threshold_exceeded_runs']}/{a['successful_runs']}  "
                  f"min {a['mae_min']} max {a['mae_max']} spread {a['max_over_min']} "
                  f"(n={a['successful_runs']})  {a['failures_by_type'] or ''}")
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
