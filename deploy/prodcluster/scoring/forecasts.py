#!/usr/bin/env python3
"""Forecast scoring for the prodcluster campaign (evaluation protocol P9: C2, C3 and the forecast part of C4), v3.

Sources (Codex Task 03 r40/r48): the operator's issuance records (extract_decisions.sh v3) are the primary record of
every arm's SERVED forecast; the evidence archive's ensemble records (extract_ensemble.sh v2) add E1/E2′ raw forecasts,
attached only to the generation that served them: same namespace, application and origin, the archive fingerprint equal
to the issuance's artifact_sha256, the archive boundary equal to its training cutoff, the arm's complete declared policy
(experiment, margin mode and quantile, partial rule "refuse"), and all six served values equal at the serving precision
(two decimals); equivalent duplicates are merged; candidates that differ in any field = ambiguous; none = unavailable.
Truth: the P3 canonical query for nginx-test/demo as a point sample at each target (archived through capacity.py's
Store; replayable).

Acceptance (P9 scorer contract, §4), per arm in log order: missing provenance (issuance id, issuance time, origin, target
anchor, model version, training time, training cutoff, artifact) → rejected; duplicates (issuance id or time),
out-of-order issuance times, a training cutoff or training time that moves backwards (millisecond precision), an anchor
other than inference_input_end, an origin off the ten-minute grid, steps other than origin + 10·s min (s = 1..6),
non-finite values and any target not strictly later than both the issuance and the training cutoff → rejected with the
record's id and reason. The first accepted issuance per (arm, origin) is used. Expected cells: every grid origin in
[start, stop) × steps whose target is before the stop, independent of the records.

Three views (P9): UNFILTERED — every cell with truth; OPERATIONAL (primary) — P8-invalid targets excluded for every
predictor; CLEAN-INPUT (secondary) — operational cells whose origin has clean dependencies for every predictor
involved. Dependency envelopes (conservative, Codex r49): hybrid and S1 — everything from the earlier of the
benchmark history start and origin − 7 d through the origin (the hybrid's blend selection chains earlier trainings'
errors and incumbents, hybrid/S1 percentiles carry rolling forecast-error state, and inference does not drop samples
before the history start); ensemble raw — 360 h before the generation boundary (ENSEMBLE_HISTORY_HOURS, verified in
the frozen ml-api configuration) through the origin; ensemble served — additionally the 24-h margin window of
recomputed past forecasts (each with its own 360 h): 16.25 d before the origin. A dependency window that the P8 result
does not cover (its collection runs from start − pre_ms, from its run identity, to its stop) is unknown; any
overlapping invalid segment = contaminated, unknown segment = unknown; only covered, untouched windows are clean.

Reported per view: per-step expected/eligible/answered, MAE and signed bias, the uniform mean over the six steps
(undefined when a step has no answered cell), coverage = answered / eligible, per-day counts; C2 = served hybrid vs S1,
C3 = E1 raw vs S1 on the intersection of answered cells with paired per-step MAE and bias; relative improvement
1 − MAE_a/MAE_b with the shared whole-day draw bank (days by target time), undefined draws counted; the "≥ 10 %" quantity
is shown, formal superiority is "not determined" (U-27); intervals are nominal 95 %, calibration not established. C4 (in
the window only): origins with both raw vectors, raw+fingerprint matches, margins. P8: the validated result's window
budget and compromise flag and the target classes of the eligible cells are carried through.
"""
import argparse, datetime, hashlib, importlib.util, json, math, os, re, statistics, sys

HERE = os.path.dirname(os.path.abspath(__file__))
MS, SLOT, HOUR, DAY = 1000, 600_000, 3_600_000, 86_400_000
STEPS = 6


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


cap = load("capacity", os.path.join(HERE, "capacity.py"))
FORECAST_APPS = {"hybrid": "nginx-test", "s1": "nginx-seasonal", "e1": "nginx-ensemble", "e2": "nginx-ensemble-q95"}
ENSEMBLE_APPS = {"nginx-ensemble", "nginx-ensemble-q95"}
PREDICTORS = ("hybrid", "s1", "e1_raw", "e2_raw", "e1_served", "e2_served")
C2, C3 = ("hybrid", "s1"), ("e1_raw", "s1")
TRUTH_Q = cap.RPM_Q.format(app="nginx-test")
ISSUANCE_FIELDS = ["issuance_id", "issued_at", "application", "namespace", "inference_input_end", "target_anchor",
                   "model_version", "model_trained_at", "training_cutoff", "artifact_sha256", "step_minutes", "forecasts"]
ENSEMBLE_FIELDS = ["ts", "application", "namespace", "origin", "experiment", "margin_mode", "margin_quantile",
                   "partial_rule", "fingerprint", "boundary", "margin", "margin_samples", "stale_generation",
                   "line_sha256", "raw", "served", "generation"]
REQUIRED = ("issuance_id", "issued_at", "inference_input_end", "target_anchor", "model_version", "model_trained_at",
            "training_cutoff", "artifact_sha256")
LOOKBACK = 7 * DAY                                        # hybrid/S1 inference lookback
ENSEMBLE_FIT = 360 * HOUR
MARGIN_REACH = 24 * HOUR + 6 * HOUR + ENSEMBLE_FIT        # 24-h margin window, its generations up to 6 h older, their fits
HEX64 = re.compile(r"^[0-9a-f]{64}$")
TS_RE = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:\d\d)?$")


def ts(s, naive_ok=False):
    """Epoch ms of an RFC 3339 timestamp, fractional seconds kept (to the millisecond). A timestamp without a zone is
    accepted only when naive_ok (the ensemble's documented naive-UTC model_trained_at)."""
    if not isinstance(s, str):
        return None
    m = TS_RE.match(s)
    if not m or (m.group(3) is None and not naive_ok):
        return None
    t = datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    ms = int(round(float(m.group(2) or 0) * 1000))
    off = 0
    if m.group(3) and m.group(3) != "Z":
        sign = 1 if m.group(3)[0] == "+" else -1
        off = sign * (int(m.group(3)[1:3]) * 60 + int(m.group(3)[4:6])) * 60_000
    return int(t.timestamp()) * MS + ms - off


def finite(v):
    return type(v) in (int, float) and not isinstance(v, bool) and math.isfinite(v)


def read_trailed(path, extractor, row_type, fields_key, fields):
    """Rows of an extractor file verified against its trailer and receipt (versioned field list, counts, source)."""
    data = open(path, "rb").read()
    lines = data.decode().splitlines()
    if not lines or not lines[-1].startswith('{"trailer"'):
        raise ValueError(f"{path}: no extractor trailer")
    t = json.loads(lines[-1])["trailer"]
    body = ("\n".join(lines[:-1]) + "\n").encode()
    if len(body) != t["bytes"] or hashlib.sha256(body).hexdigest() != t["sha256"]:
        raise ValueError(f"{path}: rows do not match the trailer")
    receipt = json.loads(lines[0])["receipt"]
    if receipt.get("extractor") != extractor:
        raise ValueError(f"{path}: extractor {receipt.get('extractor')!r}, need {extractor!r}")
    if not (isinstance(receipt.get("sha256"), str) and HEX64.match(receipt["sha256"])
            and isinstance(receipt.get("extractor_sha256"), str) and HEX64.match(receipt["extractor_sha256"])
            and type(receipt.get("lines")) is int and receipt["lines"] >= 0
            and isinstance(receipt.get("source"), dict) and receipt["source"].get("context") and receipt["source"].get("pvc")
            and receipt["source"].get("pod")):
        raise ValueError(f"{path}: receipt hashes, line count or source identity invalid")
    if type(receipt.get("malformed_lines")) is not int or receipt["malformed_lines"] != 0:
        raise ValueError(f"{path}: source had malformed lines ({receipt.get('malformed_lines')!r})")
    if list(receipt.get(fields_key) or []) != fields:
        raise ValueError(f"{path}: {fields_key} differ from the declared schema")
    rows = []
    for n, line in enumerate(lines[1:-1], 2):
        r = json.loads(line)
        if r[0] == row_type:
            if len(r) != 1 + len(fields):
                raise ValueError(f"{path}:{n}: row width")
            rows.append(dict(zip(fields, r[1:])))
        elif r[0] not in ("D", "I", "E"):
            raise ValueError(f"{path}:{n}: unexpected row type {r[0]!r}")
    expected = receipt.get({"I": "issuances", "E": "ensemble_rows"}[row_type])
    if expected != len(rows):
        raise ValueError(f"{path}: {len(rows)} {row_type} rows, receipt says {expected}")
    return receipt, rows, hashlib.sha256(data).hexdigest()


def accept(records):
    """§4 acceptance in log order for one arm. Returns (accepted, rejected[{issuance_id, issued_at, reason}])."""
    acc, rej, ids, times = [], [], set(), set()
    last_t = last_cut = last_trained = None
    for r in records:
        naive = r.get("application") in ENSEMBLE_APPS
        it, cut = ts(r.get("issued_at")), ts(r.get("training_cutoff"))
        trained, origin = ts(r.get("model_trained_at"), naive_ok=naive), ts(r.get("inference_input_end"))
        fc = sorted(r.get("forecasts") or [], key=lambda x: x[0] if x and type(x[0]) is int else 99)
        reason = None
        if any(r.get(k) in (None, "") for k in REQUIRED) or None in (it, cut, trained, origin):
            reason = "missing_provenance"
        elif r.get("issuance_id") in ids or it in times:
            reason = "duplicate"
        elif last_t is not None and it < last_t:
            reason = "out_of_order"
        elif (last_cut is not None and cut < last_cut) or (last_trained is not None and trained < last_trained):
            reason = "stale_model"
        elif r.get("target_anchor") != "inference_input_end" or origin % SLOT:
            reason = "anchor"
        elif [x[0] for x in fc] != list(range(1, STEPS + 1)) or any(ts(x[1]) != origin + x[0] * SLOT for x in fc):
            reason = "steps"
        elif any(not finite(x[2]) for x in fc):
            reason = "non_finite"
        elif any(ts(x[1]) <= it or ts(x[1]) <= cut for x in fc):
            reason = "target_not_after_issuance_or_cutoff"
        if reason:
            rej.append({"issuance_id": r.get("issuance_id"), "issued_at": r.get("issued_at"), "reason": reason})
        else:
            acc.append(dict(r, _origin=origin, _issued=it, _cutoff=cut, _values=[float(x[2]) for x in fc]))
            last_t, last_cut, last_trained = it, cut, trained
        if reason != "duplicate":
            ids.add(r.get("issuance_id"))
            if it is not None:
                times.add(it)
    return acc, rej


def first_per_origin(accepted):
    out = {}
    for r in accepted:
        out.setdefault(r["_origin"], r)
    return out


def expected_policy(app, issued):
    """The margin policy each ensemble arm was declared to serve at an issuance time (D-1084 for E2)."""
    if app == "nginx-ensemble":
        return ("seasonal-ensemble-q90-v1", "absolute", 0.9)
    return ("seasonal-ensemble-q95-v1", "absolute", 0.95) if issued < cap.E2_SWITCH else ("seasonal-ensemble-rq90-v1", "relative", 0.9)


def cents(v):
    return int(round(v * 100))


def same_raw(a, b):
    """C4: E1 and E2′ issued the same raw forecast (same generation fingerprint and the same six values in cents)."""
    return bool(a.get("fingerprint")) and a.get("fingerprint") == b.get("fingerprint") and \
        len(a["raw"]) == len(b["raw"]) and [cents(x) for x in a["raw"]] == [cents(y) for y in b["raw"]]


def attach_raw(iss, candidates):
    """(record, status): status 'matched', 'unavailable' or 'ambiguous'."""
    pol = expected_policy(iss["application"], iss["_issued"])
    ok = []
    for e in candidates:
        served, raw = e.get("served") or [], e.get("raw") or []
        q = e.get("margin_quantile")
        if (e.get("namespace") == iss.get("namespace") and e.get("application") == iss["application"]
                and ts(e.get("origin")) == iss["_origin"] and e.get("fingerprint") == iss.get("artifact_sha256")
                and ts(e.get("boundary")) == iss["_cutoff"] and (e.get("experiment"), e.get("margin_mode")) == pol[:2]
                and finite(q) and abs(q - pol[2]) < 1e-9 and e.get("partial_rule") == "refuse"
                and len(served) == STEPS and len(raw) == STEPS and all(finite(x) for x in raw) and finite(e.get("margin"))
                and all(finite(a) and cents(a) == cents(b) for a, b in zip(served, iss["_values"]))):
            ok.append(e)
    if not ok:
        return None, "unavailable"
    key = lambda e: json.dumps({k: e.get(k) for k in ("experiment", "margin_mode", "margin_quantile", "partial_rule",
                                                       "fingerprint", "boundary", "margin", "margin_samples",
                                                       "stale_generation", "generation", "raw", "served")}, sort_keys=True)
    if len({key(e) for e in ok}) > 1:
        return None, "ambiguous"
    return ok[0], "matched"


def cells(origins, stop):
    return [(o, s) for o in origins for s in range(1, STEPS + 1) if o + s * SLOT < stop]


def summarize(cellset, truth, vals, days_declared=(), expected_by_day=None):
    per, days = {}, {d: {"expected": (expected_by_day or {}).get(d, 0), "eligible": 0, "answered": 0}
                     for d in days_declared}
    for step in range(1, STEPS + 1):
        el = [c for c in cellset if c[1] == step]
        err = [vals[c] - truth[c[0] + c[1] * SLOT] for c in el if c in vals]
        per[step] = {"eligible": len(el), "answered": len(err),
                     "mae": statistics.fmean(abs(e) for e in err) if err else None,
                     "bias": statistics.fmean(err) if err else None}
    for c in cellset:
        t = c[0] + c[1] * SLOT
        d = days.setdefault(cap.iso(t - t % DAY), {"expected": (expected_by_day or {}).get(cap.iso(t - t % DAY), 0),
                                                   "eligible": 0, "answered": 0})
        d["eligible"] += 1
        d["answered"] += c in vals
    for d in days.values():
        d["missing"] = d["eligible"] - d["answered"]
    maes = [per[s]["mae"] for s in per]
    el = sum(per[s]["eligible"] for s in per)
    ans = sum(per[s]["answered"] for s in per)
    return {"per_step": per, "per_day": days, "uniform_mae": None if any(m is None for m in maes) else statistics.fmean(maes),
            "coverage": ans / el if el else None, "coverage_ok": bool(el) and ans / el >= 0.90}


def day_sums(cellset, truth, va, vb):
    sums = {}
    for c in cellset:
        if c in va and c in vb:
            t = c[0] + c[1] * SLOT
            y = truth[t]
            x = sums.setdefault(cap.iso(t - t % DAY), {s: [0.0, 0.0, 0.0, 0.0, 0] for s in range(1, STEPS + 1)})[c[1]]
            x[0] += abs(va[c] - y)
            x[1] += abs(vb[c] - y)
            x[2] += va[c] - y
            x[3] += vb[c] - y
            x[4] += 1
    return sums


def totals(sums, days):
    tot = {s: [0.0, 0.0, 0.0, 0.0, 0] for s in range(1, STEPS + 1)}
    for d in days:
        for s, row in sums.get(d, {}).items():
            for i in range(5):
                tot[s][i] += row[i]
    return tot


def rel_improvement(sums, days):
    tot = totals(sums, days)
    if any(tot[s][4] == 0 for s in tot):
        return None, None, None
    ma = statistics.fmean(tot[s][0] / tot[s][4] for s in tot)
    mb = statistics.fmean(tot[s][1] / tot[s][4] for s in tot)
    return (None if mb == 0 else 1 - ma / mb), ma, mb


def compare(cellset, truth, va, vb, draws):
    sums = day_sums(cellset, truth, va, vb)
    days = draws["days"]
    ri, ma, mb = rel_improvement(sums, days)
    tot = totals(sums, days)
    per_step = {s: {"paired": tot[s][4], "mae_a": tot[s][0] / tot[s][4] if tot[s][4] else None,
                    "mae_b": tot[s][1] / tot[s][4] if tot[s][4] else None,
                    "bias_a": tot[s][2] / tot[s][4] if tot[s][4] else None,
                    "bias_b": tot[s][3] / tot[s][4] if tot[s][4] else None} for s in tot}
    vals, undefined = [], 0
    for draw in draws["matrix"]:
        x, _, _ = rel_improvement(sums, [days[i] for i in draw])
        if x is None:
            undefined += 1
        else:
            vals.append(x)
    vals.sort()
    pct = lambda q: vals[min(len(vals) - 1, int(q * len(vals)))]
    lo, hi = (pct(0.025), pct(0.975)) if vals else (None, None)
    return {"intersection_cells": sum(v[4] for v in tot.values()), "paired_per_step": per_step,
            "paired_per_day": {d: sum(r[4] for r in row.values()) for d, row in sorted(sums.items())},
            "mae_a": ma, "mae_b": mb, "relative_improvement": ri, "nominal_95": [lo, hi] if vals else None,
            "bootstrap_defined": len(vals), "bootstrap_undefined": undefined,
            "interval_note": ("nominal 95 %, calibration not established" +
                              ("; conditional on the defined draws" if undefined and vals else "")),
            "ten_percent_lower_bound_above": None if lo is None else lo > 0.10,
            "superiority": "not determined (U-27: descriptive reporting)"}


def dependency_windows(predictor, origin, iss, history_start):
    """Conservative dependency windows of a predictor's forecast at an origin (None = provenance missing)."""
    arm = predictor.split("_")[0]
    if iss is None:
        return None
    if arm in ("hybrid", "s1"):                               # state chains back to the history start (Codex r49)
        return [(min(history_start, origin - LOOKBACK), origin)]
    cut = iss.get("_cutoff")
    if cut is None:
        return None
    if predictor.endswith("_raw"):
        return [(cut - ENSEMBLE_FIT, origin)]
    return [(origin - MARGIN_REACH, origin)]


def contamination(windows, segments, covered):
    """clean / contaminated / unknown / not_applied; covered = (from, to) of the P8 collection."""
    if segments is None:
        return "not_applied"
    if windows is None:
        return "unknown"
    if any(a < covered[0] or b > covered[1] for a, b in windows):
        return "unknown"                                      # P8 does not cover this dependency
    hit = {st for sa, sb, st in segments for a, b in windows if sa <= b and sb >= a}
    return "contaminated" if "invalid" in hit else "unknown" if "unknown" in hit else "clean"


def frozen_envelope_config():
    """The envelope inputs as deployed (frozen manifests): ENSEMBLE_HISTORY_HOURS and the trainer's TRAINING_HOURS."""
    exp = open(os.path.join(cap.PROD, "ml-engine", "ml-api-experiments.yaml")).read()
    m = re.search(r'name: ENSEMBLE_HISTORY_HOURS\n\s+value: "(\d+)"', exp)
    if not m or int(m.group(1)) * HOUR != ENSEMBLE_FIT:
        raise ValueError("ENSEMBLE_HISTORY_HOURS in the frozen ml-api configuration differs from the envelope")
    train_path = os.path.join(cap.PROD, "ml-engine", "training-env.yaml")
    train = open(train_path).read()
    return {"ENSEMBLE_HISTORY_HOURS": int(m.group(1)), "TRAINING_HOURS_overridden": "TRAINING_HOURS" in train,
            "files_sha256": {"ml-api-experiments.yaml": cap.sha(os.path.join(cap.PROD, "ml-engine", "ml-api-experiments.yaml")),
                             "training-env.yaml": cap.sha(train_path)},
            "note": "repository configuration; the effective Deployment/Job settings are verified at the freeze capture"}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="first origin (UTC day boundary)")
    ap.add_argument("--stop", required=True, help="UTC day boundary; targets at or after it are not scored")
    ap.add_argument("--rows", required=True, help="extract_decisions.sh v3 output")
    ap.add_argument("--ensemble", required=True, help="extract_ensemble.sh v2 output")
    ap.add_argument("--truth-archive", required=True, help="Store directory for the truth query (new unless --replay)")
    ap.add_argument("--prom")
    ap.add_argument("--replay", action="store_true")
    ap.add_argument("--p8-result", help="detector v5 result (a scored run); without it a diagnostic run")
    ap.add_argument("--draws", required=True, help="the draw bank shared with capacity.py (created when absent)")
    ap.add_argument("--n-boot", type=int, default=cap.SCORED_DRAWS[0])
    ap.add_argument("--seed", type=int, default=cap.SCORED_DRAWS[1])
    ap.add_argument("--json", required=True)
    ap.add_argument("--md")
    a = ap.parse_args(argv)
    ie = cap.load("infra_events", os.path.join(cap.PROD, "infra_events.py"))
    start, stop = ie.parse(a.start), ie.parse(a.stop)
    if start % DAY or stop % DAY or stop <= start:
        raise SystemExit("start and stop must be UTC day boundaries with start < stop")
    if a.p8_result and (a.n_boot, a.seed) != cap.SCORED_DRAWS:
        raise SystemExit("a scored run uses the P9 draw contract")
    rec_receipt, issuances, rows_sha = read_trailed(a.rows, "extract_decisions.sh v3", "I", "issuance_fields", ISSUANCE_FIELDS)
    ens_receipt, ensemble, ens_sha = read_trailed(a.ensemble, "extract_ensemble.sh v2", "E", "ensemble_fields", ENSEMBLE_FIELDS)
    store = cap.Store(a.prom, a.truth_archive, replay=a.replay)
    tv = cap.range_values(store, TRUTH_Q, start, stop)
    origins = list(range(start, stop, SLOT))
    p8_bytes = open(a.p8_result, "rb").read() if a.p8_result else None
    target_class = segments = p8_summary = None
    covered = None
    if p8_bytes is not None:
        frozen = {"detector": cap.sha(os.path.join(cap.PROD, "infra_events.py")),
                  "identities": cap.sha(os.path.join(cap.PROD, "infra-identities.json")),
                  "mask": cap.sha(os.path.join(cap.PROD, "validity-mask.json"))}
        res = json.loads(p8_bytes)
        chk = cap.p8_check(res, start, stop, frozen, ie)
        target_class, segments, p8_summary = chk["classifier"].target, chk["classifier"].segs, chk["summary"]
        pre = res["run_identity"]["pre_ms"]
        if pre != ie.PRE or (res.get("constants_ms") or {}).get("pre") != ie.PRE:
            raise ValueError(f"P8 result's pre_ms {pre} differs from the frozen detector constant {ie.PRE}")
        covered = (ie.parse(res["start"]) - pre, ie.parse(res["stop"]))
    mask = json.load(open(os.path.join(cap.PROD, "validity-mask.json")))
    history_start = ie.parse(mask["benchmark_history_start"])
    ens_by = {}
    for e in ensemble:
        ens_by.setdefault((e.get("application"), ts(e.get("origin"))), []).append(e)
    values = {p: {} for p in PREDICTORS}
    rejections, picked, raw_status, raw_rec = {}, {}, {}, {}
    for arm, app in FORECAST_APPS.items():
        acc, rej = accept([r for r in issuances if r.get("application") == app and r.get("namespace") == "demo"])
        rejections[arm] = rej
        picked[arm] = first_per_origin(acc)
        for o, r in picked[arm].items():
            target = values[arm] if arm in ("hybrid", "s1") else values[f"{arm}_served"]
            for s, v in enumerate(r["_values"], 1):
                target[(o, s)] = v
            if arm in ("e1", "e2"):
                e, st = attach_raw(r, ens_by.get((app, o), []))
                if start <= o < stop:
                    raw_status.setdefault(arm, {}).setdefault(st, 0)
                    raw_status[arm][st] += 1
                raw_rec[(arm, o)] = e
                if e is not None:
                    for s, v in enumerate(e["raw"], 1):
                        values[f"{arm}_raw"][(o, s)] = float(v)
    all_cells = cells(origins, stop)
    days = [cap.iso(d) for d in range(start, stop, DAY)]
    envelope_config = frozen_envelope_config()
    unfiltered = [c for c in all_cells if tv.get(c[0] + c[1] * SLOT) is not None]
    klass = {c: (target_class(c[0] + c[1] * SLOT) if target_class else "not_applied") for c in unfiltered}
    operational = [c for c in unfiltered if klass[c] != "invalid"]
    issuance_of = lambda p, o: picked[p.split("_")[0]].get(o)
    flags = {p: {o: contamination(dependency_windows(p, o, issuance_of(p, o), history_start), segments, covered)
                 for o in origins} for p in PREDICTORS}
    clean_for = lambda preds: [c for c in operational if all(flags[p][c[0]] == "clean" for p in preds)]
    if os.path.exists(a.draws):
        draws_bytes = open(a.draws, "rb").read()
        draws = json.loads(draws_bytes)
        cap.check_draws(draws, days, a.n_boot, a.seed)
    else:
        draws = cap.make_draws(days, a.n_boot, a.seed)
        draws_bytes = json.dumps(draws).encode()
        cap.write_atomic(a.draws, draws_bytes.decode())
    views = {"unfiltered": unfiltered, "operational": operational}
    exp_day = {}
    for o, s in all_cells:
        d = cap.iso(o + s * SLOT - (o + s * SLOT) % DAY)
        exp_day[d] = exp_day.get(d, 0) + 1
    summary = {v: {p: summarize(cs, tv, values[p], days, exp_day) for p in PREDICTORS} for v, cs in views.items()}
    summary["clean_input"] = {p: summarize(clean_for([p]), tv, values[p], days, exp_day) for p in PREDICTORS}
    comparisons = {}
    for name, pair in (("C2", C2), ("C3", C3)):
        comparisons[name] = {v: compare(cs, tv, values[pair[0]], values[pair[1]], draws) for v, cs in views.items()}
        comparisons[name]["clean_input"] = compare(clean_for(pair), tv, values[pair[0]], values[pair[1]], draws)
    in_window = [o for o in origins]
    both = [o for o in in_window if raw_rec.get(("e1", o)) and raw_rec.get(("e2", o))]
    match = [o for o in both if same_raw(raw_rec[("e1", o)], raw_rec[("e2", o)])]
    margins = {arm: [float(raw_rec[(arm, o)]["margin"]) for o in in_window
                     if raw_rec.get((arm, o)) and finite(raw_rec[(arm, o)].get("margin"))] for arm in ("e1", "e2")}
    c4 = {"origins_in_window": len(in_window), "origins_with_both_raw": len(both), "raw_and_fingerprint_match": len(match),
          "match_share": len(match) / len(both) if both else None,
          "margin_mean_rpm": {k: statistics.fmean(v) if v else None for k, v in margins.items()},
          "raw_attachment": raw_status,
          "note": "capacity outcomes of E2′ vs E1 are reported by capacity.py over every instant, not this subset"}
    target_classes = {}
    for c in unfiltered:
        target_classes[klass[c]] = target_classes.get(klass[c], 0) + 1
    contam = {p: {k: sum(1 for o in origins if flags[p][o] == k) for k in ("clean", "contaminated", "unknown", "not_applied")}
              for p in PREDICTORS}
    meta = {"scorer": "forecasts.py v3", "scorer_sha256": cap.sha(os.path.abspath(__file__)), "start": a.start,
            "stop": a.stop, "rows_sha256": rows_sha, "rows_receipt": rec_receipt, "ensemble_sha256": ens_sha,
            "ensemble_receipt": ens_receipt, "truth_manifest_sha256": cap.sha(store.manifest),
            "p8_result_sha256": hashlib.sha256(p8_bytes).hexdigest() if p8_bytes is not None else None, "p8": p8_summary,
            "draws_sha256": hashlib.sha256(draws_bytes).hexdigest(), "mixed_e2": start < cap.E2_SWITCH,
            "envelopes": {"hybrid_s1": "earlier of history start and origin − 7 d → origin", "lookback_ms": LOOKBACK,
                          "ensemble_raw_fit_ms": ENSEMBLE_FIT, "ensemble_served_ms": MARGIN_REACH,
                          "frozen_config": envelope_config, "p8_covered": covered},
            "note": ("P8 applied (validated)." if p8_bytes is not None else "DIAGNOSTIC: P8 not applied; not a scored "
                     "result.") + " Descriptive only (U-27); intervals nominal 95 %, calibration not established."}
    out = {"meta": meta, "cells": {"expected": len(all_cells), "with_truth": len(unfiltered), "operational": len(operational)},
           "target_classes": target_classes, "views": summary, "comparisons": comparisons, "C4": c4,
           "contamination": contam, "rejections": rejections}
    cap.write_atomic(a.json, json.dumps(out, indent=1, default=str))
    if a.md:
        f = lambda x, d=1: "—" if x is None else f"{x:.{d}f}"
        pc = lambda x: "—" if x is None else f"{100 * x:.1f}"
        L = [f"# Forecasts — origins {a.start} → {a.stop}", "", meta["note"], ""]
        if meta["mixed_e2"]:
            L += ["E2 served the absolute q95 margin until 2026-10-07T23:08:40Z (D-1084).", ""]
        if p8_summary:
            L += [f"P8 window budget: {p8_summary['budget_slots']}/{p8_summary['total']} slots → "
                  f"{'COMPROMISED' if p8_summary['compromised'] else 'within 20 %'}.", ""]
        L += [f"Cells: expected {len(all_cells)}, with truth {len(unfiltered)}, operational {len(operational)}; "
              f"target classes {target_classes}.", ""]
        for v in ("operational", "unfiltered", "clean_input"):
            L += [f"## {v} view", "", "| predictor | coverage | uniform MAE | per-step MAE 1..6 | per-step bias 1..6 |",
                  "|---|---|---|---|---|"]
            for p in PREDICTORS:
                s = summary[v][p]
                L.append(f"| {p} | {pc(s['coverage'])} % | {f(s['uniform_mae'])} | "
                         + " / ".join(f(s['per_step'][k]['mae'], 0) for k in range(1, 7)) + " | "
                         + " / ".join(f(s['per_step'][k]['bias'], 0) for k in range(1, 7)) + " |")
            L += ["", "| comparison | paired cells | MAE a / b | relative improvement [nominal 95 %] | undefined draws |",
                  "|---|---|---|---|---|"]
            for name, label in (("C2", "served hybrid vs S1"), ("C3", "E1 raw vs S1")):
                c = comparisons[name][v]
                iv = c["nominal_95"] or [None, None]
                L.append(f"| {name} {label} | {c['intersection_cells']} | {f(c['mae_a'])} / {f(c['mae_b'])} | "
                         f"{pc(c['relative_improvement'])} % [{pc(iv[0])}, {pc(iv[1])}] | {c['bootstrap_undefined']} |")
            L.append("")
        L += [f"C4 (window): origins with both raw {c4['origins_with_both_raw']}, raw+fingerprint match "
              f"{c4['raw_and_fingerprint_match']}; mean margin E1 {f(c4['margin_mean_rpm']['e1'])} / E2 "
              f"{f(c4['margin_mean_rpm']['e2'])} rpm; raw attachment {raw_status}.",
              f"Rejections: { {k: len(v) for k, v in rejections.items()} } (ids and reasons in the JSON).",
              f"Contamination per predictor: {contam}.", "",
              "Formal superiority is not determined (U-27); intervals are nominal 95 % whole-day percentiles, calibration "
              "not established."]
        cap.write_atomic(a.md, "\n".join(L) + "\n")
    print(json.dumps({v: (comparisons["C2"][v]["relative_improvement"], comparisons["C3"][v]["relative_improvement"])
                      for v in ("operational", "unfiltered", "clean_input")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
