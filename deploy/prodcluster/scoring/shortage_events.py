#!/usr/bin/env python3
"""Shortage-event diagnosis for the prodcluster arms, v4 (descriptive; not a scored result; Codex Task 03 r41–r44).

An event is a maximal observed run of 30-s instants at which an arm's Ready replicas (capacity.py's raw-sample rule) are
below the planned step reference R_p(t). A run that starts at the window start or right after a missing Ready sample is
left-censored; one that ends at the window end or right before a missing sample is right-censored; censored events are
excluded from the recovery statistics. Recovery is the first observed instant after the event with Ready >= R_p; it is
"ready_rose" when Ready increased, "demand_fell" when R_p dropped.

Evidence, not verdicts. Every event carries all applicable tags or "undetermined"; the shortage of an event is its
*associated exposure*, never shortage attributed to one mechanism. For operator-driven arms, with R_p at the onset:
  decision coverage   the decision at the onset must be fresh (<= 120 s old) and decisions must not be more than 120 s
                      apart over the lead window; otherwise "decision_coverage_gap" and no conclusion from them
  applied_ahead_ready_late   applied >= R_p(onset) in every decision from >= 60 s before the onset through the onset
  no_forecast / lead_peak_low / confidence_cut / clamp_cut / overestimate_cut   along raw -> confidence -> clamp ->
                      overestimate safeguard -> predicted (onset decision; the first step that fell below R_p(onset))
  safeguard_recorded  the onset decision lists any safeguard
  max_clip / hold / reactive_floor
  forecast_below_plan       the forecast actually used (returned_issuance_id) put the step covering the onset below the
                            planned rate there — a capacity-reference comparison, not forecast error
  forecast_below_truth      the same step below the canonical truth (nginx-test observed rpm at the target): forecast error
  issuance_join_mismatch    the returned issuance belongs to another application
Timeline per event: the pre-onset state (desired/applied of the onset decision and its age), the first *observed*
post-onset decision whose desired (applied) met R_p at its time — not when capacity was first requested — decision gaps
over 120 s inside the event, and recovery. KEDA: met / below / unobserved instants of the event against R_p at each
instant (hpa_desired_below_need when any instant is below, hpa_partially_unobserved when any is unobserved,
hpa_desired_met_ready_late when every observed instant met it, hpa_unobserved without samples).
Inputs: a capacity.py archive (replayed), an HPA archive recorded here, and decision/issuance rows extracted in-cluster
(extract_decisions.sh) — verified against the extractor's trailer and receipt (a receipt reporting malformed source
lines is rejected); the rows file hash is bound into the output.
"""
import argparse, bisect, datetime, hashlib, importlib.util, json, os, statistics, sys

HERE = os.path.dirname(os.path.abspath(__file__))
MS, STEP, FRESH = 1000, 30_000, 120_000
KEDA_HPA = "keda-hpa-myapptwo-keda-fallback"
HPA_Q = ('max(kube_horizontalpodautoscaler_status_desired_replicas{{namespace="demo",'
         'horizontalpodautoscaler="{hpa}"}})')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


cap = load("capacity", os.path.join(HERE, "capacity.py"))


def ts(s):
    return int(datetime.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()) * MS


def events(taus, ready, need):
    out, cur, prev_missing = [], None, True
    for k, t in enumerate(taus):
        r = ready.get(t)
        short = r is not None and r < need[t]
        if short and cur is None:
            cur = {"start": t, "instants": [], "left_censored": prev_missing}
        if short:
            cur["instants"].append(t)
        elif cur is not None:
            cur["right_censored"] = r is None
            cur["recovered"] = None if r is None else t
            out.append(cur)
            cur = None
        prev_missing = r is None
    if cur is not None:
        cur.update(right_censored=True, recovered=None)
        out.append(cur)
    for e in out:
        ins, last = e["instants"], e["instants"][-1]
        rec = e["recovered"]
        e.update(end=last, minutes=len(ins) * STEP / 60_000,
                 shortage=sum(need[t] - ready[t] for t in ins) * STEP / 60_000,
                 max_deficit=max(need[t] - ready[t] for t in ins), need_at_onset=need[ins[0]],
                 recovery_min=None if rec is None else (rec - e["start"]) / 60_000,
                 recovery_cause=None if rec is None else ("ready_rose" if ready[rec] > ready[last] else "demand_fell"),
                 demand_step=(ins[0] - STEP in need and need[ins[0]] > need[ins[0] - STEP]))
    return out


def covered(times, lo, hi):
    """Decisions present at most FRESH apart over [lo, hi], including both edges."""
    i, j = bisect.bisect_left(times, lo), bisect.bisect_right(times, hi)
    ts_ = times[i:j]
    if not ts_ or ts_[0] - lo > FRESH or hi - ts_[-1] > FRESH:
        return False
    return all(b - a <= FRESH for a, b in zip(ts_, ts_[1:]))


def operator_timeline(e, decs, times, issuances, app, rate_at, truth, need):
    t0, n0 = e["start"], e["need_at_onset"]
    i = bisect.bisect_right(times, t0)
    at = decs[i - 1] if i and t0 - times[i - 1] <= FRESH else None
    tags = []
    lo = t0 - 30 * 60_000
    if at is None or not covered(times, lo, t0):
        tags.append("decision_coverage_gap")
    else:
        j0 = bisect.bisect_left(times, lo)
        run = None
        for d, t in zip(decs[j0:i], times[j0:i]):
            run = (t if run is None else run) if (d.get("applied_replicas") or 0) >= n0 else None
        if run is not None and t0 - run >= 60_000:
            tags.append("applied_ahead_ready_late")
    fc = None
    if at is not None:
        if at.get("forecast_status") != "used":
            tags.append("no_forecast")
        else:
            for name, key in (("lead_peak_low", "raw_predicted_replicas"), ("confidence_cut", "confidence_adjusted_replicas"),
                              ("clamp_cut", "predicted_after_clamp"), ("overestimate_cut", "predicted_replicas")):
                v = at.get(key)
                if v is not None and v < n0:
                    tags.append(name)
                    break
        if at.get("safeguards"):
            tags.append("safeguard_recorded")
        if n0 > (at.get("max_replicas") or n0):
            tags.append("max_clip")
        if (at.get("desired_replicas") or 0) >= n0 and (at.get("applied_replicas") or 0) < n0 and \
                str(at.get("action", "")).startswith("hold"):
            tags.append("hold")
        if at.get("desired_source") == "reactive":
            tags.append("reactive_floor")
        used = issuances.get(at.get("returned_issuance_id") or "")
        if used and used["application"] != app:
            tags.append("issuance_join_mismatch")
        elif used and at.get("forecast_status") == "used":
            steps = sorted((ts(x[1]), x[2]) for x in used["forecasts"] if x[1] and x[2] is not None)
            cover = next(((t, v) for t, v in steps if t >= t0), None)
            if cover:
                plan, obs = rate_at(cover[0] // MS), truth.get(cover[0])
                fc = {"issuance_id": used["issuance_id"], "target_at": cap.iso(cover[0]), "forecast_rpm": round(cover[1], 1),
                      "planned_rpm": plan, "truth_rpm": None if obs is None else round(obs, 1),
                      "forecast_minus_truth": None if obs is None else round(cover[1] - obs, 1)}
                if cover[1] < plan:
                    tags.append("forecast_below_plan")
                if obs is not None and cover[1] < obs:
                    tags.append("forecast_below_truth")
    end = e["recovered"] or e["end"]
    k0, k1 = bisect.bisect_left(times, t0), bisect.bisect_right(times, end)
    first = lambda key: next(((t - t0) / 60_000 for d, t in zip(decs[k0:k1], times[k0:k1])
                              if (d.get(key) or 0) >= need.get(t - t % STEP, n0)), None)
    inside = [t0] + times[k0:k1] + [end]
    timeline = {"pre_onset": None if at is None else {"desired": at.get("desired_replicas"),
                                                      "applied": at.get("applied_replicas"),
                                                      "age_s": (t0 - times[i - 1]) / MS},
                "first_observed_sufficient_decision_min": first("desired_replicas"),
                "first_observed_sufficient_apply_min": first("applied_replicas"),
                "decision_gaps_over_120s": sum(1 for a, b in zip(inside, inside[1:]) if b - a > FRESH),
                "recovery_min": e["recovery_min"], "recovery_cause": e["recovery_cause"]}
    return tags or ["undetermined"], at, fc, timeline


def keda_tags(e, hpa, need):
    vals = [(hpa.get(t), need[t]) for t in e["instants"]]
    counts = {"met": sum(1 for d, n in vals if d is not None and d >= n),
              "below": sum(1 for d, n in vals if d is not None and d < n),
              "unobserved": sum(1 for d, _ in vals if d is None)}
    if counts["met"] + counts["below"] == 0:
        return ["hpa_unobserved"], counts
    tags = []
    if counts["below"]:
        tags.append("hpa_desired_below_need")
    if counts["unobserved"]:
        tags.append("hpa_partially_unobserved")
    if not counts["below"]:
        tags.append("hpa_desired_met_ready_late")
    return tags, counts


def read_rows(path):
    """Validate the extraction against its receipt: row types, counts, unique issuance ids."""
    data = open(path, "rb").read()
    lines = data.decode().splitlines()
    if not lines or not lines[-1].startswith('{"trailer"'):
        raise ValueError("rows lack the extractor's trailer")
    t = json.loads(lines[-1])["trailer"]
    body = ("\n".join(lines[:-1]) + "\n").encode()
    if len(body) != t["bytes"] or hashlib.sha256(body).hexdigest() != t["sha256"]:
        raise ValueError("rows do not match the extractor's trailer")
    lines = lines[:-1]
    receipt = json.loads(lines[0])["receipt"]
    need = ("extractor", "extractor_sha256", "source", "sha256", "lines", "malformed_lines", "decisions", "issuances",
            "decision_fields", "lookup_fields")
    missing = [k for k in need if k not in receipt]
    if missing or receipt["extractor"] != "extract_decisions.sh v2":
        raise ValueError(f"unexpected receipt (missing {missing}, extractor {receipt.get('extractor')!r})")
    bad = receipt["malformed_lines"]
    if type(bad) is not int or bad < 0:
        raise ValueError(f"malformed_lines must be a non-negative integer, got {bad!r}")
    if bad:
        raise ValueError(f"the source log had {bad} malformed lines; evidence incomplete")
    kf, lf = receipt["decision_fields"], receipt["lookup_fields"]
    decs, iss = [], {}
    for n, line in enumerate(lines[1:], 2):
        r = json.loads(line)
        if r[0] == "D" and len(r) == 1 + len(kf) + len(lf):
            d = dict(zip(kf, r[1:1 + len(kf)]))
            d.update(zip(lf, r[1 + len(kf):]))
            decs.append(d)
        elif r[0] == "I" and len(r) == 7:
            rec = {"issuance_id": r[1], "issued_at": r[2], "application": r[3], "origin": r[4], "model_version": r[5],
                   "forecasts": r[6]}
            if r[1] in iss and iss[r[1]] != rec:
                raise ValueError(f"line {n}: conflicting rows for issuance {r[1]}")
            iss[r[1]] = rec
        else:
            raise ValueError(f"line {n}: unexpected row type or width")
    if len(decs) != receipt["decisions"] or sum(1 for line in lines[1:] if line.startswith('["I"')) != receipt["issuances"]:
        raise ValueError("row counts differ from the receipt")
    return receipt, decs, iss, hashlib.sha256(data).hexdigest()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--capacity-archive", required=True, help="a capacity.py --archive directory (replayed offline)")
    ap.add_argument("--hpa-archive", required=True, help="directory for the HPA responses (new unless --replay)")
    ap.add_argument("--prom", help="vmselect URL for the HPA query (record mode)")
    ap.add_argument("--replay", action="store_true")
    ap.add_argument("--start", required=True)
    ap.add_argument("--stop", required=True)
    ap.add_argument("--rows", required=True, help="extract_decisions.sh output (receipt + D/I rows)")
    ap.add_argument("--json", required=True)
    ap.add_argument("--md")
    a = ap.parse_args(argv)
    ie = cap.load("infra_events", os.path.join(cap.PROD, "infra_events.py"))
    cp = cap.load("challenge_profile", os.path.join(cap.PROFILE_DIR, "challenge_profile.py"))
    start, stop = ie.parse(a.start), ie.parse(a.stop)
    taus, ready, rpm, _ = cap.fetch(cap.Store(None, a.capacity_archive, replay=True), start, stop, ie)
    need = {t: cap.required(cp.rate_at(t // MS)) for t in taus}
    hstore = cap.Store(a.prom, a.hpa_archive, replay=a.replay)
    hvals = cap.range_values(hstore, HPA_Q.format(hpa=KEDA_HPA), start, stop)
    hpa = {t: hvals.get(t) for t in taus}
    truth = rpm["hybrid"]                                             # nginx-test observed: every forecaster's source
    receipt, decs_all, issuances, rows_sha = read_rows(a.rows)
    by_app = {}
    for d in decs_all:
        by_app.setdefault(d["application"], []).append(d)
    for v in by_app.values():
        v.sort(key=lambda d: d["at"])
    report = {"meta": {"tool": "shortage_events.py v3", "tool_sha256": cap.sha(os.path.abspath(__file__)),
                       "start": a.start, "stop": a.stop, "rows_receipt": receipt, "rows_sha256": rows_sha,
                       "capacity_manifest_sha256": cap.sha(os.path.join(a.capacity_archive, "manifest.jsonl")),
                       "hpa_manifest_sha256": cap.sha(hstore.manifest),
                       "profile_sha256": cap.sha(os.path.join(cap.PROFILE_DIR, "profile.json")),
                       "note": "Descriptive diagnostics, pre-T0; evidence tags, not causal verdicts; an event's shortage is "
                               "associated exposure; not a scored result; P8 not applied."},
              "arms": {}}
    for arm, app in cap.ARMS.items():
        evs = events(taus, ready[arm], need)
        decs = by_app.get(app, [])
        times = [ts(d["at"]) for d in decs]
        tagc, rows = {}, []
        for e in evs:
            fc = at = timeline = share = None
            if arm == "keda":
                tags, share = keda_tags(e, hpa, need)
            elif decs:
                tags, at, fc, timeline = operator_timeline(e, decs, times, issuances, app, cp.rate_at, truth, need)
            else:
                tags = ["no_operator_records"]
            for t in tags:
                c = tagc.setdefault(t, {"events": 0, "associated_exposure": 0.0})
                c["events"] += 1
                c["associated_exposure"] += e["shortage"]
            rows.append({"start": cap.iso(e["start"]), "label": cap.label(arm, cap.iso(e["start"] - e["start"] % cap.DAY)),
                         "minutes": e["minutes"], "shortage": round(e["shortage"], 2), "need_at_onset": e["need_at_onset"],
                         "max_deficit": e["max_deficit"], "left_censored": e["left_censored"],
                         "right_censored": e["right_censored"], "demand_step": e["demand_step"], "tags": tags,
                         "timeline": timeline, "hpa_instants": share, "forecast_used": fc, "decision_at_onset": at})
        complete = [r for r, e in zip(rows, evs) if not e["left_censored"] and not e["right_censored"]]
        rec = sorted(r["timeline"]["recovery_min"] if r["timeline"] else e["recovery_min"]
                     for r, e in zip(rows, evs) if not e["left_censored"] and not e["right_censored"] and e["recovery_min"] is not None)
        report["arms"][arm] = {
            "events": len(evs), "complete_events": len(complete),
            "censored": sum(1 for e in evs if e["left_censored"] or e["right_censored"]),
            "shortage": round(sum(e["shortage"] for e in evs), 2),
            "at_demand_step": sum(1 for e in evs if e["demand_step"]),
            "recovery_min_complete_events": {"median": statistics.median(rec) if rec else None, "max": rec[-1] if rec else None},
            "recovery_by": {c: sum(1 for e in evs if e["recovery_cause"] == c) for c in ("ready_rose", "demand_fell")},
            "tags": {k: {"events": v["events"], "associated_exposure": round(v["associated_exposure"], 2)}
                     for k, v in sorted(tagc.items())},
            "event_rows": rows}
    cap.write_atomic(a.json, json.dumps(report, indent=1))
    if a.md:
        L = [f"# Shortage events — {a.start} → {a.stop}", "", report["meta"]["note"], ""]
        if start < cap.E2_SWITCH:
            L += ["E2 served the absolute q95 margin until 2026-10-07T23:08:40Z (D-1084).", ""]
        L += ["| arm | events (complete) | shortage (replica-min) | at a planned step | recovery median / max, complete events (min) | recovered by Ready / by demand | evidence tags (events / associated exposure) |",
              "|---|---|---|---|---|---|---|"]
        for arm, r in report["arms"].items():
            tg = "; ".join(f"{k} {v['events']} / {v['associated_exposure']}" for k, v in r["tags"].items())
            rm = r["recovery_min_complete_events"]
            L.append(f"| {cap.label(arm, a.start) if arm == 'e2' else cap.LABELS[arm]} | {r['events']} ({r['complete_events']}) | "
                     f"{r['shortage']} | {r['at_demand_step']} | {rm['median']} / {rm['max']} | "
                     f"{r['recovery_by']['ready_rose']} / {r['recovery_by']['demand_fell']} | {tg} |")
        L += ["", "## Operator timelines (minutes after onset)", "",
              "| arm | onset | need | pre-onset desired / applied | first observed sufficient decision / apply (min) | recovery | forecast used: target, rpm / plan / truth | tags |",
              "|---|---|---|---|---|---|---|---|"]
        r2 = lambda x: "—" if x is None else round(x, 2)
        for arm, r in report["arms"].items():
            for x in r["event_rows"]:
                if not x["timeline"]:
                    continue
                t, f = x["timeline"], x["forecast_used"]
                fs = "—" if not f else f"{f['target_at'][11:16]}, {f['forecast_rpm']} / {f['planned_rpm']} / {f['truth_rpm']}"
                po = t["pre_onset"] or {}
                L.append(f"| {arm} | {x['start'][11:19]} | {x['need_at_onset']} | {po.get('desired', '—')} / {po.get('applied', '—')} | "
                         f"{r2(t['first_observed_sufficient_decision_min'])} / {r2(t['first_observed_sufficient_apply_min'])} | "
                         f"{t['recovery_min']} ({t['recovery_cause']}) | {fs} | {', '.join(x['tags'])} |")
        cap.write_atomic(a.md, "\n".join(L) + "\n")
    print(json.dumps({arm: (r["events"], r["shortage"]) for arm, r in report["arms"].items()}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
