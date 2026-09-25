#!/usr/bin/env python3
"""Forward-looking scorer for the predictive-autoscaler benchmark (evaluation-protocol.md, v3).

Semantics (Codex C-04/C-09):
* A forecast step is scored against the TRAINED TARGET DEFINITION at ITS OWN target time: the
  canonical request-count query `sum(rate(istio_requests_total{reporter="destination",...}[1m])) * 60`
  evaluated as a point sample at target_at (Prometheus instant query), exactly as the training
  collector samples it. No ten-minute averaging.
* Forecast records come from the operator's append-only JSONL log, written at issuance. A record is
  ACCEPTED only if: it is the first with its (application, namespace, issued_at); its issued_at is not
  earlier than the previous accepted record (log order); its training_cutoff / model_trained_at does
  not move backwards; every target_at is later than issued_at and later than training_cutoff (when
  known). Everything else is REJECTED and listed with a reason (duplicate, out_of_order, stale_model,
  target_not_in_future, target_before_cutoff).
* Coverage is reported two ways: issuance coverage = accepted issuances / expected issuances, where the
  expected count comes from the window length and the operator's issuance cadence (--issuance-minutes,
  default 5 = its prediction-cache TTL); step coverage = scored steps / (accepted issuances x steps).
  Missing issuance periods therefore lower coverage instead of disappearing. --min-coverage gates both.
* Baselines use only observations available at issuance: persistence = canonical query at issued_at;
  previous-day = canonical query at target_at - 24 h (always <= issued_at for a <= 24 h horizon). The
  Prometheus client refuses to evaluate a baseline later than the issuance it belongs to.
* Observation and replica series are checked for gaps over the window (missing minutes, longest gap).
* Pod-minutes integrate replicas over the actual sample intervals, with any interval longer than
  --max-gap-minutes counted as a gap (reported) rather than credited. Scale events come from the
  operator's own counter; a sampled count of replica changes is reported separately as a cross-check.
* Zero or missing observations are reported and excluded from MAPE (APE is undefined at 0); MAE keeps
  them.
* Attribution (Codex round 28, D-136): every scored step is attributed to the FULL artifact_sha256 of
  the issuance that produced it (a record without one goes to the explicit group "unknown"). Results
  are reported pooled over all scored steps AND per artifact, each with counts; the pooled figure is
  never an equal average of per-artifact figures. Rolling retraining replaces the artifact mid-window;
  an issuance's targets stay attributed to its own artifact even when they mature after the next
  artifact appears. Targets later than --as-of (default: now) are OUTSTANDING, not gaps, and the
  output says whether the window's final horizon has matured.
* Transition events (D-136): per artifact, the first operator issuance comes from the log; publication
  and API reload come from --transition-events (JSON {sha256: {published_at, api_reloaded_at}}) and
  are null when not supplied. The three are separate events and are never inferred from one another.
* Participation (D-140): "event":"decision" lines (one per reconcile) are summarised by daily phase of
  the generator pattern -- the fraction of reconciles whose unified decision was SET by the prediction
  (desired_source == "prediction"), with ties and max-clamped decisions separate and counts always
  shown. Only complete phase instances form the headline. Decision lines are never issuances.
The script exits non-zero when nothing can be scored, a required series is missing, or coverage is
below the gate. Known-answer tests: score_test.py.

Usage:
  kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090:9090 &
  bash read-forecast-log.sh --all --out forecasts.jsonl
  python3 score.py --prom http://localhost:9090 --forecast-log forecasts.jsonl --forecast-receipt forecasts.jsonl.receipt.json \
      --start 2026-09-24T00:00:00Z --end 2026-09-26T00:00:00Z --app nginx-test --namespace demo
"""
import argparse, io, json, math, os, sys, tempfile, urllib.parse, urllib.request
from pathlib import Path
from forecast_log import load_verified
from datetime import datetime, timezone, timedelta

CANONICAL = 'sum(rate(istio_requests_total{{reporter="destination",destination_workload="{app}",destination_workload_namespace="{ns}"}}[1m])) * 60'
REPLICAS = 'kube_deployment_status_replicas{{deployment="{d}",namespace="{ns}"}}'
SCALE_EVENTS = 'increase(predictive_autoscaler_scale_events_total{{application="{d}",namespace="{ns}",direction="{dir}"}}[{secs}s])'


def parse_ts(s):
    if isinstance(s, datetime):
        return s.astimezone(timezone.utc)
    return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)


def iso(t):
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Prom:
    """Minimal Prometheus HTTP client (stdlib only). Tests inject a fake with the same two methods."""
    def __init__(self, base):
        self.base = base.rstrip("/")

    def _get(self, path, params):
        url = self.base + path + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=120) as r:
            d = json.load(r)
        if d.get("status") != "success":
            raise RuntimeError(f"prometheus error: {d}")
        return d["data"]["result"]

    def range(self, query, start, end, step_s):
        res = self._get("/api/v1/query_range", {"query": query, "start": start.timestamp(), "end": end.timestamp(), "step": str(step_s)})
        if not res:
            return []
        if len(res) > 1:
            raise RuntimeError(f"query returned {len(res)} series, expected 1: {query}")
        return [(float(t), float(v)) for t, v in res[0]["values"] if v not in ("NaN", "+Inf", "-Inf")]

    def instant(self, query, at):
        """Point sample of `query` at time `at` (None if Prometheus has no value there)."""
        res = self._get("/api/v1/query", {"query": query, "time": at.timestamp()})
        if not res:
            return None
        if len(res) > 1:
            raise RuntimeError(f"query returned {len(res)} series, expected 1: {query}")
        v = res[0]["value"][1]
        if v in ("NaN", "+Inf", "-Inf"):
            return None
        return float(v)


class NoPeeking:
    """Wraps a Prometheus client so a baseline can never read past the issuance it belongs to."""
    def __init__(self, prom):
        self.prom = prom

    def at(self, query, when, not_after):
        if when > not_after:
            raise ValueError(f"baseline would read {iso(when)} which is after issuance {iso(not_after)}")
        return self.prom.instant(query, when)


def _read_jsonl(path):
    with (io.StringIO(path.decode("utf-8")) if isinstance(path, bytes) else open(path, encoding="utf-8")) as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield n, json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"{path}:{n}: bad JSON: {e}")


def load_decisions(path, app, ns, start=None, end=None):
    """Per-reconcile decision records ("event": "decision", D-140) for one application, in log order."""
    out = []
    for n, r in _read_jsonl(path):
        if r.get("event") != "decision" or r.get("application") != app or r.get("namespace") != ns:
            continue
        at = parse_ts(r["at"])
        if (start is None or at >= start) and (end is None or at < end):
            r["_line"] = n
            out.append(r)
    return out


def load_forecasts(path, app, ns, start, end):
    """Issuance records and issuance-keyed events (sanity_rejected) in the window. Per-reconcile
    decision lines are NOT issuances and are skipped here (see load_decisions)."""
    recs = []
    with (io.StringIO(path.decode("utf-8")) if isinstance(path, bytes) else open(path, encoding="utf-8")) as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"{path}:{n}: bad JSON: {e}")
            if r.get("application") != app or r.get("namespace") != ns:
                continue
            if r.get("event") == "decision" or "issued_at" not in r:
                continue
            r["_line"] = n
            issued = parse_ts(r["issued_at"])
            if start <= issued < end:
                recs.append(r)
    return recs


def controller_rejections(recs_and_events):
    """Issuances the operator discarded by its divergence check ("event": "sanity_rejected" lines),
    keyed like accept_records: (application, namespace, issued_at ISO) -> earliest rejected_at.
    A rejection never removes a forecast from the raw-model set. It is reported as a diagnostic only:
    no "controller-used" subset is derived from it (Codex Task 03 C-19: the target_at <= rejected_at
    proxy erased earlier use and kept unused steps; a used-subset needs contemporaneous decision
    evidence from the operator, which the record format does not carry yet)."""
    out = {}
    for r in recs_and_events:
        if r.get("event") == "sanity_rejected":
            key = (r.get("application"), r.get("namespace"), iso(parse_ts(r["issued_at"])))
            at = parse_ts(r["rejected_at"]) if r.get("rejected_at") else parse_ts(r["issued_at"])
            if key not in out or at < out[key]:
                out[key] = at
    return out


def accept_records(recs):
    """Apply the STRUCTURAL acceptance rules in log order. Returns (accepted, rejected[{line, issued_at, reason}]).
    Event lines ("event" key) are never scored. Controller rejections (sanity_rejected) do NOT remove
    an issuance from the raw-model set (Codex Task 03 C-16: no retroactive selection); they are reported
    as diagnostics only (C-19)."""
    accepted, rejected, seen = [], [], set()
    last_issued, last_cutoff, last_trained = None, None, None
    for r in recs:
        if r.get("event"):
            continue
        issued = parse_ts(r["issued_at"])
        key = (r.get("application"), r.get("namespace"), iso(issued))
        reason = None
        cutoff = parse_ts(r["training_cutoff"]) if r.get("training_cutoff") else None
        trained = parse_ts(r["model_trained_at"]) if r.get("model_trained_at") else None
        if key in seen:
            reason = "duplicate"
        elif last_issued is not None and issued < last_issued:
            reason = "out_of_order"
        elif (cutoff and last_cutoff and cutoff < last_cutoff) or (trained and last_trained and trained < last_trained):
            reason = "stale_model"
        elif not r.get("forecasts"):
            reason = "no_steps"
        else:
            for fc in r["forecasts"]:
                target = parse_ts(fc["target_at"])
                if target <= issued:
                    reason = "target_not_in_future"; break
                if cutoff and target <= cutoff:
                    reason = "target_before_cutoff"; break
        if reason:
            rejected.append({"line": r.get("_line"), "issued_at": r["issued_at"], "reason": reason})
            continue
        seen.add(key); accepted.append(r)
        last_issued = issued
        if cutoff: last_cutoff = cutoff
        if trained: last_trained = trained
    return accepted, rejected


def expected_issuances(start, end, cadence_min):
    return max(1, int(math.floor((end - start).total_seconds() / (cadence_min * 60))))


UNKNOWN_ARTIFACT = "unknown"


def artifact_of(r):
    return r.get("artifact_sha256") or UNKNOWN_ARTIFACT


def score(accepted, prom, app, ns, start, end, cadence_min=5.0, as_of=None):
    """Score accepted records against point samples at their target times. prom must provide
    instant(query, at). Targets later than `as_of` (None = no limit) are OUTSTANDING: neither scored
    nor gaps. Returns (summary, rows)."""
    q = CANONICAL.format(app=app, ns=ns)
    guard = NoPeeking(prom)
    per_step, rows, gaps, zero_obs = {}, [], [], 0
    steps_total = 0
    outstanding = []
    art = {}   # full artifact hash -> counts (D-136)
    for r in accepted:
        issued = parse_ts(r["issued_at"])
        a = art.setdefault(artifact_of(r), {"issuances": 0, "steps_issued": 0, "steps_scored": 0, "gaps": 0,
                                             "outstanding": 0, "first_issued_at": r["issued_at"],
                                             "last_issued_at": r["issued_at"], "model_versions": set()})
        a["issuances"] += 1; a["last_issued_at"] = r["issued_at"]; a["model_versions"].add(r.get("model_version") or "?")
        persist = guard.at(q, issued, issued)
        for fc in r["forecasts"]:
            steps_total += 1; a["steps_issued"] += 1
            target = parse_ts(fc["target_at"])
            if as_of is not None and target > as_of:
                outstanding.append({"issued_at": r["issued_at"], "step": fc["step"], "target_at": fc["target_at"],
                                    "artifact_sha256": artifact_of(r)})
                a["outstanding"] += 1
                continue
            y = prom.instant(q, target)
            if y is None:
                gaps.append({"issued_at": r["issued_at"], "step": fc["step"], "target_at": fc["target_at"], "reason": "no observation at target",
                             "artifact_sha256": artifact_of(r)})
                a["gaps"] += 1
                continue
            f = float(fc["rpm"])
            prev = guard.at(q, target - timedelta(hours=24), issued)
            row = {"issued_at": r["issued_at"], "target_at": fc["target_at"], "step": fc["step"],
                   "model_version": r.get("model_version"), "training_cutoff": r.get("training_cutoff"),
                   "artifact_sha256": artifact_of(r),
                   "target_anchor": r.get("target_anchor"),
                   "forecast": f, "actual": y, "error": f - y, "ae": abs(f - y),
                   "persistence": persist, "persistence_error": persist - y if persist is not None else None,
                   "prevday": prev, "prevday_error": prev - y if prev is not None else None,
                   "ape": (abs(f - y) / y) if y > 0 else None,
                   "persistence_ape": (abs(persist - y) / y) if (persist is not None and y > 0) else None,
                   "persistence_ae": abs(persist - y) if persist is not None else None,
                   "prevday_ape": (abs(prev - y) / y) if (prev is not None and y > 0) else None,
                   "prevday_ae": abs(prev - y) if prev is not None else None}
            if y <= 0:
                zero_obs += 1
            rows.append(row); a["steps_scored"] += 1
            per_step.setdefault(int(fc["step"]), []).append(row)

    def agg(rs, key):
        v = [x[key] for x in rs if x.get(key) is not None]
        if not v:
            return None
        return round(100 * sum(v) / len(v), 2) if key.endswith("ape") else round(sum(v) / len(v), 1)

    n_exp = expected_issuances(start, end, cadence_min)
    out = {
        "window": [iso(start), iso(end)], "app": app, "namespace": ns,
        "issuances_expected": n_exp, "issuances_accepted": len(accepted),
        "issuance_coverage": round(len(accepted) / n_exp, 4),
        "forecast_steps_issued": steps_total, "forecast_steps_scored": len(rows),
        "step_coverage": round(len(rows) / (steps_total - len(outstanding)), 4) if steps_total > len(outstanding) else 0.0,
        "forecast_steps_outstanding": len(outstanding), "outstanding_list": outstanding[:50],
        "final_horizon_matured": not outstanding,
        "zero_observations": zero_obs, "gaps": len(gaps), "gap_list": gaps[:50],
        "overall": {"MAPE_percent": agg(rows, "ape"), "MAE_rpm": agg(rows, "ae"),
                    "persistence_MAPE_percent": agg(rows, "persistence_ape"), "persistence_MAE_rpm": agg(rows, "persistence_ae"),
                    "prevday_MAPE_percent": agg(rows, "prevday_ape"), "prevday_MAE_rpm": agg(rows, "prevday_ae"),
                    "under_predicted_share": round(sum(1 for x in rows if x["forecast"] < x["actual"]) / len(rows), 3) if rows else None},
        "per_step": {str(k): {"n": len(v), "MAPE_percent": agg(v, "ape"), "MAE_rpm": agg(v, "ae"),
                              "persistence_MAPE_percent": agg(v, "persistence_ape"), "prevday_MAPE_percent": agg(v, "prevday_ape")}
                     for k, v in sorted(per_step.items())},
        "aggregate_rule": "pooled over every scored step of every artifact; never an equal average of per-artifact figures",
        "per_artifact": {k: {"issuances": v["issuances"], "steps_issued": v["steps_issued"], "steps_scored": v["steps_scored"],
                             "gaps": v["gaps"], "outstanding": v["outstanding"],
                             "first_issued_at": v["first_issued_at"], "last_issued_at": v["last_issued_at"],
                             "model_versions": sorted(v["model_versions"]),
                             "MAPE_percent": agg([x for x in rows if x["artifact_sha256"] == k], "ape"),
                             "MAE_rpm": agg([x for x in rows if x["artifact_sha256"] == k], "ae"),
                             "persistence_MAE_rpm": agg([x for x in rows if x["artifact_sha256"] == k], "persistence_ae"),
                             "prevday_MAE_rpm": agg([x for x in rows if x["artifact_sha256"] == k], "prevday_ae")}
                         for k, v in art.items()},
        "artifacts_seen": list(art.keys()),
        "models_seen": sorted({(x["model_version"] or "?") for x in rows}),
        "training_cutoffs_seen": sorted({(x["training_cutoff"] or "?") for x in rows}),
        "target_anchors_seen": sorted({(x["target_anchor"] or "?") for x in rows}),
    }
    def bias_metrics(rs):
        """Diagnostic means use each predictor's available rows, with explicit counts."""
        metrics = {}
        for prefix in ("", "persistence_", "prevday_"):
            key = prefix + "error"
            metrics[prefix + "n"] = sum(x[key] is not None for x in rs)
            metrics[prefix + "signed_bias_rpm"] = agg(rs, key)
        return metrics

    out["overall"].update(bias_metrics(rows))
    for step, step_rows in per_step.items():
        metrics = out["per_step"][str(step)]
        metrics.update(bias_metrics(step_rows))
        for prefix in ("", "persistence_", "prevday_"):
            metrics[prefix + "MAE_rpm"] = agg(step_rows, prefix + "ae")
    for artifact, metrics in out["per_artifact"].items():
        metrics.update(bias_metrics([x for x in rows if x["artifact_sha256"] == artifact]))
    return out, rows


def transition_events(accepted, extra=None):
    """Per artifact (full hash, log order of first appearance): publication, API reload and FIRST
    OPERATOR ISSUANCE as three separate events (D-136). Only the last comes from the forecast log;
    the other two come from `extra` ({sha: {"published_at", "api_reloaded_at"}}) and are None when
    unknown -- never inferred from the issuance (the operator's cache can delay it)."""
    extra = extra or {}
    out = {}
    for r in accepted:
        k = artifact_of(r)
        if k not in out:
            e = extra.get(k) or {}
            out[k] = {"published_at": e.get("published_at"), "api_reloaded_at": e.get("api_reloaded_at"),
                      "first_operator_issuance_at": r["issued_at"], "issuances": 0}
        out[k]["issuances"] += 1
    return out


# Daily phases of the generator pattern (collect-k6-summaries.py PATTERN; peak 6000 rpm at 15Z).
PATTERN = {0: 250, 1: 200, 2: 150, 3: 150, 4: 200, 5: 300, 6: 750, 7: 1250, 8: 2000, 9: 3000, 10: 3750, 11: 4250,
           12: 4500, 13: 5000, 14: 5500, 15: 6000, 16: 5000, 17: 4250, 18: 3000, 19: 2400, 20: 1000, 21: 600, 22: 400, 23: 300}
PHASE_DEFINITION = ("UTC hour of the generator pattern: rising = 06-14Z, peak = 15Z, falling = 16-20Z, "
                    "trough = 21Z-05Z (one instance spans midnight)")


def phase_instance(t):
    """(phase, instance_start, instance_end) for UTC time t."""
    d = t.replace(minute=0, second=0, microsecond=0)
    h = t.hour
    day = d.replace(hour=0)
    if 6 <= h <= 14:
        return "rising", day + timedelta(hours=6), day + timedelta(hours=15)
    if h == 15:
        return "peak", day + timedelta(hours=15), day + timedelta(hours=16)
    if 16 <= h <= 20:
        return "falling", day + timedelta(hours=16), day + timedelta(hours=21)
    if h >= 21:
        return "trough", day + timedelta(hours=21), day + timedelta(hours=30)
    return "trough", day - timedelta(hours=3), day + timedelta(hours=6)


def participation(decisions, reconcile_s=60.0, max_gap_intervals=3):
    """Summarise the predictive component's participation (D-140) by daily phase.
    Headline = COMPLETE phase instances only: decision records cover the instance from its start to
    its end with no gap (including the edges) longer than max_gap_intervals x reconcile_s.
    'prediction_set' = desired_source == "prediction" (the prediction strictly exceeded reactive and
    minReplicas and was not max-clamped). Ties and max-clamped decisions are counted separately."""
    max_gap = max_gap_intervals * reconcile_s
    inst = {}
    for d in decisions:
        at = parse_ts(d["at"])
        ph, s0, e0 = phase_instance(at)
        inst.setdefault((ph, s0, e0), []).append((at, d))

    def blank():
        return {"reconciles": 0, "forecast_used": 0, "prediction_set": 0, "tie": 0, "reactive": 0, "min_replicas": 0,
                "max_replicas": 0, "keep_current": 0, "damping_changed": 0, "safeguard_changed": 0, "safeguards": {}}

    def add(acc, d):
        acc["reconciles"] += 1
        if d.get("forecast_status") == "used":
            acc["forecast_used"] += 1
        src = d.get("desired_source")
        key = {"prediction": "prediction_set"}.get(src, src)
        if key in acc:
            acc[key] += 1
        raw, adj = d.get("raw_predicted_replicas"), d.get("confidence_adjusted_replicas")
        if raw is not None and adj is not None and adj != raw:
            acc["damping_changed"] += 1
        if raw is not None and d.get("forecast_status") == "used" and d.get("predicted_replicas") != raw:
            acc["safeguard_changed"] += 1
        for g in d.get("safeguards") or []:
            acc["safeguards"][g] = acc["safeguards"].get(g, 0) + 1

    def finish(acc):
        n = acc["reconciles"]
        acc["prediction_set_fraction"] = round(acc["prediction_set"] / n, 4) if n else None
        acc["tie_fraction"] = round(acc["tie"] / n, 4) if n else None
        return acc

    headline, incomplete, instances = {}, [], []
    for (ph, s0, e0), items in sorted(inst.items(), key=lambda kv: kv[0][1]):
        items.sort(key=lambda x: x[0])
        times = [s0] + [t for t, _ in items] + [e0]
        longest = max((b - a).total_seconds() for a, b in zip(times, times[1:]))
        complete = longest <= max_gap
        acc = blank()
        for _, d in items:
            add(acc, d)
        row = {"phase": ph, "start": iso(s0), "end": iso(e0), "complete": complete,
               "longest_gap_seconds": round(longest, 1), **finish(acc)}
        instances.append(row)
        if complete:
            h = headline.setdefault(ph, blank())
            for _, d in items:
                add(h, d)
            h.setdefault("instances", 0)
            h["instances"] += 1
        else:
            incomplete.append({"phase": ph, "start": iso(s0), "end": iso(e0), "reconciles": acc["reconciles"],
                               "longest_gap_seconds": round(longest, 1)})
    return {"phase_definition": PHASE_DEFINITION,
            "completeness_rule": f"an instance is complete when no gap between consecutive decision records, or "
                                 f"between an edge of the instance and the nearest record, exceeds "
                                 f"{max_gap_intervals} x {reconcile_s:g} s",
            "prediction_set_definition": 'desired_source == "prediction": the prediction strictly exceeded the '
                                         "reactive replicas and minReplicas and was not max-clamped; ties and "
                                         "max-clamped decisions are reported separately",
            "headline_complete_phases": {k: finish(v) for k, v in headline.items()},
            "incomplete_instances": incomplete, "instances": instances}


def series_gaps(series, start, end, step_s=60, max_gap_s=None):
    """Missing-minute count and longest gap of a 1-minute-step range series over [start, end]."""
    expected = int((end - start).total_seconds() // step_s) + 1
    present = len(series)
    longest = 0.0
    if series:
        longest = max(series[0][0] - start.timestamp(), end.timestamp() - series[-1][0], 0.0)
        for (t0, _), (t1, _) in zip(series, series[1:]):
            longest = max(longest, t1 - t0 - step_s)
    return {"expected_samples": expected, "present_samples": present, "missing_samples": max(0, expected - present),
            "longest_gap_seconds": round(longest, 1)}


def pod_minutes(series, max_gap_s=None):
    """Integrate replicas over actual sample intervals; intervals longer than max_gap_s are not
    credited and are counted as gaps. Returns (pod_minutes, credited_seconds, gap_seconds)."""
    total, credited, gap = 0.0, 0.0, 0.0
    for (t0, v0), (t1, _) in zip(series, series[1:]):
        dt = t1 - t0
        if max_gap_s is not None and dt > max_gap_s:
            gap += dt
            continue
        total += v0 * dt / 60.0
        credited += dt
    return round(total, 1), round(credited, 1), round(gap, 1)


def sampled_changes(series):
    return sum(1 for (_, a), (_, b) in zip(series, series[1:]) if a != b)


def write_result(text, destination):
    """Publish a complete result exclusively; preserve any earlier evidence."""
    if destination == '-':
        print(text)
        return
    fd, staged = tempfile.mkstemp(prefix='.score-', dir=Path(destination).parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(text + '\n')
        try:
            os.link(staged, destination)
        except FileExistsError as exc:
            raise SystemExit('FAIL: output already exists: ' + destination) from exc
    finally:
        os.unlink(staged)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prom"); ap.add_argument("--forecast-log", required=True)
    ap.add_argument("--forecast-receipt", required=True, help="verified remote-prefix receipt from the reader")
    ap.add_argument("--allow-fixture-receipt", action="store_true", help="TEST ONLY: mark output as fixture evidence")
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True)
    ap.add_argument("--app", default="nginx-test"); ap.add_argument("--namespace", default="demo")
    ap.add_argument("--controls", default="nginx-reactive,myapptwo", help="comma-separated comparison deployments")
    ap.add_argument("--issuance-minutes", type=float, default=5.0, help="operator issuance cadence (prediction cache TTL)")
    ap.add_argument("--min-coverage", type=float, default=0.8)
    ap.add_argument("--max-gap-minutes", type=float, default=5.0, help="replica sample interval above which time is a gap")
    ap.add_argument("--out", default="-")
    ap.add_argument("--as-of", help="targets later than this are outstanding, not gaps (default: now)")
    ap.add_argument("--transition-events", help="JSON {artifact_sha256: {published_at, api_reloaded_at}}")
    ap.add_argument("--reconcile-seconds", type=float, default=60.0, help="operator reconcile interval (participation completeness)")
    ap.add_argument("--participation-only", action="store_true", help="summarise decision records only; no Prometheus needed")
    a = ap.parse_args(argv)
    if a.out != '-' and os.path.lexists(a.out):
        raise SystemExit('FAIL: output already exists: ' + a.out)
    start, end = parse_ts(a.start), parse_ts(a.end)
    snapshot, provenance = load_verified(a.forecast_log, a.forecast_receipt, end, a.allow_fixture_receipt)
    decisions = load_decisions(snapshot, a.app, a.namespace, start, end)
    part = participation(decisions, a.reconcile_seconds) if decisions else None
    if a.participation_only:
        if not decisions:
            raise SystemExit("FAIL: no decision records for the app in the window")
        text = json.dumps({"window": [iso(start), iso(end)], "app": a.app, "namespace": a.namespace,
                           "decision_records": len(decisions), "participation": part, "forecast_log_provenance": provenance}, indent=2)
        write_result(text, a.out)
        return 0
    if not a.prom:
        raise SystemExit("FAIL: --prom is required unless --participation-only")
    as_of = parse_ts(a.as_of) if a.as_of else datetime.now(timezone.utc)
    prom = Prom(a.prom)
    q = CANONICAL.format(app=a.app, ns=a.namespace)
    obs = prom.range(q, start, end + timedelta(minutes=70), 60)
    if not obs:
        raise SystemExit("FAIL: no observations for the canonical request-count query in the window")
    recs = load_forecasts(snapshot, a.app, a.namespace, start, end)
    if not recs:
        raise SystemExit("FAIL: no forecast issuances for the app in the window (is the forecast log complete?)")
    accepted, rejected = accept_records(recs)
    if not accepted:
        raise SystemExit(f"FAIL: every forecast record was rejected: {rejected[:5]}")
    # Raw-model accuracy: every structurally valid issued forecast (controller rejections never remove one).
    result, rows = score(accepted, prom, a.app, a.namespace, start, end, a.issuance_minutes, as_of=as_of)
    result["forecast_log_provenance"] = provenance
    result["set"] = "raw_model"
    result["as_of"] = iso(as_of)
    extra = json.load(open(a.transition_events)) if a.transition_events else None
    result["transition_events"] = transition_events(accepted, extra)
    result["decision_records"] = len(decisions)
    result["participation"] = part if part else "no decision records in the window"
    result["records_in_window"] = len(recs)
    result["records_rejected_structural"] = len(rejected)
    result["rejected_list"] = rejected[:50]
    # Controller rejections are diagnostics only (C-19): counts and the list, no "used" subset.
    rejections = controller_rejections(recs)
    result["controller_rejections"] = len(rejections)
    result["controller_rejection_list"] = [{"issued_at": k[2], "rejected_at": iso(v)}
                                           for k, v in sorted(rejections.items(), key=lambda kv: kv[0][2])][:50]
    result["controller_used_subset"] = "not reported: requires contemporaneous decision evidence (Codex C-19)"
    result["observation_series"] = series_gaps(obs, start, end + timedelta(minutes=70))
    reps = {}
    for d in [a.app] + [c for c in a.controls.split(",") if c]:
        s = prom.range(REPLICAS.format(d=d, ns=a.namespace), start, end, 60)
        if not s:
            raise SystemExit(f"FAIL: no replica series for deployment {d}")
        pm, credited, gap = pod_minutes(s, a.max_gap_minutes * 60)
        ev = {}
        for direction in ("up", "down"):
            e = prom.range(SCALE_EVENTS.format(d=d, ns=a.namespace, dir=direction, secs=int((end - start).total_seconds())), end, end, 60)
            ev[direction] = round(e[-1][1]) if e else None
        reps[d] = {"pod_minutes": pm, "credited_seconds": credited, "gap_seconds": gap, "series": series_gaps(s, start, end),
                   "min_replicas": min(v for _, v in s), "max_replicas": max(v for _, v in s),
                   "scale_events_from_operator_counter": ev, "sampled_replica_changes": sampled_changes(s)}
    result["replicas"] = reps
    text = json.dumps(result, indent=2)
    write_result(text, a.out)
    if a.out != "-":
        print(f"written {a.out}")
    if result["issuance_coverage"] < a.min_coverage:
        raise SystemExit(f"FAIL: issuance coverage {result['issuance_coverage']} below {a.min_coverage}")
    if result["step_coverage"] < a.min_coverage:
        raise SystemExit(f"FAIL: step coverage {result['step_coverage']} below {a.min_coverage}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
