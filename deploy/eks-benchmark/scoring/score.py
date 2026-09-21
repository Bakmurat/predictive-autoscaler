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
The script exits non-zero when nothing can be scored, a required series is missing, or coverage is
below the gate. Known-answer tests: score_test.py.

Usage:
  kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090:9090 &
  kubectl -n ml-engine exec deploy/predictive-operator -- cat /var/lib/predictive-autoscaler/forecasts.jsonl > forecasts.jsonl
  python3 score.py --prom http://localhost:9090 --forecast-log forecasts.jsonl \
      --start 2026-09-24T00:00:00Z --end 2026-09-26T00:00:00Z --app nginx-test --namespace demo
"""
import argparse, json, math, sys, urllib.parse, urllib.request
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


def load_forecasts(path, app, ns, start, end):
    recs = []
    with open(path, encoding="utf-8") as f:
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
            r["_line"] = n
            issued = parse_ts(r["issued_at"])
            if start <= issued < end:
                recs.append(r)
    return recs


def sanity_rejected_keys(recs_and_events):
    """Issuances the operator discarded by its divergence check ("event": "sanity_rejected" lines).
    Keyed like accept_records: (application, namespace, issued_at ISO)."""
    keys = set()
    for r in recs_and_events:
        if r.get("event") == "sanity_rejected":
            keys.add((r.get("application"), r.get("namespace"), iso(parse_ts(r["issued_at"]))))
    return keys


def accept_records(recs):
    """Apply the acceptance rules in log order. Returns (accepted, rejected[{line, issued_at, reason}]).
    Event lines ("event" key) are never scored; a "sanity_rejected" event excludes the forecast record
    with the same (application, namespace, issued_at)."""
    accepted, rejected, seen = [], [], set()
    last_issued, last_cutoff, last_trained = None, None, None
    rejected_keys = sanity_rejected_keys(recs)
    for r in recs:
        if r.get("event"):
            continue
        issued = parse_ts(r["issued_at"])
        key = (r.get("application"), r.get("namespace"), iso(issued))
        reason = None
        cutoff = parse_ts(r["training_cutoff"]) if r.get("training_cutoff") else None
        trained = parse_ts(r["model_trained_at"]) if r.get("model_trained_at") else None
        if key in rejected_keys:
            reason = "sanity_rejected"
        elif key in seen:
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


def score(accepted, prom, app, ns, start, end, cadence_min=5.0):
    """Score accepted records against point samples at their target times. prom must provide
    instant(query, at). Returns (summary, rows)."""
    q = CANONICAL.format(app=app, ns=ns)
    guard = NoPeeking(prom)
    per_step, rows, gaps, zero_obs = {}, [], [], 0
    steps_total = 0
    for r in accepted:
        issued = parse_ts(r["issued_at"])
        persist = guard.at(q, issued, issued)
        for fc in r["forecasts"]:
            steps_total += 1
            target = parse_ts(fc["target_at"])
            y = prom.instant(q, target)
            if y is None:
                gaps.append({"issued_at": r["issued_at"], "step": fc["step"], "target_at": fc["target_at"], "reason": "no observation at target"})
                continue
            f = float(fc["rpm"])
            prev = guard.at(q, target - timedelta(hours=24), issued)
            row = {"issued_at": r["issued_at"], "target_at": fc["target_at"], "step": fc["step"],
                   "model_version": r.get("model_version"), "training_cutoff": r.get("training_cutoff"),
                   "target_anchor": r.get("target_anchor"),
                   "forecast": f, "actual": y, "ae": abs(f - y),
                   "ape": (abs(f - y) / y) if y > 0 else None,
                   "persistence_ape": (abs(persist - y) / y) if (persist is not None and y > 0) else None,
                   "persistence_ae": abs(persist - y) if persist is not None else None,
                   "prevday_ape": (abs(prev - y) / y) if (prev is not None and y > 0) else None,
                   "prevday_ae": abs(prev - y) if prev is not None else None}
            if y <= 0:
                zero_obs += 1
            rows.append(row)
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
        "step_coverage": round(len(rows) / steps_total, 4) if steps_total else 0.0,
        "zero_observations": zero_obs, "gaps": len(gaps), "gap_list": gaps[:50],
        "overall": {"MAPE_percent": agg(rows, "ape"), "MAE_rpm": agg(rows, "ae"),
                    "persistence_MAPE_percent": agg(rows, "persistence_ape"), "persistence_MAE_rpm": agg(rows, "persistence_ae"),
                    "prevday_MAPE_percent": agg(rows, "prevday_ape"), "prevday_MAE_rpm": agg(rows, "prevday_ae"),
                    "under_predicted_share": round(sum(1 for x in rows if x["forecast"] < x["actual"]) / len(rows), 3) if rows else None},
        "per_step": {str(k): {"n": len(v), "MAPE_percent": agg(v, "ape"), "MAE_rpm": agg(v, "ae"),
                              "persistence_MAPE_percent": agg(v, "persistence_ape"), "prevday_MAPE_percent": agg(v, "prevday_ape")}
                     for k, v in sorted(per_step.items())},
        "models_seen": sorted({(x["model_version"] or "?") for x in rows}),
        "training_cutoffs_seen": sorted({(x["training_cutoff"] or "?") for x in rows}),
        "target_anchors_seen": sorted({(x["target_anchor"] or "?") for x in rows}),
    }
    return out, rows


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


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prom", required=True); ap.add_argument("--forecast-log", required=True)
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True)
    ap.add_argument("--app", default="nginx-test"); ap.add_argument("--namespace", default="demo")
    ap.add_argument("--controls", default="nginx-reactive,myapptwo", help="comma-separated comparison deployments")
    ap.add_argument("--issuance-minutes", type=float, default=5.0, help="operator issuance cadence (prediction cache TTL)")
    ap.add_argument("--min-coverage", type=float, default=0.8)
    ap.add_argument("--max-gap-minutes", type=float, default=5.0, help="replica sample interval above which time is a gap")
    ap.add_argument("--out", default="-")
    a = ap.parse_args(argv)
    start, end = parse_ts(a.start), parse_ts(a.end)
    prom = Prom(a.prom)
    q = CANONICAL.format(app=a.app, ns=a.namespace)
    obs = prom.range(q, start, end + timedelta(minutes=70), 60)
    if not obs:
        raise SystemExit("FAIL: no observations for the canonical request-count query in the window")
    recs = load_forecasts(a.forecast_log, a.app, a.namespace, start, end)
    if not recs:
        raise SystemExit("FAIL: no forecast issuances for the app in the window (is the forecast log complete?)")
    accepted, rejected = accept_records(recs)
    if not accepted:
        raise SystemExit(f"FAIL: every forecast record was rejected: {rejected[:5]}")
    result, rows = score(accepted, prom, a.app, a.namespace, start, end, a.issuance_minutes)
    result["records_in_window"] = len(recs)
    result["records_rejected"] = len(rejected)
    result["rejected_list"] = rejected[:50]
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
    if a.out == "-":
        print(text)
    else:
        open(a.out, "w").write(text + "\n"); print(f"written {a.out}")
    if result["issuance_coverage"] < a.min_coverage:
        raise SystemExit(f"FAIL: issuance coverage {result['issuance_coverage']} below {a.min_coverage}")
    if result["step_coverage"] < a.min_coverage:
        raise SystemExit(f"FAIL: step coverage {result['step_coverage']} below {a.min_coverage}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
