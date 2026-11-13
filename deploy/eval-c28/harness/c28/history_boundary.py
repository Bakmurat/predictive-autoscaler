"""The live demonstration's clean history boundary (INSTALL-PLAN Review 1 item 4, agreed with Codex in three rounds).

Isolation is of the RAW SERIES, not a time filter (VictoriaMetrics' rate() reads the sample before its window):

  * per boundary attempt k a NEW scrape object c28-demo-history-<k>, created only after the origin O, scraping the
    ingress like the common scrape object, keeping only c28_ingress_requests_total, renaming it to
    c28_demo_history_requests_total and adding the label c28_history_epoch="<run id>-h<k>" (run-scoped: another
    run's attempt k never matches; every query also names the namespace). Every raw sample of those series is
    therefore younger than the object, hence than O. The common scrape object and every accounting query are unchanged
    (the renamed metric never matches them).
  * the PA query of attempt k (the operator compiles {{ .Name }}):
      sum(rate(c28_demo_history_requests_total{namespace="<ns>",target="{{ .Name }}",
                                              c28_history_epoch="<run id>-h<k>"}[60s]))
        and on() (vector(time()) >= G)
    with G = O + 300 fixed before the contract. The guard only avoids a first partial window; it is not the isolation.
  * the acceptance proof (acceptance()) from RAW samples, per target: exactly one series with the verified labels, no
    sample at or before O, a post-O predecessor at or before G - 60 (the first accepted rate window's), at least two
    samples inside (G - 60, G], gaps no longer than two scrape intervals from the first sample to G. An empty result or
    a missing target fails. A failed attempt is kept (never deleted); a retry takes a new k, a new O and G, a new query
    hash and a new contract.
"""

from __future__ import annotations

from typing import Dict, List

from . import demo as D
from . import frozen as F
from . import render as R

HISTORY_METRIC = "c28_demo_history_requests_total"
SOURCE_METRIC = "c28_ingress_requests_total"
EPOCH_LABEL = "c28_history_epoch"
GUARD_AFTER_ORIGIN_S = 300
RATE_RANGE_S = 60
SCRAPE_S = F.SCRAPE_INTERVAL_S    # the ingress scrape object's own interval (both scrape objects use it)
MAX_GAP_S = 2 * SCRAPE_S


def epoch_id(run_id: str, attempt: int) -> str:
    """The run-scoped epoch identifier: another run's attempt k (or another namespace's) never matches."""
    if not isinstance(run_id, str) or not run_id:
        raise R.ParamsError("the epoch needs the run id")
    return f"{run_id}-h{_attempt(attempt)}"


def scrape_name(attempt: int) -> str:
    return f"c28-demo-history-{attempt}"


def _attempt(attempt) -> int:
    if not isinstance(attempt, int) or isinstance(attempt, bool) or not 1 <= attempt <= 99:
        raise R.ParamsError("the history attempt must be an integer 1..99")
    return attempt


def history_scrape(p: Dict, attempt: int) -> Dict:
    """The attempt's own VMPodScrape: the common scrape object's selector and endpoint, plus metric relabelling that
    keeps only the ingress counter, renames it and labels the attempt. VMPodScrape only (the demonstration's scrape)."""
    attempt = _attempt(attempt)
    if p.get("scrape") != "VMPodScrape":
        raise R.ParamsError("the history scrape is a VMPodScrape (params.scrape)")
    common = next(o for o in D.render(p)["common"] if o["kind"] == "VMPodScrape"
                  and o["metadata"]["name"] == "c28-ingress")
    obj = {"apiVersion": common["apiVersion"], "kind": "VMPodScrape",
           "metadata": {"name": scrape_name(attempt), "namespace": p["namespace"],
                        "labels": {**common["metadata"]["labels"], EPOCH_LABEL: epoch_id(p["run_id"], attempt)}},
           "spec": {"selector": common["spec"]["selector"],
                    "podMetricsEndpoints": [dict(common["spec"]["podMetricsEndpoints"][0], metricRelabelConfigs=[
                        {"action": "keep", "sourceLabels": ["__name__"], "regex": SOURCE_METRIC},
                        {"action": "replace", "sourceLabels": ["__name__"], "targetLabel": "__name__",
                         "regex": SOURCE_METRIC, "replacement": HISTORY_METRIC},
                        {"action": "replace", "targetLabel": EPOCH_LABEL,
                         "replacement": epoch_id(p["run_id"], attempt)}])]}}
    return obj


def guard_s(origin_s: int) -> int:
    return origin_s + GUARD_AFTER_ORIGIN_S


def guarded_query(namespace: str, epoch: str, guard: int) -> str:
    """The PA's query template of the attempt ({{ .Name }} is the operator's substitution), scoped by namespace and
    the run-scoped epoch."""
    return (f'sum(rate({HISTORY_METRIC}{{namespace="{namespace}",target="{{{{ .Name }}}}",{EPOCH_LABEL}="{epoch}"}}'
            f'[{RATE_RANGE_S}s])) and on() (vector(time()) >= {int(guard)})')


def compiled_query(namespace: str, epoch: str, guard: int, target: str) -> str:
    """The exact string the operator compiles for `target` (what the forecaster reads and hashes)."""
    return guarded_query(namespace, epoch, guard).replace("{{ .Name }}", target)


def raw_selector(namespace: str, epoch: str, target: str) -> str:
    return f'{HISTORY_METRIC}{{namespace="{namespace}",target="{target}",{EPOCH_LABEL}="{epoch}"}}'


def acceptance(series_by_target: Dict[str, List[Dict]], targets: List[str], namespace: str, epoch: str,
               origin_s: float, guard: float) -> Dict:
    """The acceptance proof from RAW samples (a range-vector query's result per target: [{"metric": {...},
    "values": [[t, v], ...]}]). Accepted only when every target passes every check."""
    out = {"accepted": False, "epoch": epoch, "namespace": namespace, "origin_s": origin_s, "guard_s": guard,
           "targets": {},
           "problems": []}
    w_lo = guard - RATE_RANGE_S
    for t in targets:
        probs: List[str] = []
        series = series_by_target.get(t)
        if not series:
            out["targets"][t] = {"problems": ["no series at all"]}
            out["problems"].append(f"{t}: no series at all")
            continue
        if len(series) != 1:
            probs.append(f"{len(series)} series (exactly one expected)")
        s = series[0]
        m = s.get("metric") or {}
        want = {"__name__": HISTORY_METRIC, "namespace": namespace, "target": t, EPOCH_LABEL: epoch}
        wrong = {k: m.get(k) for k, v in want.items() if m.get(k) != v}
        if wrong:
            probs.append(f"labels not as relabelled: {wrong}")
        ts = sorted(float(x[0]) for x in s.get("values") or [])
        if not ts:
            probs.append("no raw sample")
        else:
            early = [x for x in ts if x <= origin_s]
            if early:
                probs.append(f"{len(early)} raw samples at or before the origin (first {early[0]})")
            pred = [x for x in ts if x <= w_lo]
            if not pred or pred[-1] <= origin_s:
                probs.append("no post-origin predecessor for the first accepted rate window")
            inside = [x for x in ts if w_lo < x <= guard]
            if len(inside) < 2:
                probs.append(f"{len(inside)} samples inside the first accepted window (at least 2)")
            span = [x for x in ts if x <= guard]
            gaps = [(a, b) for a, b in zip(span, span[1:]) if b - a > MAX_GAP_S]
            if span and guard - span[-1] > MAX_GAP_S:
                gaps.append((span[-1], guard))
            if gaps:
                probs.append(f"gaps above {MAX_GAP_S} s: {gaps[:3]}")
        out["targets"][t] = {"first_sample_s": ts[0] if ts else None, "samples_to_guard": len([x for x in ts
                                                                                               if x <= guard]),
                             "problems": probs}
        out["problems"] += [f"{t}: {x}" for x in probs]
    out["accepted"] = not out["problems"] and set(out["targets"]) == set(targets)
    return out
