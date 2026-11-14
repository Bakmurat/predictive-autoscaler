#!/usr/bin/env python3
"""The live demonstration's history boundary, executed (INSTALL-PLAN Review 1 item 4; Codex: GO on the sequence
query update -> contract -> generator -> history scrape -> acceptance, with its five executor conditions).

  boundary.py --record R plan-query --attempt K          read-only, reviewable any time before the origin O
  boundary.py --record R apply-query --plan F --plan-digest D
  boundary.py --record R prove-hold --attempt K --query-api URL --ledger F   read-only: the live hold proof before G
  boundary.py --record R plan-scrape --attempt K         read-only, reviewable BEFORE O (timing enforced at apply)
  boundary.py --record R apply-scrape --plan F --plan-digest D --query-api URL
  boundary.py --record R accept --attempt K --query-api URL                  read-only reads, one receipt
  boundary.py --record R plan-retire-generator | retire-generator --plan F --plan-digest D   (a retry only)
  boundary.py --record R recover-plan | recover --plan F --plan-digest D

O is params.demonstration.schedule_origin_utc (a UTC midnight, the one params edit made before plan-query) and
G = O + 300. The epoch is history_boundary.epoch_id(run, K). Every mutation is journalled under the shared evaluation
lock; every plan and apply refuses the shared fence (open intents of any part-2 helper, open restore obligations,
any bounded workload or its pods, Terminating included).

apply-query: the two PredictiveAutoscalers are updated SEPARATELY (each its own journalled update, UID + the read
resourceVersion, expect the guarded query), then each must report the compiled query (status.metricSource, the
current generation) and the selected lead 600 for that generation. A partial update leaves the attempt INCOMPLETE
(receipt attempt-incomplete): never a basis for a contract.

apply-scrape: only when now > O, now <= G - 120 s, the continuous generator's validated start record and first send
(demo_reduce, bound to the generator configuration's digest) exist for every target, the attempt's query update is
complete, and no c28-demo-history-K ever existed (receipts and live). Missing the cutoff VOIDS the attempt.

accept: RAW reads per target (history_boundary.raw_selector over [O - 600 s, G]) through the observer, the
acceptance proof, the raw evidence written with its sha256, and ONE receipt (boundary-accepted or boundary-failed)
bound to O/G, the epoch, the scrape object's UID and spec digest, both PA and target UIDs and generations, the
compiled query hashes, the generator configuration digest and the raw evidence hash. A failed attempt is kept;
nothing is deleted. A retry is a new K with a new O (the next midnight).

retire-generator: for a retry only (the last attempt failed, voided or incomplete): the continuous generator
(StatefulSet, Service, ConfigMap) deleted by recorded UID, its pod deleted by UID and proven gone (journalled), its
logs pulled to evidence first; the history series and every receipt stay.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import bounded as BD
import common_records as C
import execlib as X
import prepare

HELPER = "boundary"
SCRAPE_MARGIN_S = 120
ACCEPT_AFTER_G_S = 30
Fetch = Callable[[str], Dict]


def _ms(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


class Ctx:
    def __init__(self, record: X.Record, kube: X.Kube, now: Callable[[], float] = lambda: time.time() * 1000,
                 sleep: Callable[[float], None] = time.sleep):
        self.record, self.kube, self.now, self.sleep = record, kube, now, sleep
        self.s = prepare._section(record)
        if prepare.stage_of(self.s) != "demonstration":
            raise X.StagingError("the history boundary belongs to the demonstration stage")
        self.params = prepare.load_params(record, "demonstration")
        self.c28 = X.import_harness()
        self.hb = importlib.import_module("c28.history_boundary")
        self.reduce = importlib.import_module("c28.demo_reduce")
        self.ns = self.params["namespace"]
        self.origin_s = _ms(self.params["demonstration"]["schedule_origin_utc"]) // 1000
        self.guard_s = self.hb.guard_s(self.origin_s)

    def epoch(self, k: int) -> str:
        return self.hb.epoch_id(self.record.run_id, k)

    def bctx(self) -> BD.Ctx:
        return BD.Ctx(self.record, self.kube, now=self.now, sleep=self.sleep)


def attempts_used(record: X.Record) -> List[int]:
    """Every attempt number this helper ever touched: receipts AND journalled intents (a crash before the summary
    receipt still uses the number)."""
    out = set()
    for e in X.receipts(record, HELPER):
        d = e.get("data") or {}
        for v in (d.get("attempt"), (d.get("context") or {}).get("attempt")):
            if isinstance(v, int):
                out.add(v)
    return sorted(out)


def _events(c: Ctx, k: int, event: str) -> List[Dict]:
    return [e["data"] for e in X.receipts(c.record, HELPER) if e["event"] == event and e["data"].get("attempt") == k]


def fence(c: Ctx) -> List[str]:
    out = BD.fence_problems(c.bctx())                      # bounded workloads, their pods, open intents, obligations
    out = [x for x in out if "c28-demo-generator" not in x]   # the continuous generator is this tool's own business
    for h in X.PART2_HELPERS:
        if h not in ("bounded", "prepare") and X.open_intents(X.receipts(c.record, h)):
            out.append(f"{h} has intents with unknown outcomes: recover first")
    return out


def _admission(c: Ctx) -> Tuple[Optional[Dict], str]:
    """The last admission run-complete: PASS, its evidence and verdict files still hashing to the receipt, and a
    binding that carries params_except_origin_sha256 (the origin may change by the reviewed transformation only)."""
    done = [e["data"] for e in X.receipts(c.record, "bounded") if e["event"] == "run-complete"
            and e["data"].get("label") == "admission"]
    if not done or done[-1].get("verdict") != "PASS":
        return None, "no current admission PASS"
    r = done[-1]
    for f, key in (("evidence.json", "evidence_sha256"), ("verdict.json", "verdict_sha256")):
        path = os.path.join(r.get("dir") or "", f)
        if not os.path.isfile(path) or C.sha256_file(path) != r.get(key):
            return None, f"the admission {f} no longer hashes to its receipt"
    if "params_except_origin_sha256" not in (r.get("binding") or {}):
        return None, "the admission binding predates the origin transformation (re-run the admission)"
    return r, ""


def strip_query_digest(spec: Dict) -> str:
    """The spec's digest without spec.metrics.requests.source.query (what a query-only change leaves unchanged)."""
    s_ = copy.deepcopy(spec or {})
    src = ((s_.get("metrics") or {}).get("requests") or {}).get("source")
    if isinstance(src, dict):
        src.pop("query", None)
    return X.json_digest(s_)


def query_only_change_ok(c: Ctx, t: str, pa: Dict) -> bool:
    """A PA whose spec differs from its recorded pre-image ONLY by a query that one of this helper's attempts wrote
    (pa-pre-image receipts, durable before each write): a proven query-only change, e.g. a partial attempt whose status
    confirmation never came. Nothing else may differ."""
    pre = [e["data"] for e in X.receipts(c.record, HELPER) if e["event"] == "pa-pre-image"
           and e["data"].get("name") == t and e["data"].get("uid") == pa["metadata"].get("uid")]
    if not pre:
        return False
    query = ((((pa.get("spec") or {}).get("metrics") or {}).get("requests") or {}).get("source") or {}).get("query")
    return strip_query_digest(pa.get("spec")) == pre[-1]["spec_wo_query_sha256"] and \
        query in {x["query"] for x in pre}


def expected_pa_spec(c: Ctx, t: str, adm: Dict) -> str:
    """The reviewed chain of a PA's spec: the admission's digest, then each completed query update's recorded digest
    (that update verified it changed only spec.metrics.requests.source); the latest one is what the PA must be."""
    want = adm["binding"]["pa_specs"].get(t)
    for e in X.receipts(c.record, HELPER):
        if e["event"] == "query-updated":
            for row in e["data"].get("pas") or []:
                if row.get("name") == t and row.get("spec_sha256"):
                    want = row["spec_sha256"]
        elif e["event"] == "pa-query-written" and e["data"].get("name") == t:
            want = e["data"]["spec_sha256"]
    return want


def basis_problems(c: Ctx) -> List[str]:
    """Everything the admission bound, re-checked LIVE through the only reviewed transformations: the params may
    differ by the schedule origin alone (params_except_origin_sha256), each PA spec must be the end of its reviewed
    chain (expected_pa_spec); images, pod templates, PA identities, node environment and the peak must be unchanged."""
    adm, why = _admission(c)
    if adm is None:
        return [why]
    b = c.bctx()
    targets, _ = BD._targets_state(b)
    live = BD.binding(b, targets)
    want = dict(adm["binding"])
    out = []
    for k in sorted(set(want) | set(live)):
        if k in ("params_sha256", "pa_specs"):
            continue
        if want.get(k) != live.get(k):
            out.append(f"{k} differs from the admission binding")
    for t in sorted(want.get("pa_specs") or {}):
        pa = c.kube.get("PredictiveAutoscaler", t, c.ns) or {"metadata": {}}
        if (live.get("pa_specs") or {}).get(t) != expected_pa_spec(c, t, adm) and not query_only_change_ok(c, t, pa):
            out.append(f"PredictiveAutoscaler {t}'s spec is not the end of its reviewed chain (admission, then the "
                       "recorded query updates)")
    return out


def startup_measured(c: Ctx, check_live: bool = True) -> Tuple[Optional[Dict], str]:
    """The last startup-complete must be measured, its evidence must still hash to the receipt, and it must name the
    current admission run; (check_live) the admission basis must still hold through the reviewed transformations."""
    done = [e["data"] for e in X.receipts(c.record, "bounded") if e["event"] == "startup-complete"]
    if not done:
        return None, "no startup step was run"
    r = done[-1]
    if not r.get("measured"):
        return None, "the last startup step was not a measurement"
    path = os.path.join(r.get("dir") or "", "evidence.json")
    if not os.path.isfile(path) or C.sha256_file(path) != r.get("evidence_sha256"):
        return None, "the startup evidence no longer hashes to its receipt"
    adm, why = _admission(c)
    if adm is None or r.get("admission") != adm.get("run"):
        return None, why or "the startup step was not run under the current admission PASS"
    if check_live:
        probs = basis_problems(c)
        if probs:
            return None, "; ".join(probs)
    return r, ""


def _pas(c: Ctx) -> Tuple[Dict[str, Dict], List[str]]:
    created = prepare.created_objects(c.record)
    out, probs = {}, []
    for t in c.c28.demo.target_names(c.params):
        ref, dref = created.get(("PredictiveAutoscaler", c.ns, t)), created.get(("Deployment", c.ns, t))
        pa, dep = c.kube.get("PredictiveAutoscaler", t, c.ns), c.kube.get("Deployment", t, c.ns)
        if not ref or not pa or pa["metadata"]["uid"] != ref["uid"]:
            probs.append(f"PredictiveAutoscaler {t} was not created by this run or was replaced")
            continue
        if not dref or not dep or dep["metadata"]["uid"] != dref["uid"]:
            probs.append(f"Deployment {t} was not created by this run or was replaced")
            continue
        if pa["spec"].get("mode") != "Recommend":
            probs.append(f"PredictiveAutoscaler {t} is not in Recommend")
        out[t] = {"pa_uid": pa["metadata"]["uid"], "pa_generation": pa["metadata"].get("generation"),
                  "deployment_uid": dep["metadata"]["uid"], "query": ((pa["spec"].get("metrics") or {}).get(
                      "requests") or {}).get("source", {}).get("query")}
    return out, probs


def plan_query(c: Ctx, k: int) -> Dict:
    epoch = c.epoch(k)
    template = c.hb.guarded_query(c.ns, epoch, c.guard_s)
    problems = fence(c)
    _, why = startup_measured(c)
    if why:
        problems.append(f"the startup step must be measured first, under a still-valid admission PASS: {why}")
    if c.origin_s % 86400:
        problems.append("schedule_origin_utc must be a UTC midnight")
    if c.now() / 1000 >= c.origin_s:
        problems.append("the origin is not in the future: the query update happens before O")
    used = attempts_used(c.record)
    if used and k <= max(used):
        problems.append(f"attempt {k} is not above the last attempt {max(used)} (receipts and journal)")
    pas, pp = _pas(c)
    problems += pp
    actions = [{"update": "PredictiveAutoscaler", "name": t, "uid": v["pa_uid"], "from_generation": v["pa_generation"],
                "query": template} for t, v in sorted(pas.items())]
    return X.new_plan(HELPER, c.record, actions, problems=problems, attempt=k, epoch=epoch,
                      origin_s=c.origin_s, guard_s=c.guard_s, template=template)


def _status_ok(pa: Dict, compiled: str) -> bool:
    st = pa.get("status") or {}
    ms = st.get("metricSource") or {}
    lead = ((st.get("prediction") or {}).get("lead") or {})
    gen = pa["metadata"].get("generation")
    return ms.get("query") == compiled and ms.get("sha256") == hashlib.sha256(compiled.encode()).hexdigest() \
        and ms.get("observedGeneration", gen) == gen and lead.get("seconds") == 600 \
        and lead.get("observedGeneration") == gen


def apply_query(c: Ctx, reviewed: Dict, reviewed_digest: str, wait_s: float = 300) -> Dict:
    k = reviewed["attempt"]
    with X.mutating(c.record, c.kube, HELPER, {"attempt": k, "step": "query"}):
        p = plan_query(c, k)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
        if p["problems"]:
            raise X.StagingError("the plan has problems: " + "; ".join(p["problems"]))
        done = []
        try:
            for a in p["actions"]:
                expected: Dict = {}

                def mutate(body, a=a):
                    if body["metadata"].get("generation") != a["from_generation"]:
                        raise X.StagingError(f"{a['name']} changed since the plan")
                    body["spec"].setdefault("metrics", {}).setdefault("requests", {})["source"] = {
                        "preset": "prometheus", "query": a["query"]}
                    expected["spec"] = copy.deepcopy(body["spec"])        # the pre-image + the query, nothing else
                pre_live = c.kube.get("PredictiveAutoscaler", a["name"], c.ns)
                # The pre-image becomes a recovery basis ONLY when it is the reviewed state: the plan's UID and
                # generation, and a spec that is the end of the reviewed chain or a proven query-only variant of an
                # earlier (itself validated) pre-image. Anything else is refused before it is recorded.
                adm, why = _admission(c)
                if pre_live is None or pre_live["metadata"].get("uid") != a["uid"] or \
                        pre_live["metadata"].get("generation") != a["from_generation"]:
                    raise X.StagingError(f"{a['name']} is not the reviewed object/generation before the write")
                if adm is None or (X.json_digest(pre_live.get("spec")) != expected_pa_spec(c, a["name"], adm)
                                   and not query_only_change_ok(c, a["name"], pre_live)):
                    raise X.StagingError(f"{a['name']}'s spec is not the reviewed state before the write "
                                         f"({why or 'outside its reviewed chain'})")
                X.receipt(c.record, HELPER, "pa-pre-image", {          # durable BEFORE the write: what may change
                    "attempt": k, "name": a["name"], "uid": a["uid"], "query": a["query"],
                    "spec_wo_query_sha256": strip_query_digest(pre_live["spec"])})
                out = c.kube.update("PredictiveAutoscaler", a["name"], c.ns, a["uid"], mutate,
                                    expect=[["/spec/metrics/requests/source/query", a["query"]]])
                if out.get("spec") != expected["spec"] or out["spec"].get("mode") != "Recommend":
                    raise X.StagingError(f"{a['name']}: the update did not produce exactly the reviewed spec")
                gen, spec_sha = out["metadata"].get("generation"), X.json_digest(out["spec"])
                X.receipt(c.record, HELPER, "pa-query-written", {"attempt": k, "name": a["name"], "uid": a["uid"],
                                                                 "generation": gen, "spec_sha256": spec_sha})
                compiled = c.hb.compiled_query(c.ns, p["epoch"], p["guard_s"], a["name"])
                end = c.now() + wait_s * 1000
                while True:
                    pa = c.kube.get("PredictiveAutoscaler", a["name"], c.ns)
                    if pa is None or pa["metadata"]["uid"] != a["uid"]:
                        raise X.StagingError(f"{a['name']} was replaced during the query update")
                    if pa["metadata"].get("generation") != gen or X.json_digest(pa.get("spec")) != spec_sha:
                        raise X.StagingError(f"{a['name']} was changed by someone else after the query update "
                                             "(generation or spec): an unreviewed change is never adopted")
                    if _status_ok(pa, compiled):
                        break
                    if c.now() >= end:
                        raise X.StagingError(f"{a['name']} did not report the compiled query and lead 600 for its "
                                             f"generation within {wait_s:.0f} s")
                    c.sleep(5)
                done.append({"name": a["name"], "uid": a["uid"], "generation": gen, "spec_sha256": spec_sha,
                             "compiled_sha256": hashlib.sha256(compiled.encode()).hexdigest()})
        except Exception as e:
            X.receipt(c.record, HELPER, "attempt-incomplete", {"attempt": k, "done": done, "why": str(e)[:300]})
            raise
        return X.receipt(c.record, HELPER, "query-updated", {"attempt": k, "epoch": p["epoch"],
                                                              "updated_s": c.now() / 1000,
                                                              "origin_s": p["origin_s"], "guard_s": p["guard_s"],
                                                              "pas": done, "plan_sha256": reviewed_digest})


MODE_MAX_GAP_S = 120
BASELINE_LOOKBACK_S = 300


def _covering_series(series: List[Dict], lo_s: float, hi_s: float, values_ok) -> Tuple[bool, str, Optional[tuple]]:
    """Exactly one series (one operator identity), distinct finite samples, a BASELINE sample at or before lo_s, a
    covered endpoint (a sample within 120 s of hi_s), no gap above 120 s from the baseline to the endpoint, and
    values_ok(values) from the baseline on. Returns (ok, why, identity) with identity = (pod, instance) labels."""
    if len(series) != 1:
        return False, f"{len(series)} series (exactly one operator identity expected)", None
    m = series[0].get("metric") or {}
    ident = (m.get("pod"), m.get("instance"))
    pts: Dict[float, float] = {}
    for t, v in series[0].get("values") or []:
        try:
            tt, vv = float(t), float(v)
        except (TypeError, ValueError):
            return False, "a sample is not a number", ident
        if not (math.isfinite(tt) and math.isfinite(vv)):
            return False, "a sample is not finite", ident
        if tt in pts and pts[tt] != vv:
            return False, f"conflicting samples at {tt}", ident
        pts[tt] = vv
    base = [t for t in pts if t <= lo_s]
    if not base:
        return False, "no baseline sample at or before the update", ident
    ts = sorted(t for t in pts if t >= max(base))
    if hi_s - ts[-1] > MODE_MAX_GAP_S or any(b - a > MODE_MAX_GAP_S for a, b in zip(ts, ts[1:])):
        return False, "the samples do not cover the interval up to its endpoint", ident
    if len([t for t in ts if t > lo_s]) < 1:
        return False, "no sample after the update", ident
    if not values_ok([pts[t] for t in ts]):
        return False, "the values are not as required", ident
    return True, "", ident


def ledger_rows(record: X.Record, path: str) -> List[Dict]:
    """The decision records of a ledger file that ledger_read.py itself wrote and receipted (out path and captured
    bytes' sha256); any other file is refused."""
    reads = [e["data"] for e in X.receipts(record, "ledger_read") if e["event"] == "read"]
    sha = C.sha256_file(path) if os.path.isfile(path) else None
    if not any(r.get("out") == path and r.get("sha256") == sha for r in reads):
        raise X.StagingError(f"{path} is not a verified ledger_read.py read (no receipt with its path and sha256)")
    with open(path) as fh:
        return [json.loads(ln) for ln in fh if ln.strip()]


def prove_hold(c: Ctx, k: int, fetch: Fetch, api: str, rows: List[Dict]) -> Dict:
    """Read-only, after the query update and BEFORE G, for the attempt's own identities: per PA the compiled query for
    the recorded generation, TelemetryAvailable False, the ledger's decision records since the update (event decision,
    application = the target, namespace) all keep_current with nothing applied, and counter evidence COVERING the
    interval: the PA's predictive_autoscaler_mode series (Recommend = 1) sampled without gaps above 120 s from the
    update to now, and the applied-write counter either absent (never incremented) or unchanged over the interval."""
    q = (_events(c, k, "query-updated") or [None])[-1]
    if q is None:
        return {"proven": False, "problems": ["the attempt's query update is not complete"], "attempt": k}
    now_s = c.now() / 1000
    probs = []
    if now_s >= c.guard_s:
        probs.append("the hold is proven before G only")
    since_s = q["updated_s"]
    per = {}
    for r in q["pas"]:
        name = r["name"]
        pa = c.kube.get("PredictiveAutoscaler", name, c.ns) or {"metadata": {}}
        compiled = c.hb.compiled_query(c.ns, q["epoch"], q["guard_s"], name)
        ident = pa["metadata"].get("uid") == r["uid"] and pa["metadata"].get("generation") == r["generation"]
        conds = {x["type"]: x for x in (pa.get("status") or {}).get("conditions") or []}
        tele = conds.get("TelemetryAvailable", {})
        mine = [x for x in rows if x.get("event") == "decision" and x.get("application") == name
                and x.get("namespace") == c.ns and _ms(str(x.get("at"))[:19] + "Z") / 1000 >= since_s]
        bad = [x for x in mine if x.get("desired_source") != "keep_current"
               or x.get("applied_replicas") != x.get("current_replicas")]
        sel = f'namespace="{c.ns}",autoscaler="{name}"'
        span = int(now_s - since_s) + 1 + BASELINE_LOOKBACK_S       # extended backwards: the baseline
        mode = BD._q(fetch, api, "query", query=f'predictive_autoscaler_mode{{{sel},mode="Recommend"}}[{span}s]',
                     time=f"{now_s:.3f}")
        writes = BD._q(fetch, api, "query", query=f'predictive_autoscaler_scale_writes_total{{{sel},result="applied"}}'
                                                   f'[{span}s]', time=f"{now_s:.3f}")
        mode_ok, mode_why, mode_id = _covering_series(mode, since_s, now_s, lambda vals: all(v == 1 for v in vals))
        # The operator initialises its write counters at zero (stage_metrics.go), so a missing series is missing
        # evidence, never "no write": exactly one series (one operator identity) covering the interval, its values
        # constant (no increase, no reset).
        writes_ok, writes_why, writes_id = _covering_series(writes, since_s, now_s, lambda vals: len(set(vals)) == 1)
        if mode_ok and writes_ok and mode_id != writes_id:
            writes_ok, writes_why = False, f"the mode series ({mode_id}) and the counter ({writes_id}) come from " \
                                           "different operator identities"
        covered, changed = mode_ok, not writes_ok
        ok = ident and _status_ok(pa, compiled) and tele.get("status") == "False" and bool(mine) and not bad \
            and covered and not changed
        per[name] = {"uid": r["uid"], "generation": r["generation"],
                     "query_sha256": hashlib.sha256(compiled.encode()).hexdigest(),
                     "telemetry": [tele.get("status"), tele.get("reason")], "decisions": len(mine),
                     "not_keep_current": len(bad), "mode": mode_why or "covered, Recommend throughout",
                     "applied_counter": writes_why or "covered, constant", "ok": bool(ok)}
        if not ok:
            probs.append(f"{name}: hold not proven {per[name]}")
    out = {"proven": not probs, "pas": per, "problems": probs, "attempt": k, "at_s": now_s}
    X.receipt(c.record, HELPER, "hold-proof", out)
    return out


# --- the history scrape ------------------------------------------------------------------------------------------------

def _generator_started(c: Ctx, logs: Callable[[str], List[str]]) -> Tuple[Optional[str], List[str]]:
    """The continuous generator's configuration digest and every target's validated first send, from its pod log."""
    cfg = c.c28.demo.generator_config(c.params)
    pod = c.kube.get("Pod", c.c28.demo.generator_pod_name(), c.ns)
    if pod is None or (pod.get("status") or {}).get("phase") != "Running":
        return None, ["the continuous generator pod is not Running"]
    ann = (pod["metadata"].get("annotations") or {}).get("c28.devkuban.com/generator-config")
    if ann != cfg["digest"]:
        return None, ["the generator pod does not run the configuration of these params"]
    fs = BD.first_sends(c.bctx(), cfg, logs(pod["metadata"]["name"]))
    late = [t for t, v in fs.items() if v is None]
    return cfg["digest"], ([f"no validated generator start and first send for {late}"] if late else [])


def plan_scrape(c: Ctx, k: int) -> Dict:
    obj = c.hb.history_scrape(c.params, k)
    problems = [x for x in fence(c)]
    if not _events(c, k, "query-updated"):
        problems.append(f"attempt {k}'s query update is not complete")
    if any(_events(c, k, e) for e in ("scrape-created", "attempt-void", "boundary-accepted", "boundary-failed")):
        problems.append(f"attempt {k} already created, voided or decided its scrape")
    if c.kube.get("VMPodScrape", obj["metadata"]["name"], c.ns) is not None:
        problems.append(f"{obj['metadata']['name']} exists (an attempt's scrape is created exactly once)")
    return X.new_plan(HELPER, c.record, [{"create": "VMPodScrape", "name": obj["metadata"]["name"],
                                          "sha256": X.json_digest(obj)}], problems=problems, attempt=k,
                      epoch=c.epoch(k), timing={"after_s": c.origin_s, "until_s": c.guard_s - SCRAPE_MARGIN_S})


def apply_scrape(c: Ctx, reviewed: Dict, reviewed_digest: str, logs: Callable[[str], List[str]]) -> Dict:
    k = reviewed["attempt"]
    with X.mutating(c.record, c.kube, HELPER, {"attempt": k, "step": "scrape"}):
        p = plan_scrape(c, k)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
        if p["problems"]:
            raise X.StagingError("the plan has problems: " + "; ".join(p["problems"]))
        now_s = c.now() / 1000
        if now_s <= c.origin_s or now_s > c.guard_s - SCRAPE_MARGIN_S:
            X.receipt(c.record, HELPER, "attempt-void", {"attempt": k, "why": f"outside (O, G - {SCRAPE_MARGIN_S} s] "
                                                                            f"at {now_s:.0f}"})
            raise X.StagingError(f"VOID: the history scrape may be created only in (O, G - {SCRAPE_MARGIN_S} s]")
        digest, why = _generator_started(c, logs)
        if why:
            raise X.StagingError("the generator is not proven started: " + "; ".join(why))
        obj = c.hb.history_scrape(c.params, k)
        if X.json_digest(obj) != p["actions"][0]["sha256"]:
            raise X.StagingError("the scrape object rendered differently from the reviewed plan")
        before_s = c.now() / 1000                                   # re-checked IMMEDIATELY before the create
        if before_s <= c.origin_s or before_s > c.guard_s - SCRAPE_MARGIN_S:
            X.receipt(c.record, HELPER, "attempt-void", {"attempt": k, "why": f"the cutoff passed while checking "
                                                                            f"({before_s:.0f})"})
            raise X.StagingError(f"VOID: the cutoff G - {SCRAPE_MARGIN_S} s passed before the create")
        made = c.kube.create(obj)
        created_s = c.now() / 1000                                  # the actual creation time, recorded
        data = {"attempt": k, "epoch": c.epoch(k), "uid": made["metadata"]["uid"],
                "spec_sha256": X.json_digest(obj["spec"]), "checked_s": now_s, "before_create_s": before_s,
                "created_s": created_s, "generator_config_digest": digest, "plan_sha256": reviewed_digest}
        if created_s > c.guard_s - SCRAPE_MARGIN_S:                 # the create itself overran: void, object kept
            X.receipt(c.record, HELPER, "attempt-void", dict(data, why="the create completed after the cutoff"))
            raise X.StagingError("VOID: the history scrape's create completed after the cutoff (object kept)")
        return X.receipt(c.record, HELPER, "scrape-created", data)


def _write_same(path: str, text: str) -> str:
    """Write once; a rerun with identical content is accepted (idempotent), different content is refused."""
    if os.path.exists(path):
        with open(path) as fh:
            if fh.read() != text:
                raise X.StagingError(f"{path} exists with other content")
        return C.sha256_file(path)
    return X.write_new(path, text)


def _live_attempt_problems(c: Ctx, k: int, qu: Dict, sc: Dict) -> List[str]:
    """The attempt's live binding: both targets, the original PA and Deployment UIDs, the PA generations recorded by
    the query update (nothing changed since), each PA's status reporting the compiled query and its hash, O/G and the
    epoch the same across receipts, the generator configuration the scrape was created under, the scrape object
    itself, and everything else of the admission binding unchanged (only the PA queries were the reviewed change)."""
    out = []
    epoch = c.epoch(k)
    if (qu.get("origin_s"), qu.get("guard_s"), qu.get("epoch")) != (c.origin_s, c.guard_s, epoch) or \
            sc.get("epoch") != epoch:
        out.append("O, G or the epoch differ between the params and the attempt's receipts")
    rec = {x["name"]: x for x in qu.get("pas") or []}
    targets = c.c28.demo.target_names(c.params)
    if set(rec) != set(targets):
        out.append(f"the query update covered {sorted(rec)}, not both targets {targets}")
    created = prepare.created_objects(c.record)
    for t in targets:
        pa = c.kube.get("PredictiveAutoscaler", t, c.ns)
        dep = c.kube.get("Deployment", t, c.ns)
        dref = created.get(("Deployment", c.ns, t))
        r = rec.get(t) or {}
        compiled = c.hb.compiled_query(c.ns, epoch, c.guard_s, t)
        if pa is None or pa["metadata"]["uid"] != r.get("uid"):
            out.append(f"PredictiveAutoscaler {t} is gone or replaced")
            continue
        if X.json_digest(pa.get("spec")) != r.get("spec_sha256") or pa["spec"].get("mode") != "Recommend":
            out.append(f"PredictiveAutoscaler {t}'s spec is not the one its query update produced (or not Recommend)")
        if pa["metadata"].get("generation") != r.get("generation"):
            out.append(f"PredictiveAutoscaler {t} changed after the query update (generation "
                       f"{pa['metadata'].get('generation')}, recorded {r.get('generation')})")
        if not _status_ok(pa, compiled) or r.get("compiled_sha256") != hashlib.sha256(compiled.encode()).hexdigest():
            out.append(f"PredictiveAutoscaler {t} does not report the attempt's compiled query")
        if dep is None or not dref or dep["metadata"]["uid"] != dref["uid"]:
            out.append(f"Deployment {t} is gone or replaced")
    live = c.kube.get("VMPodScrape", c.hb.scrape_name(k), c.ns)
    if live is None or live["metadata"]["uid"] != sc.get("uid") or X.json_digest(live.get("spec")) != sc.get("spec_sha256"):
        out.append("the history scrape object is not the one created for this attempt")
    if sc.get("generator_config_digest") != c.c28.demo.generator_config(c.params)["digest"]:
        out.append("the generator configuration differs from the one the scrape was created under")
    out += basis_problems(c)
    return out


def accept(c: Ctx, k: int, fetch: Fetch, api: str) -> Dict:
    """At G + 30 s or later, under the shared lock: raw reads, the acceptance proof, the live binding checks, the raw
    evidence and proof written idempotently (a rerun after an interruption completes the decision), then ONE
    receipt."""
    with X.mutating(c.record, c.kube, HELPER, {"attempt": k, "step": "accept"}):
        if c.now() / 1000 < c.guard_s + ACCEPT_AFTER_G_S:
            raise X.StagingError(f"acceptance reads start at G + {ACCEPT_AFTER_G_S} s")
        sc = (_events(c, k, "scrape-created") or [None])[-1]
        qu = (_events(c, k, "query-updated") or [None])[-1]
        if sc is None or qu is None:
            raise X.StagingError(f"attempt {k} has no query update or no scrape")
        if any(_events(c, k, e) for e in ("boundary-accepted", "boundary-failed", "attempt-void")):
            raise X.StagingError(f"attempt {k} is already decided")
        if k != max(attempts_used(c.record)):
            raise X.StagingError(f"attempt {k} is not the latest attempt")
        why = [x for x in fence(c) if "intents with unknown outcomes" in x or "obligation" in x]
        if why:
            raise X.StagingError("fence: " + "; ".join(why))
        epoch = c.epoch(k)
        targets = c.c28.demo.target_names(c.params)
        raw = {t: BD._q(fetch, api, "query",
                        query=f"{c.hb.raw_selector(c.ns, epoch, t)}[{c.guard_s - c.origin_s + 600}s]",
                        time=f"{c.guard_s}") for t in targets}
        proof = c.hb.acceptance(raw, targets, c.ns, epoch, c.origin_s, c.guard_s)
        live = _live_attempt_problems(c, k, qu, sc)
        if live:
            proof["accepted"] = False
            proof["problems"] += live
        out_dir = os.path.join(c.record.evidence_dir, f"boundary-{epoch}")
        os.makedirs(out_dir, exist_ok=True)
        raw_path, proof_path = os.path.join(out_dir, "raw-samples.json"), os.path.join(out_dir, "proof.json")
        raw_sha = _write_same(raw_path, json.dumps(raw, sort_keys=True, indent=1) + "\n")
        pas = {x["name"]: x for x in qu.get("pas") or []}
        binding = {"attempt": k, "epoch": epoch, "origin_s": c.origin_s, "guard_s": c.guard_s,
                   "scrape": {"uid": sc["uid"], "spec_sha256": sc["spec_sha256"], "created_s": sc["created_s"]},
                   "pas": {t: {"pa_uid": v["uid"], "pa_generation": v["generation"],
                               "compiled_sha256": v["compiled_sha256"],
                               "deployment_uid": (prepare.created_objects(c.record).get(("Deployment", c.ns, t))
                                                  or {}).get("uid")} for t, v in pas.items()},
                   "generator_config_digest": sc["generator_config_digest"], "raw_sha256": raw_sha}
        proof_sha = _write_same(proof_path, json.dumps({"proof": proof, "binding": binding}, indent=1,
                                                       sort_keys=True) + "\n")
        return X.receipt(c.record, HELPER, "boundary-accepted" if proof["accepted"] else "boundary-failed",
                         {"attempt": k, "binding": binding, "problems": proof["problems"][:20],
                          "raw_path": raw_path, "proof_path": proof_path, "proof_sha256": proof_sha})


def accepted_boundary(record: X.Record) -> Optional[Dict]:
    """What activation requires: the LATEST attempt (receipts and journal), decided as accepted, with its raw evidence
    and proof still hashing to the receipt. A newer attempt in any state (pending, incomplete, void, failed)
    invalidates an older acceptance; a failed boundary leaves the demonstration in Recommend whatever the
    qualification says."""
    used = attempts_used(record)
    if not used:
        return None
    k = max(used)
    es = [e for e in X.receipts(record, HELPER) if (e.get("data") or {}).get("attempt") == k]
    dec = [e for e in es if e["event"] in ("boundary-accepted", "boundary-failed", "attempt-void", "attempt-incomplete")]
    if not dec or dec[-1]["event"] != "boundary-accepted":
        return None
    d = dec[-1]["data"]
    raw, proof = d.get("raw_path"), d.get("proof_path")
    if not (raw and proof and os.path.isfile(raw) and os.path.isfile(proof)
            and C.sha256_file(raw) == d["binding"]["raw_sha256"] and C.sha256_file(proof) == d.get("proof_sha256")):
        return None
    return d


# --- a retry: retire the continuous generator ------------------------------------------------------------------------------

ABSENT_OP = "boundary-absent"


def _prove_absent(c: Ctx, kind: str, name: str, seconds: float) -> Dict:
    """Gone means ABSENT: no object of that name at all (a replacement under the name is not success)."""
    end = c.now() + seconds * 1000
    while c.kube.get(kind, name, c.ns) is not None:
        if c.now() >= end:
            raise X.StagingError(f"{kind}/{name} still exists {seconds:.0f} s after its delete")
        c.sleep(2)
    return {"metadata": {"name": name}}


def _reconcile_absent(kube: X.Kube, d: Dict, record) -> Dict:
    o = kube.get(d["kind"], d["name"], d["namespace"])
    return {"observed": "done", "uid": d["uid"]} if o is None else \
        {"observed": "unresolved", "why": f"{d['kind']}/{d['name']} still exists"}


X.RECONCILERS[ABSENT_OP] = _reconcile_absent


def plan_retire(c: Ctx) -> Dict:
    problems = [x for x in BD.fence_problems(c.bctx()) if "c28-demo-generator" not in x]
    used = attempts_used(c.record)
    last = [e for e in X.receipts(c.record, HELPER) if (e.get("data") or {}).get("attempt") == (max(used) if used else 0)
            and e["event"] in ("boundary-accepted", "boundary-failed", "attempt-void", "attempt-incomplete")]
    if not last or last[-1]["event"] == "boundary-accepted":
        problems.append("the generator is retired only for a retry (the latest attempt failed, voided or incomplete)")
    created = prepare.created_objects(c.record)
    name = c.c28.demo.GENERATOR_NAME
    refs, pods = [], []
    for kind in ("StatefulSet", "Service", "ConfigMap"):
        ref = created.get((kind, c.ns, name))
        live = c.kube.get(kind, name, c.ns)
        if ref and live and live["metadata"]["uid"] == ref["uid"]:
            refs.append({"kind": kind, "name": name, "uid": ref["uid"]})
        elif live is not None:
            problems.append(f"{kind}/{name} is not the generator this run created: never touched")
    sts_ref = created.get(("StatefulSet", c.ns, name))
    recorded = {x["uid"] for e in X.receipts(c.record, HELPER) if e["event"] == "retire-owned"
                for x in e["data"].get("pods") or []}                 # durable ownership from an earlier attempt
    for p_ in c.kube.list("Pod", c.ns, strict=True):
        owners = {o.get("uid") for o in p_["metadata"].get("ownerReferences") or []}
        if (sts_ref and sts_ref["uid"] in owners) or p_["metadata"]["uid"] in recorded:
            pods.append({"name": p_["metadata"]["name"], "uid": p_["metadata"]["uid"]})
        elif p_["metadata"]["name"] == c.c28.demo.generator_pod_name():
            problems.append(f"Pod/{p_['metadata']['name']} is not owned by this run's generator: never touched")
    actions = [{"delete": r["kind"], "name": r["name"], "uid": r["uid"]} for r in refs]
    actions += [{"delete": "Pod", "name": x["name"], "uid": x["uid"],
                 "owner": (sts_ref or {}).get("uid")} for x in pods]
    return X.new_plan(HELPER, c.record, actions, problems=problems)


def retire(c: Ctx, reviewed: Dict, reviewed_digest: str, logs: Callable[[str], List[str]]) -> Dict:
    """The run's own generator only (the plan's UIDs; pods bound to the recorded StatefulSet UID): logs archived first
    (a failed fetch refuses), the StatefulSet deleted and proven ABSENT, each of its pods deleted by UID, no pod of
    the generator's name left at all, then the Service and ConfigMap."""
    with X.mutating(c.record, c.kube, HELPER, {"step": "retire-generator"}):
        p = plan_retire(c)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
        if p["problems"]:
            raise X.StagingError("the plan has problems: " + "; ".join(p["problems"]))
        acts = {}
        pods = []
        for a in p["actions"]:
            (pods.append(a) if a["delete"] == "Pod" else acts.__setitem__(a["delete"], a))
        out_dir = os.path.join(c.record.evidence_dir, "generator-retired")
        os.makedirs(out_dir, exist_ok=True)                         # retryable: each attempt adds its own files
        log_shas = {}
        for x in pods:
            text = logs(x["name"])                                  # raises on a failed fetch: nothing deleted yet
            log_shas[x["name"]] = X.write_new(os.path.join(out_dir, f"{x['name']}-{x['uid'][:8]}-{int(c.now())}.log"),
                                              "\n".join(text) + "\n")
        if pods:                                                    # durable BEFORE any delete
            X.receipt(c.record, HELPER, "retire-owned", {"sts_uid": (acts.get("StatefulSet") or {}).get("uid"),
                                                          "pods": [{"name": x["name"], "uid": x["uid"]} for x in pods]})
        if "StatefulSet" in acts:
            a = acts["StatefulSet"]
            c.kube.delete("StatefulSet", a["name"], c.ns, a["uid"])
            c.kube.step(ABSENT_OP, "StatefulSet", c.ns, a["name"], a["uid"],
                        lambda mark: _prove_absent(c, "StatefulSet", a["name"], 180))
        for x in pods:
            live = c.kube.get("Pod", x["name"], c.ns)
            if live is not None and live["metadata"]["uid"] == x["uid"]:
                c.kube.delete("Pod", x["name"], c.ns, x["uid"])
            c.kube.step(ABSENT_OP, "Pod", c.ns, x["name"], x["uid"],
                        lambda mark, x=x: _prove_absent(c, "Pod", x["name"], 180))
        for kind in ("Service", "ConfigMap"):
            if kind in acts:
                c.kube.delete(kind, acts[kind]["name"], c.ns, acts[kind]["uid"])
        return X.receipt(c.record, HELPER, "generator-retired", {"plan_sha256": reviewed_digest, "logs": log_shas,
                                                                  "deleted": p["actions"]})


def _logs(c: Ctx) -> Callable[[str], List[str]]:
    def get(pod: str) -> List[str]:
        k = c.record.doc["kubeconfig"]
        r = subprocess.run(["kubectl", "--kubeconfig", k["path"], "--context", k["context"], "-n", c.ns, "logs", pod],
                           capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            raise X.StagingError(f"kubectl logs {pod} failed (exit {r.returncode})")   # never an empty archive
        return r.stdout.splitlines()
    return get


def main(argv: Optional[List[str]] = None, api=None, fetch: Fetch = BD.http_json) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--record", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("plan-query", "plan-scrape"):
        sub.add_parser(name).add_argument("--attempt", type=int, required=True)
    for name in ("apply-query", "apply-scrape", "retire-generator", "recover"):
        s = sub.add_parser(name)
        s.add_argument("--plan", required=True)
        s.add_argument("--plan-digest", required=True)
        if name == "apply-scrape":
            s.add_argument("--query-api", required=True)
    ph = sub.add_parser("prove-hold")
    ph.add_argument("--attempt", type=int, required=True)
    ph.add_argument("--query-api", required=True)
    ph.add_argument("--ledger", required=True, help="a verified ledger_read.py read of the operator ledger")
    ac = sub.add_parser("accept")
    ac.add_argument("--attempt", type=int, required=True)
    ac.add_argument("--query-api", required=True)
    sub.add_parser("plan-retire-generator")
    sub.add_parser("recover-plan")
    a = ap.parse_args(argv)
    record = X.load_record(a.record, () if a.cmd.startswith("recover") else ("prepare",))
    kube = record.kube(api)
    if a.cmd == "recover-plan":
        X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
        return 0
    if a.cmd == "recover":
        X.emit(X.recover(record, kube, X.load_plan(a.plan), a.plan_digest))
        return 0
    c = Ctx(record, kube)
    if a.cmd == "plan-query":
        X.emit(X.cli_plan(record, HELPER, plan_query(c, a.attempt), f"plan-query-{a.attempt}"))
    elif a.cmd == "apply-query":
        X.emit(apply_query(c, X.load_plan(a.plan), a.plan_digest))
    elif a.cmd == "prove-hold":
        X.emit(prove_hold(c, a.attempt, fetch, a.query_api, ledger_rows(record, os.path.abspath(a.ledger))))
    elif a.cmd == "plan-scrape":
        X.emit(X.cli_plan(record, HELPER, plan_scrape(c, a.attempt), f"plan-scrape-{a.attempt}"))
    elif a.cmd == "apply-scrape":
        X.emit(apply_scrape(c, X.load_plan(a.plan), a.plan_digest, _logs(c)))
    elif a.cmd == "accept":
        X.emit(accept(c, a.attempt, fetch, a.query_api))
    elif a.cmd == "plan-retire-generator":
        X.emit(X.cli_plan(record, HELPER, plan_retire(c), "plan-retire"))
    else:
        X.emit(retire(c, X.load_plan(a.plan), a.plan_digest, _logs(c)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
