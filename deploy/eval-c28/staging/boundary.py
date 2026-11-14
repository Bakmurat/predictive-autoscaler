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
import hashlib
import importlib
import json
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


def _events(c: Ctx, k: int, event: str) -> List[Dict]:
    return [e["data"] for e in X.receipts(c.record, HELPER) if e["event"] == event and e["data"].get("attempt") == k]


def fence(c: Ctx) -> List[str]:
    out = BD.fence_problems(c.bctx())                      # bounded workloads, their pods, open intents, obligations
    out = [x for x in out if "c28-demo-generator" not in x]   # the continuous generator is this tool's own business
    for h in X.PART2_HELPERS:
        if h not in ("bounded", "prepare") and X.open_intents(X.receipts(c.record, h)):
            out.append(f"{h} has intents with unknown outcomes: recover first")
    return out


def startup_measured(c: Ctx) -> Tuple[Optional[Dict], str]:
    """The last startup-complete must be measured and its evidence file must still hash to the receipt."""
    done = [e["data"] for e in X.receipts(c.record, "bounded") if e["event"] == "startup-complete"]
    if not done:
        return None, "no startup step was run"
    r = done[-1]
    if not r.get("measured"):
        return None, "the last startup step was not a measurement"
    path = os.path.join(r["dir"], "evidence.json")
    if not os.path.isfile(path) or C.sha256_file(path) != r.get("evidence_sha256"):
        return None, "the startup evidence no longer hashes to its receipt"
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
        problems.append(f"the startup step must be measured first: {why}")
    if c.origin_s % 86400:
        problems.append("schedule_origin_utc must be a UTC midnight")
    if c.now() / 1000 >= c.origin_s:
        problems.append("the origin is not in the future: the query update happens before O")
    if any(_events(c, k, e) for e in ("query-planned-applied", "query-updated", "attempt-incomplete", "scrape-created",
                                      "attempt-void", "boundary-accepted", "boundary-failed")):
        problems.append(f"attempt {k} was used before: a retry takes a new attempt number")
    later = [e["data"].get("attempt") for e in X.receipts(c.record, HELPER) if e["data"].get("attempt")]
    if later and k <= max(later):
        problems.append(f"attempt {k} is not above the last attempt {max(later)}")
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
                def mutate(body, a=a):
                    if body["metadata"].get("generation") != a["from_generation"]:
                        raise X.StagingError(f"{a['name']} changed since the plan")
                    body["spec"].setdefault("metrics", {}).setdefault("requests", {})["source"] = {
                        "preset": "prometheus", "query": a["query"]}
                c.kube.update("PredictiveAutoscaler", a["name"], c.ns, a["uid"], mutate,
                              expect=[["/spec/metrics/requests/source/query", a["query"]]])
                compiled = c.hb.compiled_query(c.ns, p["epoch"], p["guard_s"], a["name"])
                end = c.now() + wait_s * 1000
                while True:
                    pa = c.kube.get("PredictiveAutoscaler", a["name"], c.ns)
                    if pa is None or pa["metadata"]["uid"] != a["uid"]:
                        raise X.StagingError(f"{a['name']} was replaced during the query update")
                    if _status_ok(pa, compiled):
                        break
                    if c.now() >= end:
                        raise X.StagingError(f"{a['name']} did not report the compiled query and lead 600 for its "
                                             f"generation within {wait_s:.0f} s")
                    c.sleep(5)
                done.append({"name": a["name"], "uid": a["uid"], "generation": pa["metadata"]["generation"],
                             "compiled_sha256": hashlib.sha256(compiled.encode()).hexdigest()})
        except Exception as e:
            X.receipt(c.record, HELPER, "attempt-incomplete", {"attempt": k, "done": done, "why": str(e)[:300]})
            raise
        return X.receipt(c.record, HELPER, "query-updated", {"attempt": k, "epoch": p["epoch"],
                                                              "updated_s": c.now() / 1000,
                                                              "origin_s": p["origin_s"], "guard_s": p["guard_s"],
                                                              "pas": done, "plan_sha256": reviewed_digest})


def prove_hold(c: Ctx, k: int, fetch: Fetch, api: str, ledger_rows: List[Dict]) -> Dict:
    """Read-only, between the query update and G: per PA the compiled query and its hash for the current generation,
    TelemetryAvailable False, the ledger's decisions since the update all keep_current with nothing applied, and the
    operator's applied-write counter unchanged since the update."""
    q = (_events(c, k, "query-updated") or [None])[-1]
    probs = []
    if q is None:
        return {"proven": False, "problems": ["the attempt's query update is not complete"]}
    since_s = q["updated_s"]
    per = {}
    for row in q["pas"]:
        pa = c.kube.get("PredictiveAutoscaler", row["name"], c.ns) or {}
        compiled = c.hb.compiled_query(c.ns, q["epoch"], q["guard_s"], row["name"])
        conds = {x["type"]: x for x in (pa.get("status") or {}).get("conditions") or []}
        tele = conds.get("TelemetryAvailable", {})
        mine = [r for r in ledger_rows if r.get("autoscaler_name") == row["name"]
                and _ms(str(r.get("at"))[:19] + "Z") / 1000 >= since_s]
        bad = [r for r in mine if r.get("desired_source") != "keep_current"
               or r.get("applied_replicas") != r.get("current_replicas")]
        res = BD._q(fetch, api, "query", query=f'increase(predictive_autoscaler_scale_writes_total{{namespace="{c.ns}",'
                    f'autoscaler="{row["name"]}",result="applied"}}[1h])', time=f"{c.now() / 1000:.3f}")
        applied = sum(float(r["value"][1]) for r in res)
        ok = _status_ok(pa, compiled) and tele.get("status") == "False" and mine and not bad and applied == 0
        per[row["name"]] = {"query_sha256": hashlib.sha256(compiled.encode()).hexdigest(),
                            "telemetry": [tele.get("status"), tele.get("reason")], "decisions": len(mine),
                            "not_keep_current": len(bad), "applied_writes_1h": applied, "ok": bool(ok)}
        if not ok:
            probs.append(f"{row['name']}: hold not proven {per[row['name']]}")
    out = {"proven": not probs, "pas": per, "problems": probs, "attempt": k}
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
        made = c.kube.create(obj)
        return X.receipt(c.record, HELPER, "scrape-created", {
            "attempt": k, "epoch": c.epoch(k), "uid": made["metadata"]["uid"], "spec_sha256": X.json_digest(obj["spec"]),
            "created_s": now_s, "generator_config_digest": digest, "plan_sha256": reviewed_digest})


def accept(c: Ctx, k: int, fetch: Fetch, api: str) -> Dict:
    """Read-only reads at G + 30 s or later; one receipt bound to everything the attempt consists of."""
    if c.now() / 1000 < c.guard_s + ACCEPT_AFTER_G_S:
        raise X.StagingError(f"acceptance reads start at G + {ACCEPT_AFTER_G_S} s")
    sc = (_events(c, k, "scrape-created") or [None])[-1]
    qu = (_events(c, k, "query-updated") or [None])[-1]
    if sc is None or qu is None:
        raise X.StagingError(f"attempt {k} has no query update or no scrape")
    if _events(c, k, "boundary-accepted") or _events(c, k, "boundary-failed"):
        raise X.StagingError(f"attempt {k} is already decided")
    epoch = c.epoch(k)
    targets = c.c28.demo.target_names(c.params)
    raw = {t: BD._q(fetch, api, "query", query=f"{c.hb.raw_selector(c.ns, epoch, t)}[{c.guard_s - c.origin_s + 600}s]",
                    time=f"{c.guard_s}") for t in targets}
    proof = c.hb.acceptance(raw, targets, c.ns, epoch, c.origin_s, c.guard_s)
    live = c.kube.get("VMPodScrape", c.hb.scrape_name(k), c.ns)
    if live is None or live["metadata"]["uid"] != sc["uid"] or X.json_digest(live.get("spec")) != sc["spec_sha256"]:
        proof["accepted"] = False
        proof["problems"].append("the history scrape object is not the one created for this attempt")
    pas, _ = _pas(c)
    out_dir = os.path.join(c.record.evidence_dir, f"boundary-{epoch}")
    os.makedirs(out_dir, exist_ok=True)
    raw_sha = X.write_new(os.path.join(out_dir, "raw-samples.json"), json.dumps(raw, sort_keys=True, indent=1) + "\n")
    binding = {"attempt": k, "epoch": epoch, "origin_s": c.origin_s, "guard_s": c.guard_s,
               "scrape": {"uid": sc["uid"], "spec_sha256": sc["spec_sha256"]},
               "pas": {t: {"pa_uid": v["pa_uid"], "pa_generation": v["pa_generation"],
                           "deployment_uid": v["deployment_uid"],
                           "compiled_sha256": hashlib.sha256(c.hb.compiled_query(c.ns, epoch, c.guard_s, t).encode())
                           .hexdigest()} for t, v in pas.items()},
               "generator_config_digest": sc["generator_config_digest"], "raw_sha256": raw_sha}
    X.write_new(os.path.join(out_dir, "proof.json"), json.dumps({"proof": proof, "binding": binding}, indent=1,
                                                                 sort_keys=True) + "\n")
    return X.receipt(c.record, HELPER, "boundary-accepted" if proof["accepted"] else "boundary-failed",
                     {"attempt": k, "binding": binding, "problems": proof["problems"][:20]})


def accepted_boundary(record: X.Record) -> Optional[Dict]:
    """What activation requires: the last decided attempt, accepted (a failed one leaves the demonstration in
    Recommend whatever the qualification says)."""
    es = [e for e in X.receipts(record, HELPER) if e["event"] in ("boundary-accepted", "boundary-failed")]
    return es[-1]["data"] if es and es[-1]["event"] == "boundary-accepted" else None


# --- a retry: retire the continuous generator ------------------------------------------------------------------------------

def plan_retire(c: Ctx) -> Dict:
    problems = BD.fence_problems(c.bctx())
    problems = [x for x in problems if "c28-demo-generator" not in x]
    decided = [e for e in X.receipts(c.record, HELPER)
               if e["event"] in ("boundary-accepted", "boundary-failed", "attempt-void", "attempt-incomplete")]
    if not decided or decided[-1]["event"] == "boundary-accepted":
        problems.append("the generator is retired only for a retry (the last attempt failed, voided or incomplete)")
    created = prepare.created_objects(c.record)
    name = c.c28.demo.GENERATOR_NAME
    refs = []
    for kind in ("StatefulSet", "Service", "ConfigMap"):
        ref = created.get((kind, c.ns, name))
        live = c.kube.get(kind, name, c.ns)
        if ref and live and live["metadata"]["uid"] == ref["uid"]:
            refs.append({"kind": kind, "name": name, "uid": ref["uid"]})
        elif live is not None:
            problems.append(f"{kind}/{name} is not the generator this run created: never touched")
    return X.new_plan(HELPER, c.record, [{"delete": r["kind"], "name": r["name"], "uid": r["uid"]} for r in refs],
                      problems=problems)


def retire(c: Ctx, reviewed: Dict, reviewed_digest: str, logs: Callable[[str], List[str]]) -> Dict:
    with X.mutating(c.record, c.kube, HELPER, {"step": "retire-generator"}):
        p = plan_retire(c)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, p)
        if p["problems"]:
            raise X.StagingError("the plan has problems: " + "; ".join(p["problems"]))
        pod_name = c.c28.demo.generator_pod_name()
        pod = c.kube.get("Pod", pod_name, c.ns)
        out_dir = os.path.join(c.record.evidence_dir, f"generator-retired-{reviewed_digest[:12]}")
        os.makedirs(out_dir)
        log_sha = X.write_new(os.path.join(out_dir, "generator.log"), "\n".join(logs(pod_name) if pod else []) + "\n")
        acts = {a["delete"]: a for a in p["actions"]}
        if "StatefulSet" in acts:
            c.kube.delete("StatefulSet", acts["StatefulSet"]["name"], c.ns, acts["StatefulSet"]["uid"])
        if pod is not None:
            live = c.kube.get("Pod", pod_name, c.ns)
            if live is not None and live["metadata"]["uid"] == pod["metadata"]["uid"]:
                c.kube.delete("Pod", pod_name, c.ns, pod["metadata"]["uid"])
            c.kube.step(BD.GONE_OP, "Pod", c.ns, pod_name, pod["metadata"]["uid"],
                        lambda mark: BD._prove_gone(c.bctx(), "Pod", pod_name, pod["metadata"]["uid"], 180))
        for kind in ("Service", "ConfigMap"):
            if kind in acts:
                c.kube.delete(kind, acts[kind]["name"], c.ns, acts[kind]["uid"])
        return X.receipt(c.record, HELPER, "generator-retired", {"plan_sha256": reviewed_digest, "log_sha256": log_sha,
                                                                  "deleted": p["actions"]})


def _logs(c: Ctx) -> Callable[[str], List[str]]:
    def get(pod: str) -> List[str]:
        k = c.record.doc["kubeconfig"]
        r = subprocess.run(["kubectl", "--kubeconfig", k["path"], "--context", k["context"], "-n", c.ns, "logs", pod],
                           capture_output=True, text=True, timeout=120)
        return r.stdout.splitlines() if r.returncode == 0 else []
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
        with open(a.ledger) as fh:
            rows = [json.loads(ln) for ln in fh if ln.strip()]
        X.emit(prove_hold(c, a.attempt, fetch, a.query_api, rows))
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
