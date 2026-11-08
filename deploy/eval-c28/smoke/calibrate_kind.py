#!/usr/bin/env python3
"""The kind smoke of the S5a calibration path (staging/calibrate.py), driven by calibrate-kind.sh on its OWN throwaway
kind cluster and kubeconfig (never a lab cluster; nothing here is C2.8 evidence). It renders the calibration fixture,
ingress and k6 Jobs with calibrate.py's own renderers from the reviewed images (c28-images/v1, pulled by digest) and
checks what the offline tests can only simulate:

  1. summary delivery: the k6 container runs NON-ROOT (runAsUser 12345, runAsNonRoot) and its c28-calibration-k6/v1
     summary arrives intact in status.containerStatuses[].state.terminated.message (size recorded against the 4096-byte
     kubelet limit), with the actual per-scenario start (start_ms);
  2. timing: calibrate.measurement_window() accepts the recorded measure-scenario start (between container start +
     settle and + INIT_SLACK_S); the observed initialization delay is reported;
  3. zero drops, reconciled: at a rate the VUs sustain, every scenario's iterations == scheduled (or k6's one boundary
     iteration more: calibrate.offered_ok), no
     dropped_iterations, http_reqs == iterations, and the ingress's own counters (read from its /metrics through a
     port-forward, before and after) received == answered == dispatched;
  4. non-zero drops: with the VUs starved, dropped_iterations is reported and scheduled == dropped + dispatched exactly;
     the zero-drop criterion classifies the trial LIMIT/censored;
  5. a truncated or malformed summary is refused (evaluate_trial reports the k6 summary missing or malformed);
  6. stop and cleanup: a running trial Job deleted with a UID precondition (calibrate's KubectlApi body, Background
     propagation) loses its pods within the bound; a Job whose activeDeadlineSeconds expires is Failed
     (DeadlineExceeded) and its pod gone, so a dead executor cannot leave traffic; the calibration objects are deleted
     and every labelled pod is proven gone;
  7. image identity: every fixture/ingress/k6 container reports the reviewed digest in its imageID;
  8. the real admission gate: calibrate.pod_problems / object_problems find NO difference between the rendered and the
     ADMITTED fixture, ingress and k6 pods and their Deployments, Services, ConfigMap and Job (the API server's
     defaults and the DefaultTolerationSeconds not-ready/unreachable tolerations included, which must actually be
     present), while copies with an added command or a changed default toleration are refused.

  calibrate_kind.py --kubeconfig K --context C --images images.json --workload W --support S --out report.json
"""

import argparse
import copy
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "staging"))
import calibrate as K          # noqa: E402
import common_records as C     # noqa: E402

NS = "c28-cal-smoke"
RUN = "c28-cal-smoke"
SETTLE, MEASURE = 20, 60       # smoke durations (the contract's 300/900 are not exercised here: this is not evidence)


class Smoke:
    def __init__(self, a):
        self.a, self.report, self.failures = a, {"checks": {}}, []
        self.base = ["kubectl", "--kubeconfig", a.kubeconfig, "--context", a.context, "--request-timeout=30s"]

    def k(self, *args, stdin=None, check=True):
        p = subprocess.run(self.base + list(args), input=stdin, capture_output=True, text=True)
        if check and p.returncode != 0:
            raise RuntimeError(f"kubectl {' '.join(args[:3])}: exit {p.returncode}: {p.stderr.strip()[:300]}")
        return p.stdout

    def kj(self, *args):
        out = self.k(*args, "-o", "json")
        return json.loads(out) if out.strip() else {}

    def ok(self, name, cond, detail):
        self.report["checks"].setdefault(name, []).append({"ok": bool(cond), "detail": detail})
        print(f"{'PASS' if cond else 'FAIL'} {name}: {json.dumps(detail, sort_keys=True)[:400]}", flush=True)
        if not cond:
            self.failures.append(name)

    # --- setup ---------------------------------------------------------------------------------------------------
    def contract(self, images):
        con = {"namespace": NS, "run_id": RUN,
               "nodes": {"workload": {"name": self.a.workload}, "support": {"name": self.a.support}},
               "images": {"fixture": images["fixture"], "generator": images["generator"]},
               "candidate": {"cpu_request": "50m", "cpu_limit": "50m", "memory_request": "32Mi",
                             "memory_limit": "32Mi", "response_bytes": 1024, "max_inflight": 1, "max_queue": 32,
                             "queue_timeout_seconds": 1.5, "gomaxprocs": 2, "startup_delay_seconds": 5},
               "work_selection": {"values": [600, 900], "settle_s": SETTLE, "measure_s": MEASURE},
               "ingress": {"cpu_request": "100m", "cpu_limit": "500m", "memory_request": "64Mi",
                           "memory_limit": "64Mi"},
               "generator": {"k6_version": "0.57.0", "cpu_request": "200m", "cpu_limit": "1",
                             "memory_request": "128Mi", "memory_limit": "256Mi"},
               "observer": {"scrape": "annotations", "step_s": 15},
               "search": {"settle_s": 300, "measure_s": 900, "job_slack_s": 60},
               "criteria": {"max_dropped_arrivals": 0}}
        params = {"reservation": {"taint_key": "c28.devkuban.com/reserved", "taint_value": RUN,
                                  "role_label": "c28.devkuban.com/role"},
                  "nodes": {"workload": self.a.workload, "support": self.a.support}}
        return con, params

    def apply(self, obj):
        return json.loads(self.k("create", "-o", "json", "-f", "-", stdin=json.dumps(obj)))

    def wait(self, what, fn, timeout):
        end = time.time() + timeout
        while True:
            v = fn()
            if v:
                return v
            if time.time() > end:
                raise RuntimeError(f"timeout: {what}")
            time.sleep(2)

    def pods(self, selector=""):
        args = ["get", "pods", "-n", NS] + (["-l", selector] if selector else [])
        return self.kj(*args).get("items", [])

    # --- the ingress counters, read from the pod itself ------------------------------------------------------------
    def ingress_counts(self, target):
        port = free_port()
        pf = subprocess.Popen(self.base + ["-n", NS, "port-forward", f"deploy/{self.nm['ingress']}", f"{port}:8080"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            text = self.wait("ingress /metrics", lambda: fetch_text(f"http://127.0.0.1:{port}/metrics"), 30)
        finally:
            pf.terminate()
            pf.wait()
        req = sum(float(v) for v in re.findall(
            rf'^c28_ingress_requests_total\{{target="{target}"\}} (\S+)$', text, re.M))
        resp = sum(float(v) for v in re.findall(
            rf'^c28_ingress_responses_total\{{[^}}]*target="{target}"[^}}]*\}} (\S+)$', text, re.M))
        return req, resp

    # --- one trial -------------------------------------------------------------------------------------------------
    def trial(self, index, target, rate, vus=None, deadline=None, delete_after=None):
        meta = {"id": f"t{index:03d}", "kind": "selection", "target": target, "rate_per_min": rate,
                "seed": 1000 + index}
        job = K.generator_job(self.con, self.cal, self.params, meta)
        args = job["spec"]["template"]["spec"]["containers"][0]["args"]
        if vus:
            for i, a in enumerate(args):
                if a.startswith("C28CAL_PREALLOCATED_VUS="):
                    args[i] = f"C28CAL_PREALLOCATED_VUS={vus[0]}"
                if a.startswith("C28CAL_MAX_VUS="):
                    args[i] = f"C28CAL_MAX_VUS={vus[1]}"
        if deadline:
            job["spec"]["activeDeadlineSeconds"] = deadline
        before = self.ingress_counts(target)
        created = self.apply(job)
        uid, name = created["metadata"]["uid"], job["metadata"]["name"]
        if delete_after is not None:
            time.sleep(delete_after)
            body = {"kind": "DeleteOptions", "apiVersion": "v1", "preconditions": {"uid": uid},
                    "propagationPolicy": "Background"}
            t0 = time.time()
            self.k("delete", "--raw", f"/apis/batch/v1/namespaces/{NS}/jobs/{name}", "-f", "-",
                   stdin=json.dumps(body))
            self.wait("the deleted Job's pods gone", lambda: not [
                p for p in self.pods() if any(r.get("uid") == uid for r in p["metadata"].get("ownerReferences") or [])],
                120)
            return {"job": name, "uid": uid, "pods_gone_after_s": round(time.time() - t0, 1)}
        job_live = self.kj("get", "job", "-n", NS, name)
        final = self.wait("the Job finishes", lambda: (lambda j: j if any(
            c.get("type") in ("Complete", "Failed") and c.get("status") == "True"
            for c in (j.get("status") or {}).get("conditions") or []) else None)(
            self.kj("get", "job", "-n", NS, name)), SETTLE + MEASURE + 240)
        pod = next((p for p in self.pods(f"job-name={name}")), None)
        time.sleep(K.REQUEST_TIMEOUT_S + 2)
        after = self.ingress_counts(target)
        return {"job": name, "uid": uid, "final": final, "pod": pod, "before": before, "after": after, "meta": meta,
                "job_rendered": job, "job_live": job_live}

    # --- the run ---------------------------------------------------------------------------------------------------
    def run(self):
        images = read_images(self.a.images)
        self.con, self.params = self.contract(images)
        self.cal = "c28cal-0000000001"
        self.nm = K.names(self.cal)
        self.k("create", "namespace", NS)
        self.k("label", "namespace", NS, "istio-injection=enabled")       # as demo: the opt-outs must hold
        objs = K.render_objects(self.con, self.cal, self.params, open(K.LOAD_SCRIPT).read())
        for o in objs:
            o["metadata"]["namespace"] = NS
            self.apply(o)
        variant = self.nm["variants"][0]
        self.k("scale", "-n", NS, f"deploy/{variant}", "--replicas=1")
        self.wait("fixture and ingress Ready", lambda: all(
            any(c.get("type") == "Ready" and c.get("status") == "True" for c in p["status"].get("conditions", []))
            for p in self.pods(f"{K.CAL_LABEL}={self.cal}")) and len(self.pods(f"{K.CAL_LABEL}={self.cal}")) == 2, 180)
        cal_pods = self.pods(f"{K.CAL_LABEL}={self.cal}")
        fx_digest = images["fixture"].split("@")[1]
        for p in cal_pods:
            cs = p["status"]["containerStatuses"][0]
            self.ok("7-image-identity", cs["imageID"].endswith(fx_digest) and p["spec"]["nodeName"] ==
                    self.a.workload and not [c for c in p["spec"]["containers"] if c["name"].startswith("istio")],
                    {"pod": p["metadata"]["name"], "imageID": cs["imageID"], "node": p["spec"]["nodeName"]})

        self.check_admission(objs, variant)
        # trial A: a sustainable rate, zero drops
        a = self.trial(1, variant, 300)
        self.check_summary("A", a, images, expect_drops=False)
        self.check_job_admission(a)
        # trial B: starved VUs, drops
        b = self.trial(2, variant, 3000, vus=(1, 1))
        self.check_summary("B", b, images, expect_drops=True)
        # truncated / malformed summaries are refused
        self.check_truncation(a)
        # stop: delete a running Job; and a Job whose deadline expires
        c = self.trial(3, variant, 300, delete_after=30)
        self.ok("6-stop-deletes-pods", c["pods_gone_after_s"] <= 120, c)
        d = self.trial(4, variant, 300, deadline=40)
        conds = {x["type"]: x for x in (d["final"].get("status") or {}).get("conditions") or []}
        gone = self.wait("the deadline-expired Job's pods gone", lambda: not self.pods(f"job-name={d['job']}"), 120)
        self.ok("6-active-deadline", conds.get("Failed", {}).get("reason") == "DeadlineExceeded" and gone,
                {"conditions": list(conds), "reason": conds.get("Failed", {}).get("reason")})
        # cleanup: delete everything this smoke created (UID-bound), prove every labelled pod gone
        t0 = time.time()
        for kind, plural, api in (("Job", "jobs", "/apis/batch/v1"),):
            for j in self.kj("get", plural, "-n", NS).get("items", []):
                self.k("delete", "--raw", f"{api}/namespaces/{NS}/{plural}/{j['metadata']['name']}", "-f", "-",
                       stdin=json.dumps({"kind": "DeleteOptions", "apiVersion": "v1", "propagationPolicy": "Background",
                                         "preconditions": {"uid": j["metadata"]["uid"]}}))
        self.wait("generator pods gone", lambda: not self.pods(f"{K.TRIAL_LABEL}"), 120)
        for o in objs:
            self.k("delete", o["kind"].lower(), "-n", NS, o["metadata"]["name"], "--wait=false")
        self.wait("calibration pods gone", lambda: not self.pods(f"{K.CAL_LABEL}={self.cal}"), 180)
        self.ok("6-cleanup", True, {"seconds": round(time.time() - t0, 1)})

    def check_admission(self, objs, variant):
        """The rendered objects and pod templates against what the API server actually admitted."""
        rendered = {(o["kind"], o["metadata"]["name"]): o for o in objs}
        for (kind, name), want in sorted(rendered.items()):
            live = self.kj("get", kind.lower(), "-n", NS, name)
            ignore = ("/spec/replicas",) if kind == "Deployment" and name in self.nm["variants"] else ()
            diff = K.object_problems(kind, want, live, ignore)
            self.ok("8-admitted-object-equals-rendering", diff == [], {"object": f"{kind}/{name}", "diff": diff[:5]})
        for role, dep in ((variant, variant), ("ingress", self.nm["ingress"])):
            tmpl = rendered[("Deployment", dep)]["spec"]["template"]
            for pod in self.pods(f"{K.CAL_LABEL}={self.cal}"):
                if not pod["metadata"]["name"].startswith(dep + "-") or \
                        (dep == variant and pod["metadata"]["name"].startswith(self.nm["ingress"])):
                    continue
                self.pod_gate(pod["metadata"]["name"], tmpl, pod)

    def pod_gate(self, name, tmpl, pod):
        probs = K.pod_problems(name, tmpl, pod)
        tols = pod["spec"].get("tolerations") or []
        defaults_present = all(d in tols for d in K.ADMISSION_TOLERATIONS)
        self.ok("8-admitted-pod-equals-template", probs == [] and defaults_present,
                {"pod": name, "problems": probs[:3], "admission_default_tolerations_present": defaults_present,
                 "tolerations": tols})
        cmd = copy.deepcopy(pod)
        cmd["spec"]["containers"][0]["command"] = ["/bin/sh", "-c", "exec true"]
        tol = copy.deepcopy(pod)
        for t in tol["spec"].get("tolerations") or []:
            if t.get("key") == "node.kubernetes.io/unreachable":
                t["tolerationSeconds"] = 600
        self.ok("8-gate-refuses-tampered-copies", bool(K.pod_problems(name, tmpl, cmd)) and
                bool(K.pod_problems(name, tmpl, tol)), {"pod": name})

    def check_job_admission(self, t):
        diff = K.object_problems("Job", t["job_rendered"], t["job_live"])
        self.ok("8-admitted-job-equals-rendering", diff == [], {"job": t["job"], "diff": diff[:5]})
        self.pod_gate(t["pod"]["metadata"]["name"], t["job_rendered"]["spec"]["template"], t["pod"])

    def check_summary(self, label, t, images, expect_drops):
        pod = t["pod"]
        term = K._terminated(pod, "k6")
        sc = pod["spec"]["securityContext"]
        msg = (term or {}).get("message") or ""
        try:
            summary = json.loads(msg)
        except ValueError:
            summary = {}
        self.ok(f"1-summary-delivered-{label}",
                term is not None and term.get("exitCode") == 0 and summary.get("schema") == K.K6_SCHEMA and
                sc.get("runAsNonRoot") is True and sc.get("runAsUser") == 12345,
                {"exitCode": (term or {}).get("exitCode"), "bytes": len(msg), "limit": 4096,
                 "runAsUser": sc.get("runAsUser"), "schema": summary.get("schema")})
        self.ok(f"7-image-identity-k6-{label}", (term or {}).get("imageID", "").endswith(
            images["generator"].split("@")[1]), {"imageID": (term or {}).get("imageID")})
        if not summary:
            return
        started = K._utc(term["startedAt"])
        window, why = K.measurement_window(self.con, "selection", summary, started)
        init = (window["start"] - started - SETTLE) if window else None
        self.ok(f"2-timing-{label}", window is not None, {"why": why, "window": window,
                                                         "init_delay_s": init, "container_start": started})
        rate = t["meta"]["rate_per_min"]
        detail, exact = {}, True
        for s, dur in (("settle", SETTLE), ("measure", MEASURE)):
            sched = rate * dur / 60
            disp = K._metric(summary, s, "iterations", "count")
            drop = K._metric(summary, s, "dropped_iterations", "count")
            reqs = K._metric(summary, s, "http_reqs", "count")
            detail[s] = {"scheduled": sched, "dispatched": disp, "dropped": drop, "http_reqs": reqs}
            exact &= disp is not None and K.offered_ok(sched, (drop or 0) + disp) and reqs == disp and \
                (drop is not None or disp <= sched + K.K6_BOUNDARY_EXTRA)
        dispatched = sum(detail[s]["dispatched"] or 0 for s in detail)
        received = t["after"][0] - t["before"][0]
        answered = t["after"][1] - t["before"][1]
        detail["ingress"] = {"received": received, "answered": answered, "dispatched": dispatched}
        drops = sum(detail[s]["dropped"] or 0 for s in ("settle", "measure"))
        if expect_drops:
            self.ok(f"4-drops-reported-{label}", exact and drops > 0 and received == dispatched == answered, detail)
            limited = drops > self.con["criteria"]["max_dropped_arrivals"]
            self.ok(f"4-zero-drop-criterion-{label}", limited, {"dropped": drops, "classification":
                                                                "LIMIT/censored" if limited else "PASS"})
        else:
            self.ok(f"3-zero-drops-exact-{label}", exact and drops == 0 and received == dispatched == answered,
                    detail)

    def check_truncation(self, a):
        pod = copy.deepcopy(a["pod"])
        cs = next(c for c in pod["status"]["containerStatuses"] if c["name"] == "k6")
        msg = cs["state"]["terminated"]["message"]
        con = dict(self.con, criteria=dict(self.con["criteria"], **{
            "p95_ms": 200, "p99_ms": 500, "max_failure_fraction": 0.001, "reconcile_tolerance": {"absolute": 0,
                                                                                                "relative": 0},
            "queue_growth": {"bin_s": 60, "edge_bins": 2, "max_increase": 2, "max_slope_per_min": 0.2,
                             "max_end_backlog_per_replica": 4},
            "max_generator_throttled_fraction": 0.05, "max_ingress_throttled_fraction": 0.05}),
                   observer=dict(self.con["observer"], max_staleness_s=60, pod_label="pod"),
                   stops={"node_label": "node", "steal_percent": 5, "steal_bins": 5},
                   work_selection=dict(self.con["work_selection"], idle_s=60, cpu_ms_per_request={
                       "min": 2.5, "target": 2.75, "max": 3.0}))
        for label, bad in (("truncated", msg[: len(msg) // 2]), ("malformed", msg.replace('"', "'", 3)),
                           ("empty", "")):
            cs["state"]["terminated"]["message"] = bad
            raw = {"trial": dict(a["meta"], variant="w0", work_iterations=600, n=1, rep=0, settle_s=SETTLE,
                                 measure_s=MEASURE), "generator_pod": pod, "pods_begin": {}, "pods_end": {},
                   "observations": {}}
            r = K.evaluate_trial(con, raw)
            self.ok(f"5-refuse-{label}-summary", r["verdict"] == "INCOMPLETE" and any(
                "summary (termination message)" in w for w in r["incomplete"]), {"incomplete": r["incomplete"][:3]})


def read_images(path):
    doc = json.load(open(path))
    if doc.get("schema") != "c28-images/v1" or doc.get("verified") is not True:
        raise SystemExit("images: not a verified c28-images/v1 record")
    out = {}
    for role in ("fixture", "generator"):
        ref = doc["images"][role]["ref"]
        if not re.fullmatch(r"[a-z0-9][a-z0-9./:-]*@sha256:[0-9a-f]{64}", ref):
            raise SystemExit(f"images: {role} is not pinned by digest")
        out[role] = ref
    return out


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def fetch_text(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.read().decode()
    except OSError:
        return ""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for a in ("kubeconfig", "context", "images", "workload", "support", "out"):
        ap.add_argument(f"--{a}", required=True)
    a = ap.parse_args()
    s = Smoke(a)
    try:
        s.run()
    except Exception as e:          # noqa: BLE001 - recorded as a failed smoke
        s.ok("run", False, {"error": f"{type(e).__name__}: {e}"[:500]})
    s.report["result"] = "PASS" if not s.failures else "FAIL"
    s.report["failures"] = sorted(set(s.failures))
    text = json.dumps(s.report, indent=1, sort_keys=True, default=str)
    C.assert_clean(text)
    open(a.out, "w").write(text + "\n")
    print(f"calibrate smoke: {s.report['result']}")
    return 0 if not s.failures else 1


if __name__ == "__main__":
    sys.exit(main())
