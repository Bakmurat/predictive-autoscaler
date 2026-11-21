#!/usr/bin/env python3
"""The image-roll smoke's driver (smoke/roll-kind.sh runs it on its OWN throwaway kind cluster; never a lab cluster).

It installs the demonstration the way a real installation is made (reserve.py for an owner run, prepare.py phase by
phase, the chart release with prepare's staging values) and then drives staging/imageroll.py against a real API
server, a real Deployment controller and a real scrape path. Its results are functional smoke evidence, not
production performance or admission evidence.

  roll_kind.py --work DIR --kubeconfig F --context C --images images.json --new-fixture REF --query-api URL STAGE...

Stages (each leaves its records, plans, receipts and observations under DIR):
  install    params and records from the live cluster, reserve, prepare phases, CRD, release; then prepare.py verify,

  bedcheck   the permission proof and the UNMODIFIED telemetry gate on this bed, and a baseline of the cluster
  forward    = roll, then complete:
  roll       open; roll, with the executor killed (SIGKILL, its whole process group) right after --kill-after
             member-rolled receipts; the stale lock, the recovery and the fence; the reviewed resume; one applied
             patch per member; what the cluster itself shows afterwards
  complete   cold completion (the zero-counter evidence and the telemetry gate, nothing substituted) before any
             request has been sent, re-planned until a fresh window is clean or the budget ends
  reverse    (a second cluster) open; roll, held by a smoke-only barrier before the fourth member's patch and killed
             there; the mixed state asserted; a successor transition with the ORIGINAL params (kind reverse); the
             fence continuous; the three members rolled back; completion without an observer, with its limitation
  churn      after the admission run: a target pod is deleted and replaced; from the ingress's own counters, without and
             then with requests: the removed endpoint keeps its count and gets nothing, the replacement is a new
             identity from zero; a completed transition cannot be superseded
  admission  ONE unmodified bounded admission run; asserts the origin's first minute (generator, raw ingress samples,
             reducer) and the cleanup
"""

import argparse
import copy
import glob
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
STAGING = os.path.join(ROOT, "deploy", "eval-c28", "staging")
HARNESS = os.path.join(ROOT, "deploy", "eval-c28", "harness")
PARAMS_DIR = os.path.join(ROOT, "deploy", "eval-c28", "params")
CHART = os.path.join(ROOT, "charts", "predictive-autoscaler")
OWNER, DEMO = "c28-roll-owner", "c28-roll-demo"
STORAGE_CLASS = "c28-retained"
TARGETS = ("c28-demo-pa-n", "c28-demo-pa-r")
# What the UNMODIFIED telemetry gate says about an image whose counters are born at the first request, and nothing else
COLD_GAP = frozenset(f"telemetry {kind}/{t}: no series" for kind in ("ingress", "requests") for t in TARGETS)
MEMBERS = (("pa-eval-system", "c28-forecast-route"), ("pa-eval-system", "c28-metrics-route"), ("demo", "c28-ingress"),
           ("demo", "c28-demo-pa-n"), ("demo", "c28-demo-pa-r"))
SYSTEM_DAEMONSETS = (("kube-system", "kindnet"), ("kube-system", "kube-proxy"),
                     ("monitoring", "vms-prometheus-node-exporter"))


class Failed(Exception):
    pass


class Smoke:
    def __init__(self, a):
        self.a, self.work = a, os.path.abspath(a.work)
        self.kcfg, self.ctx = os.path.abspath(a.kubeconfig), a.context
        self.py = sys.executable
        os.makedirs(self.work, exist_ok=True)
        with open(a.images) as fh:
            d = json.load(fh)
        if d.get("schema") != "c28-images/v1" or d.get("verified") is not True:
            raise Failed("--images is not a verified c28-images/v1 record")
        self.images = {k: v["ref"] for k, v in d["images"].items()}
        self.installing = "install" in a.stages
        self.checks = []
        prior = os.path.join(self.work, "checks.json")       # stages may run as separate invocations: one record
        if os.path.exists(prior):
            with open(prior) as fh:
                self.checks = json.load(fh)

    # --- plumbing ----------------------------------------------------------------------------------------------------
    def log(self, text):
        print(f"{time.strftime('%H:%M:%SZ', time.gmtime())} roll-smoke: {text}", flush=True)

    def ok(self, name, detail=""):
        self.checks.append({"check": name, "detail": detail})
        self.log(f"ok: {name}" + (f" ({detail})" if detail else ""))

    def run(self, args, check=True, **kw):
        p = subprocess.run(args, capture_output=True, text=True, **kw)
        if check and p.returncode != 0:
            raise Failed(f"{' '.join(args[:6])} ...: exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
        return p

    def guard(self):
        """Only ever this cluster: the work directory's kubeconfig, its context, a local API server, the recorded UID."""
        server = self.run(["kubectl", "--kubeconfig", self.kcfg, "config", "view", "--minify", "--context", self.ctx,
                           "-o", "jsonpath={.clusters[0].cluster.server}"]).stdout
        if not self.ctx.startswith("kind-") or not server.startswith("https://127.0.0.1:"):
            raise Failed(f"not a local kind API server: context {self.ctx}, server {server}")
        uid = self.kj("get", "namespace", "kube-system")["metadata"]["uid"]
        recorded = self.recorded_cluster()
        if recorded is None:
            if not self.installing:
                raise Failed("this work directory records no cluster identity: only the install stage may start one")
            fd = os.open(os.path.join(self.work, "cluster-uid"), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
            with os.fdopen(fd, "w") as fh:               # written once, before anything is installed
                fh.write(uid + "\n")
        elif recorded != uid:
            raise Failed(f"the cluster answering ({uid}) is not the one this work directory recorded ({recorded})")
        self.cluster_uid = uid

    def recorded_cluster(self):
        """The cluster this work directory belongs to: recorded by the first guard of the install stage, and equal to
        the records' cluster_uid once they exist. Every later invocation, and every direct write, requires it."""
        path, found = os.path.join(self.work, "cluster-uid"), set()
        if os.path.exists(path):
            with open(path) as fh:
                found.add(fh.read().strip())
        for name in ("demo.json", "owner.json"):
            if os.path.exists(os.path.join(self.work, name)):
                with open(os.path.join(self.work, name)) as fh:
                    found.add(json.load(fh)["cluster_uid"])
        if len(found) > 1:
            raise Failed(f"the work directory names more than one cluster: {sorted(found)}")
        return found.pop() if found else None

    def k(self, *args, check=True, stdin=None):
        return self.run(["kubectl", "--request-timeout=30s", "--kubeconfig", self.kcfg, "--context", self.ctx, *args],
                        check=check, input=stdin)

    def kj(self, *args):
        return json.loads(self.k(*args, "-o", "json").stdout)

    def helper(self, name, record, *args, check=True, timeout=None):
        """One staging helper command with this smoke's record; (exit code, parsed stdout or None, stderr)."""
        p = self.run([self.py, os.path.join(STAGING, name), "--record", record, *args], check=False, timeout=timeout,
                     env={**os.environ, "KUBECONFIG": "/dev/null"})
        try:
            out = json.loads(p.stdout) if p.stdout.strip() else None
        except ValueError:
            out = None
        if check and p.returncode != 0:
            raise Failed(f"{name} {' '.join(args)}: exit {p.returncode}: {p.stderr[-800:] or p.stdout[-800:]}")
        return p.returncode, out, p.stderr

    def reviewed(self, name, record, plan_args, apply_args, what):
        """plan -> no problems -> apply with exactly that plan and digest (the smoke is its own reviewer)."""
        _rc, saved, _err = self.helper(name, record, *plan_args)
        saved = saved[0] if isinstance(saved, list) else saved
        if saved.get("problems"):
            raise Failed(f"{what}: the plan has problems: {saved['problems'][:6]}")
        _rc, out, _err = self.helper(name, record, *apply_args, "--plan", saved["plan"], "--plan-digest",
                                     saved["plan_digest"])
        self.ok(what, saved["plan_digest"][:12])
        return saved, out

    def write(self, name, doc):
        path = os.path.join(self.work, name)
        with open(path, "w") as fh:
            fh.write(json.dumps(doc, indent=1, sort_keys=True) + "\n")
        return path

    # --- install -----------------------------------------------------------------------------------------------------
    def node(self, name):
        n = self.kj("get", "node", name)
        return {"name": name, "uid": n["metadata"]["uid"], "allocatable": n["status"]["allocatable"]}

    def params_and_records(self):
        cluster = self.ctx[len("kind-"):]
        w, s = self.node(f"{cluster}-worker"), self.node(f"{cluster}-worker2")
        commit = self.run(["git", "-C", ROOT, "rev-parse", "HEAD"]).stdout.strip()
        residency = {role: {"allocatable": {"cpu": n["allocatable"]["cpu"], "memory": n["allocatable"]["memory"]},
                            "system_requests": {"cpu": "300m", "memory": "512Mi"}}
                     for role, n in (("workload", w), ("support", s))}
        residency["support_extra"] = []
        nodes = {"workload": w["name"], "support": s["name"]}
        with open(os.path.join(PARAMS_DIR, "kind-smoke.json")) as fh:
            owner = json.load(fh)
        owner.update(run_id=OWNER, nodes=nodes, image=self.images["fixture"], residency=residency)
        owner["reservation"]["taint_value"] = OWNER
        with open(os.path.join(PARAMS_DIR, "kind-demo.json")) as fh:
            demo = json.load(fh)
        demo.update(run_id=DEMO, nodes=nodes, image=self.images["fixture"], residency=copy.deepcopy(residency),
                    scrape="VMPodScrape")
        demo["reservation"]["taint_value"] = OWNER
        demo["routes"].update(metrics_upstream="vmsingle-vmst.monitoring.svc:8428", metrics_query_path="/prometheus")
        d = demo["demonstration"]
        d.update(run_id=DEMO, shadow_replicas=self.a.replicas)
        d["reservation_ref"] = {"run_id": OWNER, "taint_value": OWNER,
                                "nodes": {"workload": {"name": w["name"], "uid": w["uid"]},
                                          "support": {"name": s["name"], "uid": s["uid"]}}}
        d["generator"]["image"] = self.images["generator"]
        self.write("owner-params.json", owner)
        self.write("demo-params.json", demo)

        def record(run, params, evidence, **sections):
            return {"schema": "c28-preparation/v1", "run_id": run, "cluster_uid": self.cluster_uid,
                    "kubeconfig": {"path": self.kcfg, "context": self.ctx}, "evidence_dir": evidence,
                    "lock_dir": os.path.join(self.work, "locks"), "source": {"commit": commit}, "params": params,
                    **sections}
        reserve = {"nodes": {"workload": {"name": w["name"], "uid": w["uid"]},
                             "support": {"name": s["name"], "uid": s["uid"]}},
                   "taint_key": "c28.devkuban.com/reserved", "role_label": "c28.devkuban.com/role",
                   "allowed": {"daemonsets": [{"namespace": ns, "name": n} for ns, n in SYSTEM_DAEMONSETS],
                               "mirror_pod_prefixes": [], "broad_tolerations": []},
                   "movable": []}
        self.owner = self.write("owner.json", record(OWNER, "owner-params.json", "evidence-owner", reserve=reserve))
        sha = self.run(["sha256sum", self.owner]).stdout.split()[0]
        prepare = {"stage": "demonstration",
                   "storage": {"storage_class": STORAGE_CLASS, "access_mode": "ReadWriteOnce",
                               "ledger_claim": {"name": "c28-roll-operator-ledger", "size": "1Gi"},
                               "state_claim": {"name": "c28-roll-forecast-state", "size": "1Gi"}},
                   "release": {"operator_image": self.images["operator"],
                               "statistical_image": self.images["statistical"],
                               "image_pull": {"mode": "anonymous"}, "network_policy": self.network_policy()},
                   "reservation": {"record": "owner.json", "sha256": sha}}
        self.demo = self.write("demo.json", record(DEMO, "demo-params.json", "evidence-demo", prepare=prepare))
        self.ok("params and records written from the live cluster", f"commit {commit[:8]}, W {w['name']}, S {s['name']}")

    def network_policy(self):
        """The chart's NetworkPolicy values for this bed, in the shape a real installation records them: DNS, the API
        server by its addresses (kind has no Cilium entity for it), the metrics upstream only from the evaluation's
        own metrics route, and scrapes only from the bed's vmagent."""
        api = self.kj("-n", "default", "get", "endpoints", "kubernetes")["subsets"][0]
        svc = self.kj("-n", "default", "get", "service", "kubernetes")["spec"]["clusterIP"]
        return {"enabled": True,
                "dns": {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                        "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}},
                "kubeAPI": {"cidrs": sorted([f"{a['ip']}/32" for a in api["addresses"]] + [f"{svc}/32"]),
                            "ports": sorted({443} | {p["port"] for p in api["ports"]}), "cilium": False},
                "prometheus": {"namespaceSelector": {}, "ipBlocks": [], "ports": [8428],
                               "podSelector": {"matchLabels": {"c28.devkuban.com/run": DEMO,
                                                               "app.kubernetes.io/name": "c28-metrics-route"}}},
                "metricsFrom": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}},
                                 "podSelector": {"matchLabels": {"app.kubernetes.io/name": "vmagent",
                                                                 "app.kubernetes.io/instance": "vmst"}}}]}

    def install(self):
        self.guard()
        self.params_and_records()
        self.k("apply", "-f", "-", stdin=json.dumps({
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": STORAGE_CLASS},
            "provisioner": "rancher.io/local-path", "reclaimPolicy": "Retain",
            "volumeBindingMode": "WaitForFirstConsumer"}))
        self.reviewed("reserve.py", self.owner, ["plan"], ["apply"], "reserve: W and S tainted and labelled for the owner")
        for phase in ("namespaces", "storage", "values"):
            self.reviewed("prepare.py", self.demo, ["plan", "--phase", phase], ["apply", "--phase", phase],
                          f"prepare {phase}")
        self.guard()
        crd = os.path.join(CHART, "crds")
        self.k("apply", "--server-side", "-f", crd)
        self.reviewed("prepare.py", self.demo, ["plan", "--phase", "common"], ["apply", "--phase", "common"],
                      "prepare common")
        values = os.path.join(self.work, "evidence-demo", "release-staging.yaml")
        self.guard()
        self.run(["helm", "--kubeconfig", self.kcfg, "--kube-context", self.ctx, "install", "pa-eval", CHART,
                  "-n", "pa-eval-system", "-f", values, "--wait", "--timeout", "10m"])
        self.ok("the chart release pa-eval installed with prepare's staging values")
        for phase in ("targets", "pas"):
            self.reviewed("prepare.py", self.demo, ["plan", "--phase", phase], ["apply", "--phase", phase],
                          f"prepare {phase}")
        self.wait_rolled_out()
        rc, out, err = self.helper("prepare.py", self.demo, "verify", check=False)
        if rc != 0 or out.get("result") != "PASS":
            raise Failed(f"prepare.py verify: exit {rc}: {(out or {}).get('problems') or err[-600:]}")
        self.ok("prepare.py verify PASS", f"{len(out['objects'])} objects")

    # --- bedcheck: the unmodified gates on this bed ------------------------------------------------------------------
    def telemetry(self, name):
        """bounded.py check-telemetry, UNMODIFIED; its output is archived. Returns (exit code, problems, observed)."""
        rc, out, err = self.helper("bounded.py", self.demo, "check-telemetry", "--query-api", self.a.query_api,
                                   check=False)
        if out is None:
            raise Failed(f"check-telemetry gave no answer: exit {rc}: {err[-500:]}")
        self.write(name, out)
        return rc, out["problems"], (out.get("telemetry") or {}).get("observed") or {}

    def snapshot(self, name):
        """What the cluster itself says, independent of the helper: the workloads of both namespaces."""
        doc = {}
        for ns in ("demo", "pa-eval-system"):
            for kind in ("deployments", "replicasets", "pods", "endpointslices"):
                doc[f"{ns}/{kind}"] = self.kj("-n", ns, "get", kind)["items"]
        self.write(name, doc)
        return doc

    def bedcheck(self):
        self.guard()
        self.demo = getattr(self, "demo", os.path.join(self.work, "demo.json"))
        targets = "c28-demo-pa-n,c28-demo-pa-r"
        out = os.path.join(self.work, "permissions-operator-before.json")
        rc, verdict, err = self.helper("permissions.py", self.demo, "check", "--targets", targets, "--out", out,
                                       check=False)
        if rc != 0 or verdict != {"verdict": "pass", "refused": 0, "unproven": 0}:
            raise Failed(f"the permission proof is not a pass on this bed: exit {rc}: {verdict or err[-400:]}")
        self.ok("permissions.py: the operator has no replica-write permission (the API server's authorizer)")
        deadline = time.time() + 300                       # the first scrapes of 26 new pods need a few intervals
        while True:
            rc, problems, observed = self.telemetry("check-telemetry-before.json")
            cold = sorted(set(problems) & COLD_GAP)
            if set(problems) in (set(), set(COLD_GAP)) or time.time() > deadline:
                break
            time.sleep(15)
        other = sorted(set(problems) - COLD_GAP)
        if cold and set(cold) != set(COLD_GAP):
            other.append(f"only part of the cold-counter series are missing: {cold}")
        if other:
            raise Failed(f"the UNMODIFIED telemetry gate does not work on this bed: {other[:6]} (a bed mismatch is "
                         "diagnosed, never hidden)")
        covered = {k: (v.get("series"), v.get("expected")) for k, v in sorted(observed.items())
                   if k.split("/")[0] in ("queue", "cfs_periods", "cfs_throttled")}
        if not covered or any(a != b for a, b in covered.values()):
            raise Failed(f"queue or CFS series do not cover the pods: {covered}")
        self.ok("the unmodified telemetry gate reads this bed: queue, CFS and steal series are as it requires",
                json.dumps(covered))
        if cold:
            self.ok("the installed fixture image has no counters before its first request (the gate's only refusals)",
                    f"{len(cold)}: {sorted(cold)}")
        else:
            self.ok("the installed fixture image already exposes its counters before any request")
        rc, st, _err = self.helper("imageroll.py", self.demo, "status")
        if st["fence"] is not None:
            raise Failed(f"an image transition is already open: {st}")
        self.snapshot("cluster-before.json")
        self.ok("baseline archived (cluster-before.json)")

    # --- forward: open, an interrupted roll, recovery, resume, completion ---------------------------------------------
    def journal_path(self):
        return os.path.join(self.work, "evidence-demo", "receipts", "imageroll.jsonl")

    def journal(self):
        """The helper's receipts. The file is appended to while it is read: a last line WITHOUT its newline is a write
        in progress and is left for the next read; a complete line that is not a receipt is corruption and raises."""
        path = self.journal_path()
        if not os.path.exists(path):
            return []
        with open(path) as fh:
            text = fh.read()
        lines = text.split("\n")
        lines.pop()                                         # "" after a final newline, or an incomplete last line
        return [json.loads(line) for line in lines if line.strip()]

    def status(self):
        return self.helper("imageroll.py", self.demo, "status")[1]

    def states(self):
        return [m["state"] for m in self.status()["members"]]

    def lock_files(self):
        return sorted(glob.glob(os.path.join(self.work, "locks", "*.lock")))

    def barrier(self, after):
        """A SMOKE-ONLY synchronization barrier, outside the helper's code: a `kubectl` in front of the real one on the
        executor's PATH. It passes every call through unchanged until the journal holds `after` member-rolled
        receipts; from then on every call of that executor waits. The helper's next step after a member-rolled
        receipt is its safety check, so it stops BEFORE the next member's patch can be submitted. Nothing is answered
        on the API server's behalf and no production code path is changed."""
        folder = os.path.join(self.work, "barrier-bin")
        os.makedirs(folder, exist_ok=True)
        reached = os.path.join(self.work, "barrier-reached")
        script = os.path.join(folder, "kubectl")
        with open(script, "w") as fh:
            fh.write("#!/bin/sh\n# roll smoke: synchronization barrier (see smoke/roll_kind.py)\n"
                     f"n=$(grep -c '\"event\": \"member-rolled\"' '{self.journal_path()}' 2>/dev/null)\n"
                     f"if [ \"${{n:-0}}\" -ge {after} ]; then : > '{reached}'; while :; do sleep 3600; done; fi\n"
                     f"exec '{shutil.which('kubectl')}' \"$@\"\n")
        os.chmod(script, os.stat(script).st_mode | stat.S_IXUSR)
        return folder, reached

    def roll_killed(self, saved, after, synchronized=False):
        """`roll` as a process group of its own, killed with SIGKILL after `after` member-rolled receipts. Without the
        barrier the kill is not synchronized with the next member: whatever state results is what recovery has to
        handle. With it (synchronized) the executor is held before the next member's patch and killed there."""
        log = open(os.path.join(self.work, "roll-killed.log"), "w")
        env = {**os.environ, "KUBECONFIG": "/dev/null"}
        reached = None
        if synchronized:
            folder, reached = self.barrier(after)
            env["PATH"] = folder + os.pathsep + env["PATH"]
        p = subprocess.Popen(self.executor(saved), stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                             env=env)
        deadline = time.time() + self.a.interrupt_budget
        try:                                               # whatever happens while waiting: the group never survives
            while len([e for e in self.journal() if e["event"] == "member-rolled"]) < after or \
                    (reached and not os.path.exists(reached)):
                if p.poll() is not None:
                    raise Failed(f"the roll ended (exit {p.returncode}) before the interruption point")
                if time.time() > deadline:
                    raise Failed(f"the interruption point was not reached within {self.a.interrupt_budget} s")
                time.sleep(0.2)
        finally:
            self.end_group(p)
        self.write("roll-killed.json", {"process_group": p.pid, "signal": "SIGKILL", "exit": p.returncode,
                                        "after_member_rolled_receipts": after, "synchronized": synchronized})
        self.ok(f"the executor was killed after {after} member-rolled receipts; its process group is empty",
                f"pgid {p.pid}")

    def executor(self, saved):
        return [self.py, os.path.join(STAGING, "imageroll.py"), "--record", self.demo, "roll", "--plan", saved["plan"],
                "--plan-digest", saved["plan_digest"]]

    @staticmethod
    def end_group(p):
        """SIGKILL to the executor's whole process group, the executor reaped, and the group proven empty."""
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        p.wait()
        for _ in range(100):                               # the executor AND its child commands are gone
            try:
                os.killpg(p.pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.1)
        raise Failed("the killed executor's process group still has a process")

    def recover(self):
        """The normal recovery protocol, whatever the kill left: an unknown patch that can still commit gets the
        fencing write first; then recover releases the stale lock."""
        for _ in range(3):
            _rc, saved, _err = self.helper("imageroll.py", self.demo, "recover-plan")
            with open(saved["plan"]) as fh:
                unresolved = json.load(fh)["unresolved"]
            if not unresolved:
                self.helper("imageroll.py", self.demo, "recover", "--plan", saved["plan"], "--plan-digest",
                            saved["plan_digest"])
                return self.ok("recover: every intent resolved, the stale lock released", saved["plan_digest"][:12])
            if not all(u.get("fence") == "possible" for u in unresolved):
                raise Failed(f"recovery cannot decide: {unresolved}")
            self.reviewed("imageroll.py", self.demo, ["plan-void"], ["void"], "void: the fencing write for a lost patch")
        raise Failed("recovery did not converge")

    def patch_accounting(self):
        """Per member: every patch intent and what became of it. Exactly one applied patch each, no intent unexplained."""
        by, steps = {}, {}
        for e in self.journal():
            kind, _, n = e["event"].partition("-")
            if kind in ("intent", "step", "reconciled") and n.isdigit():
                steps.setdefault(n, {})[kind] = e["data"]
        for n, s in sorted(steps.items(), key=lambda kv: int(kv[0])):
            d = s["intent"]
            if d["op"] != "image-roll":
                continue
            outcome = (s.get("step") or {}).get("result") or {"done": "applied", "not-done": "not-applied"}.get(
                (s.get("reconciled") or {}).get("observed"), "UNRESOLVED")
            by.setdefault(d["name"], []).append({"step": n, "outcome": outcome})
        bad = {k: v for k, v in by.items() if [x["outcome"] for x in v].count("applied") != 1 or
               any(x["outcome"] == "UNRESOLVED" for x in v)}
        if bad or sorted(by) != sorted(n for _ns, n in MEMBERS):
            raise Failed(f"patch intents do not add up to one applied patch per member: {by}")
        self.write("patch-accounting.json", by)
        self.ok("one applied patch per member, every intent accounted for",
                ", ".join(f"{k}: {len(v)}" for k, v in sorted(by.items())))

    def cluster_check(self, before, new, old):
        """From the cluster, not from the helper: only the image changed; the pods, their images and the endpoints."""
        after = self.snapshot("cluster-after.json")
        self.compare_cluster(before, after, new, old)
        self.ok("the cluster shows it: five Deployments differ in the image only; new pods on the new digest; ready "
                "endpoints = the new pods; the release's pods untouched")

    @staticmethod
    def compare_cluster(before, after, new, old):
        """Raises unless, for every member: the same Deployment whose spec differs from the earlier one in exactly one
        container's `image` field (old -> new); as many pods as replicas, none of them a pod from before, none
        terminating, each with a status of THAT container whose image id is non-empty and ends with the new digest;
        and the Service's ready endpoints being exactly those pods. And the release's pods are the ones from before."""
        digest = new.rsplit("@", 1)[1]
        for ns, name in MEMBERS:
            b = next(d for d in before[f"{ns}/deployments"] if d["metadata"]["name"] == name)
            a = next(d for d in after[f"{ns}/deployments"] if d["metadata"]["name"] == name)
            at = [i for i, c in enumerate(b["spec"]["template"]["spec"]["containers"]) if c["image"] == old]
            if len(at) != 1:
                raise Failed(f"{name}: not exactly one container ran the old image before")
            ctr = a["spec"]["template"]["spec"]["containers"][at[0]]
            spec = json.loads(json.dumps(a["spec"]))
            spec["template"]["spec"]["containers"][at[0]]["image"] = old       # that one field, nothing else
            if a["metadata"]["uid"] != b["metadata"]["uid"] or ctr["image"] != new or spec != b["spec"]:
                raise Failed(f"{name}: the Deployment changed in more than that container's image (or was replaced)")
            rs = {r["metadata"]["uid"]: r for r in after[f"{ns}/replicasets"]
                  if any(o.get("uid") == a["metadata"]["uid"] for o in r["metadata"].get("ownerReferences") or [])}
            pods = [p for p in after[f"{ns}/pods"]
                    if any(o.get("uid") in rs for o in p["metadata"].get("ownerReferences") or [])]
            old_uids = {p["metadata"]["uid"] for p in before[f"{ns}/pods"]}
            ids = [[s.get("imageID") or "" for s in p["status"].get("containerStatuses") or []
                    if s.get("name") == ctr["name"]] for p in pods]
            if len(pods) != a["spec"]["replicas"] or {p["metadata"]["uid"] for p in pods} & old_uids or \
                    any(p["metadata"].get("deletionTimestamp") for p in pods) or \
                    any(len(i) != 1 or not i[0] or not i[0].endswith(digest) for i in ids):
                raise Failed(f"{name}: its pods are not exactly the new ones, each with a status of container "
                             f"{ctr['name']} on the new image: {ids}")
            ready = {(e.get("targetRef") or {}).get("uid") for s in after[f"{ns}/endpointslices"]
                     if s["metadata"]["labels"].get("kubernetes.io/service-name") == name
                     for e in s.get("endpoints") or [] if (e.get("conditions") or {}).get("ready")}
            if ready != {p["metadata"]["uid"] for p in pods}:
                raise Failed(f"{name}: the Service's ready endpoints are not exactly the new pods")
        release = lambda doc: sorted((p["metadata"]["uid"], s["restartCount"]) for p in doc["pa-eval-system/pods"]  # noqa: E731
                                     if p["metadata"]["name"].startswith("pa-eval-")
                                     for s in p["status"].get("containerStatuses") or [])
        if release(before) != release(after) or not release(after):
            raise Failed("the release's pods are not the ones from before the roll")

    def forward(self):
        self.roll()
        self.complete()

    def roll(self):
        self.guard()
        self.demo = getattr(self, "demo", os.path.join(self.work, "demo.json"))
        new, old = self.a.new_fixture, self.images["fixture"]
        if not new:
            raise Failed("forward needs --new-fixture")
        with open(os.path.join(self.work, "cluster-before.json")) as fh:
            before = json.load(fh)
        with open(os.path.join(self.work, "demo-params.json")) as fh:
            self.write("demo-params-new.json", dict(json.load(fh), image=new))
        _saved, out = self.reviewed("imageroll.py", self.demo, ["plan-open", "--new-params", "demo-params-new.json"],
                                    ["open"], "open: the transition journalled, then the record published")
        tid = out["data"]["id"]
        if self.status()["fence"] != tid or set(self.states()) != {"from"}:
            raise Failed(f"after open: {self.status()}")
        if self.helper("prepare.py", self.demo, "verify", check=False)[0] != 5:
            raise Failed("prepare.py verify is not IN_TRANSITION (exit 5) while the transition is open")
        self.ok("the fence holds: status names the transition, prepare.py verify answers IN_TRANSITION", tid)
        _rc, saved, _err = self.helper("imageroll.py", self.demo, "plan-roll")
        if saved["problems"]:
            raise Failed(f"plan-roll: {saved['problems'][:6]}")
        self.roll_killed(saved, self.a.kill_after)
        # The precise contract after an interruption: the dead executor's lock is still there and stops any apply;
        # reading and planning still work.
        if not self.lock_files():
            raise Failed("the killed executor left no lock")
        _rc, st, _err = self.helper("imageroll.py", self.demo, "status")
        rc, replan, _err = self.helper("imageroll.py", self.demo, "plan-roll", check=False)
        rc, _out, err = self.helper("imageroll.py", self.demo, "roll", "--plan", replan["plan"], "--plan-digest",
                                    replan["plan_digest"], check=False)
        if rc == 0:
            raise Failed("a roll was applied while the dead executor's lock was still held")
        self.write("after-kill.json", {"status": st, "plan_roll_problems": replan["problems"],
                                       "apply_refused_with": err.strip().splitlines()[-1][-300:] if err.strip() else ""})
        self.ok("before recovery: status and plan-roll answer; an apply is refused by the dead executor's lock",
                f"states {[m['state'] for m in st['members']]}")
        self.recover()
        if self.lock_files() or self.status()["fence"] != tid:
            raise Failed("after recovery the lock must be gone and the fence must still hold")
        _rc, plans, _err = self.helper("prepare.py", self.demo, "plan", "--phase", "generator", check=False)
        fenced = [p for p in (plans[0] if isinstance(plans, list) else plans)["problems"] if tid in p]
        if not fenced:
            raise Failed("after recovery an unrelated mutation's plan does not show the fence")
        self.ok("after recovery: the lock is gone, the fence stays, an unrelated mutation is still refused")
        _rc, saved, _err = self.helper("imageroll.py", self.demo, "plan-roll")
        with open(saved["plan"]) as fh:
            dos = [a["do"] for a in json.load(fh)["actions"]]
        if saved["problems"] or dos[:self.a.kill_after] != ["verify"] * self.a.kill_after or "patch" not in dos:
            raise Failed(f"the resume plan is not verify x{self.a.kill_after} then the rest: {dos} {saved['problems']}")
        self.helper("imageroll.py", self.demo, "roll", "--plan", saved["plan"], "--plan-digest", saved["plan_digest"])
        if set(self.states()) != {"rolled"}:
            raise Failed(f"after the resume: {self.states()}")
        self.ok("the reviewed resume finished the roll", f"resume plan {dos}")
        self.patch_accounting()
        self.cluster_check(before, new, old)

    def dispatch_series(self):
        """What the bed's store holds of the ingress's dispatch counters right now, per target: for the record of how
        the removed endpoints' series are kept, end with a staleness marker and leave the window."""
        out, now = {}, time.time()
        for target in ("c28-demo-pa-n", "c28-demo-pa-r"):
            q = urllib.parse.urlencode({"time": f"{now:.3f}", "query":
                                        f'c28_ingress_dispatched_total{{namespace="demo",target="{target}"}}[180s]'})
            with urllib.request.urlopen(f"{self.a.query_api}/api/v1/query?{q}", timeout=30) as r:
                res = json.load(r)["data"]["result"]
            out[target] = {"series": len(res),
                           "ending_in_a_staleness_marker": sum(1 for x in res if str(x["values"][-1][1]) == "NaN"),
                           "nonzero": sum(1 for x in res if any(v not in ("0", "NaN") for _t, v in x["values"]))}
        return out

    def complete(self):
        """Cold completion, repeated read-only until a fresh window is clean or the budget ends. The budget is a
        planning allowance (the ingress's retention of removed endpoints, the evidence's lookback and scrapes), never
        an acceptance condition: only a clean plan-complete completes. On a timeout the transition stays fenced."""
        self.guard()
        self.demo = getattr(self, "demo", os.path.join(self.work, "demo.json"))
        tid = self.status()["fence"]
        if not tid or set(self.states()) != {"rolled"}:
            raise Failed(f"complete needs an open transition with every member rolled: {self.status()}")
        polls, deadline = [], time.time() + self.a.complete_budget
        while True:
            _rc, saved, _err = self.helper("imageroll.py", self.demo, "plan-complete", "--query-api", self.a.query_api)
            polls.append({"at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "problems": len(saved["problems"]),
                          "first": saved["problems"][:2], "dispatch": self.dispatch_series()})
            self.write("completion-polls.json", polls)
            if not saved["problems"] or time.time() > deadline:
                break
            time.sleep(30)
        if saved["problems"]:
            raise Failed(f"forward completion is still refused after {self.a.complete_budget} s; the transition stays "
                         f"fenced: {saved['problems'][:4]}")
        refused = [p for p in polls if p["problems"]]
        self.ok("completion was refused while removed endpoints' series were in the window, and clean afterwards",
                f"{len(refused)} refused polls, then clean; series per target at first/last poll: "
                f"{[p['dispatch']['c28-demo-pa-n']['series'] for p in (polls[0], polls[-1])]}")
        _rc, out, _err = self.helper("imageroll.py", self.demo, "complete", "--plan", saved["plan"], "--plan-digest",
                                     saved["plan_digest"], "--query-api", self.a.query_api)
        if out["event"] != "transition-complete" or out["data"]["kind"] != "forward":
            raise Failed(f"complete: {out}")
        rc, problems, _obs = self.telemetry("check-telemetry-after.json")
        if rc != 0 or problems:
            raise Failed(f"the telemetry gate is not clean on the new image: {problems[:6]}")
        rc, _v, _err = self.helper("prepare.py", self.demo, "verify", check=False)
        if rc != 0 or self.status()["fence"] is not None:
            raise Failed(f"after completion: verify exit {rc}, status {self.status()}")
        self.ok("cold completion: zero-valued counters of the replacement processes and a clean telemetry gate, "
                "before any request was sent", out["data"]["zero_counters"]["file"])

    # --- reverse: a partial roll taken back through a successor transition (the second cluster) ----------------------
    def reverse(self):
        self.guard()
        self.demo = getattr(self, "demo", os.path.join(self.work, "demo.json"))
        new, old = self.a.new_fixture, self.images["fixture"]
        with open(os.path.join(self.work, "cluster-before.json")) as fh:
            before = json.load(fh)
        with open(os.path.join(self.work, "demo-params.json")) as fh:
            self.write("demo-params-new.json", dict(json.load(fh), image=new))
        _saved, out = self.reviewed("imageroll.py", self.demo, ["plan-open", "--new-params", "demo-params-new.json"],
                                    ["open"], "open (forward)")
        first = out["data"]["id"]
        _rc, saved, _err = self.helper("imageroll.py", self.demo, "plan-roll")
        if saved["problems"]:
            raise Failed(f"plan-roll: {saved['problems'][:6]}")
        self.roll_killed(saved, 3, synchronized=True)
        self.recover()
        intents = [e for e in self.journal() if e["event"].startswith("intent-")]
        if self.states() != ["rolled", "rolled", "rolled", "from", "from"] or len(intents) != 3:
            raise Failed(f"the mixed state is not three rolled members and two untouched ones: {self.states()}, "
                         f"{len(intents)} patch intents")
        self.ok("the mixed state is asserted: the routes and the ingress on the new image, both targets untouched, "
                "three patch intents in the journal")
        plan_args = ["plan-open", "--new-params", "demo-params.json", "--supersedes", first]
        _rc, saved, _err = self.helper("imageroll.py", self.demo, *plan_args)
        with open(saved["plan"]) as fh:
            binding = json.load(fh)["binding"]
        starts = [m["start"] for m in binding["members"]]
        if saved["problems"] or binding["kind"] != "reverse" or starts != ["from"] * 3 + ["target"] * 2:
            raise Failed(f"the successor is not a reverse of the first three: {binding['kind']} {starts} "
                         f"{saved['problems']}")
        _saved, out = self.reviewed("imageroll.py", self.demo, plan_args, ["open"],
                                    "open (the successor): the original params file, kind reverse")
        second = out["data"]["id"]
        events = [(e["event"], e["data"].get("id")) for e in self.journal() if e["event"].startswith("transition-")]
        if self.status()["fence"] != second or events[-2:] != [("transition-prepared", second),
                                                               ("transition-open", second)] or \
                self.helper("prepare.py", self.demo, "verify", check=False)[0] != 5:
            raise Failed(f"the fence was not continuous across the supersession: {events} {self.status()['fence']}")
        self.ok("the fence is continuous: the successor is prepared and open, the parent superseded, verify still "
                "answers IN_TRANSITION", f"{first} -> {second}")
        self.reviewed("imageroll.py", self.demo, ["plan-roll"], ["roll"], "roll (reverse): three members back")
        if self.states() != ["rolled"] * 3 + ["target"] * 2:
            raise Failed(f"after the reverse roll: {self.states()}")
        _saved, out = self.reviewed("imageroll.py", self.demo, ["plan-complete"], ["complete"],
                                    "complete (reverse), without an observer")
        if out["data"]["kind"] != "reverse" or "limitation" not in out["data"] or "zero_counters" in out["data"] or \
                glob.glob(os.path.join(self.work, "evidence-demo", "imageroll", "zero-counters-*")):
            raise Failed(f"reverse completion: {out['data']}")
        self.reversecheck()

    def reversecheck(self):
        """After the reverse completion, from the cluster and from the unmodified gate."""
        self.guard()
        self.demo = getattr(self, "demo", os.path.join(self.work, "demo.json"))
        with open(os.path.join(self.work, "cluster-before.json")) as fh:
            before = json.load(fh)
        after = self.snapshot("cluster-after.json")
        for ns, name in MEMBERS:
            b = next(d for d in before[f"{ns}/deployments"] if d["metadata"]["name"] == name)
            a = next(d for d in after[f"{ns}/deployments"] if d["metadata"]["name"] == name)
            if a["spec"] != b["spec"] or a["metadata"]["uid"] != b["metadata"]["uid"]:
                raise Failed(f"{name}: after the reverse the Deployment is not what it was")
        uids = lambda doc, name: sorted(p["metadata"]["uid"] for p in doc["demo/pods"]  # noqa: E731
                                        if p["metadata"]["name"].startswith(name + "-"))
        for name in ("c28-demo-pa-n", "c28-demo-pa-r"):
            if uids(before, name) != uids(after, name) or len(uids(after, name)) != self.a.replicas:
                raise Failed(f"{name}: an untouched target's pods changed")
        if uids(before, "c28-ingress") == uids(after, "c28-ingress"):
            raise Failed("the ingress pod is the one from before, although it was rolled twice")
        deadline = time.time() + 420          # the replaced ingress's last series must first leave the gate's lookback
        while True:
            rc, problems, _obs = self.telemetry("check-telemetry-after-reverse.json")
            cold = sorted(problems)
            if (rc != 0 and set(problems) == set(COLD_GAP)) or time.time() > deadline:
                break
            time.sleep(15)
        if rc == 0 or set(problems) != set(COLD_GAP) or len(problems) != len(COLD_GAP):
            raise Failed(f"after a reverse to an image without zero-valued counters the gate should refuse for "
                         f"exactly that reason: exit {rc}, {problems[:6]}")
        if self.helper("prepare.py", self.demo, "verify", check=False)[0] != 0 or self.status()["fence"] is not None:
            raise Failed("after the reverse completion verify must pass and the fence must be gone")
        self.ok("the reverse restored the original specs, left both targets' pods untouched, and recorded its "
                "limitation, which is real: the telemetry gate refuses again for the missing cold counters",
                f"{len(cold)} refusals; replicas {self.a.replicas} (a reduced size when not 12)")

    # --- admission: the origin's first minute through the real generator, collector and reducer ----------------------
    def admission(self):
        """ONE unmodified bounded admission run after the cold completion (about 45 minutes). What is asserted is the
        origin's FIRST minute: the generator ran before the origin and covered minute 0 completely; the archived raw
        ingress samples bracket both minute boundaries, starting from the zero the counters had before any request;
        the reducer observes minute 0 for both targets; the run cleaned up after itself. Its verdict is reported as
        functional evidence of this bed, never as an admission qualification."""
        self.guard()
        self.demo = getattr(self, "demo", os.path.join(self.work, "demo.json"))
        origin = (int(time.time()) + 150 + 59) // 60 * 60        # a whole minute, 150 to 210 s ahead
        utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(origin))
        _rc, saved, _err = self.helper("bounded.py", self.demo, "plan-admission", "--origin", utc, "--query-api",
                                       self.a.query_api)
        if saved["problems"]:
            raise Failed(f"plan-admission: {saved['problems'][:6]}")
        left = origin - time.time()
        if not 125 <= left <= 300:
            raise Failed(f"the launch window was missed ({left:.0f} s before the origin)")
        self.log(f"admission run for origin {utc} (about 45 minutes)")
        rc, out, err = self.helper("bounded.py", self.demo, "run-admission", "--plan", saved["plan"], "--plan-digest",
                                   saved["plan_digest"], "--query-api", self.a.query_api, check=False, timeout=3600)
        self.write("admission-run.json", {"exit": rc, "out": out, "stderr_tail": err[-800:]})
        if rc != 0:
            raise Failed(f"run-admission: exit {rc}: {err[-600:]}")
        self.minute0()

    def minute0(self):
        """The assertions on the finished admission run's own evidence (also callable alone, on the same directory)."""
        self.demo = getattr(self, "demo", os.path.join(self.work, "demo.json"))
        dirs = sorted(glob.glob(os.path.join(self.work, "evidence-demo", "admission-a-*")))
        if len(dirs) != 1:
            raise Failed(f"expected exactly one admission evidence directory: {dirs}")
        with open(os.path.join(dirs[0], "evidence.json")) as fh:
            ev = json.load(fh)
        with open(os.path.join(dirs[0], "verdict.json")) as fh:
            verdict = json.load(fh)
        if ev["job"] != {**ev["job"], "status": "Complete", "exit_code": 0} or ev["stops"] or ev["collection_problems"]:
            raise Failed(f"the run did not complete cleanly: job {ev['job']}, stops {ev['stops']}, "
                         f"collection {ev['collection_problems']}")
        sys.path.insert(0, HARNESS)
        from c28 import demo_reduce
        lines = []
        for x in ev["lines"]:                               # the generator's records; k6's own log lines are not JSON
            try:
                rec = json.loads(x)
            except ValueError:
                continue
            if isinstance(rec, dict) and rec not in lines:      # the log is collected repeatedly: one copy each
                lines.append(rec)
        origin = ev["config"]["origin_ms"] / 1000.0
        utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(origin))
        facts = {}
        for t in ("c28-demo-pa-n", "c28-demo-pa-r"):
            starts = [x for x in lines if x.get("event") == "start" and x.get("target") == t]
            minute0 = [x for x in lines if x.get("schema", "").startswith("c28-demo-minute/") and x.get("target") == t
                       and x.get("minute") == 0]
            samples = [(float(ts), float(v)) for ts, v in ev["ingress"][t]]     # as the reducer takes them
            row = demo_reduce._ingress_minute(samples, origin, origin + 60.0, demo_reduce.DEFAULT_MAX_SCRAPE_GAP_S)
            before = [v for ts, v in samples if ts <= origin]
            facts[t] = {"generator_started_ms": [x["started_ms"] for x in starts], "minute0": minute0[:1],
                        "samples_before_origin": len(before), "value_before_origin": before[-1] if before else None,
                        "samples_after_minute": sum(1 for ts, _v in samples if ts >= origin + 60), "row": row}
            m0 = minute0[0] if minute0 else {}
            if not starts or any(x["started_ms"] >= origin * 1000 for x in starts):
                raise Failed(f"{t}: the generator was not running before the origin")
            if not minute0 or m0["due"] != m0["dispatched"] or m0["unresolved"] or any(m0["dropped"].values()):
                raise Failed(f"{t}: the generator did not cover minute 0 completely: {minute0[:1]}")
            if not before or before[-1] != 0 or not facts[t]["samples_after_minute"]:
                raise Failed(f"{t}: the raw ingress samples do not bracket minute 0 from a zero before the origin: "
                             f"{facts[t]}")
            lo, hi = row.get("received_bounds") or (0, -1)      # the reducer's bracket of what the ingress received
            if row.get("state") != "ok" or row.get("reasons") or not lo <= m0["dispatched"] <= hi:
                raise Failed(f"{t}: the reducer does not observe minute 0 consistently with the generator's "
                             f"{m0['dispatched']} dispatched requests: {row}")
        self.write("admission-minute0.json", {"origin_utc": utc, "targets": facts})
        self.ok("the origin's first minute is observed for both targets: the generator ran before the origin, raw "
                "ingress samples bracket the minute from a zero, the reducer reports it without a gap",
                ", ".join(f"{t}: {f['row']['received']} received" for t, f in facts.items()))
        left = [o for kind in ("jobs", "configmaps", "pods") for o in self.kj("-n", "demo", "get", kind)["items"]
                if "admission" in o["metadata"]["name"]]
        if left or self.lock_files():
            raise Failed(f"the admission run left something behind: {[o['metadata']['name'] for o in left]}, "
                         f"locks {self.lock_files()}")
        telemetry = {k: v["result"] for k, v in verdict["criteria"].items() if k in ("collection", "freshness") or
                     k.endswith("/rows")}
        if set(telemetry.values()) != {"PASS"}:
            raise Failed(f"the run's telemetry criteria did not pass on this bed: {telemetry}")
        self.ok("the run cleaned up (no generator object, no lock); its collection, freshness and row criteria pass",
                f"verdict {verdict['verdict']} (functional evidence of this bed, not an admission qualification)")

    # --- churn: an endpoint leaves, another one arrives (after the admission run, the ingress never restarted) ---------
    def ingress(self):
        pods = [p for p in self.kj("-n", "demo", "get", "pods")["items"]
                if p["metadata"]["name"].startswith("c28-ingress-") and not p["metadata"].get("deletionTimestamp")]
        if len(pods) != 1:
            raise Failed(f"expected one ingress pod: {[p['metadata']['name'] for p in pods]}")
        return pods[0]

    def dispatched(self, target):
        """The ingress's own dispatch counters of one target, read from its /metrics: {endpoint pod: count}."""
        text = self.k("get", "--raw", f"/api/v1/namespaces/demo/pods/{self.ingress()['metadata']['name']}:8080"
                                      "/proxy/metrics").stdout
        rx = re.compile(r'^c28_ingress_dispatched_total\{target="%s",endpoint_pod="([^"]+)"\} (\d+)$' % target, re.M)
        return {pod: int(n) for pod, n in rx.findall(text)}

    def churn(self):
        """A target pod is deleted (a graceful delete; its ReplicaSet makes another one) while NO traffic runs. Then,
        from the ingress's own counters: the removed endpoint's count is kept and does not move; the new pod is a
        different identity whose counter starts at zero; after requests are sent, only live endpoints receive them.
        The same identity leaving and returning is covered by the fixture's unit tests, not here. Also: a completed
        transition cannot be superseded."""
        self.guard()
        self.demo = getattr(self, "demo", os.path.join(self.work, "demo.json"))
        target, ingress = "c28-demo-pa-n", self.ingress()
        live = lambda: sorted(p["metadata"]["name"] for p in self.kj("-n", "demo", "get", "pods", "-l",  # noqa: E731
                              f"c28.devkuban.com/target={target}")["items"]
                              if not p["metadata"].get("deletionTimestamp"))
        before, pods = self.dispatched(target), live()
        if sorted(before) != pods or not all(before.values()):
            raise Failed(f"before the churn the ingress should count exactly the live pods, each above zero: {before}")
        victim = pods[0]
        self.guard()                                       # a direct write: only ever on the recorded cluster
        self.k("-n", "demo", "delete", "pod", victim, "--wait=true", "--timeout=120s")
        self.wait_rolled_out()
        new = sorted(set(live()) - set(pods))
        if len(new) != 1 or victim in live():
            raise Failed(f"expected exactly one replacement pod for {victim}: {new}")
        deadline = time.time() + 60
        while new[0] not in self.dispatched(target) and time.time() < deadline:
            time.sleep(2)
        quiet = self.dispatched(target)
        if quiet.get(victim) != before[victim] or quiet.get(new[0]) != 0 or \
                any(quiet[p] != before[p] for p in pods if p != victim):
            raise Failed(f"with no traffic: the removed endpoint must keep its count, the new one start at zero, the "
                         f"others stay: before {before}, after {quiet}")
        self.ok("no traffic: the removed endpoint's count is kept, the replacement is a new identity at zero",
                f"{victim} {before[victim]} kept; {new[0]} 0")
        sent = 0
        self.guard()
        for _ in range(5 * len(pods)):
            p = self.k("get", "--raw", f"/api/v1/namespaces/demo/services/c28-ingress:80/proxy/t/{target}/work",
                       check=False)
            sent += p.returncode == 0
        after = self.dispatched(target)
        gained = {p: after[p] - quiet[p] for p in after}
        if after[victim] != before[victim] or gained[new[0]] <= 0 or sum(gained.values()) != sent or sent == 0:
            raise Failed(f"after {sent} requests: the removed endpoint must receive nothing and the new one its "
                         f"share: gained {gained}")
        now = self.ingress()
        restarts = sum(c["restartCount"] for c in now["status"]["containerStatuses"])
        if now["metadata"]["uid"] != ingress["metadata"]["uid"] or restarts:
            raise Failed("the ingress was replaced or restarted during the churn check")
        self.write("churn.json", {"target": target, "removed": victim, "replacement": new[0], "before": before,
                                  "quiet": quiet, "after": after, "requests_answered": sent})
        self.ok("with traffic: the removed endpoint receives nothing, the replacement counts from zero, every "
                "answered request is one dispatch; the ingress was never restarted",
                f"{sent} requests; {new[0]} +{gained[new[0]]}")
        done = [e["data"]["id"] for e in self.journal() if e["event"] == "transition-complete"]
        _rc, saved, _err = self.helper("imageroll.py", self.demo, "plan-open", "--new-params", "demo-params.json",
                                       "--supersedes", done[-1], check=False)
        if not any("is not the open image transition" in p for p in (saved or {}).get("problems") or []):
            raise Failed(f"a completed transition was accepted as the parent of a successor: {saved}")
        self.ok("a completed transition cannot be superseded", done[-1])

    def wait_rolled_out(self, seconds=900):
        deadline = time.time() + seconds
        while True:
            bad = []
            for ns in ("demo", "pa-eval-system"):
                for d in self.kj("-n", ns, "get", "deployments")["items"]:
                    st, want = d.get("status") or {}, d["spec"].get("replicas", 1)
                    if st.get("readyReplicas", 0) != want or st.get("updatedReplicas", 0) != want or \
                            st.get("observedGeneration", 0) < d["metadata"]["generation"]:
                        bad.append(f"{ns}/{d['metadata']['name']} {st.get('readyReplicas', 0)}/{want}")
            if not bad:
                return self.ok("every Deployment of both namespaces is rolled out and Ready")
            if time.time() > deadline:
                raise Failed(f"not rolled out after {seconds} s: {bad}")
            time.sleep(10)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--work", required=True)
    ap.add_argument("--kubeconfig", required=True)
    ap.add_argument("--context", required=True)
    ap.add_argument("--images", required=True, help="c28-images/v1: the INSTALLED image set (fixture, operator, "
                                                    "statistical, generator), by digest")
    ap.add_argument("--new-fixture", default="", help="the fixture image the roll moves to, by digest")
    ap.add_argument("--query-api", default="", help="the bed's Prometheus API base")
    ap.add_argument("--replicas", type=int, default=12)
    ap.add_argument("--kill-after", type=int, default=2, help="member-rolled receipts before the executor is killed")
    ap.add_argument("--interrupt-budget", type=int, default=1800,
                    help="seconds to wait for the interruption point before the executor is killed and the smoke fails")
    ap.add_argument("--complete-budget", type=int, default=1500,
                    help="seconds to keep re-planning completion (the ingress keeps removed endpoints 15 minutes)")
    ap.add_argument("stages", nargs="+", choices=["install", "bedcheck", "forward", "roll", "complete", "admission",
                                                  "minute0", "churn", "reverse", "reversecheck"])
    a = ap.parse_args()
    s = Smoke(a)
    try:
        for stage in a.stages:
            s.log(f"stage {stage}")
            getattr(s, stage)()
    except Failed as e:
        s.log(f"FAIL: {e}")
        return 1
    finally:
        s.write("checks.json", s.checks)
    return 0


if __name__ == "__main__":
    sys.exit(main())
