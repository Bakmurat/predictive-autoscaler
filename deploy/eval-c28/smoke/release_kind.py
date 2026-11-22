#!/usr/bin/env python3
"""The release helper's smoke driver (smoke/release-kind.sh runs it on its OWN throwaway kind cluster; never a lab
cluster). staging/release.py with the REAL pinned Helm and the real c28-secretmeta against a real API server.
Functional smoke evidence, not production evidence.

  release_kind.py --work DIR --kubeconfig F --context C --images images.json --new-statistical REF
                  --helm /abs/helm --secretmeta /abs/c28-secretmeta --from-commit SHA STAGE...

Stages (each leaves its records, plans, receipts and observations under DIR):
  install  the demonstration as a real one is made (reserve.py, prepare.py phase by phase) and the chart release as
           REVISION 1 from the INSTALLED chart tree (release.chart_artifact of --from-commit, not the working tree)
           with prepare's staging values and the installed statistical image; then the CANDIDATE record: the same
           record with the new statistical image and the `release_upgrade` section (both values files, the pinned
           executables by sha256)
  A        upgrade, then rollback: `applied` twice; the operation label and the storage UIDs through the real
           metadata reader; from the cluster itself, the forecaster Deployment differs in the image and the one
           environment entry ONLY and every other object of the release is the same object, unchanged; the operator's
           and the trainer's pods untouched; the TLS Secret's UID and resourceVersion and the certificate the NEW
           forecaster pod serves unchanged across upgrade and rollback; the three permission proofs
  B        a FAILED upgrade (the new statistical digest does not exist, a short Helm timeout): `failed`, the lock
           kept, nothing retried; the guarded rollback under the retained lock; the lock released
  C        a PENDING upgrade: the driver kills Helm's process group (from the journalled identity, after the pending
           revision is in the history and the group is seen alive); the helper reports `unresolved`; the guarded
           rollback from `pending-upgrade` with the real Helm
  D        the EXECUTOR dies while Helm works: after observed progress the driver stops Helm's group (SIGSTOP, an
           explicit barrier), kills the helper, and shows that recovery refuses while the group is alive; then lets
           Helm finish (SIGCONT) and recovery reconciles the upgrade as done, without a second Helm run
  I        as D, but the evidenced UPGRADED state is settled by plan-dispose / dispose
  E        an INTERRUPTED rollback after an applied upgrade (same barrier): recovery refuses (Helm does not mark a
           rollback as ours); plan-dispose shows the installed state; dispose settles exactly that intent
  H        a guarded rollback interrupted INSIDE the recovery guard, both intents open: one disposition settles
           both, entering through the takeover of the dead recoverer's guard record
  F        FINALIZE: a failed upgrade, then the guarded rollback in a child interpreter that kills itself (SIGKILL)
           right before the upgrade's reconciliation, after the rollback's applied receipt is durable (a wrapper
           around the receipt writer in that child only; no helper code differs). The stale lock and the populated
           guard record remain; the next plan-rollback is `finalize` for the same revision and storage UID; applying
           it takes the guard over, reconciles and releases, and submits no further upgrade or rollback
  G        NO REVISION EVIDENCE for an upgrade, by a RECORDED SHIM in place of Helm (it passes every read to the real
           Helm and exits 1 on `upgrade` without contacting the cluster): a synthetic refusal test, not transport-loss
           evidence. `unresolved`; plan-dispose refuses, again and again; the lock stays; no further submission
  K        as G for a ROLLBACK without its next revision (the shim fails `rollback` after a real upgrade)

Mutating Helm commands are counted from the helper's own journal (one helm-launch receipt per released command) and
compared with the number of revisions in the release's history; the pinned executable is never replaced silently (the
shim of G and K is the record's declared executable for those scenarios, with its source and hash archived).
"""

import argparse
import copy
import hashlib
import json
import os
import signal
import socket
import ssl
import stat
import subprocess
import sys
import time

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import roll_kind as RK  # noqa: E402

sys.path.insert(0, RK.STAGING)

RELEASE, CONTROL_NS, NS = "pa-eval", "pa-eval-system", "demo"
INSTANCE = f"app.kubernetes.io/instance={RELEASE}"
FORECASTER = f"{RELEASE}-predictive-autoscaler-forecaster"
ROLES = ("operator", "forecaster", "trainer")
OP_LABEL = "c28.devkuban.com/release-op"
# What a release snapshot holds: EVERY object the installed chart renders, whatever its namespace or scope (the
# identities come from the pinned Helm's offline rendering), anything else that carries the release's instance label,
# and the objects the cluster generates for the release's workloads. Secrets are never fetched: their uid and
# resourceVersion come from c28-secretmeta.
GENERATED = ("replicaset.apps", "pod", "endpointslice.discovery.k8s.io")
DEPLOYMENT = f"Deployment/{CONTROL_NS}/{FORECASTER}"
REAP_S = 20                                # the bound of every wait in the cleanup
# Fragments of what Helm prints on success: none of it may be in any file of the work directory.
HELM_OUTPUT = ("has been upgraded", "Happy Helming", "Rollback was a success", "REVISION:", "LAST DEPLOYED:")
Failed = RK.Failed
MISSING = "d" * 64                        # a digest no registry holds: the upgrade to it fails
# What each scenario's candidate record differs in: the target image, Helm's timeout, a shim as the record's helm.
VARIANTS = {"A": {}, "D": {}, "I": {}, "E": {},
            "B": {"missing": True, "timeout": "60s"}, "H": {"missing": True, "timeout": "60s"},
            "F": {"missing": True, "timeout": "60s"}, "C": {"missing": True, "timeout": "5m"},
            "G": {"shim": "upgrade"}, "K": {"shim": "rollback"}}
SHIM = """#!/bin/sh
# SMOKE SHIM, NOT HELM. The declared `helm` of a release-smoke record for the no-revision-evidence scenarios: every
# command goes to the real Helm unchanged, except that `upgrade` (or `rollback`) exits 1 WITHOUT contacting the
# cluster while its flag file exists. Real Helm: {real} (sha256 {sha}).
case "$1" in
  upgrade|rollback) [ ! -e "{flags}/fail-$1" ] || exit 1 ;;
esac
exec "{real}" "$@"
"""
# The child interpreter of scenario F: release.py unchanged, with the receipt writer wrapped so that this process
# kills itself right before the first reconciliation receipt of the release journal (after everything before it,
# the rollback's applied receipt included, is durable).
FINALIZE_CHILD = """
import os, signal, sys
sys.path.insert(0, sys.argv[1])
import execlib as X
real = X.receipt
def receipt(record, helper, event, data, clock=None):
    if helper == "release" and event.startswith("reconciled-"):
        os.kill(os.getpid(), signal.SIGKILL)
    return real(record, helper, event, data, clock)
X.receipt = receipt
import release
sys.exit(release.main(sys.argv[2:]))
"""


def okey(o):
    """An object's key in a snapshot: kind, namespace ("-" for a cluster-scoped object) and name."""
    m = o["metadata"]
    return f"{o['kind']}/{m.get('namespace') or '-'}/{m['name']}"


def _ppid(pid):
    try:
        with open(f"/proc/{pid}/stat") as fh:
            return int(fh.read().rpartition(")")[2].split()[1])
    except (OSError, ValueError, IndexError):
        return 0


class Smoke(RK.Smoke):
    def __init__(self, a):
        super().__init__(a)
        self.helm, self.meta = a.helm, a.secretmeta
        self.demo, self.owner = os.path.join(self.work, "demo.json"), os.path.join(self.work, "owner.json")
        self.variant = VARIANTS[a.scenario]
        self.flags = os.path.join(self.work, "shim-flags")
        self.procs = []                                    # helper processes this driver started and must reap
        self.known = []                                    # every Helm group identity read from the journal so far
        self._ids = None                                   # the rendered identities of the installed release

    def log(self, text):
        print(f"{time.strftime('%H:%M:%SZ', time.gmtime())} release-smoke: {text}", flush=True)

    def sha(self, path):
        with open(path, "rb") as fh:
            return hashlib.sha256(fh.read()).hexdigest()

    def release(self, *args, check=True):
        return self.helper("release.py", self.demo, *args, check=check, timeout=1800)

    def load(self, path):
        with open(path) as fh:
            return json.load(fh)

    # --- install: revision 1 from the installed chart tree, then the candidate record ---------------------------------
    def install(self):
        import execlib as X
        import release
        self.guard()
        self.params_and_records()
        self.k("apply", "-f", "-", stdin=json.dumps({
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": RK.STORAGE_CLASS},
            "provisioner": "rancher.io/local-path", "reclaimPolicy": "Retain",
            "volumeBindingMode": "WaitForFirstConsumer"}))
        self.reviewed("reserve.py", self.owner, ["plan"], ["apply"], "reserve: W and S tainted and labelled for the owner")
        for phase in ("namespaces", "storage", "values"):
            self.reviewed("prepare.py", self.demo, ["plan", "--phase", phase], ["apply", "--phase", phase],
                          f"prepare {phase}")
        chart = release.chart_artifact(X.load_record(self.demo, ("prepare",)), self.a.from_commit, RK.ROOT)
        self.write("chart-installed.json", chart)
        self.ok("the INSTALLED chart tree extracted and verified against git's listing",
                f"commit {chart['commit'][:8]}, tree {chart['tree'][:12]}, {chart['files']} files")
        self.guard()
        self.k("apply", "--server-side", "-f", os.path.join(chart["dir"], "crds"))
        self.reviewed("prepare.py", self.demo, ["plan", "--phase", "common"], ["apply", "--phase", "common"],
                      "prepare common")
        values = os.path.join(self.work, "evidence-demo", "release-staging.yaml")
        self.guard()
        self.run([self.helm, "install", RELEASE, chart["dir"], "--namespace", CONTROL_NS, "--kubeconfig", self.kcfg,
                  "--kube-context", self.ctx, "-f", values, "--wait", "--timeout", "10m"])
        self.ok("revision 1 installed from the installed chart tree with prepare's staging values (the pinned Helm)")
        for phase in ("targets", "pas"):
            self.reviewed("prepare.py", self.demo, ["plan", "--phase", phase], ["apply", "--phase", phase],
                          f"prepare {phase}")
        self.wait_rolled_out()
        rc, out, err = self.helper("prepare.py", self.demo, "verify", check=False)
        if rc != 0 or out.get("result") != "PASS":
            raise Failed(f"prepare.py verify: exit {rc}: {(out or {}).get('problems') or err[-600:]}")
        self.ok("prepare.py verify PASS", f"{len(out['objects'])} objects")
        self.candidate(values)

    def candidate(self, values):
        """The record the upgrade is planned with: the installed record plus the new statistical image and the
        release_upgrade section. The installed record is kept beside it."""
        with open(values) as fh:
            doc = yaml.safe_load(fh)
        repo, digest = self.a.new_statistical.rsplit("@", 1)
        if self.variant.get("missing"):
            digest = "sha256:" + MISSING
        target = f"{repo}@{digest}"
        helm = self.helm
        if self.variant.get("shim"):                       # the record's DECLARED helm for this scenario is the shim
            os.makedirs(self.flags, exist_ok=True)
            helm = os.path.join(self.work, "shim-helm")
            with open(helm, "w") as fh:
                fh.write(SHIM.format(real=self.helm, sha=self.sha(self.helm), flags=self.flags))
            os.chmod(helm, os.stat(helm).st_mode | stat.S_IXUSR)
            self.write("shim.json", {"what": "a smoke shim in place of Helm: a synthetic refusal test, not "
                                             "transport-loss evidence", "path": helm, "sha256": self.sha(helm),
                                     "real_helm": self.helm, "real_helm_sha256": self.sha(self.helm),
                                     "fails": self.variant["shim"]})
        old = doc["images"]["statisticalForecaster"]
        if (old["repository"], old["digest"]) != tuple(self.images["statistical"].rsplit("@", 1)):
            raise Failed("the staging values do not name the installed statistical image")
        new = copy.deepcopy(doc)
        new["images"]["statisticalForecaster"].update(repository=repo, digest=digest)
        values2 = os.path.join(self.work, "evidence-demo", "release-staging-2.yaml")
        fd = os.open(values2, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)       # a NEW immutable file
        with os.fdopen(fd, "w") as fh:
            yaml.safe_dump(new, fh, sort_keys=True)
        version = self.run([self.helm, "version", "--template", "{{.Version}}"]).stdout.strip()
        record = self.load(self.demo)
        self.write("demo.installed.json", record)
        record["prepare"]["release"]["statistical_image"] = target
        record["release_upgrade"] = {
            "from": {"revision": 1, "commit": self.a.from_commit, "values": values, "values_sha256": self.sha(values)},
            "to": {"values": values2, "values_sha256": self.sha(values2)},
            "helm": {"binary": helm, "sha256": self.sha(helm), "version": version,
                     "timeout": self.variant.get("timeout", self.a.helm_timeout), "kube_version": self.kube_version(),
                     "api_versions": []},
            "metadata_reader": {"binary": self.meta, "sha256": self.sha(self.meta)}}
        self.write("demo.json", record)
        self.write("pinned-tools.json", {"helm": record["release_upgrade"]["helm"],
                                         "metadata_reader": record["release_upgrade"]["metadata_reader"]})
        rc, st, _err = self.release("status")
        if [h["revision"] for h in st.get("history", [])] != [1] or st["lock_held"] or st["open_intents"]:
            raise Failed(f"the candidate record's status is not a quiet revision 1: {st}")
        self.ok("the candidate record: new statistical image, release_upgrade, the executables pinned by sha256",
                f"helm {version}, history {[h['revision'] for h in st['history']]}, variant {self.variant or 'plain'}")

    def kube_version(self):
        v = json.loads(self.k("version", "-o", "json").stdout)["serverVersion"]["gitVersion"]
        return v.lstrip("v")

    # --- observation, independent of the helper -----------------------------------------------------------------------
    def identities(self):
        """Kind, API group, namespace and name of every document the INSTALLED chart renders with the installed values
        (the pinned Helm, offline). The rendering stays in memory and is never shown; only these identities are kept.
        The helper's own plan refuses an upgrade whose rendering differs in anything but the forecaster's Deployment,
        so the upgraded release has the same inventory."""
        if self._ids is None:
            chart = self.load(os.path.join(self.work, "chart-installed.json"))
            values = os.path.join(self.work, "evidence-demo", "release-staging.yaml")
            p = self.run([self.helm, "template", RELEASE, chart["dir"], "--namespace", CONTROL_NS, "-f", values,
                          "--kube-version", self.kube_version()], check=False)
            if p.returncode != 0:
                raise Failed(f"helm template of the installed chart: exit {p.returncode}")    # its output is not shown
            try:
                ids = sorted(({"kind": d["kind"], "group": d["apiVersion"].rpartition("/")[0],
                               "namespace": d["metadata"].get("namespace"), "name": d["metadata"]["name"]}
                              for d in yaml.safe_load_all(p.stdout) if d),
                             key=lambda i: (i["kind"], i["namespace"] or "", i["name"]))
            except (yaml.YAMLError, KeyError, TypeError, AttributeError):
                raise Failed("helm template of the installed chart gave a document without an identity") from None
            if not ids or len({json.dumps(i, sort_keys=True) for i in ids}) != len(ids):
                raise Failed("the installed chart's rendering is empty or names an object twice")
            self.write("release-identities.json", ids)
            self._ids = ids
        return self._ids

    @staticmethod
    def archived(o):
        """What of an object reaches a FILE: its identity and the counters the comparison reads. No spec, no data, no
        labels or annotations, no environment values: nothing an object could carry a credential in."""
        m = o["metadata"]
        out = {"uid": m.get("uid"), "resourceVersion": m.get("resourceVersion"), "generation": m.get("generation"),
               "owners": sorted(str(r.get("uid")) for r in m.get("ownerReferences") or [])}
        if o.get("kind") == "Pod":
            out["containers"] = [[c.get("name"), c.get("containerID"), c.get("restartCount")]
                                 for c in (o.get("status") or {}).get("containerStatuses") or []]
        if o.get("kind") == "Deployment":
            ctrs = (((o.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
            out["containers"] = [{"name": c.get("name"), "image": c.get("image"),
                                  "env_names": [e.get("name") for e in c.get("env") or []]} for c in ctrs]
        return out

    def snapshot(self, name):
        """The release's objects as the cluster holds them:
          - every rendered identity, fetched by its own kind, namespace and name (cluster-scoped objects and objects
            of other namespaces included; one that is missing fails the run); a Secret only through the metadata
            reader (uid and resourceVersion);
          - every further object of those kinds that carries the release's instance label, in any namespace;
          - the ReplicaSets, pods and EndpointSlices of the control namespace (owned objects carry no such label).
        The objects are returned for the comparison and stay in memory; the file holds `archived()` of each."""
        doc, kinds = {}, []
        for i in self.identities():
            ns = i["namespace"] or CONTROL_NS
            if i["kind"] == "Secret":
                meta = self.secret_meta(i["name"], namespace=ns)
                if not meta.get("found"):
                    raise Failed(f"the rendered Secret {ns}/{i['name']} was not found by the metadata reader")
                o = {"kind": "Secret", "metadata": {"name": i["name"], "namespace": ns, "uid": meta["uid"],
                                                    "resourceVersion": meta["resourceVersion"]}}
            else:
                res = i["kind"].lower() + (f".{i['group']}" if i["group"] else "")
                if res not in kinds:
                    kinds.append(res)
                p = self.k("-n", ns, "get", res, i["name"], "-o", "json", check=False)
                if p.returncode != 0:
                    raise Failed(f"the rendered {i['kind']} {i['namespace'] or '-'}/{i['name']} is not in the cluster")
                o = json.loads(p.stdout)
            doc[okey(o)] = o
        for res in kinds:                                    # anything else that claims to belong to the release
            for o in self.kj("get", res, "-A", "-l", INSTANCE)["items"]:
                doc.setdefault(okey(o), o)
        for res in GENERATED:                                # owned objects do not all carry the label
            for o in self.kj("-n", CONTROL_NS, "get", res)["items"]:
                doc.setdefault(okey(o), o)
        self.write(name, {k: self.archived(o) for k, o in sorted(doc.items())})
        return doc

    def secret_meta(self, name, *label_keys, namespace=CONTROL_NS):
        argv = [self.meta, "--kubeconfig", self.kcfg, "--context", self.ctx, "--namespace", namespace, "--name", name]
        for k in label_keys:
            argv += ["--label-key", k]
        return json.loads(self.run(argv, env={"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
                                   ).stdout)

    def forecaster_pod(self):
        pods = [p for p in self.kj("-n", CONTROL_NS, "get", "pods", "-l",
                                   f"{INSTANCE},app.kubernetes.io/component=forecaster")["items"]
                if not p["metadata"].get("deletionTimestamp")]
        ready = [p for p in pods if any(c["type"] == "Ready" and c["status"] == "True"
                                        for c in p["status"].get("conditions") or [])]
        if len(pods) != 1 or len(ready) != 1:
            raise Failed(f"not exactly one ready forecaster pod: {[p['metadata']['name'] for p in pods]}")
        return ready[0]

    def tls_state(self, name):
        """The TLS Secret's identity through the metadata reader, and the certificate the CURRENT forecaster pod
        serves, taken over a fresh connection to that pod (a port-forward to the pod itself, not to the Service)."""
        meta = self.secret_meta(f"{FORECASTER}-tls")
        if not meta.get("found"):
            raise Failed("the forecaster's TLS Secret was not found by the metadata reader")
        pod = self.forecaster_pod()
        port = next(p["containerPort"] for c in pod["spec"]["containers"] if c["name"] == "forecaster"
                    for p in c.get("ports") or [])
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            local = s.getsockname()[1]
        fwd = subprocess.Popen(["kubectl", "--kubeconfig", self.kcfg, "--context", self.ctx, "-n", CONTROL_NS,
                                "port-forward", f"pod/{pod['metadata']['name']}", f"{local}:{port}"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE        # the fingerprint is the evidence
            der, deadline = None, time.time() + 60
            while der is None:
                try:
                    with socket.create_connection(("127.0.0.1", local), timeout=5) as raw:
                        with ctx.wrap_socket(raw) as tls:
                            der = tls.getpeercert(binary_form=True)
                except OSError:
                    if time.time() > deadline:
                        raise Failed("no TLS connection to the forecaster pod within 60 s") from None
                    time.sleep(1)
        finally:
            fwd.kill()
            fwd.wait()
        state = {"secret_uid": meta["uid"], "secret_resourceVersion": meta["resourceVersion"],
                 "served_certificate_sha256": hashlib.sha256(der).hexdigest(),
                 "pod": {"name": pod["metadata"]["name"], "uid": pod["metadata"]["uid"]}}
        self.write(name, state)
        return state

    def permissions(self, tag):
        for role in ROLES:
            out = os.path.join(self.work, f"permissions-{role}-{tag}.json")
            rc, verdict, err = self.helper("permissions.py", self.demo, "check", "--targets", ",".join(RK.TARGETS),
                                           "--out", out, "--role", role, check=False)
            if rc != 0 or verdict != {"verdict": "pass", "refused": 0, "unproven": 0}:
                raise Failed(f"the {role}'s permission proof is not a pass: exit {rc}: {verdict or err[-400:]}")
        self.ok(f"permissions.py: operator, forecaster and trainer have no replica-write permission ({tag})")

    def history(self):
        _rc, st, _err = self.release("status")
        return st["history"]

    def receipts(self, journal="release"):
        """A journal of the run as it is on disk. It may be appended to while it is read: a last line WITHOUT its
        newline is a write in progress and is left for the next read; a complete line that is not a receipt raises."""
        path = os.path.join(self.work, "evidence-demo", "receipts", f"{journal}.jsonl")
        if not os.path.exists(path):
            return []
        with open(path) as fh:
            lines = fh.read().split("\n")
        lines.pop()
        return [json.loads(line) for line in lines if line.strip()]

    def journal(self):
        return self.receipts("release")

    def launches(self):
        """How many mutating Helm commands the helper released, from its own journal (one helm-launch receipt each);
        the release's history is the cluster's own count of the revisions they wrote."""
        return sum(1 for e in self.journal() if e["event"] == "helm-launch")

    def no_helm_output(self):
        hits = []
        for root, _dirs, names in os.walk(self.work):
            if os.path.relpath(root, self.work).split(os.sep)[0] in ("helm", "evidence-demo/release") or \
                    "/chart-" in root:
                continue
            for n in names:
                if n.endswith(".tgz") or n == "kubeconfig":
                    continue
                with open(os.path.join(root, n), "rb") as fh:
                    data = fh.read()
                hits += [f"{n}: {frag}" for frag in HELM_OUTPUT if frag.encode() in data]
        if hits:
            raise Failed(f"Helm's output reached a file: {hits[:5]}")
        self.ok("nothing Helm printed is in any file of the work directory")

    # --- what must and must not have changed ----------------------------------------------------------------------------
    @staticmethod
    def forecaster_family(doc):
        """The objects that necessarily change with the forecaster's pod template: its ReplicaSets, pods and
        EndpointSlices (found by ownership and by the Service name)."""
        dep = doc[DEPLOYMENT]
        rs = {k for k, o in doc.items() if o.get("kind") == "ReplicaSet" and any(
            r.get("uid") == dep["metadata"]["uid"] for r in o["metadata"].get("ownerReferences") or [])}
        rs_uids = {doc[k]["metadata"]["uid"] for k in rs}
        pods = {k for k, o in doc.items() if o.get("kind") == "Pod" and any(
            r.get("uid") in rs_uids for r in o["metadata"].get("ownerReferences") or [])}
        slices = {k for k, o in doc.items() if o.get("kind") == "EndpointSlice" and
                  (o["metadata"].get("labels") or {}).get("kubernetes.io/service-name") == FORECASTER}
        return rs | pods | slices

    @staticmethod
    def compare(before, after, old_image, new_image, env_entry, env="added"):
        """Raises unless `after` is `before` with exactly: the forecaster Deployment's container image old -> new and
        the one environment entry added (or, for a rollback, removed) on the same object, its ReplicaSets, pods and
        EndpointSlices changed, and EVERY other object of the release the same object with the same resourceVersion."""
        fam = Smoke.forecaster_family(before) | Smoke.forecaster_family(after)
        key = DEPLOYMENT
        rest_b, rest_a = set(before) - fam - {key}, set(after) - fam - {key}
        if rest_b != rest_a:
            raise Failed(f"objects appeared or vanished: +{sorted(rest_a - rest_b)[:5]} -{sorted(rest_b - rest_a)[:5]}")
        moved = [k for k in sorted(rest_b) if (before[k]["metadata"]["uid"], before[k]["metadata"]["resourceVersion"])
                 != (after[k]["metadata"]["uid"], after[k]["metadata"]["resourceVersion"])]
        if moved:
            raise Failed(f"objects other than the forecaster's changed (uid or resourceVersion): {moved[:8]}")
        b, a = before[key], after[key]
        expected = copy.deepcopy(b["spec"])
        ctr = next(c for c in expected["template"]["spec"]["containers"] if c["name"] == "forecaster")
        if ctr["image"] != old_image:
            raise Failed("the forecaster did not run the expected image before")
        ctr["image"] = new_image
        got = copy.deepcopy(a["spec"])
        gctr = next(c for c in got["template"]["spec"]["containers"] if c["name"] == "forecaster")
        has, had = gctr.get("env", []).count(env_entry), ctr.get("env", []).count(env_entry)
        if (had, has) != ((0, 1) if env == "added" else (1, 0)):
            raise Failed(f"the one environment entry was not {env} exactly once (before {had}, after {has})")
        for c in (ctr, gctr):                              # compared without it: everything else must be equal
            c["env"] = [e for e in c.get("env", []) if e != env_entry]
        if a["metadata"]["uid"] != b["metadata"]["uid"] or got != expected:
            raise Failed("the forecaster Deployment changed in more than its image and the one environment entry")
        for k in sorted(rest_b):
            if before[k].get("kind") == "Pod":
                ids = lambda o: [(s["name"], s.get("containerID"), s.get("restartCount"))  # noqa: E731
                                 for s in o["status"].get("containerStatuses") or []]
                if ids(before[k]) != ids(after[k]) or not ids(after[k]):
                    raise Failed(f"{k}: a container of an untouched pod restarted or was replaced")
        return {"unchanged_objects": len(rest_b), "forecaster_family": len(fam),
                "generation": [b["metadata"]["generation"], a["metadata"]["generation"]]}

    # --- scenario A: upgrade, then rollback -----------------------------------------------------------------------------
    def A(self):
        self.guard()
        old, new = self.images["statistical"], self.a.new_statistical
        entry = {"name": "WATCH_NAMESPACES", "value": NS}
        self.permissions("before")
        before = self.snapshot("A-cluster-before.json")
        tls0 = self.tls_state("A-tls-before.json")
        saved, out = self.reviewed("release.py", self.demo, ["plan-upgrade"], ["apply-upgrade"],
                                   "the upgrade: planned without problems and applied")
        plan = self.load(saved["plan"])
        if out["data"]["result"] != "applied" or out["data"]["post"]["revision"] != 2:
            raise Failed(f"the upgrade's receipt is not `applied` for revision 2: {out['data'].get('post')}")
        hist = self.history()
        if [(h["revision"], h["status"], h["description_class"]) for h in hist] != [
                (1, "superseded", "Install complete"), (2, "deployed", "Upgrade complete")]:
            raise Failed(f"the history after the upgrade: {hist}")
        st2 = self.secret_meta(f"sh.helm.release.v1.{RELEASE}.v2", OP_LABEL, "owner", "version")
        if st2["labels"] != {OP_LABEL: plan["operation"], "owner": "helm", "version": "2"} or \
                st2["uid"] != out["data"]["post"]["uid"]:
            raise Failed(f"revision 2's storage Secret does not carry the operation label and the receipted UID: {st2}")
        self.ok("revision 2 is ours: the operation label and the UID through the real metadata reader",
                f"op {plan['operation']}, uid {st2['uid']}")
        self.wait_rolled_out()
        after = self.snapshot("A-cluster-after-upgrade.json")
        self.ok("after the upgrade the cluster differs in the forecaster's image and the one environment entry only",
                json.dumps(self.compare(before, after, old, new, entry)))
        tls1 = self.tls_state("A-tls-after-upgrade.json")
        self.same_tls(tls0, tls1, "the upgrade")
        self.permissions("after-upgrade")
        saved, out = self.reviewed("release.py", self.demo, ["plan-rollback"], ["apply-rollback"],
                                   "the rollback: planned without problems and applied")
        if out["data"]["result"] != "applied" or out["data"]["post"]["revision"] != 3:
            raise Failed(f"the rollback's receipt is not `applied` for revision 3: {out['data'].get('post')}")
        hist = self.history()
        if [(h["revision"], h["status"], h["description_class"]) for h in hist][-2:] != [
                (2, "superseded", "Upgrade complete"), (3, "deployed", "Rollback to 1")]:
            raise Failed(f"the history after the rollback: {hist}")
        self.wait_rolled_out()
        back = self.snapshot("A-cluster-after-rollback.json")
        key = DEPLOYMENT
        if back[key]["spec"] != before[key]["spec"] or back[key]["metadata"]["uid"] != before[key]["metadata"]["uid"]:
            raise Failed("after the rollback the forecaster Deployment is not the installed one")
        self.ok("after the rollback the forecaster Deployment's spec is the installed one again",
                json.dumps(self.compare(after, back, new, old, entry, env="removed")))
        tls2 = self.tls_state("A-tls-after-rollback.json")
        self.same_tls(tls1, tls2, "the rollback")
        if self.launches() != 2 or len(hist) != 3:
            raise Failed(f"{self.launches()} mutating Helm commands released, {len(hist)} revisions: expected 2 and 3")
        _rc, st, _err = self.release("status")
        if st["lock_held"] or st["open_intents"]:
            raise Failed(f"the helper is not at rest: {st}")
        self.ok("two mutating Helm commands, three revisions, no lock, no open intent")

    def same_tls(self, a, b, what):
        same = {k: a[k] == b[k] for k in ("secret_uid", "secret_resourceVersion", "served_certificate_sha256")}
        if not all(same.values()):
            raise Failed(f"the TLS Secret or the served certificate changed across {what}: {same}")
        if a["pod"]["uid"] == b["pod"]["uid"]:
            raise Failed(f"the forecaster pod did not change across {what}: the certificate check would prove nothing")
        self.ok(f"across {what}: the TLS Secret's UID and resourceVersion and the certificate served by the NEW "
                "forecaster pod are unchanged", f"{a['pod']['name']} -> {b['pod']['name']}")


class Scenarios(Smoke):
    """B to K: the paths that need a failure or an interruption."""

    # --- plumbing ----------------------------------------------------------------------------------------------------
    def events(self):
        return [e["event"] for e in self.journal()]

    def status(self, name=None):
        _rc, st, _err = self.release("status")
        if name:
            self.write(name, st)
        return st

    def plan(self, cmd):
        _rc, saved, _err = self.release(cmd)
        return saved, self.load(saved["plan"])

    def apply(self, cmd, saved):
        return self.release(cmd, "--plan", saved["plan"], "--plan-digest", saved["plan_digest"], check=False)

    def start(self, cmd, saved, name, interpreter=None):
        """A helper command as a process of its own (its own session), its output in a file; registered for reaping."""
        argv = interpreter or [self.py, os.path.join(RK.STAGING, "release.py")]
        out = open(os.path.join(self.work, name), "w")
        p = subprocess.Popen(argv + ["--record", self.demo, cmd, "--plan", saved["plan"], "--plan-digest",
                                     saved["plan_digest"]], stdout=out, stderr=subprocess.STDOUT,
                             start_new_session=True, env={**os.environ, "KUBECONFIG": "/dev/null"})
        out.close()
        self.procs.append(p)
        return p

    def wait_for(self, what, cond, seconds, every=0.5):
        """A bounded wait for a scenario's boundary. A boundary that is not reached is an unsuccessful SETUP."""
        end = time.time() + seconds
        while True:
            got = cond()
            if got:
                return got
            if time.time() > end:
                raise Failed(f"scenario setup failed: {what} was not reached within {seconds} s")
            time.sleep(every)

    def launched(self):
        """The process-group identities of the mutating Helm commands, from the helper's journal. Each is remembered,
        so the cleanup knows it even when the journal cannot be read later."""
        found = [e["data"]["identity"] for e in self.journal() if e["event"] == "helm-launch"]
        for ident in found:
            if ident not in self.known:
                self.known.append(ident)
        return found

    def launch(self, n):
        """The process-group identity of the n-th mutating Helm command."""
        found = self.launched()
        return found[n - 1] if len(found) >= n else None

    def group(self, ident):
        """LIVENESS of a recorded group: "empty" | "alive" | "unknown". It is not ownership: `alive` is also the
        answer while other processes occupy a reused group id."""
        import procgroup
        try:
            return procgroup.group_state(ident)
        except Exception:                                   # a malformed identity proves nothing
            return "unknown"

    def owned(self, ident):
        """The recorded group's id when a signal to it is justified, else None. A group id can be reused, so the
        recorded LEADER must exist with the recorded start time on this host and boot and lead its own group; and
        the group is never the driver's own, its session's, its parent's, init's or 0."""
        import procgroup
        try:
            pgid, pid = int(ident["pgid"]), int(ident["pid"])
            now = procgroup.identity_of(pid)
            mine = {0, 1, os.getpid(), os.getpgid(0), os.getsid(0), os.getppid(), os.getpgid(os.getppid())}
        except (KeyError, TypeError, ValueError, OSError):
            return None
        if pgid != pid or pgid in mine or now is None or \
                any(now.get(k) != ident.get(k) for k in ("pid", "pgid", "start", "host", "boot_id")):
            return None
        return pgid

    def signal_group(self, ident, sig):
        """The ONLY place a scenario signals a Helm group."""
        pgid = self.owned(ident)
        if pgid is None:
            raise Failed(f"scenario setup failed: Helm's recorded group is {self.group(ident)}, not a live group "
                         f"this run's helper launched")
        os.killpg(pgid, sig)

    def revision(self, n):
        return next((h for h in self.status()["history"] if h["revision"] == n), None)

    def progressed(self, launch_no, revision):
        """Helm's n-th command has been released, its revision is in the history, and its group is alive."""
        ident = self.launch(launch_no)
        if ident is None or self.revision(revision) is None or self.owned(ident) is None:
            return None
        return ident

    def strays(self):
        """Processes whose command line names THIS run's kubeconfig (the work directory's; no other run has it), apart
        from this driver and its ancestors: for example a Helm the helper launched but did not get to journal. Each
        is ended by its PID. Returns the problems: finding one is already a problem."""
        needle, skip, pid = os.fsencode(self.kcfg), set(), os.getpid()
        while pid > 1 and pid not in skip:
            skip.add(pid)
            pid = _ppid(pid)

        def found():
            out = []
            for name in os.listdir("/proc"):
                if name.isdigit() and int(name) not in skip:
                    try:
                        with open(f"/proc/{name}/cmdline", "rb") as fh:
                            if needle in fh.read():
                                out.append(int(name))
                    except OSError:
                        pass
            return sorted(out)
        first = found()
        for pid in first:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        end = time.time() + REAP_S
        while first and found() and time.time() < end:
            time.sleep(0.1)
        left = found() if first else []
        return ([f"{len(first)} process(es) naming this run's kubeconfig were still running at cleanup"] if first
                else []) + ([f"processes naming this run's kubeconfig could not be ended: {left}"] if left else [])

    def reap(self):
        """Every path ends here. Returns the problems; an EMPTY list is the proof that no helper process, no Helm
        group this run knows of and no process naming this run's kubeconfig is left. The order matters: the helpers
        are ended FIRST (a living helper can still launch Helm and publish its receipt), then their final launch
        receipts are read, then each recorded group is ended, if it is provably ours, and must be seen empty within
        a bound. Nothing here stops the rest of the cleanup, and anything unproven is a problem, never a pass."""
        problems = []
        for p in self.procs:
            try:
                if p.poll() is None:
                    p.kill()
                p.wait(timeout=REAP_S)
            except Exception as e:
                problems.append(f"helper process {getattr(p, 'pid', '?')} could not be ended ({type(e).__name__})")
        try:
            self.launched()
        except Exception as e:                              # the identities already known are still cleaned up
            problems.append(f"the helper's journal could not be read for the cleanup ({type(e).__name__})")
        for ident in self.known:
            pgid = self.owned(ident)
            if pgid is not None:
                for sig in (signal.SIGCONT, signal.SIGKILL):
                    try:
                        os.killpg(pgid, sig)
                    except ProcessLookupError:
                        pass
            end = time.time() + REAP_S
            while self.group(ident) != "empty" and time.time() < end:
                time.sleep(0.1)
            if self.group(ident) != "empty":
                problems.append(f"Helm's recorded group {ident.get('pgid') if isinstance(ident, dict) else '?'} is "
                                f"{self.group(ident)} after the cleanup: not proven empty")
        return problems + self.strays()

    def expect_outcome(self, classification, step, op):
        st = self.status()
        last = st.get("last_outcome") or {}
        if (last.get("classification"), last.get("step"), last.get("op")) != (classification, step, op) or \
                not st["lock_held"] or {"step": step, "op": op} not in st["open_intents"]:
            raise Failed(f"expected {op} step {step} to be {classification} with the lock kept: {st}")
        return st

    def at_rest(self, launches, revisions, what):
        st = self.status()
        hist = st["history"]
        if st["lock_held"] or st["open_intents"] or self.launches() != launches or len(hist) != revisions or \
                hist[-1]["status"] != "deployed":
            raise Failed(f"{what}: not at rest as expected ({self.launches()} launches, {len(hist)} revisions): {st}")
        if C_unresolved(self.demo):
            raise Failed(f"{what}: unresolved intents remain: {C_unresolved(self.demo)}")
        self.ok(f"{what}: no lock, no open intent, {launches} mutating Helm commands, {revisions} revisions")

    def failed_upgrade(self, classification="failed"):
        """A reviewed upgrade to an image that does not exist: Helm's wait times out."""
        saved, plan = self.plan("plan-upgrade")
        if plan["problems"]:
            raise Failed(f"the upgrade plan has problems: {plan['problems'][:4]}")
        rc, _out, err = self.apply("apply-upgrade", saved)
        if rc == 0 or f"is {classification}" not in err:
            raise Failed(f"the upgrade to a missing image did not end as {classification}: exit {rc}: {err[-300:]}")
        self.expect_outcome(classification, "1", "helm-upgrade")
        if self.launches() != 1:
            raise Failed("the failed upgrade was retried")
        self.ok(f"the upgrade to an image that does not exist is `{classification}`: the lock kept, nothing retried")
        return plan

    def guarded_rollback_plan(self):
        saved, plan = self.plan("plan-rollback")
        if plan["form"] != "unresolved" or plan["problems"] or not plan["lock_sha256"]:
            raise Failed(f"the guarded rollback's plan: form {plan['form']}, problems {plan['problems'][:4]}")
        return saved, plan

    def takeovers(self):
        return [e for e in self.receipts("recover") if e["event"] == "recovery-guard-takeover"]

    # --- B: failed upgrade, guarded rollback ----------------------------------------------------------------------------
    def B(self):
        self.guard()
        before = self.snapshot("B-cluster-before.json")
        self.failed_upgrade()
        if self.revision(2)["status"] != "failed":
            raise Failed(f"revision 2 is not failed: {self.revision(2)}")
        saved, _plan = self.guarded_rollback_plan()
        rc, out, err = self.apply("apply-rollback", saved)
        if rc != 0 or out["data"]["result"] != "applied" or out["data"]["post"]["revision"] != 3:
            raise Failed(f"the guarded rollback: exit {rc}: {err[-300:]}")
        rec1 = next(e for e in self.journal() if e["event"] == "reconciled-1")["data"]
        if (rec1["observed"], rec1["by"]) != ("rolled-back", "step-2"):
            raise Failed(f"the upgrade's intent was not reconciled as rolled back: {rec1}")
        self.ok("the guarded rollback of the failed upgrade applied under the retained lock; reconciled-1 rolled-back")
        self.wait_rolled_out()
        key = DEPLOYMENT
        if self.snapshot("B-cluster-after.json")[key]["spec"] != before[key]["spec"]:
            raise Failed("after the rollback the forecaster Deployment is not the installed one")
        self.at_rest(2, 3, "B")

    # --- C: pending upgrade (Helm killed), guarded rollback -------------------------------------------------------------
    def C(self):
        self.guard()
        saved, plan = self.plan("plan-upgrade")
        if plan["problems"]:
            raise Failed(f"the upgrade plan has problems: {plan['problems'][:4]}")
        p = self.start("apply-upgrade", saved, "C-apply-upgrade.log")
        ident = self.wait_for("Helm's pending revision 2 with its group alive",
                              lambda: self.progressed(1, 2) if (self.revision(2) or {}).get("status") ==
                              "pending-upgrade" else None, 120)
        self.signal_group(ident, signal.SIGKILL)
        self.ok("Helm's process group killed from the journalled identity, after revision 2 was pending and the "
                "group was seen alive", f"pgid {ident['pgid']}")
        p.wait(timeout=180)
        if p.returncode == 0 or self.group(ident) != "empty":
            raise Failed(f"the helper ended with {p.returncode}, Helm's group is {self.group(ident)}")
        st = self.expect_outcome("unresolved", "1", "helm-upgrade")
        if self.revision(2)["status"] != "pending-upgrade":
            raise Failed(f"revision 2 is {self.revision(2)['status']}, not pending-upgrade")
        self.ok("the helper itself reports `unresolved` for the pending revision; the lock kept", st["last_outcome"]["why"])
        saved, _plan = self.guarded_rollback_plan()
        rc, out, err = self.apply("apply-rollback", saved)
        if rc != 0 or out["data"]["result"] != "applied":
            raise Failed(f"the guarded rollback from pending-upgrade with the real Helm: exit {rc}: {err[-400:]}")
        self.ok("the guarded rollback from `pending-upgrade` applied with the real Helm", f"revision {out['data']['post']['revision']}")
        self.wait_rolled_out()
        self.at_rest(2, 3, "C")

    # --- D and I: the executor dies, Helm finishes ----------------------------------------------------------------------
    def executor_dies(self, tag):
        saved, plan = self.plan("plan-upgrade")
        if plan["problems"]:
            raise Failed(f"the upgrade plan has problems: {plan['problems'][:4]}")
        p = self.start("apply-upgrade", saved, f"{tag}-apply-upgrade.log")
        ident = self.wait_for("Helm's revision 2 with its group alive", lambda: self.progressed(1, 2), 120, every=0.3)
        self.signal_group(ident, signal.SIGSTOP)           # the barrier: Helm is held exactly here
        p.kill()
        p.wait()
        if self.group(ident) != "alive":
            raise Failed("scenario setup failed: Helm's group did not survive its executor")
        self.ok("the executor killed after Helm had written revision 2; Helm's group, held by SIGSTOP, is alive",
                f"helper pid {p.pid}, Helm pgid {ident['pgid']}")
        rc, saved_r, _err = self.release("recover-plan")
        rplan = self.load(saved_r["plan"])
        [a] = rplan["actions"]
        if a["observed"] != "unresolved" or "it is alive" not in a["why"]:
            raise Failed(f"recovery does not see the live Helm group: {a}")
        rc, _out, err = self.apply("recover", saved_r)
        if rc == 0 or not self.status()["lock_held"]:
            raise Failed("recovery released the lock while Helm's group was alive")
        self.ok("while Helm's group lives, recovery answers unresolved (\"it is alive\") and keeps the lock")
        os.killpg(ident["pgid"], signal.SIGCONT)
        self.wait_for("Helm's group to end by itself", lambda: self.group(ident) == "empty", 420, every=1)
        self.wait_for("revision 2 deployed", lambda: (self.revision(2) or {}).get("status") == "deployed", 60)
        self.wait_rolled_out()
        return plan

    def D(self):
        self.guard()
        self.executor_dies("D")
        _rc, saved_r, _err = self.release("recover-plan")
        [a] = self.load(saved_r["plan"])["actions"]
        if a["observed"] != "done":
            raise Failed(f"after Helm finished, recovery does not find the upgrade done: {a}")
        rc, _out, err = self.apply("recover", saved_r)
        if rc != 0:
            raise Failed(f"recover: exit {rc}: {err[-300:]}")
        self.ok("after Helm finished: recovery reconciles the upgrade as done from the history and the storage "
                "metadata, and releases the lock")
        self.at_rest(1, 2, "D")

    def I(self):  # noqa: E743
        self.guard()
        self.executor_dies("I")
        saved, plan = self.plan("plan-dispose")
        if plan["problems"] or plan["state"] != "upgraded" or [a["step"] for a in plan["actions"]] != ["1"]:
            raise Failed(f"the disposition plan: state {plan['state']}, problems {plan['problems'][:4]}")
        rc, out, err = self.apply("dispose", saved)
        if rc != 0 or (out["data"]["observed"], out["data"]["state"]) != ("disposed", "upgraded"):
            raise Failed(f"dispose: exit {rc}: {err[-300:]}")
        self.ok("the evidenced upgraded state settled by a disposition: reconciled-1 disposed, state upgraded")
        self.at_rest(1, 2, "I")

    # --- E and H: an interrupted rollback, settled by a disposition ------------------------------------------------------
    def interrupt_rollback(self, saved, tag, launch_no, revision):
        p = self.start("apply-rollback", saved, f"{tag}-apply-rollback.log")
        ident = self.wait_for(f"Helm's revision {revision} with its group alive",
                              lambda: self.progressed(launch_no, revision), 120, every=0.2)
        self.signal_group(ident, signal.SIGSTOP)
        p.kill()
        p.wait()
        if self.group(ident) != "alive":
            raise Failed("scenario setup failed: Helm's group did not survive its executor")
        os.killpg(ident["pgid"], signal.SIGCONT)
        self.wait_for("Helm's group to end by itself", lambda: self.group(ident) == "empty", 420, every=1)
        self.wait_for(f"revision {revision} deployed",
                      lambda: (self.revision(revision) or {}).get("status") == "deployed", 60)
        self.wait_rolled_out()
        self.ok(f"the rollback's executor killed after Helm had written revision {revision}; Helm finished by itself",
                f"helper pid {p.pid}, Helm pgid {ident['pgid']}")

    def blocked(self, what):
        """Unrelated mutation stays blocked until the settlement: the lock is held and a new plan says why."""
        st = self.status()
        _saved, plan = self.plan("plan-upgrade")
        if not st["lock_held"] or not any("intents with unknown outcomes" in p for p in plan["problems"]):
            raise Failed(f"{what}: mutation is not blocked: lock {st['lock_held']}, {plan['problems'][:3]}")

    def E(self):
        self.guard()
        saved, out = self.reviewed("release.py", self.demo, ["plan-upgrade"], ["apply-upgrade"], "the upgrade applied")
        self.wait_rolled_out()
        saved, plan = self.plan("plan-rollback")
        if plan["form"] != "applied" or plan["problems"]:
            raise Failed(f"the rollback plan: form {plan['form']}, problems {plan['problems'][:4]}")
        self.interrupt_rollback(saved, "E", 2, 3)
        _rc, saved_r, _err = self.release("recover-plan")
        [a] = self.load(saved_r["plan"])["actions"]
        rc, _out, _err = self.apply("recover", saved_r)
        if a["observed"] != "unresolved" or "ambiguous provenance" not in a["why"] or rc == 0:
            raise Failed(f"recovery decided an interrupted rollback: {a}, exit {rc}")
        self.ok("recovery refuses the interrupted rollback (Helm does not mark a rollback as ours); the lock kept")
        self.blocked("E")
        saved, plan = self.plan("plan-dispose")
        if plan["problems"] or plan["state"] != "installed" or [a["step"] for a in plan["actions"]] != ["2"]:
            raise Failed(f"the disposition plan: state {plan['state']}, problems {plan['problems'][:4]}")
        rc, out, err = self.apply("dispose", saved)
        if rc != 0 or (out["event"], out["data"]["state"]) != ("reconciled-2", "installed"):
            raise Failed(f"dispose: exit {rc}: {err[-300:]}")
        self.ok("the disposition settled exactly the rollback's intent (reconciled-2 disposed, state installed)")
        self.at_rest(2, 3, "E")

    def H(self):
        self.guard()
        self.failed_upgrade()
        saved, _plan = self.guarded_rollback_plan()
        self.interrupt_rollback(saved, "H", 2, 3)          # the killed helper was INSIDE the recovery guard
        st = self.status()
        if [i["step"] for i in st["open_intents"]] != ["1", "2"] or not st["lock_held"]:
            raise Failed(f"expected both intents open under the lock: {st}")
        self.blocked("H")
        saved, plan = self.plan("plan-dispose")
        if plan["problems"] or plan["state"] != "installed" or [a["step"] for a in plan["actions"]] != ["1", "2"]:
            raise Failed(f"the disposition plan: state {plan['state']}, problems {plan['problems'][:4]}")
        rc, out, err = self.apply("dispose", saved)
        if rc != 0:
            raise Failed(f"dispose: exit {rc}: {err[-300:]}")
        got = {e["event"]: e["data"]["observed"] for e in self.journal() if e["event"].startswith("reconciled-")}
        if got != {"reconciled-1": "disposed", "reconciled-2": "disposed"} or len(self.takeovers()) != 1:
            raise Failed(f"expected both intents disposed through one guard takeover: {got}, {len(self.takeovers())}")
        self.ok("one disposition settled both intents, entering through the takeover of the dead recoverer's guard",
                f"stale guard of pid {self.takeovers()[0]['data']['stale_guard']['owner']['pid']}")
        self.at_rest(2, 3, "H")

    # --- F: finalize after a SIGKILL inside the guard -------------------------------------------------------------------
    def F(self):
        self.guard()
        self.failed_upgrade()
        saved, _plan = self.guarded_rollback_plan()
        p = self.start("apply-rollback", saved, "F-apply-rollback.log",
                       interpreter=[self.py, "-c", FINALIZE_CHILD, RK.STAGING])
        p.wait(timeout=600)
        ev = self.events()
        lock = os.path.join(self.work, "locks")
        guard = [n for n in os.listdir(lock) if n.endswith(".recovery")]
        if p.returncode != -signal.SIGKILL or "step-2" not in ev or "reconciled-1" in ev or len(guard) != 1:
            raise Failed(f"scenario setup failed: the child ended with {p.returncode}; events {ev[-4:]}; guard {guard}")
        left = self.load(os.path.join(lock, guard[0]))
        step2 = next(e for e in self.journal() if e["event"] == "step-2")["data"]
        if left["pid"] != p.pid or step2["result"] != "applied" or not self.status()["lock_held"]:
            raise Failed("scenario setup failed: not the state right before the reconciliation")
        self.ok("the rollback applied and receipted; the helper was killed (SIGKILL) inside the recovery guard right "
                "before the upgrade's reconciliation: the stale lock and its populated guard record remain",
                f"pid {p.pid}")
        saved, plan = self.plan("plan-rollback")
        a = plan["actions"][0]
        if plan["form"] != "finalize" or plan["problems"] or a["revision"] != step2["post"]["revision"] or \
                plan["storage"]["uid"] != step2["post"]["uid"] or "argv" in a:
            raise Failed(f"the next plan is not `finalize` for the same revision and storage UID: {plan['form']}, "
                         f"{plan['problems'][:3]}, {a}")
        rc, out, err = self.apply("apply-rollback", saved)
        if rc != 0 or (out["event"], out["data"]["observed"], out["data"]["by"]) != (
                "reconciled-1", "rolled-back", "step-2"):
            raise Failed(f"finalize: exit {rc}: {err[-300:]}")
        t = self.takeovers()
        if len(t) != 1 or t[0]["data"]["stale_guard"]["owner"]["pid"] != p.pid:
            raise Failed(f"the guard was not taken over from the dead recoverer: {t}")
        self.ok("`finalize` took the dead recoverer's guard over (journalled), reconciled the upgrade's intent and "
                "released the lock; it submitted no further upgrade or rollback")
        self.at_rest(2, 3, "F")

    # --- G and K: no revision evidence (the recorded shim) ---------------------------------------------------------------
    def refused_disposition(self, step, revision, tag):
        seen = []
        for attempt in (1, 2, 3):                          # repeated observations change nothing
            saved, plan = self.plan("plan-dispose")
            hit = [p for p in plan["problems"] if f"step {step}: Helm executed and revision {revision} does not exist"
                   in p]
            if not hit:
                raise Failed(f"plan-dispose does not refuse the missing revision (attempt {attempt}): {plan['problems']}")
            seen.append(plan["history"])
            time.sleep(5)
        rc, _out, _err = self.apply("dispose", saved)
        st = self.status(f"{tag}-status-refused.json")
        if rc == 0 or not st["lock_held"] or seen[0] != seen[-1]:
            raise Failed(f"a disposition without revision evidence was not refused: exit {rc}, {st}")
        self.ok(f"plan-dispose refuses step {step} three times (revision {revision} does not exist), dispose is "
                "refused, the lock stays", "a synthetic refusal test: the shim is recorded in shim.json")

    def G(self):
        self.guard()
        open(os.path.join(self.flags, "fail-upgrade"), "w").close()
        saved, plan = self.plan("plan-upgrade")
        if plan["problems"]:
            raise Failed(f"the upgrade plan has problems: {plan['problems'][:4]}")
        rc, _out, err = self.apply("apply-upgrade", saved)
        if rc == 0 or "is unresolved" not in err:
            raise Failed(f"the shimmed upgrade did not end unresolved: exit {rc}: {err[-300:]}")
        self.expect_outcome("unresolved", "1", "helm-upgrade")
        if [h["revision"] for h in self.status()["history"]] != [1]:
            raise Failed("a revision exists although the shim never contacted the cluster")
        self.refused_disposition("1", 2, "G")
        _saved, again = self.plan("plan-upgrade")
        if not any("already submitted" in p for p in again["problems"]) or self.launches() != 1:
            raise Failed(f"a second submission is not prevented: {again['problems'][:4]}")
        self.ok("the operation cannot be submitted again; one mutating command was released, no revision written")
        self.snapshot("G-cluster-kept-state.json")

    def K(self):
        self.guard()
        self.reviewed("release.py", self.demo, ["plan-upgrade"], ["apply-upgrade"],
                      "the upgrade applied (through the shim, by the real Helm)")
        self.wait_rolled_out()
        open(os.path.join(self.flags, "fail-rollback"), "w").close()
        saved, plan = self.plan("plan-rollback")
        if plan["problems"]:
            raise Failed(f"the rollback plan has problems: {plan['problems'][:4]}")
        rc, _out, err = self.apply("apply-rollback", saved)
        if rc == 0 or "is unresolved" not in err:
            raise Failed(f"the shimmed rollback did not end unresolved: exit {rc}: {err[-300:]}")
        self.expect_outcome("unresolved", "2", "helm-rollback")
        self.refused_disposition("2", 3, "K")
        _saved, again = self.plan("plan-rollback")
        if not again["problems"] or self.launches() != 2 or len(self.status()["history"]) != 2:
            raise Failed(f"another rollback is not prevented: {again['problems'][:4]}")
        self.ok("no further rollback is planned; two mutating commands were released, two revisions exist")
        self.snapshot("K-cluster-kept-state.json")


def C_unresolved(record_path):
    import common_records as C
    with open(record_path) as fh:
        doc = json.load(fh)
    doc["evidence_dir"] = os.path.join(os.path.dirname(record_path), doc["evidence_dir"])
    return C.unresolved_intents(doc)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    for name in ("--work", "--kubeconfig", "--context", "--images", "--new-statistical", "--helm", "--secretmeta",
                 "--from-commit"):
        ap.add_argument(name, required=True)
    ap.add_argument("--replicas", type=int, default=2)
    ap.add_argument("--helm-timeout", default="5m")
    ap.add_argument("--scenario", required=True, choices=sorted(VARIANTS))
    ap.add_argument("stages", nargs="+")
    a = ap.parse_args(argv)
    a.query_api = ""
    s = Scenarios(a)
    rc = 0
    try:
        for stage in a.stages:
            s.log(f"stage {stage}")
            if stage != "install" and stage != a.scenario:
                raise Failed(f"stage {stage} does not belong to scenario {a.scenario}")
            getattr(s, stage)()
            if stage != "install":
                s.no_helm_output()                         # after EVERY scenario
    except Failed as e:
        s.log(f"FAIL: {e}")
        rc = 1
    finally:
        try:
            problems = s.reap()                            # no helper process and no Helm group is left behind
        except Exception as e:
            problems = [f"the cleanup itself failed ({type(e).__name__})"]
        for problem in problems:
            s.log(f"FAIL: cleanup: {problem}")
        if not problems:
            s.ok("cleanup: the helper processes ended, every recorded Helm group proven empty, no process of this "
                 "run left", f"{len(s.known)} recorded group(s)")
        s.write("checks.json", s.checks)
    return 1 if problems else rc


if __name__ == "__main__":
    sys.exit(main())
