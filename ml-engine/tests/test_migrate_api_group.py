"""hack/migrate-api-group.py: the legacy → autoscaling.devkuban.com hand-over (B5a; Codex task-08 r23–r26).

The tool runs as a subprocess against a stub kubectl ($KUBECTL) that keeps a small API state in a JSON file: objects
carry UIDs, generations and resourceVersions; patches, scales and deletes enforce their preconditions as the API server
does; every call is logged. Hooks change the cluster in the middle of a run, so the tests check both the outcome and
that nothing is written unless it is bound to the object that was checked."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
TOOL = ROOT / "hack" / "migrate-api-group.py"
LEGACY = "predictiveautoscalers.autoscaler.example.com"
NEW = "predictiveautoscalers.autoscaling.devkuban.com"

STUB = r'''
import json, os, re, sys
state_path = os.environ["STUB_STATE"]
state = json.load(open(state_path))
args = sys.argv[1:]
if args[:1] == ["--context"]:
    args = args[2:]
stdin = sys.stdin.read() if args[:3] == ["create", "-f", "-"] else ""
body = json.load(open(args[args.index("-f") + 1])) if args[:2] == ["delete", "--raw"] else None
open(os.environ["STUB_LOG"], "a").write(json.dumps({"args": args, "body": body}) + "\n")
objs = state["objects"]


def save():
    json.dump(state, open(state_path, "w"))


def opt(flag):
    for a in args:
        if a.startswith(flag + "="):
            return a.split("=", 1)[1]
    return args[args.index(flag) + 1] if flag in args else None


def out(obj):
    print(json.dumps(obj)); save(); sys.exit(0)


def fail(msg):
    print(msg, file=sys.stderr); save(); sys.exit(1)


def bump(o):
    o["metadata"]["resourceVersion"] = str(int(o["metadata"].get("resourceVersion", "1")) + 1)


def hook(name):
    """Fire a once-only hook named in STUB_HOOKS (comma-separated)."""
    if name in os.environ.get("STUB_HOOKS", "").split(",") and name not in state.setdefault("fired", []):
        state["fired"].append(name)
        return True
    return False


verb = args[0]
if verb == "get" and args[1] == "%(legacy)s" and not state.get("legacy_crd", True):
    fail('error: the server doesn\'t have a resource type "predictiveautoscalers"')
if verb == "get":
    res = args[1]
    if "-A" in args:
        out({"items": [o for k, o in sorted(objs.items()) if k.split("|")[0] == res]})
    ns = opt("-n")
    if res == "pods":
        if state.get("scaled") and state.get("pod_gets_after_scale", 0) >= 1 and hook("respawn"):
            objs["pods|" + ns + "|legacy-operator-9"] = {"metadata": {"name": "legacy-operator-9", "namespace": ns,
                                                                    "labels": {"app": "predictive-operator"}}}
        if state.get("scaled"):
            state["pod_gets_after_scale"] = state.get("pod_gets_after_scale", 0) + 1
        want = dict(kv.split("=") for kv in opt("-l").split(","))
        out({"items": [o for k, o in objs.items() if k.startswith("pods|" + ns + "|")
                       and all(o["metadata"].get("labels", {}).get(a) == b for a, b in want.items())]})
    key = "|".join([res, ns, args[4]])            # get RES -n NS NAME
    if res == "%(new)s" and hook("recreate-operator-during-verify"):
        objs["deployment|ml-engine|predictive-operator"]["metadata"].update(uid="uid-operator-2", resourceVersion="7")
    if key not in objs:
        fail('Error from server (NotFound): %%s "%%s" not found' %% (res, args[4]))
    out(objs[key])
if verb == "create":
    o = json.loads(stdin)
    key = "|".join(["predictiveautoscalers." + o["apiVersion"].split("/")[0], o["metadata"]["namespace"], o["metadata"]["name"]])
    if key in objs:
        fail("Error from server (AlreadyExists): already exists")
    o["metadata"].update(uid="uid-new-" + o["metadata"]["name"], generation=1, resourceVersion="1")
    objs[key] = o
    out(o)
if verb == "patch":
    key = "|".join([args[1], opt("-n"), args[4]])
    o, patch = objs[key], json.loads(opt("-p"))
    if hook("bump-on-patch"):
        bump(o)                                      # a concurrent status update
    pm = patch.get("metadata", {})
    if "uid" in pm and pm["uid"] != o["metadata"]["uid"]:
        fail("Error from server (Conflict): Precondition failed: UID in precondition does not match")
    if "resourceVersion" in pm and pm["resourceVersion"] != o["metadata"]["resourceVersion"]:
        fail("Error from server (Conflict): Operation cannot be fulfilled: the object has been modified")
    o["spec"].update(patch["spec"])
    o["metadata"]["generation"] += 1
    bump(o)
    if hook("recreate-new-after-patch"):             # deleted and recreated right after the activation patch
        o["metadata"]["uid"] = "uid-recreated-new"
    if hook("retarget-new-after-patch"):
        o["spec"]["targetDeployment"]["name"] = "other"
    if os.environ.get("STUB_REFLECT_MODE"):
        st = o.setdefault("status", {})
        st.update(mode=o["spec"]["mode"], observedGeneration=o["metadata"]["generation"], conflicts=[])
        st["conditions"] = [c for c in st.get("conditions", []) if c["type"] != "ConflictDetected"] + [
            {"type": "ConflictDetected", "status": "False", "reason": "NoConflict", "message": "none",
             "observedGeneration": o["metadata"]["generation"]}]
    out(o)
if verb == "scale":
    ns, name = opt("-n"), args[4]
    o = objs["|".join(["deployment", ns, name])]
    if hook("bump-before-scale"):
        bump(o)                                      # the Deployment changed after the tool read it
    if opt("--resource-version") != o["metadata"]["resourceVersion"]:
        fail("Error from server (Conflict): the object has been modified")
    o["spec"]["replicas"] = 0
    bump(o)
    state["scaled"] = True
    if not os.environ.get("STUB_PODS_LINGER"):
        for k in [k for k in objs if k.startswith("pods|" + ns + "|legacy-operator")]:
            del objs[k]
    if hook("retarget-on-scale"):                    # someone retargets a replacement after it was verified
        r = objs["%(new)s|shop|web-pa"]
        r["spec"]["targetDeployment"]["name"] = "other"
        r["metadata"]["generation"] += 1
        r["status"]["observedGeneration"] = r["metadata"]["generation"]
        for c in r["status"]["conditions"]:
            c["observedGeneration"] = r["metadata"]["generation"]
        bump(r)
    if hook("recreate-on-scale"):                    # someone deletes and recreates a legacy autoscaler
        objs["%(legacy)s|shop|web-pa"]["metadata"].update(uid="uid-recreated", resourceVersion="1")
    out({})
if verb == "delete" and args[1] == "--raw":
    m = re.match(r"/apis/([^/]+)/v1alpha1/namespaces/([^/]+)/predictiveautoscalers/([^/]+)$", args[2])
    key = "|".join(["predictiveautoscalers." + m.group(1), m.group(2), m.group(3)])
    if key not in objs:
        fail("Error from server (NotFound): not found")
    o = objs[key]
    if hook("bump-before-delete"):
        bump(o)                                      # a write between the check and the delete
    pre = body.get("preconditions", {})
    if pre.get("uid") not in (None, o["metadata"]["uid"]) or pre.get("resourceVersion") not in (None, o["metadata"]["resourceVersion"]):
        fail("Error from server (Conflict): Precondition failed: the object changed")
    if o["metadata"].get("finalizers"):              # accepted, but held until the finalizers are removed
        o["metadata"]["deletionTimestamp"] = "2026-10-10T03:00:00Z"
        bump(o)
        out({})
    del objs[key]
    # Changes made while the tool is still deleting (the first delete is ml-engine/api-pa, then shop/web-pa):
    if hook("retarget-after-first-delete"):
        r = objs["%(new)s|shop|web-pa"]
        r["spec"]["targetDeployment"]["name"] = "other"
        bump(r)
    if hook("edit-legacy-after-first-delete"):       # same UID, new spec: a fresh resourceVersion would pass
        old = objs["%(legacy)s|shop|web-pa"]
        old["spec"]["maxReplicas"] = 3
        old["metadata"]["generation"] += 1
        bump(old)
    out({})
fail("stub: unsupported " + " ".join(args))
''' % {"legacy": LEGACY, "new": NEW}


def k(res, ns, name):
    return f"{res}|{ns}|{name}"


def legacy_pa(name, ns, target_ns=None, target="web", uid=None, **meta):
    return {"apiVersion": "autoscaler.example.com/v1alpha1", "kind": "PredictiveAutoscaler",
            "metadata": {"name": name, "namespace": ns, "uid": uid or f"uid-old-{ns}-{name}", "resourceVersion": "42",
                         "generation": 7, "creationTimestamp": "2026-10-05T06:40:00Z",
                         "managedFields": [{"manager": "kubectl"}],
                         "annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{}", "team": "shop",
                                         **meta.pop("annotations", {})}, **meta},
            "spec": {"targetDeployment": {"name": target, "namespace": target_ns or ns}, "minReplicas": 1, "maxReplicas": 12,
                     "metrics": {"requests": {"enabled": True, "targetRPS": 10}},
                     "prediction": {"horizonMinutes": 60, "leadTimeMinutes": 20}},
            "status": {"currentReplicas": 3}}


def operator_deployment(**meta):
    return {"metadata": {"name": "predictive-operator", "namespace": "ml-engine", "uid": "uid-operator",
                         "resourceVersion": "100", **meta},
            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "predictive-operator"}}}}


def operator_pod(i=0):
    return {"metadata": {"name": f"legacy-operator-{i}", "namespace": "ml-engine", "labels": {"app": "predictive-operator"}}}


def base_objects(**extra):
    return {k(LEGACY, "shop", "web-pa"): legacy_pa("web-pa", "shop"),
            k(LEGACY, "ml-engine", "api-pa"): legacy_pa("api-pa", "ml-engine", target_ns="shop", target="api"),
            k("deployment", "ml-engine", "predictive-operator"): operator_deployment(),
            k("pods", "ml-engine", "legacy-operator-0"): operator_pod(), **extra}


class Cluster:
    def __init__(self, tmp_path, objects, legacy_crd=True):
        self.state = tmp_path / "state.json"
        self.log = tmp_path / "calls.log"
        self.inventory = tmp_path / "inventory.json"
        stub = tmp_path / "kubectl"
        stub.write_text(f"#!{sys.executable}\n" + STUB)
        stub.chmod(0o755)
        self.stub = stub
        self.state.write_text(json.dumps({"legacy_crd": legacy_crd, "objects": objects}))

    def run(self, *args, **env):
        e = {**os.environ, "KUBECTL": str(self.stub), "STUB_STATE": str(self.state), "STUB_LOG": str(self.log),
             **{k: str(v) for k, v in env.items()}}
        return subprocess.run([sys.executable, str(TOOL), "--timeout", "1", "--poll", "0.1", *args], capture_output=True,
                              text=True, env=e, timeout=60)

    def plan(self, *extra):
        return self.run("plan", "--legacy-operator", "ml-engine/predictive-operator", "--out", str(self.inventory), *extra)

    def step(self, cmd, *extra, **env):
        return self.run(cmd, "--inventory", str(self.inventory), *extra, **env)

    @property
    def objects(self):
        return json.loads(self.state.read_text())["objects"]

    def edit(self, key, fn):
        s = json.loads(self.state.read_text())
        fn(s["objects"][key])
        self.state.write_text(json.dumps(s))

    def put(self, key, obj):
        s = json.loads(self.state.read_text())
        s["objects"][key] = obj
        self.state.write_text(json.dumps(s))

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def writes(self):
        return [c for c in self.calls() if c["args"][0] in ("create", "patch", "delete", "scale")]

    def verbs(self):
        return [c["args"][0] for c in self.writes()]


def reconciled(o, forecast="True", telemetry="True", conflicts=None):
    """What the new operator would report for a replacement in Recommend mode next to its legacy PA."""
    gen = o["metadata"]["generation"]
    src_ns, src_name = o["metadata"]["annotations"]["autoscaling.devkuban.com/migrated-from"].split("/")
    if conflicts is None:
        conflicts = [{"group": "autoscaler.example.com", "kind": "PredictiveAutoscaler", "namespace": src_ns,
                      "name": src_name, "reason": "LegacyAPIGroup"}]

    def cond(typ, status, reason):
        return {"type": typ, "status": status, "reason": reason, "observedGeneration": gen, "message": ""}
    o["status"] = {"observedGeneration": gen, "mode": "Recommend", "calculatedReplicas": 4, "conflicts": conflicts,
                   "conditions": [cond("Ready", "True", "ReconcileCompleted"), cond("TelemetryAvailable", telemetry, "Measured"),
                                  cond("ForecastAvailable", forecast, "Used"),
                                  cond("ConflictDetected", "True" if conflicts else "False", "ReplicaWriter")]}


@pytest.fixture
def cluster(tmp_path):
    return Cluster(tmp_path, base_objects())


def ready(cluster):
    """plan → prepare → the operator reconciles both replacements."""
    assert cluster.plan().returncode == 0
    assert cluster.step("prepare").returncode == 0
    for name in ("web-pa", "api-pa"):
        cluster.edit(k(NEW, "shop", name), reconciled)


# --- plan ----------------------------------------------------------------------------------------------------------

def test_plan_records_the_inventory_and_writes_nothing(cluster):
    p = cluster.plan()
    assert p.returncode == 0, p.stdout + p.stderr
    inv = json.loads(cluster.inventory.read_text())
    assert inv["legacy_operators"] == [{"namespace": "ml-engine", "name": "predictive-operator", "uid": "uid-operator"}]
    by_src = {(e["source"]["namespace"], e["source"]["name"]): e for e in inv["autoscalers"]}
    web = by_src[("shop", "web-pa")]
    assert web["source"]["uid"] == "uid-old-shop-web-pa" and web["source"]["generation"] == 7
    assert web["source"]["spec"]["maxReplicas"] == 12 and web["replacement"] == {"namespace": "shop", "name": "web-pa"}
    assert by_src[("ml-engine", "api-pa")]["replacement"] == {"namespace": "shop", "name": "api-pa"}   # moved
    assert "moves to namespace shop" in p.stdout and cluster.writes() == []


def test_without_the_legacy_crd_there_is_nothing_to_migrate(tmp_path):
    c = Cluster(tmp_path, {}, legacy_crd=False)
    p = c.plan()
    assert p.returncode == 0 and "nothing to migrate" in p.stdout


@pytest.mark.parametrize("extra", [[], ["--legacy-operator", "ml-engine/missing"]])
def test_plan_needs_every_legacy_operator(cluster, extra):
    p = cluster.run("plan", "--out", str(cluster.inventory), *extra)
    assert p.returncode == 1 and not cluster.inventory.exists()


def test_two_sources_for_one_replacement_need_a_rename(tmp_path):
    objs = base_objects(**{k(LEGACY, "ml-engine", "web-pa"): legacy_pa("web-pa", "ml-engine", target_ns="shop", target="web2")})
    c = Cluster(tmp_path, objs)
    p = c.plan()
    assert p.returncode == 1 and "would both become shop/web-pa" in p.stdout and not c.inventory.exists()
    p = c.plan("--rename", "ml-engine/web-pa=web2-pa")
    assert p.returncode == 0, p.stdout + p.stderr
    reps = {e["replacement"]["name"] for e in json.loads(c.inventory.read_text())["autoscalers"]}
    assert reps == {"web-pa", "web2-pa", "api-pa"}


def test_plan_refuses_an_unrelated_namesake(tmp_path):
    c = Cluster(tmp_path, base_objects(**{k(NEW, "shop", "web-pa"): {
        "metadata": {"name": "web-pa", "namespace": "shop", "uid": "someone-else", "generation": 1}, "spec": {"mode": "Active"}}}))
    p = c.plan()
    assert p.returncode == 1 and "COLLISION" in p.stdout and not c.inventory.exists()


# --- prepare -------------------------------------------------------------------------------------------------------

def test_prepare_creates_bound_recommend_replacements(tmp_path):
    marked = legacy_pa("web-pa", "shop", labels={"argocd.argoproj.io/instance": "shop", "app.kubernetes.io/instance": "x",
                                                  "app.kubernetes.io/managed-by": "Helm", "tier": "web"},
                       annotations={"argocd.argoproj.io/tracking-id": "shop:x"})
    c = Cluster(tmp_path, base_objects(**{k(LEGACY, "shop", "web-pa"): marked}))
    assert c.plan().returncode == 0
    p = c.step("prepare")
    assert p.returncode == 0, p.stdout + p.stderr
    new = c.objects[k(NEW, "shop", "web-pa")]
    assert new["spec"]["mode"] == "Recommend" and new["spec"]["prediction"]["leadTimeMinutes"] == 20
    ann = new["metadata"]["annotations"]
    assert ann["autoscaling.devkuban.com/migrated-from"] == "shop/web-pa"
    assert ann["autoscaling.devkuban.com/migrated-from-uid"] == "uid-old-shop-web-pa"
    assert "kubectl.kubernetes.io/last-applied-configuration" not in ann and "argocd.argoproj.io/tracking-id" not in ann
    assert new["metadata"]["labels"] == {"tier": "web"}                    # no (possible) GitOps ownership copied
    assert "creationTimestamp" not in new["metadata"] and "managedFields" not in new["metadata"]
    assert new["metadata"]["resourceVersion"] == "1" and "status" not in new  # the stub's own, not the legacy "42"
    assert c.objects[k(NEW, "shop", "api-pa")]["spec"]["targetDeployment"] == {"name": "api", "namespace": "shop"}
    assert c.verbs() == ["create", "create"]
    assert c.step("prepare").returncode == 0 and c.verbs() == ["create", "create"], "prepare is idempotent"


@pytest.mark.parametrize("change", ["added", "replaced", "edited", "operator_replaced"])
def test_prepare_refuses_when_the_cluster_changed_since_plan(cluster, change):
    assert cluster.plan().returncode == 0
    if change == "added":
        cluster.put(k(LEGACY, "shop", "late"), legacy_pa("late", "shop", target="late"))
    elif change == "replaced":
        cluster.edit(k(LEGACY, "shop", "web-pa"), lambda o: o["metadata"].update(uid="uid-other"))
    elif change == "edited":
        cluster.edit(k(LEGACY, "shop", "web-pa"), lambda o: (o["spec"].update(maxReplicas=6), o["metadata"].update(generation=8)))
    else:
        cluster.edit(k("deployment", "ml-engine", "predictive-operator"), lambda o: o["metadata"].update(uid="uid-new-op"))
    p = cluster.step("prepare")
    assert p.returncode == 1 and "no longer matches the inventory" in p.stderr and cluster.writes() == []


def test_prepare_refuses_a_namesake_created_after_plan(cluster):
    assert cluster.plan().returncode == 0
    cluster.put(k(NEW, "shop", "web-pa"), {"metadata": {"name": "web-pa", "namespace": "shop", "uid": "x", "generation": 1},
                                           "spec": {"mode": "Active"}})
    p = cluster.step("prepare")
    assert p.returncode == 1 and "is not the replacement of shop/web-pa" in p.stderr


# --- verify --------------------------------------------------------------------------------------------------------

def test_verify_passes_only_when_every_replacement_is_ready(cluster):
    assert cluster.plan().returncode == 0
    assert cluster.step("verify").returncode == 1                       # nothing prepared
    assert cluster.step("prepare").returncode == 0
    for name in ("web-pa", "api-pa"):
        cluster.edit(k(NEW, "shop", name), reconciled)
    p = cluster.step("verify")
    assert p.returncode == 0, p.stdout + p.stderr
    assert "all 2 replacements ready" in p.stdout and cluster.verbs() == ["create", "create"]


def _hpa(o):
    o["status"]["conflicts"].append({"group": "autoscaling", "kind": "HorizontalPodAutoscaler", "namespace": "shop", "name": "h"})


def _foreign_legacy(o):
    o["status"]["conflicts"] = [{"group": "autoscaler.example.com", "kind": "PredictiveAutoscaler", "namespace": "x", "name": "y",
                                 "reason": "LegacyAPIGroup"}]


def _condition(typ, **upd):
    return lambda o: next(c for c in o["status"]["conditions"] if c["type"] == typ).update(**upd)


@pytest.mark.parametrize("breakage, message", [
    (lambda o: o["status"].update(observedGeneration=0), "has not observed generation"),
    (_condition("Ready", status="False", reason="InvalidMetricQuery"), "Ready is False"),
    (_condition("TelemetryAvailable", status="False", reason="MetricsUnavailable"), "TelemetryAvailable is False"),
    (_condition("TelemetryAvailable", observedGeneration=0), "TelemetryAvailable is stale"),
    (_condition("ForecastAvailable", status="False"), "ForecastAvailable is False"),
    (_condition("ConflictDetected", observedGeneration=0), "ConflictDetected is stale"),
    (_condition("ConflictDetected", status="Unknown"), "ConflictDetected is Unknown"),
    (_hpa, "another replica writer on the target: autoscaling HorizontalPodAutoscaler shop/h"),
    (_foreign_legacy, "another replica writer on the target: autoscaler.example.com PredictiveAutoscaler x/y"),
    (lambda o: o["status"].pop("conflicts"), "status.conflicts is missing"),
    (lambda o: o["spec"].update(maxReplicas=6), "maxReplicas: legacy 12, replacement 6"),
    (lambda o: o["spec"].update(mode="Active"), "want Recommend"),
    (lambda o: o["status"].update(mode="Active"), "want Recommend"),
    (lambda o: o["metadata"]["annotations"].pop("autoscaling.devkuban.com/migrated-from-uid"), "not the migrated replacement"),
    (lambda o: o["metadata"]["annotations"].update({"autoscaling.devkuban.com/migrated-from-uid": "uid-other"}),
     "not the migrated replacement"),
    (lambda o: o["spec"]["targetDeployment"].update(name="other"), "differs from the recorded target"),
    (lambda o: o["status"].pop("calculatedReplicas"), "no recommendation"),
])
def test_verify_names_what_is_not_ready(cluster, breakage, message):
    ready(cluster)
    cluster.edit(k(NEW, "shop", "web-pa"), breakage)
    p = cluster.step("verify")
    assert p.returncode == 1 and message in p.stdout, p.stdout


def test_allow_reactive_waives_only_the_forecast(cluster):
    ready(cluster)
    cluster.edit(k(NEW, "shop", "web-pa"), _condition("ForecastAvailable", status="False"))
    assert cluster.step("verify", "--allow-reactive").returncode == 0
    cluster.edit(k(NEW, "shop", "web-pa"), _condition("TelemetryAvailable", status="False"))
    p = cluster.step("verify", "--allow-reactive")
    assert p.returncode == 1 and "TelemetryAvailable is False" in p.stdout


# --- cutover -------------------------------------------------------------------------------------------------------

def test_cutover_changes_nothing_until_every_replacement_verifies(cluster):
    assert cluster.plan().returncode == 0
    assert cluster.step("prepare").returncode == 0
    cluster.edit(k(NEW, "shop", "web-pa"), reconciled)                  # api-pa not reconciled yet
    p = cluster.step("cutover")
    assert p.returncode == 1 and "1 of 2 replacements are not ready" in p.stderr
    assert cluster.verbs() == ["create", "create"]


def test_cutover_stops_the_operator_then_deletes_with_preconditions(cluster):
    ready(cluster)
    p = cluster.step("cutover")
    assert p.returncode == 0, p.stdout + p.stderr
    assert cluster.verbs() == ["create", "create", "scale", "delete", "delete"]
    scale = next(c for c in cluster.writes() if c["args"][0] == "scale")
    assert "--resource-version=100" in scale["args"]
    for d in [c for c in cluster.writes() if c["args"][0] == "delete"]:
        assert d["args"][1] == "--raw" and d["body"]["preconditions"]["uid"].startswith("uid-old-")
        assert d["body"]["preconditions"]["resourceVersion"] == "42"
    assert not [x for x in cluster.objects if x.startswith(LEGACY)]
    again = cluster.step("cutover")                                     # run again: nothing left to do
    assert again.returncode == 0 and "already deleted" in again.stdout
    assert cluster.verbs() == ["create", "create", "scale", "delete", "delete"]


@pytest.mark.parametrize("where, marker", [
    ("legacy_pa", {"labels": {"kustomize.toolkit.fluxcd.io/name": "apps"}}),
    ("legacy_pa", {"annotations": {"argocd.argoproj.io/tracking-id": "apps:x"}}),
    ("legacy_pa", {"labels": {"app.kubernetes.io/instance": "apps"}}),                # Argo label tracking, or Helm
    ("operator", {"labels": {"app.kubernetes.io/instance": "predictive-autoscaler"}}),
    ("operator", {"labels": {"helm.toolkit.fluxcd.io/name": "pa"}}),
])
def test_cutover_refuses_possible_gitops_ownership_until_acknowledged(tmp_path, where, marker):
    objs = base_objects()
    if where == "legacy_pa":
        objs[k(LEGACY, "shop", "web-pa")] = legacy_pa("web-pa", "shop", **marker)
    else:
        objs[k("deployment", "ml-engine", "predictive-operator")] = operator_deployment(**marker)
    c = Cluster(tmp_path, objs)
    ready(c)
    p = c.step("cutover")
    assert p.returncode == 1 and "may be restored by GitOps" in p.stderr, p.stdout + p.stderr
    assert c.verbs() == ["create", "create"]
    assert c.step("cutover", "--gitops-handled").returncode == 0


def test_cutover_deletes_nothing_while_a_legacy_operator_pod_remains(cluster):
    ready(cluster)
    p = cluster.step("cutover", STUB_PODS_LINGER=1)
    assert p.returncode == 1 and "no legacy PA was deleted" in p.stderr
    assert cluster.verbs() == ["create", "create", "scale"]


def test_cutover_stops_when_a_legacy_operator_pod_reappears(cluster):
    ready(cluster)
    p = cluster.step("cutover", STUB_HOOKS="respawn")
    assert p.returncode == 1 and "reappeared" in p.stderr, p.stdout + p.stderr
    assert "delete" not in cluster.verbs()


def test_cutover_refuses_a_stale_operator_scale(cluster):
    ready(cluster)
    p = cluster.step("cutover", STUB_HOOKS="bump-before-scale")
    assert p.returncode == 1 and "changed while being stopped" in p.stderr, p.stdout + p.stderr
    assert cluster.objects[k("deployment", "ml-engine", "predictive-operator")]["spec"]["replicas"] == 1
    assert "delete" not in cluster.verbs()


@pytest.mark.parametrize("hook, message", [
    ("retarget-on-scale", "differs from the recorded target"),      # a replacement changed after the first verify
    ("recreate-on-scale", "was replaced"),                          # a legacy autoscaler recreated under the same name
])
def test_cutover_verifies_everything_again_after_the_stop(cluster, hook, message):
    ready(cluster)
    p = cluster.step("cutover", STUB_HOOKS=hook)
    assert p.returncode == 1 and message in p.stdout + p.stderr, p.stdout + p.stderr
    assert "delete" not in cluster.verbs()


@pytest.mark.parametrize("hook, message", [
    ("retarget-after-first-delete", "the replacement of shop/web-pa is no longer ready"),
    ("edit-legacy-after-first-delete", "is no longer the recorded object"),
])
def test_each_delete_rechecks_its_pair(cluster, hook, message):
    ready(cluster)
    p = cluster.step("cutover", STUB_HOOKS=hook)
    assert p.returncode == 1 and message in p.stderr, p.stdout + p.stderr
    assert [x for x in cluster.objects if x.startswith(LEGACY)] == [f"{LEGACY}|shop|web-pa"]   # only the first went


def test_cutover_refuses_an_operator_recreated_before_its_scale(cluster):
    ready(cluster)
    p = cluster.step("cutover", STUB_HOOKS="recreate-operator-during-verify")
    assert p.returncode == 1 and "was replaced" in p.stderr, p.stdout + p.stderr
    assert "scale" not in cluster.verbs()


def test_cutover_waits_until_a_deleted_object_is_gone(tmp_path):
    held = legacy_pa("api-pa", "ml-engine", target_ns="shop", target="api", finalizers=["example.com/hold"])
    c = Cluster(tmp_path, base_objects(**{k(LEGACY, "ml-engine", "api-pa"): held}))
    ready(c)
    p = c.step("cutover")
    assert p.returncode == 1 and "the deletion was accepted but the object still exists" in p.stderr, p.stdout + p.stderr
    assert k(LEGACY, "shop", "web-pa") in c.objects                     # the next one is not deleted
    assert "deleted predictiveautoscalers" not in p.stdout


def test_a_legacy_autoscaler_changed_after_its_check_is_not_deleted(cluster):
    ready(cluster)
    p = cluster.step("cutover", STUB_HOOKS="bump-before-delete")
    assert p.returncode == 1 and "changed after it was checked; not deleted" in p.stderr, p.stdout + p.stderr
    assert len([x for x in cluster.objects if x.startswith(LEGACY)]) == 2


# --- activate ------------------------------------------------------------------------------------------------------

def test_activate_refuses_while_a_legacy_autoscaler_targets_the_deployment(cluster):
    ready(cluster)
    p = cluster.step("activate", "shop/web-pa", STUB_REFLECT_MODE=1)
    assert p.returncode == 1 and "still targets Deployment shop/web" in p.stderr
    assert "patch" not in cluster.verbs()


def _after_cutover(cluster):
    ready(cluster)
    assert cluster.step("cutover").returncode == 0
    for name in ("web-pa", "api-pa"):                                   # the operator's next reconcile
        cluster.edit(k(NEW, "shop", name), lambda o: reconciled(o, conflicts=[]))


def test_activate_binds_the_patch_to_the_checked_object_and_retries_a_concurrent_change(cluster):
    _after_cutover(cluster)
    p = cluster.step("activate", "shop/web-pa", STUB_REFLECT_MODE=1, STUB_HOOKS="bump-on-patch")
    assert p.returncode == 0 and "is Active with no other replica writer" in p.stdout, p.stdout + p.stderr
    patches = [c for c in cluster.writes() if c["args"][0] == "patch"]
    assert len(patches) == 2                                            # the first hit the concurrent change
    sent = json.loads(patches[-1]["args"][patches[-1]["args"].index("-p") + 1])
    assert sent["metadata"]["uid"] == "uid-new-web-pa" and sent["metadata"]["resourceVersion"]
    assert sent["spec"] == {"mode": "Active"}
    assert cluster.objects[k(NEW, "shop", "web-pa")]["spec"]["mode"] == "Active"
    assert cluster.objects[k(NEW, "shop", "api-pa")]["spec"]["mode"] == "Recommend"   # one autoscaler at a time


def test_activate_refuses_an_object_that_is_not_the_migrated_replacement(cluster):
    _after_cutover(cluster)
    cluster.edit(k(NEW, "shop", "web-pa"), lambda o: o["metadata"].update(annotations={}))
    p = cluster.step("activate", "shop/web-pa", STUB_REFLECT_MODE=1)
    assert p.returncode == 1 and "not the migrated replacement" in p.stdout and "patch" not in cluster.verbs()


def test_activate_checks_an_already_active_replacement_like_a_new_one(cluster):
    _after_cutover(cluster)
    cluster.edit(k(NEW, "shop", "web-pa"), lambda o: (o["spec"].update(mode="Active"),
                                                      o["spec"]["targetDeployment"].update(name="other")))
    p = cluster.step("activate", "shop/web-pa", STUB_REFLECT_MODE=1)
    assert p.returncode == 1 and "is Active but not as recorded" in p.stderr, p.stdout + p.stderr


@pytest.mark.parametrize("hook, message", [
    ("recreate-new-after-patch", "deleted or replaced after it was activated"),
    ("retarget-new-after-patch", "changed after it was activated"),
])
def test_activate_confirms_only_the_object_it_patched(cluster, hook, message):
    _after_cutover(cluster)
    p = cluster.step("activate", "shop/web-pa", STUB_REFLECT_MODE=1, STUB_HOOKS=hook)
    assert p.returncode == 1 and message in p.stderr, p.stdout + p.stderr


def test_activate_checks_the_inventory(cluster):
    _after_cutover(cluster)
    cluster.put(k(LEGACY, "shop", "late"), legacy_pa("late", "shop", target="late"))
    p = cluster.step("activate", "shop/web-pa", STUB_REFLECT_MODE=1)
    assert p.returncode == 1 and "no longer matches the inventory" in p.stderr and "patch" not in cluster.verbs()


def test_activate_needs_a_replacement_from_the_inventory(cluster):
    _after_cutover(cluster)
    p = cluster.step("activate", "shop/unknown")
    assert p.returncode == 1 and "is not a replacement in the inventory" in p.stderr


def test_activate_reports_an_operator_that_does_not_confirm(cluster):
    _after_cutover(cluster)
    p = cluster.step("activate", "shop/web-pa")                          # no operator reflecting the change
    assert p.returncode == 1 and "has not reported Active" in p.stderr
