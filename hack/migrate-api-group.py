#!/usr/bin/env python3
"""Move PredictiveAutoscalers from the legacy API group (autoscaler.example.com) to autoscaling.devkuban.com.

A different group is a different CRD, so objects are not converted automatically. The legacy operator has no Recommend
mode: it writes on every reconcile, for every legacy PA it watches. The new operator treats every legacy PA on its
target as another replica writer and refuses to write next to it. This tool walks the hand-over in explicit steps
(docs/upgrading.md), all bound to one inventory recorded by `plan`:

  plan --legacy-operator NS/NAME ... --out FILE
        read-only: records every legacy PA (namespace, name, UID, generation, spec), the replacement each one maps to,
        and every legacy operator Deployment (UID). Refuses two sources that map to one replacement (use --rename) and
        an unrelated object already holding a replacement's name.
  prepare --inventory FILE     create each missing replacement in Recommend mode, with its provenance (source
                               namespace/name and UID); never touches a legacy object
  verify --inventory FILE      check every replacement against its recorded source (see check_replacement)
  cutover --inventory FILE     only when the live legacy PAs and operators still match the inventory and every
                               replacement verifies: refuse on GitOps ownership (unless --gitops-handled), scale every
                               legacy operator to 0 (bound to its resourceVersion), wait until none of their pods
                               remain, verify everything again, then delete each legacy PA with UID and
                               resourceVersion preconditions
  activate --inventory FILE NS/NAME
                               set one replacement to Active (a patch bound to its UID and resourceVersion) once no
                               legacy PA targets its Deployment; wait for the operator to report it

Exit status: 0 done, 1 a check failed (nothing further was changed), 2 usage. kubectl comes from $KUBECTL (default
"kubectl"); --context selects the kube context.
"""

import argparse
import copy
import json
import os
import subprocess
import sys
import tempfile
import time

LEGACY_GROUP = "autoscaler.example.com"
NEW_GROUP = "autoscaling.devkuban.com"
LEGACY = f"predictiveautoscalers.{LEGACY_GROUP}"
NEW = f"predictiveautoscalers.{NEW_GROUP}"
MIGRATED_FROM = f"{NEW_GROUP}/migrated-from"          # "<namespace>/<name>" of the legacy source
MIGRATED_FROM_UID = f"{NEW_GROUP}/migrated-from-uid"  # its UID
INVENTORY_VERSION = 1
# Ownership by a GitOps tool that would restore what cutover stops or deletes. app.kubernetes.io/instance is Argo CD's
# label-tracking key, and Helm sets it too: ambiguous, so it needs the same explicit acknowledgement.
GITOPS_ANNOTATION_PREFIXES = ("argocd.argoproj.io/",)
GITOPS_LABELS = ("argocd.argoproj.io/instance", "kustomize.toolkit.fluxcd.io/name", "helm.toolkit.fluxcd.io/name")
AMBIGUOUS_LABELS = ("app.kubernetes.io/instance",)
NOT_COPIED_LABELS = GITOPS_LABELS + AMBIGUOUS_LABELS + ("app.kubernetes.io/managed-by",)
DROP_ANNOTATIONS = ("kubectl.kubernetes.io/last-applied-configuration",)


class CheckFailed(Exception):
    pass


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


class Kube:
    def __init__(self, context=None):
        self.base = [os.environ.get("KUBECTL", "kubectl")] + (["--context", context] if context else [])

    def run(self, *args, stdin=None):
        p = subprocess.run(self.base + list(args), input=stdin, capture_output=True, text=True, timeout=120)
        if p.returncode != 0:
            err = p.stderr.strip() or p.stdout.strip()
            if "NotFound" in err or "not found" in err:
                raise NotFound(err)
            if "Conflict" in err or "has been modified" in err or "Precondition failed" in err:
                raise Conflict(err)
            raise CheckFailed(f"kubectl {' '.join(args[:3])}: {err or 'exit ' + str(p.returncode)}")
        return p.stdout

    def json(self, *args):
        return json.loads(self.run(*args, "-o", "json"))

    def legacy_pas(self):
        try:
            return self.json("get", LEGACY, "-A")["items"]
        except CheckFailed as e:
            if "doesn't have a resource type" in str(e):
                return None          # no legacy CRD: nothing to migrate
            raise

    def get(self, resource, ns, name):
        try:
            return self.json("get", resource, "-n", ns, name)
        except NotFound:
            return None

    def delete_legacy(self, ns, name, uid, resource_version):
        """DELETE with preconditions: the server refuses if the object is not exactly the one inspected."""
        body = {"kind": "DeleteOptions", "apiVersion": "v1",
                "preconditions": {"uid": uid, "resourceVersion": resource_version}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(body, fh)
        try:
            self.run("delete", "--raw", f"/apis/{LEGACY_GROUP}/v1alpha1/namespaces/{ns}/predictiveautoscalers/{name}",
                     "-f", fh.name)
        finally:
            os.unlink(fh.name)


# --- objects -------------------------------------------------------------------------------------------------------

def ref(o):
    return f"{o['metadata']['namespace']}/{o['metadata']['name']}"


def target_of(o):
    td = (o.get("spec") or {}).get("targetDeployment") or {}
    return td.get("namespace") or o["metadata"]["namespace"], td.get("name", "")


def provenance(o):
    ann = o["metadata"].get("annotations") or {}
    return ann.get(MIGRATED_FROM), ann.get(MIGRATED_FROM_UID)


def gitops_markers(o):
    meta = o.get("metadata", {})
    found = [k for k in (meta.get("annotations") or {}) if k.startswith(GITOPS_ANNOTATION_PREFIXES)]
    found += [k for k in (meta.get("labels") or {}) if k in GITOPS_LABELS]
    found += [f"{k} (Argo CD label tracking, or Helm)" for k in (meta.get("labels") or {}) if k in AMBIGUOUS_LABELS]
    return found


def replacement_for(legacy, name):
    """The new-group object for a legacy PA: in its target's namespace (the new operator refuses cross-namespace
    targets), Recommend mode, its provenance, and no server metadata, status or ownership markers."""
    tns, _ = target_of(legacy)
    meta = legacy["metadata"]
    spec = copy.deepcopy(legacy.get("spec") or {})
    spec["mode"] = "Recommend"
    spec.setdefault("targetDeployment", {})["namespace"] = tns
    labels = {k: v for k, v in (meta.get("labels") or {}).items() if k not in NOT_COPIED_LABELS}
    annotations = {k: v for k, v in (meta.get("annotations") or {}).items()
                   if k not in DROP_ANNOTATIONS and not k.startswith(GITOPS_ANNOTATION_PREFIXES)}
    annotations[MIGRATED_FROM] = ref(legacy)
    annotations[MIGRATED_FROM_UID] = meta["uid"]
    out_meta = {"name": name, "namespace": tns, "annotations": annotations}
    if labels:
        out_meta["labels"] = labels
    return {"apiVersion": f"{NEW_GROUP}/v1alpha1", "kind": "PredictiveAutoscaler", "metadata": out_meta, "spec": spec}


def spec_differences(legacy_spec, new_spec, path=""):
    """Every field set in the legacy spec must have the same value in the replacement (the server may add defaults)."""
    diffs = []
    for k, v in legacy_spec.items():
        p = f"{path}.{k}" if path else k
        if p in ("mode", "targetDeployment.namespace"):
            continue
        nv = new_spec.get(k) if isinstance(new_spec, dict) else None
        if isinstance(v, dict) and isinstance(nv, dict):
            diffs += spec_differences(v, nv, p)
        elif v != nv:
            diffs.append(f"{p}: legacy {v!r}, replacement {nv!r}")
    return diffs


def condition_at(o, typ, gen):
    for c in (o.get("status") or {}).get("conditions") or []:
        if c.get("type") == typ:
            return c if c.get("observedGeneration") == gen else {"status": "stale", "reason": "for an older generation"}
    return {"status": "absent", "reason": "no such condition"}


# --- the inventory -------------------------------------------------------------------------------------------------

def load_inventory(path):
    try:
        with open(path) as fh:
            inv = json.load(fh)
    except (OSError, ValueError) as e:
        raise CheckFailed(f"cannot read the inventory {path}: {e}")
    if inv.get("version") != INVENTORY_VERSION or not inv.get("legacy_operators") or "autoscalers" not in inv:
        raise CheckFailed(f"{path} is not a version-{INVENTORY_VERSION} inventory written by `plan`")
    return inv


def entry_by_replacement(inv, ns, name):
    for e in inv["autoscalers"]:
        if (e["replacement"]["namespace"], e["replacement"]["name"]) == (ns, name):
            return e
    raise CheckFailed(f"{ns}/{name} is not a replacement in the inventory")


def check_inventory(kube, inv, allow_deleted=False):
    """The live legacy PAs and operators must be exactly the recorded ones (same UIDs, same spec and generation). With
    allow_deleted, recorded PAs that are already gone are accepted (a cutover that is run again). Returns the live
    legacy objects by (namespace, name)."""
    live = kube.legacy_pas() or []
    live_by_key = {(o["metadata"]["namespace"], o["metadata"]["name"]): o for o in live}
    recorded = {(e["source"]["namespace"], e["source"]["name"]): e["source"] for e in inv["autoscalers"]}
    problems = []
    for key, o in live_by_key.items():
        src = recorded.get(key)
        if src is None:
            problems.append(f"legacy {key[0]}/{key[1]} is not in the inventory (created after plan?)")
        elif o["metadata"]["uid"] != src["uid"]:
            problems.append(f"legacy {key[0]}/{key[1]} was replaced (UID {o['metadata']['uid']}, recorded {src['uid']})")
        elif o["metadata"].get("generation") != src["generation"] or (o.get("spec") or {}) != src["spec"]:
            problems.append(f"legacy {key[0]}/{key[1]} changed since plan (generation {o['metadata'].get('generation')}, "
                            f"recorded {src['generation']})")
    if not allow_deleted:
        problems += [f"legacy {k[0]}/{k[1]} is gone (recorded in the inventory)" for k in recorded if k not in live_by_key]
    for op in inv["legacy_operators"]:
        dep = kube.get("deployment", op["namespace"], op["name"])
        if dep is None:
            problems.append(f"legacy operator deployment/{op['namespace']}/{op['name']} is gone")
        elif dep["metadata"]["uid"] != op["uid"]:
            problems.append(f"legacy operator deployment/{op['namespace']}/{op['name']} was replaced")
    if problems:
        for p in problems:
            print(f"    - {p}")
        raise CheckFailed("the cluster no longer matches the inventory: run plan again")
    return live_by_key


def identity_problems(new, entry):
    """The replacement is the recorded source's migrated object, with the recorded target and configuration."""
    src = entry["source"]
    problems = []
    if provenance(new) != (f"{src['namespace']}/{src['name']}", src["uid"]):
        problems.append(f"not the migrated replacement of {src['namespace']}/{src['name']} (provenance {provenance(new)})")
    if target_of(new) != tuple(entry["target"]):
        problems.append(f"target {target_of(new)} differs from the recorded target {tuple(entry['target'])}")
    return problems + spec_differences(src["spec"], new.get("spec") or {})


def check_replacement(kube, entry, legacy_keys, allow_reactive, legacy_conflicts_ok=True):
    """Problems with one replacement in Recommend mode (empty when ready), and the object as read:
    - it exists, with the provenance of its recorded source (namespace/name and UID);
    - its target and every field the legacy spec set match the recorded source;
    - spec.mode and status.mode are Recommend;
    - the operator observed its current generation: Ready, TelemetryAvailable and (unless allow_reactive)
      ForecastAvailable are True for it, and ConflictDetected is False or names only inventoried legacy PAs;
    - a recommendation is in its status."""
    src, rep = entry["source"], entry["replacement"]
    new = kube.get(NEW, rep["namespace"], rep["name"])
    if new is None:
        return [f"no replacement {NEW}/{rep['namespace']}/{rep['name']} (run prepare)"], None
    problems = identity_problems(new, entry)
    st, gen = new.get("status") or {}, new["metadata"].get("generation")
    if (new.get("spec") or {}).get("mode") != "Recommend" or st.get("mode") != "Recommend":
        problems.append(f"mode is spec {(new.get('spec') or {}).get('mode')!r} / status {st.get('mode')!r}, want Recommend")
    if st.get("observedGeneration") != gen:
        problems.append(f"the operator has not observed generation {gen} yet (status is for {st.get('observedGeneration')})")
    for typ in ["Ready", "TelemetryAvailable"] + ([] if allow_reactive else ["ForecastAvailable"]):
        c = condition_at(new, typ, gen)
        if c.get("status") != "True":
            problems.append(f"{typ} is {c.get('status')} for generation {gen} ({c.get('reason', '')})")
    c = condition_at(new, "ConflictDetected", gen)
    if c.get("status") == "True":
        for w in st.get("conflicts") or [{"kind": "?", "name": "status.conflicts is missing"}]:
            legacy = w.get("group") == LEGACY_GROUP and (w.get("namespace"), w.get("name")) in legacy_keys
            if not (legacy and legacy_conflicts_ok):
                problems.append(f"another replica writer on the target: {w.get('group', '')} {w.get('kind')} "
                                f"{w.get('namespace', '')}/{w.get('name')}")
    elif c.get("status") != "False":
        problems.append(f"ConflictDetected is {c.get('status')} for generation {gen} ({c.get('reason', '')})")
    if st.get("calculatedReplicas") is None:
        problems.append("no recommendation in status yet")
    return problems, new


def verify_all(kube, inv, legacy_by_key, allow_reactive, quiet=False):
    keys = {(e["source"]["namespace"], e["source"]["name"]) for e in inv["autoscalers"]}
    failed = 0
    for e in inv["autoscalers"]:
        if (e["source"]["namespace"], e["source"]["name"]) not in legacy_by_key:
            continue                                   # its source is already deleted (a cutover run again)
        problems, _ = check_replacement(kube, e, keys, allow_reactive)
        failed += bool(problems)
        if not quiet or problems:
            print(f"{'ok' if not problems else 'NOT READY'}: {e['source']['namespace']}/{e['source']['name']} → "
                  f"{e['replacement']['namespace']}/{e['replacement']['name']}")
        for p in problems:
            print(f"    - {p}")
    if failed:
        raise CheckFailed(f"{failed} of {len(inv['autoscalers'])} replacements are not ready")


# --- commands ------------------------------------------------------------------------------------------------------

def cmd_plan(kube, args):
    legacy = kube.legacy_pas()
    if legacy is None:
        print("no legacy CRD (autoscaler.example.com): nothing to migrate")
        return 0
    if not legacy:
        print("no legacy PredictiveAutoscalers remain: remove the legacy operator and CRD (docs/upgrading.md)")
        return 0
    if not args.legacy_operator:
        raise CheckFailed("name every legacy operator Deployment that watches these autoscalers (--legacy-operator "
                          "NS/NAME, repeatable): cutover stops all of them before deleting any legacy autoscaler")
    renames = {}
    for r in args.rename or []:
        src, sep, new = r.partition("=")
        if not sep or "/" not in src or not new:
            raise CheckFailed(f"--rename {r!r}: want NAMESPACE/NAME=NEWNAME")
        renames[src] = new
    unknown = set(renames) - {ref(o) for o in legacy}
    if unknown:
        raise CheckFailed(f"--rename names no legacy autoscaler: {', '.join(sorted(unknown))}")
    entries, seen, problems = [], {}, []
    for o in sorted(legacy, key=ref):
        tns, tname = target_of(o)
        rname = renames.get(ref(o), o["metadata"]["name"])
        if (tns, rname) in seen:
            problems.append(f"{ref(o)} and {seen[(tns, rname)]} would both become {tns}/{rname}: pass --rename")
            continue
        seen[(tns, rname)] = ref(o)
        existing = kube.get(NEW, tns, rname)
        if existing is None:
            state = "missing"
        elif provenance(existing) == (ref(o), o["metadata"]["uid"]):
            state = f"present (mode {(existing.get('spec') or {}).get('mode')})"
        else:
            state = "COLLISION"
            problems.append(f"{NEW}/{tns}/{rname} exists and is not the replacement of {ref(o)}: pass --rename")
        moved = "" if tns == o["metadata"]["namespace"] else f", moves to namespace {tns}"
        print(f"{LEGACY}/{ref(o)} → {NEW}/{tns}/{rname} (Deployment {tns}/{tname}{moved}): replacement {state}")
        for m in gitops_markers(o):
            print(f"    ! GitOps ownership marker {m}: remove it from Git or suspend the sync before cutover")
        entries.append({"source": {"namespace": o["metadata"]["namespace"], "name": o["metadata"]["name"],
                                   "uid": o["metadata"]["uid"], "generation": o["metadata"].get("generation"),
                                   "spec": o.get("spec") or {}},
                        "replacement": {"namespace": tns, "name": rname}, "target": [tns, tname]})
    operators = []
    for op in args.legacy_operator:
        ns, _, name = op.partition("/")
        dep = kube.get("deployment", ns, name) if ns and name else None
        if dep is None:
            problems.append(f"legacy operator deployment/{op} not found")
            continue
        operators.append({"namespace": ns, "name": name, "uid": dep["metadata"]["uid"]})
        print(f"legacy operator deployment/{op}: {(dep.get('spec') or {}).get('replicas')} replicas")
        for m in gitops_markers(dep):
            print(f"    ! GitOps ownership marker {m}: remove it from Git or suspend the sync before cutover")
    if problems:
        for p in problems:
            print(f"    - {p}")
        raise CheckFailed("not planned (nothing written)")
    inv = {"version": INVENTORY_VERSION, "context": args.context, "legacy_operators": operators, "autoscalers": entries}
    with open(args.out, "w") as fh:
        json.dump(inv, fh, indent=2)
    print(f"plan: {len(entries)} autoscalers and {len(operators)} legacy operators recorded in {args.out}")
    return 0


def cmd_prepare(kube, args):
    inv = load_inventory(args.inventory)
    legacy_by_key = check_inventory(kube, inv)
    created = 0
    for e in inv["autoscalers"]:
        src = legacy_by_key[(e["source"]["namespace"], e["source"]["name"])]
        rep = e["replacement"]
        existing = kube.get(NEW, rep["namespace"], rep["name"])
        if existing is not None:
            if provenance(existing) != (ref(src), src["metadata"]["uid"]):
                raise CheckFailed(f"{NEW}/{rep['namespace']}/{rep['name']} exists and is not the replacement of "
                                  f"{ref(src)}: plan again with --rename")
            print(f"exists: {NEW}/{rep['namespace']}/{rep['name']}")
            continue
        kube.run("create", "-f", "-", stdin=json.dumps(replacement_for(src, rep["name"])))
        created += 1
        print(f"created: {NEW}/{rep['namespace']}/{rep['name']} (Recommend) from {LEGACY}/{ref(src)}")
    print(f"prepare: {created} created. Next: train for each new autoscaler, then verify.")
    return 0


def cmd_verify(kube, args):
    inv = load_inventory(args.inventory)
    legacy_by_key = check_inventory(kube, inv)
    verify_all(kube, inv, legacy_by_key, args.allow_reactive)
    print(f"verify: all {len(inv['autoscalers'])} replacements ready")
    return 0


def recorded_operator(kube, op):
    """The legacy operator Deployment, only if it is still the recorded object (same UID)."""
    dep = kube.get("deployment", op["namespace"], op["name"])
    if dep is None:
        raise CheckFailed(f"legacy operator deployment/{op['namespace']}/{op['name']} disappeared; stopped")
    if dep["metadata"]["uid"] != op["uid"]:
        raise CheckFailed(f"legacy operator deployment/{op['namespace']}/{op['name']} was replaced (UID "
                          f"{dep['metadata']['uid']}, recorded {op['uid']}); stopped, run plan again")
    return dep


def selector_string(dep):
    sel = (dep.get("spec") or {}).get("selector") or {}
    if sel.get("matchExpressions") or not sel.get("matchLabels"):
        raise CheckFailed(f"deployment/{ref(dep)} has no plain matchLabels selector; stop it by hand and confirm its "
                          f"pods are gone")
    return ",".join(f"{k}={v}" for k, v in sorted(sel["matchLabels"].items()))


def operator_pods(kube, inv):
    pods = []
    for op in inv["legacy_operators"]:
        dep = recorded_operator(kube, op)
        pods += kube.json("get", "pods", "-n", op["namespace"], "-l", selector_string(dep))["items"]
    return pods


def cmd_cutover(kube, args):
    inv = load_inventory(args.inventory)
    legacy_by_key = check_inventory(kube, inv, allow_deleted=True)
    owned = [(f"{LEGACY}/{ref(o)}", m) for o in legacy_by_key.values() for m in gitops_markers(o)]
    for op in inv["legacy_operators"]:
        dep = recorded_operator(kube, op)
        owned += [(f"deployment/{op['namespace']}/{op['name']}", m) for m in gitops_markers(dep)]
    if owned and not args.gitops_handled:
        for what, marker in owned:
            print(f"    - {what}: {marker}")
        raise CheckFailed("these may be restored by GitOps: remove them from Git or suspend the sync, then pass "
                          "--gitops-handled")
    verify_all(kube, inv, legacy_by_key, args.allow_reactive, quiet=True)      # before anything stops
    for op in inv["legacy_operators"]:
        dep = recorded_operator(kube, op)                  # this exact read is the one the scale is bound to
        selector_string(dep)
        if (dep.get("spec") or {}).get("replicas") != 0:
            try:
                kube.run("scale", "deployment", "-n", op["namespace"], op["name"], "--replicas=0",
                         f"--resource-version={dep['metadata']['resourceVersion']}")
            except Conflict as err:
                raise CheckFailed(f"deployment/{op['namespace']}/{op['name']} changed while being stopped; nothing was "
                                  f"deleted, run cutover again ({err})")
            print(f"scaled deployment/{op['namespace']}/{op['name']} to 0")
    deadline = time.monotonic() + args.timeout
    while True:
        pods = operator_pods(kube, inv)
        if not pods:
            break
        if time.monotonic() >= deadline:
            raise CheckFailed(f"{len(pods)} legacy operator pods still exist after {args.timeout:.0f} s; no legacy PA "
                              f"was deleted")
        time.sleep(args.poll)
    print("no legacy operator pod remains; verifying everything again")
    legacy_by_key = check_inventory(kube, inv, allow_deleted=True)
    verify_all(kube, inv, legacy_by_key, args.allow_reactive, quiet=True)
    keys = {(e["source"]["namespace"], e["source"]["name"]) for e in inv["autoscalers"]}
    for e in inv["autoscalers"]:
        s = e["source"]
        if operator_pods(kube, inv):
            raise CheckFailed("a legacy operator pod reappeared (is something scaling it back up?); stopped")
        live = kube.get(LEGACY, s["namespace"], s["name"])
        if live is None:
            print(f"already deleted: {LEGACY}/{s['namespace']}/{s['name']}")
            continue
        if live["metadata"]["uid"] != s["uid"] or (live.get("spec") or {}) != s["spec"]:
            raise CheckFailed(f"{LEGACY}/{s['namespace']}/{s['name']} is no longer the recorded object; stopped")
        problems, _ = check_replacement(kube, e, keys, args.allow_reactive)
        if problems:
            raise CheckFailed(f"the replacement of {s['namespace']}/{s['name']} is no longer ready: {problems[0]}")
        try:
            kube.delete_legacy(s["namespace"], s["name"], s["uid"], live["metadata"]["resourceVersion"])
        except Conflict as err:
            raise CheckFailed(f"{LEGACY}/{s['namespace']}/{s['name']} changed after it was checked; not deleted ({err})")
        gone_by = time.monotonic() + args.timeout
        while True:
            still = kube.get(LEGACY, s["namespace"], s["name"])
            if still is None or still["metadata"]["uid"] != s["uid"]:
                break
            if time.monotonic() >= gone_by:
                raise CheckFailed(f"{LEGACY}/{s['namespace']}/{s['name']}: the deletion was accepted but the object "
                                  f"still exists after {args.timeout:.0f} s (finalizers: "
                                  f"{still['metadata'].get('finalizers')}); stopped")
            time.sleep(args.poll)
        print(f"deleted {LEGACY}/{s['namespace']}/{s['name']}")
    print("cutover: done. Next: activate each replacement (an explicit decision per autoscaler).")
    return 0


def cmd_activate(kube, args):
    inv = load_inventory(args.inventory)
    ns, _, name = args.target.partition("/")
    entry = entry_by_replacement(inv, ns, name)
    check_inventory(kube, inv, allow_deleted=True)
    target = tuple(entry["target"])
    for o in kube.legacy_pas() or []:
        if target_of(o) == target:
            raise CheckFailed(f"legacy {LEGACY}/{ref(o)} still targets Deployment {target[0]}/{target[1]} (run cutover)")
    pinned = None                                      # the UID of the object that was checked (and patched)
    for _ in range(3):
        new = kube.get(NEW, ns, name)
        if new is not None and (new.get("spec") or {}).get("mode") == "Active":
            problems = identity_problems(new, entry)   # already set (activate run again): same checks of identity
            if problems:
                raise CheckFailed(f"{NEW}/{args.target} is Active but not as recorded: {problems[0]}")
            pinned = new["metadata"]["uid"]
            break
        problems, new = check_replacement(kube, entry, set(), args.allow_reactive, legacy_conflicts_ok=False)
        if problems:
            for p in problems:
                print(f"    - {p}")
            raise CheckFailed(f"{NEW}/{args.target} is not ready to activate")
        patch = {"metadata": {"uid": new["metadata"]["uid"], "resourceVersion": new["metadata"]["resourceVersion"]},
                 "spec": {"mode": "Active"}}
        try:
            kube.run("patch", NEW, "-n", ns, name, "--type", "merge", "-p", json.dumps(patch))
        except Conflict:
            continue                                   # it changed after the check: check again
        pinned = new["metadata"]["uid"]
        print(f"set {NEW}/{args.target} to Active; waiting for the operator")
        break
    else:
        raise CheckFailed(f"{NEW}/{args.target} kept changing between the check and the patch; not activated")
    deadline = time.monotonic() + args.timeout
    while True:
        new = kube.get(NEW, ns, name)
        if new is None or new["metadata"]["uid"] != pinned:
            raise CheckFailed(f"{NEW}/{args.target} was deleted or replaced after it was activated; check it by hand")
        problems = identity_problems(new, entry)
        if problems:
            raise CheckFailed(f"{NEW}/{args.target} changed after it was activated: {problems[0]}")
        st, gen = new.get("status") or {}, new["metadata"].get("generation")
        conflict = condition_at(new, "ConflictDetected", gen)
        if st.get("mode") == "Active" and st.get("observedGeneration") == gen and conflict.get("status") == "False":
            print(f"activate: {NEW}/{args.target} is Active with no other replica writer")
            return 0
        if time.monotonic() >= deadline:
            raise CheckFailed(f"the operator has not reported Active without conflict after {args.timeout:.0f} s "
                              f"(status mode {st.get('mode')}, ConflictDetected {conflict.get('status')})")
        time.sleep(args.poll)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--context")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--poll", type=float, default=5.0)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--legacy-operator", action="append", metavar="NS/DEPLOYMENT")
    p.add_argument("--rename", action="append", metavar="NS/NAME=NEWNAME")
    p.add_argument("--out", required=True, metavar="INVENTORY")
    for cmd in ("prepare", "verify", "cutover", "activate"):
        p = sub.add_parser(cmd)
        p.add_argument("--inventory", required=True)
        if cmd != "prepare":
            p.add_argument("--allow-reactive", action="store_true", help="accept a replacement without a forecast yet")
        if cmd == "cutover":
            p.add_argument("--gitops-handled", action="store_true")
        if cmd == "activate":
            p.add_argument("target", metavar="NS/NAME")
    args = ap.parse_args(argv)
    kube = Kube(args.context)
    try:
        return {"plan": cmd_plan, "prepare": cmd_prepare, "verify": cmd_verify, "cutover": cmd_cutover,
                "activate": cmd_activate}[args.cmd](kube, args)
    except CheckFailed as e:
        print(f"{args.cmd}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
