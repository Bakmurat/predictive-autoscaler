#!/usr/bin/env python3
"""S3/S5a preparation of the evaluation's own objects (STAGING-C28-PROD.md, S3 and S5a).

  prepare.py --record R plan --phase P                     read-only: write that phase's plan file, print its digest
  prepare.py --record R apply --phase P --plan F --plan-digest D   create exactly that phase's reviewed objects
  prepare.py --record R verify                           read-only: identities, fixed replicas, empty faults, no writer

Phases, in order (each a separate reviewed plan; nothing is applied as one unattended start):
  namespaces  the watched and control namespaces, only those that are missing (labelled with the run)
  storage     the retained operator-ledger and statistical-state claims, on the storage owner's approved class
              (its reclaimPolicy must be Retain); the chart then uses them as existingClaim
  values      the reviewed staging overlay for the evaluation release (a new local file, release-staging.yaml):
              digest-pinned images, statistical engine, no training, watched namespace only, one operator with the
              ledger claim, retained state claim, S placement, auth and TLS on with the route's name in the certificate,
              and NO lab qualification (labQualification.configMap "")
  common      the run-wide fixture objects (PriorityClass, scoped ResourceQuota, ingress, the two fault routes, their
              EMPTY fault schedules, route NetworkPolicies, the scrape object)
  targets     the ten target Deployments (fixed replicas), Services and EMPTY fault ConfigMaps; their UIDs are captured
              at creation into the receipts and a new targets.json
  pas         the six PredictiveAutoscalers, every one in Recommend
HPA and KEDA writers are never created here: session.py enables exactly one target's writer for its session.

Every phase refuses any name that already exists (cluster-scoped PriorityClass included), any rendered object outside
the c28-* names, any existing writer that references a target name, an unrendered fault schedule that is not empty,
and a PredictiveAutoscaler not in Recommend. It never updates, adopts or deletes an existing object.

The record's "prepare" section:
  {"storage": {"storage_class": "...", "access_mode": "ReadWriteOnce",
               "ledger_claim": {"name": "...", "size": "1Gi"}, "state_claim": {"name": "...", "size": "2Gi"}},
   "release": {"operator_image": "<repo>@sha256:...", "statistical_image": "<repo>@sha256:...",
               "registry_pull_ref": "<name of the platform-managed pull Secret (referenced, never read)>",
               "network_policy": {chart networkPolicy values; enabled must stay true}}}
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sys
from typing import Dict, List, Optional

import yaml

import execlib as X

HELPER = "prepare"
PHASES = ("namespaces", "storage", "values", "common", "targets", "pas")
_DIGEST_IMAGE = re.compile(r"^([a-z0-9][a-z0-9./:-]*)@(sha256:[0-9a-f]{64})$")
_QUANTITY = re.compile(r"^[0-9]+(Mi|Gi|Ti)$")


def _section(record: X.Record) -> Dict:
    s = record.section("prepare")
    st, rel = s.get("storage") or {}, s.get("release") or {}
    for c in ("ledger_claim", "state_claim"):
        claim = st.get(c) or {}
        if not (X._DNS.match(str(claim.get("name", ""))) and _QUANTITY.match(str(claim.get("size", "")))):
            raise X.StagingError(f"prepare.storage.{c} needs name and size")
    if st["ledger_claim"]["name"] == st["state_claim"]["name"]:
        raise X.StagingError("the ledger and state claims must differ")
    if not X._DNS.match(str(st.get("storage_class", ""))) or st.get("access_mode") != "ReadWriteOnce":
        raise X.StagingError("prepare.storage needs storage_class and access_mode ReadWriteOnce (Recreate, one pod)")
    for k in ("operator_image", "statistical_image"):
        if not _DIGEST_IMAGE.match(str(rel.get(k, ""))):
            raise X.StagingError(f"prepare.release.{k} must be pinned by digest")
    if not X._DNS.match(str(rel.get("registry_pull_ref", ""))):
        raise X.StagingError("prepare.release.registry_pull_ref must name the platform-managed pull Secret")
    np_ = rel.get("network_policy") or {}
    if np_.get("enabled") is not True:
        raise X.StagingError("prepare.release.network_policy.enabled must be true (never disabled to pass staging)")
    return s


def split(params: Dict) -> Dict[str, List[Dict]]:
    """The rendered objects by phase: common, targets (Deployment, Service, faults ConfigMap), pas, writers."""
    c28 = X.import_harness()
    common = c28.render.render_common(params)
    targets, pas, writers = [], [], []
    for profile in c28.frozen.PROFILES:
        for arm in c28.frozen.ARMS:
            for o in c28.render.render_arm(params, arm, profile):
                if o["kind"] == "PredictiveAutoscaler":
                    pas.append(o)
                elif o["kind"] in ("HorizontalPodAutoscaler", "ScaledObject"):
                    writers.append(o)
                else:
                    targets.append(o)
    c28.render.audit_rendered(common + targets + pas + writers)
    return {"common": common, "targets": targets, "pas": pas, "writers": writers}


def staging_values(record: X.Record, params: Dict, s: Dict) -> Dict:
    c28 = X.import_harness()
    routes = c28.render.eval_release_values(params)
    rel, st, r = s["release"], s["storage"], params["reservation"]
    support = {"tolerations": [{"key": r["taint_key"], "operator": "Equal", "value": r["taint_value"],
                                "effect": "NoSchedule"}],
               "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{
                   "matchExpressions": [{"key": r["role_label"], "operator": "In", "values": ["support"]},
                                        {"key": "kubernetes.io/hostname", "operator": "In",
                                         "values": [params["nodes"]["support"]]}]}]}}}}
    op_repo, op_digest = _DIGEST_IMAGE.match(rel["operator_image"]).groups()
    st_repo, st_digest = _DIGEST_IMAGE.match(rel["statistical_image"]).groups()
    return {
        "images": {"operator": {"repository": op_repo, "tag": "", "digest": op_digest},
                   "statisticalForecaster": {"repository": st_repo, "tag": "", "digest": st_digest},
                   "pullPolicy": "IfNotPresent", "pullSecrets": [{"name": rel["registry_pull_ref"]}]},
        "prometheus": routes["prometheus"],
        "operator": {"replicas": 1, "watchNamespaces": [params["namespace"]],
                     "forecasterAddress": routes["operator"]["forecasterAddress"],
                     "forecastLedger": {"enabled": True, "persistence": {"existingClaim": st["ledger_claim"]["name"]}},
                     **copy.deepcopy(support)},
        "forecaster": {"engine": "statistical",
                       "statistical": {"persistence": {"existingClaim": st["state_claim"]["name"], "retain": True},
                                       "labQualification": {"configMap": ""}},
                       "auth": {"enabled": True},
                       "tls": {"extraDNSNames": routes["forecaster"]["tls"]["extraDNSNames"]},
                       **copy.deepcopy(support)},
        "training": {"targets": []},
        "networkPolicy": copy.deepcopy(rel["network_policy"]),
    }


def values_problems(v: Dict, params: Dict) -> List[str]:
    """The overlay's must-haves (STAGING S3), checked on the final document."""
    out = []
    route = f"c28-forecast-route.{params['control_namespace']}.svc"
    if v["forecaster"]["engine"] != "statistical" or v["training"]["targets"] != []:
        out.append("engine must be statistical with no training targets")
    if v["operator"]["watchNamespaces"] != [params["namespace"]] or v["operator"]["replicas"] != 1:
        out.append("one operator watching only the fixture namespace")
    if not v["operator"]["forecastLedger"]["enabled"] or not v["operator"]["forecastLedger"]["persistence"]["existingClaim"]:
        out.append("the forecast ledger must be on, on its existing claim")
    if not v["forecaster"]["statistical"]["persistence"]["existingClaim"]:
        out.append("the statistical state must be on its existing claim")
    if v["forecaster"]["statistical"]["labQualification"]["configMap"] != "":
        out.append("no lab qualification at installation")
    if v["forecaster"]["auth"]["enabled"] is not True or route not in v["forecaster"]["tls"]["extraDNSNames"]:
        out.append("auth on, and the forecast route's name in the certificate at first issuance")
    if not v["operator"]["forecasterAddress"].startswith(route + ":"):
        out.append("operator.forecasterAddress must be the forecast route")
    if v["networkPolicy"].get("enabled") is not True:
        out.append("NetworkPolicies stay enabled")
    for k in ("operator", "statisticalForecaster"):
        if not str(v["images"][k]["digest"]).startswith("sha256:"):
            out.append(f"images.{k} must be digest-pinned")
    return out


def _exists(kube: X.Kube, obj: Dict) -> Optional[Dict]:
    return kube.get(obj["kind"], obj["metadata"]["name"], obj["metadata"].get("namespace"))


def _writer_refs(kube: X.Kube, namespace: str, names: List[str]) -> List[str]:
    inv = X.writer_inventory(kube, namespace, names)
    return [f"{w} already writes {namespace}/{n}" for n, ws in sorted(inv.items()) for w in ws]


def _objects_for(phase: str, record: X.Record, params: Dict, s: Dict, parts: Dict) -> List[Dict]:
    if phase == "namespaces":
        return [{"apiVersion": "v1", "kind": "Namespace",
                 "metadata": {"name": n, "labels": {X.RUN_LABEL: record.run_id,
                                                    "app.kubernetes.io/part-of": "c28-evaluation"}}}
                for n in sorted({params["namespace"], params["control_namespace"]})]
    if phase == "storage":
        st = s["storage"]
        return [{"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                 "metadata": {"name": st[c]["name"], "namespace": params["control_namespace"],
                              "labels": {X.RUN_LABEL: record.run_id, "app.kubernetes.io/part-of": "c28-evaluation"}},
                 "spec": {"accessModes": [st["access_mode"]], "storageClassName": st["storage_class"],
                          "resources": {"requests": {"storage": st[c]["size"]}}}}
                for c in ("ledger_claim", "state_claim")]
    return copy.deepcopy(parts[phase])


def plan(record: X.Record, kube: X.Kube, phase: str) -> Dict:
    if phase not in PHASES:
        raise X.StagingError(f"unknown phase {phase}")
    params = X.load_params(record)
    s = _section(record)
    parts = split(params)
    problems: List[str] = []
    out = {"phase": phase}
    if phase == "values":
        v = staging_values(record, params, s)
        problems += values_problems(v, params)
        path = os.path.join(record.evidence_dir, "release-staging.yaml")
        if os.path.exists(path):
            problems.append(f"{path} exists (never overwritten)")
        text = yaml.safe_dump(v, sort_keys=True)
        out.update({"file": path, "values_sha256": X.sha256_bytes(text.encode()), "values": v})
        return X.new_plan(HELPER, record, [{"write": path, "sha256": out["values_sha256"]}], problems=problems, **out)
    objs = _objects_for(phase, record, params, s, parts)
    create, existing = [], []
    for o in objs:
        m = o["metadata"]
        live = _exists(kube, o)
        if phase == "namespaces":
            if live is not None:
                if live.get("status", {}).get("phase") == "Terminating":
                    problems.append(f"namespace {m['name']} is terminating")
                existing.append({"name": m["name"], "uid": live["metadata"]["uid"]})
                continue
        elif live is not None:
            problems.append(f"{o['kind']} {m.get('namespace') or ''}/{m['name']} already exists (never adopted)")
        if phase in ("common", "targets", "pas") and not m["name"].startswith("c28-"):
            problems.append(f"{o['kind']}/{m['name']} is not a c28-* name")
        if o["kind"] == "ConfigMap" and m["name"].endswith("-faults") and o["data"].get("faults.json") != "{}":
            problems.append(f"{m['name']}: a prepared fault schedule must be empty")
        if o["kind"] == "PredictiveAutoscaler" and o["spec"].get("mode") != "Recommend":
            problems.append(f"PredictiveAutoscaler/{m['name']} is not in Recommend")
        if o["kind"] in ("HorizontalPodAutoscaler", "ScaledObject"):
            problems.append(f"{o['kind']}/{m['name']}: writers are session.py's, never prepare's")
        create.append(o)
    if phase == "storage":
        sc = kube.get("StorageClass", s["storage"]["storage_class"])
        if sc is None:
            problems.append(f"StorageClass {s['storage']['storage_class']} does not exist")
        elif sc.get("reclaimPolicy") != "Retain":
            problems.append(f"StorageClass {s['storage']['storage_class']} reclaims with "
                            f"{sc.get('reclaimPolicy', 'Delete')}: the claims need the storage owner's Retain class")
    if phase in ("namespaces", "storage", "common", "targets", "pas"):
        for ns in sorted({params["namespace"], params["control_namespace"]}):
            if phase != "namespaces" and kube.get("Namespace", ns) is None:
                problems.append(f"namespace {ns} does not exist (phase namespaces first)")
    if phase == "targets":
        names = sorted(o["metadata"]["name"] for o in create if o["kind"] == "Deployment")
        problems += _writer_refs(kube, params["namespace"], names)
        if os.path.exists(os.path.join(record.evidence_dir, "targets.json")):
            problems.append("targets.json exists: targets are created once per run")
    if phase == "common":
        rq = [o for o in create if o["kind"] == "ResourceQuota"]
        if len(rq) != 1 or rq[0]["spec"]["hard"]["pods"] != str(params["quota"]["pods_normal"]):
            problems.append("the scoped quota must start at its approved normal limit")
    actions = [{"create": o["kind"], "namespace": o["metadata"].get("namespace"), "name": o["metadata"]["name"],
                "sha256": X.json_digest(o)} for o in create]
    return X.new_plan(HELPER, record, actions, existing=existing, problems=problems, **out)


def apply(record: X.Record, kube: X.Kube, phase: str, reviewed: Dict, reviewed_digest: str) -> Dict:
    p = plan(record, kube, phase)
    X.require_live(reviewed, reviewed_digest, HELPER, record, p)
    if p["problems"]:
        raise X.StagingError("the plan has problems: " + "; ".join(p["problems"]))
    if phase == "values":
        text = yaml.safe_dump(p["values"], sort_keys=True)
        sha = X.write_new(p["file"], text)
        return X.receipt(record, HELPER, "values-written", {"phase": phase, "plan_sha256": reviewed_digest,
                                                            "file": p["file"], "sha256": sha})
    params = X.load_params(record)
    s = _section(record)
    objs = {(o["kind"], o["metadata"].get("namespace"), o["metadata"]["name"]): o
            for o in _objects_for(phase, record, params, s, split(params))}
    created = []
    for c in p["actions"]:
        o = objs[(c["create"], c["namespace"], c["name"])]
        if X.json_digest(o) != c["sha256"]:
            raise X.StagingError(f"{c['create']}/{c['name']} rendered differently from the reviewed plan; refusing")
        live = kube.create(o)
        ref = X.object_ref(live)
        created.append(ref)
        X.receipt(record, HELPER, "created", {"phase": phase, "plan_sha256": reviewed_digest, "object": ref})
    if phase == "targets":
        uids = {r["name"]: r["uid"] for r in created if r["kind"] == "Deployment"}
        X.write_new(os.path.join(record.evidence_dir, "targets.json"),
                    json.dumps({"schema": "c28-targets/v1", "run_id": record.run_id,
                                "cluster_uid": record.cluster_uid, "namespace": params["namespace"],
                                "deployments": uids}, indent=1, sort_keys=True) + "\n")
    return X.receipt(record, HELPER, "phase-complete", {"phase": phase, "plan_sha256": reviewed_digest,
                                                        "created": created})


def created_objects(record: X.Record) -> Dict[tuple, Dict]:
    return {(e["data"]["object"]["kind"], e["data"]["object"]["namespace"], e["data"]["object"]["name"]):
            e["data"]["object"] for e in X.receipts(record, HELPER) if e["event"] == "created"}


def verify(record: X.Record, kube: X.Kube) -> Dict:
    params = X.load_params(record)
    s = _section(record)
    problems, seen = [], []
    for (kind, ns, name), ref in sorted(created_objects(record).items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        live = kube.get(kind, name, ns)
        if live is None:
            problems.append(f"{kind} {ns or ''}/{name} is missing")
            continue
        if live["metadata"]["uid"] != ref["uid"]:
            problems.append(f"{kind} {ns or ''}/{name} was replaced (UID differs): requalification territory")
            continue
        seen.append(ref)
        if kind == "ConfigMap" and name.endswith("-faults") and live.get("data", {}).get("faults.json") != "{}":
            problems.append(f"{name}: fault schedule is not empty")
        if kind == "PredictiveAutoscaler" and live["spec"].get("mode") != "Recommend":
            problems.append(f"PredictiveAutoscaler/{name} is {live['spec'].get('mode')}, not Recommend")
        if kind == "PersistentVolumeClaim":
            pv_name = live.get("spec", {}).get("volumeName")
            if live.get("status", {}).get("phase") != "Bound" or not pv_name:
                problems.append(f"claim {name} is not Bound")
            else:
                pv = kube.get("PersistentVolume", pv_name)
                if not pv or pv.get("spec", {}).get("persistentVolumeReclaimPolicy") != "Retain":
                    problems.append(f"claim {name}: its volume {pv_name} does not reclaim with Retain")
    targets = sorted(r["name"] for r in seen if r["kind"] == "Deployment" and r["namespace"] == params["namespace"]
                     and r["name"] != "c28-ingress")
    pas = {r["name"] for r in seen if r["kind"] == "PredictiveAutoscaler"}
    for name, writers in sorted(X.writer_inventory(kube, params["namespace"], targets).items()):
        want = [f"PredictiveAutoscaler/{name}"] if name in pas else []
        if writers != want:
            problems.append(f"{name}: writers {writers or 'none'}, expected {want or 'none'} outside a session")
        live = kube.get("Deployment", name, params["namespace"])
        if live and live["spec"].get("replicas") != 2:
            problems.append(f"{name} has {live['spec'].get('replicas')} replicas, not the fixed 2, outside a session")
    return {"helper": HELPER, "action": "verify", "run_id": record.run_id, "objects": seen, "problems": problems,
            "result": "PASS" if not problems else "FAIL", "storage_class": s["storage"]["storage_class"]}


def main(argv=None, api=None) -> int:
    ap = argparse.ArgumentParser(prog="prepare.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", required=True)
    ap.add_argument("command", nargs="?", default="plan", choices=["plan", "apply", "verify"])
    ap.add_argument("--phase", default="", choices=("",) + PHASES)
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    args = ap.parse_args(argv)
    try:
        record = X.load_record(args.record, ("prepare",))
        kube = record.kube(api)
        if args.command == "plan":
            X.emit([X.cli_plan(record, HELPER, plan(record, kube, ph), f"plan-{ph}")
                    for ph in ([args.phase] if args.phase else PHASES)])
        elif args.command == "apply":
            if not args.phase or not args.plan:
                raise X.StagingError("apply needs --phase and --plan: phases are applied one reviewed plan at a time")
            X.emit(apply(record, kube, args.phase, X.load_plan(args.plan), args.plan_digest))
        else:
            out = verify(record, kube)
            X.emit(out)
            return 0 if out["result"] == "PASS" else 1
    except X.StagingError as e:
        print(f"prepare: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
