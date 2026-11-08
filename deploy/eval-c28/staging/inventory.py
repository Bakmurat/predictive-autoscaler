#!/usr/bin/env python3
"""Read-only prod inventory for C2.8 staging (STAGING-C28-PROD.md S0/S2, "Helpers to implement": inventory.py).

  inventory.py --record preparation.json [--out FILE]

Reads, never writes, the cluster: its identity (kube-system UID, checked against the record), every served API group
(GET /apis: a kind whose group is not served is reported absent, never silently skipped), nodes (UID, labels,
taints, allocatable, conditions), and in the legacy namespaces (record["inventory"]["namespaces"], default demo and
ml-engine) the workloads, CronJobs/Jobs, PDBs, PVCs with their PVs, Leases, Pods (owner, node, phase), and every
replica WRITER cluster-wide: HPAs (autoscaling/v2), KEDA ScaledObjects, PredictiveAutoscalers of both API groups and
Argo CD Applications. Each object is reduced to identity plus the fields a reviewer needs (sanitized: no Secret is read,
no ConfigMap content, env value, managedFields or last-applied configuration). The exact legacy allowlist
(legacy_allowlist.py) is resolved against it: found (with UID) or missing, and every unlisted object in the legacy
namespaces is listed as unclassified; a writer of a legacy target, or a legacy PA, outside the allowlist is a blocker.
The output (schema c28-inventory/v1) is validated before it is written, and written once.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Mapping

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common_records as C  # noqa: E402
import legacy_allowlist as L  # noqa: E402

SCHEMA = "c28-inventory/v1"
NAMESPACED = ("deployments", "statefulsets", "daemonsets", "cronjobs", "jobs", "poddisruptionbudgets",
              "persistentvolumeclaims", "leases", "pods", "services")
WRITERS = ("horizontalpodautoscalers", "scaledobjects", "pa-legacy", "pa-new", "applications")


def _ident(o: Mapping) -> Dict:
    m = o.get("metadata") or {}
    return {"name": m.get("name"), "namespace": m.get("namespace"), "uid": m.get("uid"),
            "resourceVersion": m.get("resourceVersion"), "generation": m.get("generation"),
            "labels": dict(m.get("labels") or {}),
            "ownerReferences": [{k: r.get(k) for k in ("kind", "name", "uid", "controller")}
                                for r in m.get("ownerReferences") or []],
            "finalizers": list(m.get("finalizers") or []), "deleting": bool(m.get("deletionTimestamp"))}


def _images(pod_spec: Mapping) -> List[str]:
    return [c.get("image") for k in ("initContainers", "containers") for c in pod_spec.get(k) or []]


def reduce_object(kind: str, o: Mapping) -> Dict:
    """Identity plus the reviewable fields of one kind (nothing else is kept)."""
    o = C.sanitize_object(o)
    spec, status, r = o.get("spec") or {}, o.get("status") or {}, _ident(o)
    if kind in ("deployments", "statefulsets"):
        tmpl = (spec.get("template") or {}).get("spec") or {}
        r.update(replicas=spec.get("replicas"), ready=status.get("readyReplicas", 0), images=_images(tmpl),
                 node_selector=tmpl.get("nodeSelector"), priority_class=tmpl.get("priorityClassName"),
                 claims=[v["persistentVolumeClaim"]["claimName"] for v in tmpl.get("volumes") or []
                         if "persistentVolumeClaim" in v])
    elif kind == "daemonsets":
        r.update(images=_images((spec.get("template") or {}).get("spec") or {}),
                 scheduled=status.get("currentNumberScheduled"))
    elif kind == "cronjobs":
        tmpl = (((spec.get("jobTemplate") or {}).get("spec") or {}).get("template") or {}).get("spec") or {}
        r.update(schedule=spec.get("schedule"), suspend=bool(spec.get("suspend")), images=_images(tmpl),
                 active=[a.get("name") for a in status.get("active") or []],
                 last_schedule=status.get("lastScheduleTime"))
    elif kind == "jobs":
        r.update(active=status.get("active", 0), succeeded=status.get("succeeded", 0),
                 failed=status.get("failed", 0), start=status.get("startTime"),
                 completion=status.get("completionTime"))
    elif kind == "poddisruptionbudgets":
        r.update(min_available=spec.get("minAvailable"), max_unavailable=spec.get("maxUnavailable"),
                 selector=spec.get("selector"), disruptions_allowed=status.get("disruptionsAllowed"))
    elif kind == "persistentvolumeclaims":
        r.update(volume=spec.get("volumeName"), storage_class=spec.get("storageClassName"),
                 access_modes=spec.get("accessModes"), request=((spec.get("resources") or {}).get("requests")
                                                                or {}).get("storage"), phase=status.get("phase"))
    elif kind == "persistentvolumes":
        claim = spec.get("claimRef") or {}
        r.update(reclaim_policy=spec.get("persistentVolumeReclaimPolicy"), storage_class=spec.get("storageClassName"),
                 claim={k: claim.get(k) for k in ("namespace", "name", "uid")}, node_affinity=spec.get("nodeAffinity"),
                 phase=status.get("phase"))
    elif kind == "leases":
        r.update(holder=spec.get("holderIdentity"), renew=spec.get("renewTime"))
    elif kind == "pods":
        r.update(node=spec.get("nodeName"), phase=status.get("phase"), images=_images(spec),
                 priority_class=spec.get("priorityClassName"), tolerations=spec.get("tolerations"))
    elif kind == "services":
        r.update(selector=spec.get("selector"), type=spec.get("type"))
    elif kind == "nodes":
        r.update(taints=spec.get("taints") or [], unschedulable=bool(spec.get("unschedulable")),
                 allocatable=status.get("allocatable"),
                 conditions={c.get("type"): c.get("status") for c in status.get("conditions") or []})
    elif kind == "horizontalpodautoscalers":
        r.update(target=spec.get("scaleTargetRef"), min=spec.get("minReplicas"), max=spec.get("maxReplicas"),
                 current=status.get("currentReplicas"), desired=status.get("desiredReplicas"))
    elif kind == "scaledobjects":
        ann = (o.get("metadata") or {}).get("annotations") or {}
        r.update(target=spec.get("scaleTargetRef"), min=spec.get("minReplicaCount"), max=spec.get("maxReplicaCount"),
                 paused=ann.get("autoscaling.keda.sh/paused"),
                 restore_to_original=(spec.get("advanced") or {}).get("restoreToOriginalReplicaCount", False),
                 hpa_name=((spec.get("advanced") or {}).get("horizontalPodAutoscalerConfig") or {}).get("name"),
                 trigger_types=[t.get("type") for t in spec.get("triggers") or []])
    elif kind in ("pa-legacy", "pa-new"):
        td = spec.get("targetDeployment") or {}
        r.update(target={"kind": "Deployment", "name": td.get("name"), "namespace": td.get("namespace")},
                 mode=spec.get("mode"), min=spec.get("minReplicas"), max=spec.get("maxReplicas"))
    elif kind == "applications":
        dst = spec.get("destination") or {}
        r.update(destination={k: dst.get(k) for k in ("server", "name", "namespace")},
                 automated=bool(((spec.get("syncPolicy") or {}).get("automated"))),
                 source_repo=(spec.get("source") or {}).get("repoURL"))
    elif kind == "customresourcedefinitions":
        r.update(group=spec.get("group"), versions=[v.get("name") for v in spec.get("versions") or []])
    return r


def _writer_target(kind: str, w: Mapping, default_ns: str):
    t = w.get("target") or {}
    if kind == "applications":
        return None
    return (t.get("namespace") or default_ns, t.get("name"))


def collect(api, rec: Mapping) -> Dict:
    """The inventory document (read-only)."""
    C.require_cluster(api, rec["cluster_uid"])
    groups = C.discover_groups(api)
    namespaces = list((rec.get("inventory") or {}).get("namespaces") or L.NAMESPACES)
    served = lambda k: C.kind_group(k) in groups  # noqa: E731
    absent = sorted({C.kind_group(k) for k in C.KINDS if not served(k) and C.kind_group(k)})
    doc = {"schema": SCHEMA, "run_id": rec["run_id"], "cluster_uid": rec["cluster_uid"],
           "api_groups": {g: groups[g] for g in sorted(groups)}, "absent_groups": absent,
           "namespaces": namespaces, "nodes": [reduce_object("nodes", n) for n in C.list_items(api, "nodes")],
           "objects": {}, "writers": {}, "crds": []}
    for kind in NAMESPACED:
        doc["objects"][kind] = [reduce_object(kind, o) for ns in namespaces for o in C.list_items(api, kind, ns)]
    pvs = {p["metadata"]["name"]: p for p in C.list_items(api, "persistentvolumes")}
    doc["objects"]["persistentvolumes"] = [reduce_object("persistentvolumes", pvs[c["volume"]])
                                           for c in doc["objects"]["persistentvolumeclaims"] if c["volume"] in pvs]
    for kind in WRITERS:                         # cluster-wide: a consumer outside the two namespaces counts too
        doc["writers"][kind] = [reduce_object(kind, o) for o in C.list_items(api, kind)] if served(kind) else None
    doc["crds"] = [reduce_object("customresourcedefinitions", o)
                   for o in C.list_items(api, "customresourcedefinitions")
                   if (o.get("spec") or {}).get("group") in (L.LEGACY_GROUP, "autoscaling.devkuban.com", "keda.sh")]
    doc["legacy"] = resolve_allowlist(doc)
    errs = problems(doc)
    if errs:
        raise C.StagingError("inventory schema: " + "; ".join(errs))
    return doc


def find(doc: Mapping, kind: str, namespace, name: str):
    if kind == "customresourcedefinitions":
        pool = doc["crds"]
    elif kind in doc["writers"]:
        pool = doc["writers"][kind] or []
    else:
        pool = doc["objects"].get(kind, [])
    return next((o for o in pool if o["name"] == name and (namespace is None or o.get("namespace") == namespace)),
                None)


def _writer_blockers(doc: Mapping) -> List[str]:
    """Writers checked by their ACTUAL targets and ownership: an allowlisted PA or ScaledObject must target its own arm;
    an HPA is the benchmark's only when a listed ScaledObject (same UID) controls it and it targets that object's arm;
    any other writer of a legacy target, any legacy PA that is not an arm, and any reconciler of the legacy namespaces
    blocks."""
    listed = {(k, ns, n) for k, ns, n, _ in L.ALLOWLIST}
    targets = {("demo", a) for a in L.ARMS}
    sos = {w["uid"]: w for w in doc["writers"].get("scaledobjects") or []
           if ("scaledobjects", w["namespace"], w["name"]) in listed}
    blockers = []
    for kind in ("horizontalpodautoscalers", "scaledobjects", "pa-legacy", "pa-new"):
        for w in doc["writers"].get(kind) or []:
            tgt = _writer_target(kind, w, w["namespace"])
            if (kind, w["namespace"], w["name"]) in listed:
                want = L.expected_target(kind, w["name"])
                if want is not None and tgt != want:
                    blockers.append(f"{kind} {w['namespace']}/{w['name']} targets {tgt}, not its arm {want}")
                continue
            if kind == "horizontalpodautoscalers":
                owner = next((r for r in w.get("ownerReferences") or [] if r.get("controller")), None)
                so = sos.get((owner or {}).get("uid")) if owner and owner.get("kind") == "ScaledObject" else None
                if so is not None:
                    so_tgt = _writer_target("scaledobjects", so, so["namespace"])
                    if tgt != so_tgt or owner.get("name") != so["name"]:
                        blockers.append(f"HPA {w['namespace']}/{w['name']} is owned by {so['name']} but targets "
                                        f"{tgt}, not {so_tgt}")
                    continue
            if kind == "pa-legacy":
                blockers.append(f"legacy PA {w['namespace']}/{w['name']} is not a benchmark arm: the old operator has "
                                f"another consumer")
            elif tgt in targets:
                blockers.append(f"{kind} {w['namespace']}/{w['name']} writes legacy target {tgt[1]} and is not on the "
                                f"allowlist")
    if doc["writers"].get("scaledobjects") is None:
        blockers.append("the keda.sh API group is not served: the benchmark ScaledObjects cannot be verified")
    for a in doc["writers"].get("applications") or []:
        if (a.get("destination") or {}).get("namespace") in doc["namespaces"]:
            blockers.append(f"Argo CD application {a['namespace']}/{a['name']} reconciles "
                            f"{a['destination']['namespace']}: suspend it at its source first")
    return blockers


def resolve_allowlist(doc: Mapping) -> Dict:
    """Allowlist entries found (with UID) or missing; unclassified objects; blockers."""
    found, missing = [], []
    for kind, ns, name, role in L.ALLOWLIST:
        o = find(doc, kind, ns, name)
        item = {"kind": kind, "namespace": ns, "name": name, "role": role}
        (found if o else missing).append(dict(item, uid=o["uid"]) if o else item)
    listed = {(k, ns, n) for k, ns, n, _ in L.ALLOWLIST}
    unclassified = []
    for kind in ("deployments", "statefulsets", "daemonsets", "cronjobs"):
        for o in doc["objects"].get(kind, []):
            if (kind, o["namespace"], o["name"]) not in listed:
                unclassified.append({"kind": kind, "namespace": o["namespace"], "name": o["name"], "uid": o["uid"]})
    return {"found": found, "missing": missing, "unclassified": unclassified, "blockers": _writer_blockers(doc)}


def live_blockers(api, rec: Mapping) -> List[str]:
    """The consumer, writer and reconciler blockers read live (cluster-wide), for a re-check right before a
    shutdown step."""
    groups = C.discover_groups(api)
    writers = {k: ([reduce_object(k, o) for o in C.list_items(api, k)] if C.kind_group(k) in groups else None)
               for k in WRITERS}
    namespaces = list((rec.get("inventory") or {}).get("namespaces") or L.NAMESPACES)
    return _writer_blockers({"writers": writers, "namespaces": namespaces})


def problems(doc: Mapping) -> List[str]:
    """Schema validation of an inventory document."""
    out = []
    for k, t in (("schema", str), ("cluster_uid", str), ("api_groups", dict), ("nodes", list), ("objects", dict),
                 ("writers", dict), ("legacy", dict), ("namespaces", list)):
        if not isinstance(doc.get(k), t):
            out.append(f"{k} must be a {t.__name__}")
    if out:
        return out
    if doc["schema"] != SCHEMA:
        out.append(f"schema must be {SCHEMA}")
    for kind, items in list(doc["objects"].items()) + [("nodes", doc["nodes"])]:
        for o in items:
            if not o.get("name") or not o.get("uid"):
                out.append(f"{kind}: every object needs a name and a UID")
                break
    if set(doc["legacy"]) != {"found", "missing", "unclassified", "blockers"}:
        out.append("legacy must hold found, missing, unclassified and blockers")
    return out


def main(argv=None, api=None) -> int:
    ap = argparse.ArgumentParser(prog="inventory.py", description=__doc__.splitlines()[0])
    C.add_common_args(ap)
    ap.add_argument("--out", default="", help="write the inventory here (once); default: stdout")
    a = ap.parse_args(argv)
    try:
        rec = C.load_record(a.record)
        doc = collect(api or C.api_for(rec), rec)
        text = json.dumps(doc, indent=1, sort_keys=True) + "\n"
        C.assert_clean(text)
        if a.out:
            C.write_once(a.out, text.encode())
            print(f"inventory written: {a.out} sha256 {C.digest(doc)}")
        else:
            sys.stdout.write(text)
    except C.StagingError as e:
        print(f"inventory: refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
