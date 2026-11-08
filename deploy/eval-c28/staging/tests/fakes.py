"""An in-memory Kubernetes API for the staging helpers' offline tests: discovery, lists, objects, the Deployment scale
subresource, optimistic concurrency (resourceVersion), UID preconditions on delete, owner-reference garbage collection
of generated HPAs, and a fake prod legacy benchmark carrying secrets that must never leak."""

import copy
import json
import uuid

import common_records as C

CLUSTER = "11111111-2222-4333-8444-555555555555"
SECRET_VALUE = "hunter2-SECRET-LITERAL"            # literal env value under a credential name
SECRET_DATA = "c2VjcmV0LWRhdGEtTUFSS0VS"           # a Secret's base64 data
LAST_APPLIED_MARKER = "LASTAPPLIED-MARKER-42"      # inside a last-applied annotation
URL_PASSWORD = "pw-in-url-MARKER"
MARKERS = (SECRET_VALUE, SECRET_DATA, LAST_APPLIED_MARKER, URL_PASSWORD)

_PREFIXES = sorted({v[0] for v in C.KINDS.values()}, key=len, reverse=True)
_PLURAL = {(v[0], v[1]): k for k, v in C.KINDS.items()}


class Conflict(C.ApiError):
    pass


class FakeApi:
    def __init__(self, groups=None):
        self.objects = {}                        # (kind, namespace, name) -> object
        self.calls = []                          # (verb, path)
        self.groups = groups if groups is not None else [
            "apps", "batch", "autoscaling", "policy", "coordination.k8s.io", "apiextensions.k8s.io", "keda.sh",
            "autoscaler.example.com", "autoscaling.devkuban.com", "velero.io"]
        self._rv = 1000
        self.on_poll = []                        # callables run at each list/get (simulate progress)
        self.instant_pods = True                 # scaling creates/removes the Deployment's pods at once

    # --- helpers -------------------------------------------------------------------------------------------------
    def _next_rv(self):
        self._rv += 1
        return str(self._rv)

    def add(self, kind, obj):
        m = obj.setdefault("metadata", {})
        m.setdefault("uid", str(uuid.uuid4()))
        m.setdefault("resourceVersion", self._next_rv())
        m.setdefault("generation", 1)
        self.objects[(kind, m.get("namespace"), m["name"])] = obj
        return obj

    def _parse(self, path):
        if path == "/apis":
            return ("discovery", None, None, None)
        prefix = next(p for p in _PREFIXES if path.startswith(p + "/"))
        rest = path[len(prefix) + 1:].split("/")
        ns = None
        if rest[0] == "namespaces" and len(rest) >= 3:
            ns, rest = rest[1], rest[2:]
        plural = rest[0]
        if (prefix, plural) not in _PLURAL and plural == "namespaces":
            return ("namespaces", None, rest[1] if len(rest) > 1 else None, None)
        kind = _PLURAL[(prefix, plural)]
        if kind == "namespaces" and ns:            # /api/v1/namespaces/<name>
            return ("namespaces", None, ns, None)
        name = rest[1] if len(rest) > 1 else None
        sub = rest[2] if len(rest) > 2 else None
        return (kind, ns, name, sub)

    def _served(self, kind):
        g = C.kind_group(kind)
        return g == "" or g in self.groups

    # --- the API -----------------------------------------------------------------------------------------------
    def get(self, path):
        self.calls.append(("get", path))
        for f in self.on_poll:
            f(self)
        kind, ns, name, sub = self._parse(path)
        if kind == "discovery":
            return {"groups": [{"name": g, "versions": [{"version": "v1"}]} for g in self.groups]}
        if not self._served(kind):
            return None
        if name is None:
            return {"items": [copy.deepcopy(o) for (k, n, _), o in sorted(self.objects.items(), key=lambda x: str(x[0]))
                              if k == kind and (ns is None or n == ns)]}
        o = self.objects.get((kind, ns, name))
        if o is None:
            return None
        if sub == "scale":
            m = o["metadata"]
            return {"kind": "Scale", "metadata": {"name": m["name"], "namespace": m["namespace"], "uid": m["uid"],
                                                  "resourceVersion": m["resourceVersion"]},
                    "spec": {"replicas": o["spec"].get("replicas")}}
        return copy.deepcopy(o)

    def replace(self, path, body):
        self.calls.append(("replace", path))
        kind, ns, name, sub = self._parse(path)
        o = self.objects.get((kind, ns, name))
        if o is None:
            raise C.NotFound("not found", 404)
        bm = body.get("metadata") or {}
        if bm.get("resourceVersion") != o["metadata"]["resourceVersion"]:
            raise Conflict("resourceVersion conflict", 409)
        if bm.get("uid") and bm["uid"] != o["metadata"]["uid"]:
            raise Conflict("UID precondition failed", 409)
        if sub == "scale":
            if o["spec"].get("replicas") != body["spec"]["replicas"]:
                o["metadata"]["generation"] += 1         # a spec change raises the generation by one
            o["spec"]["replicas"] = body["spec"]["replicas"]
            if self.instant_pods:
                self.sync_pods(o)
        else:
            if body.get("spec") != o.get("spec"):
                o["metadata"]["generation"] += 1
            o["spec"] = copy.deepcopy(body["spec"])
        o["metadata"]["resourceVersion"] = self._next_rv()
        return copy.deepcopy(o)

    def create(self, path, body):
        self.calls.append(("create", path))
        kind, ns, _name, _ = self._parse(path)
        name = body["metadata"]["name"]
        if (kind, ns, name) in self.objects:
            raise Conflict("exists", 409)
        o = copy.deepcopy(body)
        o["metadata"].pop("uid", None)
        o["metadata"].pop("resourceVersion", None)
        return copy.deepcopy(self.add(kind, o))

    def delete(self, path, uid, resource_version=None):
        self.calls.append(("delete", path))
        kind, ns, name, _ = self._parse(path)
        o = self.objects.get((kind, ns, name))
        if o is None:
            raise C.NotFound("not found", 404)
        if o["metadata"]["uid"] != uid:
            raise Conflict("UID precondition failed", 409)
        del self.objects[(kind, ns, name)]
        for key, x in list(self.objects.items()):                 # garbage collection by owner reference
            if any(r.get("uid") == uid for r in x["metadata"].get("ownerReferences") or []):
                del self.objects[key]
        return {}

    def sync_pods(self, dep):
        """Make the Deployment's ReplicaSet own exactly spec.replicas pods (and report them ready)."""
        m = dep["metadata"]
        rs_key = ("replicasets", m["namespace"], f"{m['name']}-rs")
        if rs_key not in self.objects:
            self.add("replicasets", {"kind": "ReplicaSet", "metadata": {
                "name": rs_key[2], "namespace": m["namespace"],
                "ownerReferences": [{"kind": "Deployment", "name": m["name"], "uid": m["uid"], "controller": True}]}})
        rs_uid = self.objects[rs_key]["metadata"]["uid"]
        pods = sorted(k for k, v in self.objects.items() if k[0] == "pods" and
                      any(r.get("uid") == rs_uid for r in v["metadata"].get("ownerReferences") or []))
        want = dep["spec"].get("replicas") or 0
        for k in pods[want:]:
            del self.objects[k]
        for i in range(len(pods), want):
            self.add("pods", {"kind": "Pod", "metadata": {
                "name": f"{m['name']}-{uuid.uuid4().hex[:5]}", "namespace": m["namespace"],
                "ownerReferences": [{"kind": "ReplicaSet", "name": rs_key[2], "uid": rs_uid, "controller": True}]},
                "spec": {}, "status": {"phase": "Running"}})
        dep.setdefault("status", {}).update(replicas=want, readyReplicas=want)

    def mutations(self):
        return [c for c in self.calls if c[0] != "get"]


def legacy_cluster(extra_groups=None):
    """A fake prodcluster with the legacy benchmark as deploy/prodcluster deploys it (old API group), plus secrets."""
    api = FakeApi(groups=extra_groups)
    api.add("namespaces", {"kind": "Namespace", "metadata": {"name": "kube-system", "uid": CLUSTER}})
    for n in ("w1", "w2"):
        api.add("nodes", {"kind": "Node", "metadata": {"name": n, "labels": {"kubernetes.io/hostname": n}},
                          "spec": {}, "status": {"allocatable": {"cpu": "8", "memory": "32Gi"},
                                                 "conditions": [{"type": "Ready", "status": "True"}]}})
    leaked_annotations = {C.LAST_APPLIED: json.dumps({"spec": {"env": [{"name": "PASSWORD",
                                                                        "value": LAST_APPLIED_MARKER}]}})}

    def deployment(ns, name, replicas, env=None, annotations=None):
        d = api.add("deployments", {
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": name, "namespace": ns, "labels": {"app": name}, "annotations": dict(annotations or {}),
                         "managedFields": [{"manager": "kubectl"}]},
            "spec": {"replicas": replicas, "template": {"spec": {"containers": [
                {"name": "c", "image": f"harbor.example/{name}@sha256:" + "a" * 64, "env": env or [],
                 "args": [f"--api-token={SECRET_VALUE}"]}]}}},
            "status": {"replicas": replicas, "readyReplicas": replicas}})
        api.sync_pods(d)
        return d

    arms = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
    for a in arms:
        deployment("demo", a, 3, env=[{"name": "DB_PASSWORD", "value": SECRET_VALUE},
                                      {"name": "VM_URL", "value": f"http://user:{URL_PASSWORD}@vm:8428"},
                                      {"name": "TLS_CERT", "value": "/etc/tls/tls.crt"},
                                      {"name": "TOKEN_REF", "valueFrom": {"secretKeyRef": {"name": "s", "key": "t"}}}],
                   annotations=leaked_annotations)
        deployment("demo", f"k6-{a}", 1)
        api.add("services", {"kind": "Service", "metadata": {"name": a, "namespace": "demo"},
                             "spec": {"selector": {"app": a}}})
    for a in ("nginx-test", "nginx-reactive", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95"):
        api.add("pa-legacy", {"apiVersion": "autoscaler.example.com/v1alpha1", "kind": "PredictiveAutoscaler",
                              "metadata": {"name": f"{a}-autoscaler", "namespace": "demo",
                                           "annotations": dict(leaked_annotations)},
                              "spec": {"targetDeployment": {"name": a, "namespace": "demo"}, "minReplicas": 1,
                                       "maxReplicas": 12}})
    for so, tgt, paused in (("nginx-test-keda-fallback", "nginx-test", "true"),
                            ("myapptwo-keda-fallback", "myapptwo", None)):
        ann = {"autoscaling.keda.sh/paused": paused} if paused else {}
        s = api.add("scaledobjects", {"apiVersion": "keda.sh/v1alpha1", "kind": "ScaledObject",
                                      "metadata": {"name": so, "namespace": "demo", "annotations": ann},
                                      "spec": {"scaleTargetRef": {"name": tgt}, "minReplicaCount": 1,
                                               "maxReplicaCount": 12, "triggers": [{"type": "prometheus", "metadata": {
                                                   "serverAddress": "http://vmselect:8481/select/0/prometheus",
                                                   "threshold": "600"}}]}})
        api.add("horizontalpodautoscalers", {"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                                             "metadata": {"name": f"keda-hpa-{so}", "namespace": "demo",
                                                          "ownerReferences": [{"kind": "ScaledObject", "name": so,
                                                                               "uid": s["metadata"]["uid"],
                                                                               "controller": True}]},
                                             "spec": {"scaleTargetRef": {"kind": "Deployment", "name": tgt},
                                                      "minReplicas": 1, "maxReplicas": 12}})
    # A Secret in the legacy namespace: no helper may ever read it (the fake has no route to it at all).
    api.objects[("secrets", "demo", "harbor-creds")] = {"kind": "Secret", "metadata": {"name": "harbor-creds",
                                                                                      "namespace": "demo"},
                                                        "data": {".dockerconfigjson": SECRET_DATA}}
    deployment("ml-engine", "predictive-operator", 1, env=[{"name": "VM_PASSWORD", "value": SECRET_VALUE}],
               annotations=leaked_annotations)
    deployment("ml-engine", "ml-api", 1, env=[{"name": "API_TOKEN", "value": SECRET_VALUE}])
    for cj in ("ml-training", "evidence-archive", "load-evidence"):
        api.add("cronjobs", {"apiVersion": "batch/v1", "kind": "CronJob",
                             "metadata": {"name": cj, "namespace": "ml-engine",
                                          "annotations": dict(leaked_annotations)},
                             "spec": {"schedule": "0 */6 * * *", "suspend": False, "jobTemplate": {"spec": {
                                 "template": {"spec": {"containers": [{"name": "c", "image": "x@sha256:" + "b" * 64,
                                                                       "env": [{"name": "SECRET_KEY",
                                                                                "value": SECRET_VALUE}]}]}}}}},
                             "status": {}})
    api.add("leases", {"kind": "Lease", "metadata": {"name": "predictive-autoscaler-leader", "namespace": "ml-engine"},
                       "spec": {"holderIdentity": "predictive-operator-abc", "renewTime": "2026-11-08T00:00:00Z"}})
    for pvc in ("ml-models-pvc", "forecast-log-pvc", "evidence-archive-pvc", "load-evidence-pvc", "p8-evidence-pvc"):
        api.add("persistentvolumes", {"kind": "PersistentVolume", "metadata": {"name": f"pv-{pvc}"},
                                      "spec": {"persistentVolumeReclaimPolicy": "Retain",
                                               "storageClassName": "harvester",
                                               "claimRef": {"namespace": "ml-engine", "name": pvc}},
                                      "status": {"phase": "Bound"}})
        api.add("persistentvolumeclaims", {"kind": "PersistentVolumeClaim",
                                           "metadata": {"name": pvc, "namespace": "ml-engine"},
                                           "spec": {"volumeName": f"pv-{pvc}", "storageClassName": "harvester",
                                                    "accessModes": ["ReadWriteOnce"],
                                                    "resources": {"requests": {"storage": "8Gi"}}},
                                           "status": {"phase": "Bound"}})
    api.add("customresourcedefinitions", {"kind": "CustomResourceDefinition",
                                          "metadata": {"name": "predictiveautoscalers.autoscaler.example.com"},
                                          "spec": {"group": "autoscaler.example.com",
                                                   "versions": [{"name": "v1alpha1"}]}})
    return api


def record(tmp_path, **sections):
    rec = {"schema": "c28-preparation/v1", "run_id": "c28-r1", "cluster_uid": CLUSTER,
           "kubeconfig": {"path": "/home/ops/.kube/eval-prod.yaml", "context": "prodcluster"},
           "evidence_dir": str(tmp_path / "evidence"), "lock_dir": str(tmp_path / "shared-locks"),
           "source": {"commit": "a" * 40}}
    rec.update(sections)
    return rec
