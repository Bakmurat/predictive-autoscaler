"""Part 2's view of part 1's in-memory API (fakes.FakeApi, not edited): the extra kinds part 2 touches (ConfigMaps,
ResourceQuotas, ReplicaSets, PriorityClasses, StorageClasses, NetworkPolicies, scrape objects) are routed too, and a
replace stores the whole body (metadata labels, data, spec), still refusing a stale resourceVersion or another UID."""

import copy
import uuid

import common_records as C
import execlib as X
import fakes

CLUSTER = fakes.CLUSTER
_TABLE = {**C.KINDS, **X.EXTRA_KINDS}
_PREFIXES = sorted({v[0] for v in _TABLE.values()}, key=len, reverse=True)
_PLURAL = {(v[0], v[1]): k for k, v in _TABLE.items()}


def new_uid() -> str:
    return str(uuid.uuid4())


class FakeApi2(fakes.FakeApi):
    def __init__(self, groups=None):
        super().__init__(groups=groups)
        self.add("namespaces", {"kind": "Namespace", "metadata": {"name": "kube-system", "uid": CLUSTER}})

    def _parse(self, path):
        if path == "/apis":
            return ("discovery", None, None, None)
        prefix = next(p for p in _PREFIXES if path.startswith(p + "/"))
        rest = path[len(prefix) + 1:].split("/")
        ns = None
        if rest[0] == "namespaces" and len(rest) >= 3:
            ns, rest = rest[1], rest[2:]
        elif rest[0] == "namespaces":
            return ("namespaces", None, rest[1] if len(rest) > 1 else None, None)
        kind = _PLURAL[(prefix, rest[0])]
        return (kind, ns, rest[1] if len(rest) > 1 else None, rest[2] if len(rest) > 2 else None)

    def _served(self, kind):
        prefix = _TABLE[kind][0]
        g = "" if prefix == "/api/v1" else prefix.split("/")[2]
        return g == "" or g in self.groups or kind in X.EXTRA_KINDS

    def replace(self, path, body):
        self.calls.append(("replace", path))
        kind, ns, name, sub = self._parse(path)
        o = self.objects.get((kind, ns, name))
        if o is None:
            raise C.NotFound("not found", 404)
        bm = body.get("metadata") or {}
        if bm.get("resourceVersion") != o["metadata"]["resourceVersion"]:
            raise fakes.Conflict("resourceVersion conflict", 409)
        if bm.get("uid") != o["metadata"]["uid"]:
            raise fakes.Conflict("UID precondition failed", 409)
        new = copy.deepcopy(body)
        new["metadata"]["generation"] = o["metadata"].get("generation", 1) + (body.get("spec") != o.get("spec"))
        new["metadata"]["resourceVersion"] = self._next_rv()
        self.objects[(kind, ns, name)] = new
        return copy.deepcopy(new)

    # --- test conveniences ---
    def put(self, obj):
        """Add an object by its kind (CamelCase)."""
        key = X.KIND_KEY[obj["kind"]]
        if _TABLE[key][2]:
            obj.setdefault("metadata", {}).setdefault("namespace", "default")
        return self.add(key, copy.deepcopy(obj))

    def obj(self, kind, name, namespace=None):
        key = X.KIND_KEY[kind]
        return self.objects.get((key, namespace if _TABLE[key][2] else None, name))


def node(name, labels=None, taints=None, unschedulable=False, uid=None):
    spec = {}
    if taints is not None:
        spec["taints"] = taints
    if unschedulable:
        spec["unschedulable"] = True
    md = {"name": name, "labels": {"kubernetes.io/hostname": name, **(labels or {})}}
    if uid:
        md["uid"] = uid
    return {"apiVersion": "v1", "kind": "Node", "metadata": md, "spec": spec,
            "status": {"conditions": [{"type": "Ready", "status": "True"},
                                      {"type": "MemoryPressure", "status": "False"},
                                      {"type": "DiskPressure", "status": "False"}]}}


def pod(name, namespace, node_name, owner=None, labels=None, tolerations=None, phase="Running"):
    meta = {"name": name, "namespace": namespace, "labels": labels or {}}
    if owner:
        meta["ownerReferences"] = [{"kind": owner[0], "name": owner[1], "controller": True, "uid": new_uid()}]
    return {"apiVersion": "v1", "kind": "Pod", "metadata": meta,
            "spec": {"nodeName": node_name, "tolerations": tolerations or []}, "status": {"phase": phase}}
