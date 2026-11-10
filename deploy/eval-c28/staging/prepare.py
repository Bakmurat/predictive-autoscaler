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
              ledger claim and NO replica write (operator.rbac.scaleWrite false: deployments/scale get only),
              retained state claim, S placement, auth and TLS on with the route's name in the certificate, and NO lab
              qualification (labQualification.configMap "")
  common      the run-wide fixture objects (PriorityClass, scoped ResourceQuota, ingress, the two fault routes, their
              EMPTY fault schedules, route NetworkPolicies, the scrape object)
  targets     the ten target Deployments (fixed replicas), Services and EMPTY fault ConfigMaps; their UIDs are captured
              at creation into the receipts and a new targets.json
  pas         the six PredictiveAutoscalers, every one in Recommend
HPA and KEDA writers are never created here: session.py enables exactly one target's writer for its session.

Every phase requires the live reservation verdict (reserve.verify: reservation PASS and rendered placement PASS on
complete parameters, ready_for_experiment), in its plan and again in apply's recomputation under the shared lock, so
a reservation lost between review and apply creates nothing. Every phase refuses any name that already exists (cluster-scoped PriorityClass included), any rendered object outside
the c28-* names, any existing writer that references a target name, an unrendered fault schedule that is not empty,
and a PredictiveAutoscaler not in Recommend. It never updates, adopts or deletes an existing object.

The record's "prepare" section:
  {"storage": {"storage_class": "...", "access_mode": "ReadWriteOnce",
               "ledger_claim": {"name": "...", "size": "1Gi"}, "state_claim": {"name": "...", "size": "2Gi"}},
   "release": {"operator_image": "<repo>@sha256:...", "statistical_image": "<repo>@sha256:...",
               "image_pull": {"mode": "secret", "pull_ref": "<the platform-managed pull Secret's name (referenced,
                                                          never read)>"}
                             | {"mode": "anonymous"},
               "network_policy": {chart networkPolicy values; enabled must stay true}}}

image_pull is required and explicit: a record without it (or with the retired registry_pull_ref) is refused, never
read as anonymous. Secret mode references the Secret by name in images.pullSecrets. Anonymous mode renders
images.pullSecrets as [] and the values plan verifies every required image digest anonymously: a registry v2 manifest
HEAD by digest (OCI and Docker manifest Accept headers, no credentials; a Bearer challenge is answered only with an
anonymous token from the challenge's realm, which is never recorded). Each check (image, digest, URL, HTTP status,
time; failures only as fixed categories, never exception text or header values) is recorded in the plan; anything but
HTTP 200 with a Docker-Content-Digest equal to the expected digest (missing or different) is a plan problem, and apply
checks again. A manifest HEAD from the checker proves that the registry serves the manifest anonymously to THIS host
only: the nodes' pulls (their network path, mirrors, containerd configuration) remain an installation check.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Dict, List, Optional, Tuple

import yaml

import execlib as X

HELPER = "prepare"
PHASES = ("namespaces", "storage", "values", "common", "targets", "pas")
_DIGEST_IMAGE = re.compile(r"^([a-z0-9][a-z0-9./:-]*)@(sha256:[0-9a-f]{64})$")
_QUANTITY = re.compile(r"^[0-9]+(Mi|Gi|Ti)$")
PULL_MODES = ("secret", "anonymous")
MANIFEST_ACCEPT = ", ".join(("application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json",
                             "application/vnd.docker.distribution.manifest.list.v2+json",
                             "application/vnd.docker.distribution.manifest.v2+json"))


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
    _image_pull(rel)
    np_ = rel.get("network_policy") or {}
    if np_.get("enabled") is not True:
        raise X.StagingError("prepare.release.network_policy.enabled must be true (never disabled to pass staging)")
    return s


def _image_pull(rel: Dict) -> Dict:
    """The explicit pull mode: {"mode": "secret", "pull_ref": ...} or {"mode": "anonymous"}; nothing is defaulted."""
    if "registry_pull_ref" in rel:
        raise X.StagingError('prepare.release.registry_pull_ref is retired: set prepare.release.image_pull to '
                             '{"mode": "secret", "pull_ref": ...} (or {"mode": "anonymous"})')
    p = rel.get("image_pull")
    if not isinstance(p, dict) or p.get("mode") not in PULL_MODES:
        raise X.StagingError("prepare.release.image_pull.mode must be set explicitly to secret or anonymous (a missing "
                             "setting is never read as anonymous)")
    if p["mode"] == "secret" and (set(p) != {"mode", "pull_ref"} or not X._DNS.match(str(p["pull_ref"]))):
        raise X.StagingError("prepare.release.image_pull secret mode needs exactly pull_ref, the platform-managed "
                             "pull Secret")
    if p["mode"] == "anonymous" and set(p) != {"mode"}:
        raise X.StagingError("prepare.release.image_pull anonymous mode takes no other field")
    return p


# --- anonymous registry verification ----------------------------------------------------------------------------------

# fetch(method, url, headers) -> (status, response headers with lower-case names, body bytes): the only network access,
# injectable. It never receives credentials: the headers are Accept and, after a Bearer challenge, an anonymous token.
Fetch = Callable[[str, str, Dict[str, str]], Tuple[int, Dict[str, str], bytes]]


def _http_fetch(method: str, url: str, headers: Dict[str, str]) -> Tuple[int, Dict[str, str], bytes]:
    req = urllib.request.Request(url, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:     # noqa: S310 - the record's digest-pinned registry
            return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read(1 << 16)
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, b""


def _image_parts(image: str) -> Tuple[str, str, str]:
    """registry host, repository, digest of a digest-pinned reference (the host is the first path element)."""
    repo, dig = _DIGEST_IMAGE.match(image).groups()
    host, _, path = repo.partition("/")
    if not path or not ("." in host or ":" in host or host == "localhost"):
        raise X.StagingError(f"{image}: anonymous verification needs the registry host in the image reference")
    return host, path, dig


def _bearer_challenge(value: str) -> Dict[str, str]:
    scheme, _, rest = value.partition(" ")
    if scheme.lower() != "bearer":
        return {}
    return {k.lower(): v for k, v in re.findall(r'(\w+)="([^"]*)"', rest)}


# The only error texts a check ever records: fixed categories, never exception text or header values (an exception
# from the HTTP layer can quote the request's headers, the anonymous token included).
PULL_ERRORS = ("network-error", "invalid-header", "token-handshake-failed", "digest-missing", "digest-mismatch")
_B64TOKEN = re.compile(r"^[A-Za-z0-9._~+/-]+=*$")               # RFC 6750 b64token: no whitespace, no control bytes


class _PullError(Exception):
    def __init__(self, category: str):
        super().__init__(category)
        self.category = category


def _call(fetch: Fetch, method: str, url: str, headers: Dict[str, str]) -> Tuple[int, Dict[str, str], bytes]:
    """fetch, with every failure reduced to its category (the exception itself is dropped, never formatted)."""
    try:
        status, rh, body = fetch(method, url, headers)
    except ValueError:
        raise _PullError("invalid-header") from None
    except Exception:                                          # noqa: BLE001 - URLError, timeouts, TLS, HTTP framing
        raise _PullError("network-error") from None
    return status, {str(k).lower(): v for k, v in (rh or {}).items()}, body or b""


def _anonymous_token(fetch: Fetch, challenge: Dict[str, str]) -> str:
    """An anonymous token from the challenge's https realm (no Authorization header). Anything else - no realm, a
    refusal, a malformed answer, a token that is not a plain b64token - is token-handshake-failed."""
    realm = challenge.get("realm", "")
    if not realm.startswith("https://"):
        raise _PullError("token-handshake-failed")
    q = urllib.parse.urlencode({k: challenge[k] for k in ("service", "scope") if k in challenge})
    try:
        status, _, body = _call(fetch, "GET", realm + ("&" if "?" in realm else "?") + q if q else realm, {})
        doc = json.loads(body.decode() or "{}") if status == 200 else {}
    except (_PullError, ValueError):
        raise _PullError("token-handshake-failed") from None
    token = doc.get("token") or doc.get("access_token") if isinstance(doc, dict) else None
    if not isinstance(token, str) or not _B64TOKEN.match(token):
        raise _PullError("token-handshake-failed")
    return token


def verify_anonymous_pull(images: List[str], fetch: Fetch, now: Callable[[], str] = X.utcnow) -> List[Dict]:
    """One manifest HEAD by digest per image, without credentials. A check passes only with HTTP 200 AND a
    Docker-Content-Digest equal to the expected digest. Each check records image, digest, URL, status, whether a Bearer
    challenge was answered anonymously, whether the digest was confirmed, a fixed error category (PULL_ERRORS or
    http-<status>; "" when it passed) and the time - never the token, a header value or exception text. Proves anonymous
    access from the checking host only (see the module docstring)."""
    out = []
    for image in images:
        host, path, dig = _image_parts(image)
        url = f"https://{host}/v2/{path}/manifests/{dig}"
        headers = {"Accept": MANIFEST_ACCEPT}
        status, answered, error = None, False, ""
        try:
            status, rh, _ = _call(fetch, "HEAD", url, headers)
            if status == 401:
                challenge = _bearer_challenge(str(rh.get("www-authenticate", "")))
                if challenge:
                    token = _anonymous_token(fetch, challenge)
                    answered, status = True, None             # a failed retry has no status of its own
                    status, rh, _ = _call(fetch, "HEAD", url, {**headers, "Authorization": f"Bearer {token}"})
            if status != 200:
                raise _PullError(f"http-{int(status)}" if isinstance(status, int) else "network-error")
            got = rh.get("docker-content-digest")
            if not got:
                raise _PullError("digest-missing")
            if got != dig:
                raise _PullError("digest-mismatch")
        except _PullError as e:
            error = e.category
        out.append({"image": image, "digest": dig, "url": url, "status": status if isinstance(status, int) else None,
                    "anonymous_challenge": answered, "digest_confirmed": error == "", "error": error,
                    "checked_at": now()})
    return out


def pull_problems(checks: List[Dict]) -> List[str]:
    out = []
    for c in checks:
        if c["error"] or c["status"] != 200 or not c["digest_confirmed"]:
            out.append(f"anonymous pull: {c['image']}: {c['error'] or 'unconfirmed'}"
                       + (f" (HTTP {c['status']})" if c["status"] is not None else ""))
    return out


def _review_times(live: Dict, reviewed: Dict) -> Dict:
    """The live plan with the reviewed check times when the checks otherwise match one to one (apply checks again; only
    the time may differ). Anything else differing still differs."""
    lv, rv = (live.get("image_pull") or {}).get("verification"), (reviewed.get("image_pull") or {}).get("verification")
    if not lv or not rv or len(lv) != len(rv):
        return live
    strip = lambda cs: [{k: v for k, v in c.items() if k != "checked_at"} for c in cs]
    if strip(lv) != strip(rv):
        return live
    out = copy.deepcopy(live)
    for c, r in zip(out["image_pull"]["verification"], rv):
        c["checked_at"] = r["checked_at"]
    return out


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
    pull = _image_pull(rel)
    pull_secrets = [{"name": pull["pull_ref"]}] if pull["mode"] == "secret" else []
    return {
        "images": {"operator": {"repository": op_repo, "tag": "", "digest": op_digest},
                   "statisticalForecaster": {"repository": st_repo, "tag": "", "digest": st_digest},
                   "pullPolicy": "IfNotPresent", "pullSecrets": pull_secrets},
        "prometheus": routes["prometheus"],
        "operator": {"replicas": 1, "watchNamespaces": [params["namespace"]],
                     "rbac": {"scaleWrite": False},       # a shadow preparation: no replica write is authorized
                     "forecasterAddress": routes["operator"]["forecasterAddress"],
                     "forecastLedger": {"enabled": True, "persistence": {"existingClaim": st["ledger_claim"]["name"]}},
                     "podLabels": dict(routes["operator"]["podLabels"]),
                     "podAnnotations": dict(routes["operator"]["podAnnotations"]),
                     "resources": copy.deepcopy(routes["operator"]["resources"]),
                     **copy.deepcopy(support)},
        "forecaster": {"engine": "statistical",
                       "statistical": {"persistence": {"existingClaim": st["state_claim"]["name"], "retain": True},
                                       "labQualification": {"configMap": ""},
                                       "resources": copy.deepcopy(routes["forecaster"]["statistical"]["resources"])},
                       "auth": {"enabled": True},
                       "tls": {"extraDNSNames": routes["forecaster"]["tls"]["extraDNSNames"]},
                       "podLabels": dict(routes["forecaster"]["podLabels"]),
                       "podAnnotations": dict(routes["forecaster"]["podAnnotations"]),
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
    if (v["operator"].get("rbac") or {}).get("scaleWrite") is not False:
        out.append("operator.rbac.scaleWrite must be false: a shadow preparation authorizes no replica write")
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
    c28 = X.import_harness()
    for k in ("operator", "forecaster"):
        for part in ("podLabels", "podAnnotations"):
            if (v[k].get(part) or {}).get(c28.render.SIDECAR_ANNOTATION) != "false":
                out.append(f"{k}.{part} must carry {c28.render.SIDECAR_ANNOTATION}: \"false\" (sidecar-free)")
    for k in ("operator", "statisticalForecaster"):
        if not str(v["images"][k]["digest"]).startswith("sha256:"):
            out.append(f"images.{k} must be digest-pinned")
    return out


def admission_problems(record: X.Record, params: Dict, s: Dict, parts: Dict) -> List[str]:
    """The complete sidecar-free and residency audits of the whole evaluation (every phase's documents plus the release
    overlay), run before every preparation plan and so again at apply time: a phase is never prepared on its own
    while the evaluation as a whole would not be admitted."""
    c28 = X.import_harness()
    objs = parts["common"] + parts["targets"] + parts["pas"] + parts["writers"]
    try:
        c28.render.audit_sidecar_free(objs)
        values = staging_values(record, params, s)
        rep = c28.render.residency_report(params, objs, chart=c28.render.chart_values(values))
    except c28.render.ParamsError as e:
        return [str(e)]
    return [f"residency: {p}" for p in rep["problems"]]


def reservation_gate(record: X.Record, kube: X.Kube) -> Tuple[Dict, List[str]]:
    """The LIVE reservation verdict every preparation plan requires (reserve.verify, recomputed again under the shared
    lock when apply re-plans): reservation PASS and rendered placement PASS on complete parameters
    (ready_for_experiment), never an empty problems list alone. Returns the compact verdict the plan records (part of
    its digest) and the blocking problems, each reason kept."""
    if not record.doc.get("reserve"):
        return {"reservation": "MISSING"}, ["reservation gate: the record has no reserve section (reserve.py first)"]
    import reserve
    out = reserve.verify(record, kube)
    gate = {k: out[k] for k in ("reservation", "placement", "ready_for_experiment", "result")}
    try:
        reserve.require_ready_for_experiment(out)
    except X.StagingError as e:
        return gate, [f"reservation gate: {p}" for p in out["problems"]] or [f"reservation gate: {e}"]
    return gate, []


def _exists(kube: X.Kube, obj: Dict) -> Optional[Dict]:
    return kube.get(obj["kind"], obj["metadata"]["name"], obj["metadata"].get("namespace"))


def _writer_refs(kube: X.Kube, namespace: str, names: List[str]) -> List[str]:
    inv = X.writer_inventory(kube, namespace, names)
    return [f"{X.ident_str(w)} already writes {namespace}/{n}" for n, ws in sorted(inv.items()) for w in ws]


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


def plan(record: X.Record, kube: X.Kube, phase: str, fetch: Optional[Fetch] = None) -> Dict:
    if phase not in PHASES:
        raise X.StagingError(f"unknown phase {phase}")
    params = X.load_params(record)
    s = _section(record)
    parts = split(params)
    problems: List[str] = admission_problems(record, params, s, parts)
    gate, gate_problems = reservation_gate(record, kube)
    problems += gate_problems
    out = {"phase": phase, "reservation_gate": gate}
    if phase == "values":
        v = staging_values(record, params, s)
        problems += values_problems(v, params)
        pull = _image_pull(s["release"])
        out["image_pull"] = {"mode": pull["mode"]}
        if pull["mode"] == "anonymous":
            checks = verify_anonymous_pull([s["release"][k] for k in ("operator_image", "statistical_image")],
                                           fetch or _http_fetch)
            out["image_pull"]["verification"] = checks
            problems += pull_problems(checks)
        path = os.path.join(record.evidence_dir, "release-staging.yaml")
        if os.path.exists(path):
            problems.append(f"{path} exists (never overwritten)")
        text = yaml.safe_dump(v, sort_keys=True)
        out.update({"file": path, "values_sha256": X.sha256_bytes(text.encode()), "values": v})
        return X.new_plan(HELPER, record, [{"write": path, "sha256": out["values_sha256"]}], problems=problems, **out)
    objs = _objects_for(phase, record, params, s, parts)
    create, existing, ours = [], [], []
    entries = X.receipts(record, HELPER)
    mine = {e["data"]["mark"] for e in X.applied_intents(entries)}
    unknown = X.open_intents(entries)
    if unknown:
        problems.append("the journal has intents with unknown outcomes: run recover first")
    for o in objs:
        m = o["metadata"]
        live = _exists(kube, o)
        mark = ((live or {}).get("metadata", {}).get("annotations") or {}).get(X.INTENT_ANNOTATION)
        if live is not None and mark in mine:          # created by this run's journalled create (e.g. before a crash)
            ours.append({"kind": o["kind"], "namespace": m.get("namespace"), "name": m["name"],
                         "uid": live["metadata"]["uid"]})
            continue
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
        names += sorted(r["name"] for r in ours if r["kind"] == "Deployment")
        problems += _writer_refs(kube, params["namespace"], names)
        if os.path.exists(os.path.join(record.evidence_dir, "targets.json")):
            problems.append("targets.json exists: targets are created once per run")
    if phase == "common":
        rq = [o for o in create if o["kind"] == "ResourceQuota"]
        if len(rq) != 1 or rq[0]["spec"]["hard"]["pods"] != str(params["quota"]["pods_normal"]):
            problems.append("the scoped quota must start at its approved normal limit")
    actions = [{"create": o["kind"], "namespace": o["metadata"].get("namespace"), "name": o["metadata"]["name"],
                "sha256": X.json_digest(o)} for o in create]
    return X.new_plan(HELPER, record, actions, existing=existing, already_created_by_this_run=ours,
                      problems=problems, **out)


def apply(record: X.Record, kube: X.Kube, phase: str, reviewed: Dict, reviewed_digest: str,
          fetch: Optional[Fetch] = None) -> Dict:
    with X.mutating(record, kube, HELPER, {"phase": phase}):
        p = plan(record, kube, phase, fetch)            # validated under the shared lock (anonymous pulls checked again)
        X.require_live(reviewed, reviewed_digest, HELPER, record, _review_times(p, reviewed))
        if p["problems"]:
            raise X.StagingError("the plan has problems: " + "; ".join(p["problems"]))
        if phase == "values":
            text = yaml.safe_dump(p["values"], sort_keys=True)
            sha = X.write_new(p["file"], text)
            return X.receipt(record, HELPER, "values-written", {"phase": phase, "plan_sha256": reviewed_digest,
                                                                "file": p["file"], "sha256": sha,
                                                                "image_pull": p["image_pull"]})
        params = X.load_params(record)
        s = _section(record)
        objs = {(o["kind"], o["metadata"].get("namespace"), o["metadata"]["name"]): o
                for o in _objects_for(phase, record, params, s, split(params))}
        created = []
        for c in p["actions"]:
            o = objs[(c["create"], c["namespace"], c["name"])]
            if X.json_digest(o) != c["sha256"]:
                raise X.StagingError(f"{c['create']}/{c['name']} rendered differently from the reviewed plan; "
                                     "refusing")
            created.append(X.object_ref(kube.create(o)))
        if phase == "targets":
            uids = {r["name"]: r["uid"] for (k, ns, n), r in created_objects(record).items()
                    if k == "Deployment" and ns == params["namespace"] and n.startswith("c28-") and n != "c28-ingress"}
            X.write_new(os.path.join(record.evidence_dir, "targets.json"),
                        json.dumps({"schema": "c28-targets/v1", "run_id": record.run_id,
                                    "cluster_uid": record.cluster_uid, "namespace": params["namespace"],
                                    "deployments": uids}, indent=1, sort_keys=True) + "\n")
        return X.receipt(record, HELPER, "phase-complete", {"phase": phase, "plan_sha256": reviewed_digest,
                                                            "created": created,
                                                            "already_created": p["already_created_by_this_run"]})


def created_objects(record: X.Record) -> Dict[tuple, Dict]:
    """Everything this run's prepare created, from its journal (applied creates, live or recovered)."""
    out = {}
    for it in X.applied_intents(X.receipts(record, HELPER)):
        d = it["data"]
        if d["op"] == "create":
            out[(d["kind"], d["namespace"], d["name"])] = {"kind": d["kind"], "namespace": d["namespace"],
                                                           "name": d["name"], "uid": d["result_uid"]}
    return out


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
    pas = {r["name"]: r for r in seen if r["kind"] == "PredictiveAutoscaler"}
    for name, writers in sorted(X.writer_inventory(kube, params["namespace"], targets).items()):
        want = [{"kind": "PredictiveAutoscaler", "namespace": params["namespace"], "name": name,
                 "uid": pas[name]["uid"]}] if name in pas else []
        if writers != want:
            got = [f"{X.ident_str(w)}#{w['uid']}" for w in writers] or "none"
            problems.append(f"{name}: writers {got}, expected {[X.ident_str(w) for w in want] or 'none'} "
                            "outside a session")
        live = kube.get("Deployment", name, params["namespace"])
        if live and live["spec"].get("replicas") != 2:
            problems.append(f"{name} has {live['spec'].get('replicas')} replicas, not the fixed 2, outside a session")
    problems += injection_problems(kube, params)
    return {"helper": HELPER, "action": "verify", "run_id": record.run_id, "objects": seen, "problems": problems,
            "result": "PASS" if not problems else "FAIL", "storage_class": s["storage"]["storage_class"]}


def injection_problems(kube: X.Kube, params: Dict) -> List[str]:
    """Admitted evaluation pods (the run's label in the fixture and control namespaces, and the evaluation release's
    pods) must carry no istio-proxy/istio-init container: injection despite the opt-out is a problem."""
    return X.injection_problems(kube, params)


def main(argv=None, api=None, fetch: Optional[Fetch] = None) -> int:
    ap = argparse.ArgumentParser(prog="prepare.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", required=True)
    ap.add_argument("command", nargs="?", default="plan", choices=["plan", "apply", "verify", "recover-plan", "recover"])
    ap.add_argument("--phase", default="", choices=("",) + PHASES)
    ap.add_argument("--plan", default="")
    ap.add_argument("--plan-digest", default="")
    args = ap.parse_args(argv)
    try:
        record = X.load_record(args.record, ("prepare",))
        kube = record.kube(api)
        if args.command == "recover-plan":
            X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
        elif args.command == "recover":
            if not args.plan:
                raise X.StagingError("recover needs --plan <the reviewed recover plan> and --plan-digest")
            X.emit(X.recover(record, kube, X.load_plan(args.plan), args.plan_digest))
        elif args.command == "plan":
            X.emit([X.cli_plan(record, HELPER, plan(record, kube, ph, fetch), f"plan-{ph}")
                    for ph in ([args.phase] if args.phase else PHASES)])
        elif args.command == "apply":
            if not args.phase or not args.plan:
                raise X.StagingError("apply needs --phase and --plan: phases are applied one reviewed plan at a time")
            X.emit(apply(record, kube, args.phase, X.load_plan(args.plan), args.plan_digest, fetch))
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
