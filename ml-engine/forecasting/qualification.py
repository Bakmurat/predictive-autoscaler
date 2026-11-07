"""Lab qualification versus release qualification (evaluation manifest C2.8, prerequisite 3).

Release qualification is the product decision that G0-G5 passed (forecasting.service.RELEASE_GATES_PASSED, false in
every build so far): until then no forecast is ever PredictionReady, so no answer is predictive_eligible. G5 needs
controlled Active experiments before that, so a LAB qualification may stand in for the release gate, and only there:

  - only in an EVALUATION BUILD: the image records its source tree and evaluation flag at build time
    (/app/build-info.json from Dockerfile.statistical's SOURCE_TREE and EVALUATION_BUILD); a release image refuses to
    start with a lab qualification at all;
  - only in the image it names: the image digest recorded in the file must equal the digest the pod was started from
    (FORECAST_IMAGE_DIGEST, set by the chart from the digest-pinned image reference the kubelet verified);
  - only from a reviewed, hash-identified file (lab-qualification/v1) naming that exact source tree, the run-lock it
    belongs to, the G0-G4 results it rests on (each must be "pass"), at least two distinct reviewers (executor and
    reviewer), a validity window of at most 60 days, and its scope: one cluster UID (the kube-system namespace's UID,
    read live), its namespaces and every target Deployment by namespace, name AND UID;
  - only for requests whose live cluster, namespace and target match that scope at that moment, while it is valid.
It replaces exactly one input: the release gate (Global.replay_suite_passed). Every runtime guard stays as it is:
identity, settings, calibration, coverage, availability, prospective evidence, selection and its hysteresis, the
issuance deadline, caps, telemetry, and the operator's own verification and write guards. It forges no eligibility,
writes nothing to the journal but its digest beside the verdict it affected, changes no threshold and offers no
benchmark switch. Anything malformed or out of scope fails closed (no lab qualification).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import FrozenSet, Optional, Tuple

from forecasting.contracts import sha256_hex

SCHEMA = "lab-qualification/v1"
BUILD_INFO_PATH = "/app/build-info.json"
GATES = ("G0", "G1", "G2", "G3", "G4")
MAX_VALIDITY_SECONDS = 60 * 86400
MAX_TARGETS = 16
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_TREE = re.compile(r"^[0-9a-f]{40}$")
_KEYS = {"schema", "source_tree", "image_digest", "run_lock_sha256", "gates", "reviewers", "not_before", "expires",
         "scope"}


class QualificationRefused(Exception):
    """A lab qualification that must not be honoured; the service refuses to start with it."""


@dataclass(frozen=True)
class BuildInfo:
    source_tree: str
    evaluation_build: bool


def read_build_info(path: str = BUILD_INFO_PATH) -> BuildInfo:
    """The image's build record; without one (a checkout, a release image without it) nothing is an evaluation build."""
    try:
        with open(path, "rb") as fh:
            doc = json.loads(fh.read())
        tree, flag = doc.get("source_tree"), doc.get("evaluation_build")
        if isinstance(tree, str) and isinstance(flag, bool):
            return BuildInfo(tree, flag)
    except (OSError, ValueError, AttributeError):
        pass
    return BuildInfo("unknown", False)


@dataclass(frozen=True)
class LabQualification:
    sha256: str
    source_tree: str
    image_digest: str
    run_lock_sha256: str
    cluster_uid: str
    namespaces: FrozenSet[str]
    targets: FrozenSet[Tuple[str, str, str]]       # (namespace, name, uid)
    not_before: int
    expires: int

    def covers(self, cluster_uid: Optional[str], autoscaler_namespace: str, namespace: str, name: str, uid: str,
               now_s: int) -> bool:
        """The live cluster, the autoscaler's namespace and its live target are all in scope, and it is valid now."""
        return (cluster_uid is not None and cluster_uid == self.cluster_uid
                and autoscaler_namespace in self.namespaces and namespace in self.namespaces
                and (namespace, name, uid) in self.targets and self.not_before <= now_s < self.expires)

    def summary(self) -> dict:
        return {"sha256": self.sha256, "image_digest": self.image_digest, "run_lock_sha256": self.run_lock_sha256,
                "cluster_uid": self.cluster_uid,
                "namespaces": sorted(self.namespaces), "targets": len(self.targets), "expires": self.expires}


def _ts(value, what: str) -> int:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise QualificationRefused(f"{what} must be an RFC 3339 UTC time ending in Z")
    try:
        return int(datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
    except ValueError as e:
        raise QualificationRefused(f"{what}: {e}") from e


def parse(data: bytes, build: BuildInfo, now_s: int, image_digest: Optional[str]) -> LabQualification:
    if not build.evaluation_build:
        raise QualificationRefused("this is a release build: it never honours a lab qualification")
    if not _TREE.match(build.source_tree):
        raise QualificationRefused("the evaluation build records no source tree")
    if not (isinstance(image_digest, str) and _DIGEST.match(image_digest)):
        raise QualificationRefused("the pod's image digest is unknown (run the image pinned by digest)")
    try:
        doc = json.loads(data)
    except ValueError as e:
        raise QualificationRefused(f"not JSON: {e}") from e
    if not isinstance(doc, dict) or set(doc) != _KEYS or doc["schema"] != SCHEMA:
        raise QualificationRefused(f"a {SCHEMA} document needs exactly the keys {sorted(_KEYS)}")
    if doc["source_tree"] != build.source_tree:
        raise QualificationRefused("the qualification names another source tree than this build")
    if doc["image_digest"] != image_digest:
        raise QualificationRefused("the qualification names another image digest than this pod runs")
    if not (isinstance(doc["run_lock_sha256"], str) and _SHA256.match(doc["run_lock_sha256"])):
        raise QualificationRefused("run_lock_sha256 must be a sha256")
    gates = doc["gates"]
    if not isinstance(gates, dict) or set(gates) != set(GATES) or any(gates[g] != "pass" for g in GATES):
        raise QualificationRefused(f"every gate of {GATES} must be recorded as pass")
    reviewers = doc["reviewers"]
    if not isinstance(reviewers, list) or len({r for r in reviewers if isinstance(r, str) and r.strip()}) < 2:
        raise QualificationRefused("at least two distinct reviewers (executor and reviewer) are required")
    nb, exp = _ts(doc["not_before"], "not_before"), _ts(doc["expires"], "expires")
    if not nb < exp or exp - nb > MAX_VALIDITY_SECONDS:
        raise QualificationRefused("the validity window must be positive and at most 60 days")
    if now_s >= exp:
        raise QualificationRefused("the qualification has expired")
    scope = doc["scope"]
    if not isinstance(scope, dict) or set(scope) != {"cluster_uid", "namespaces", "targets"}:
        raise QualificationRefused("scope needs exactly cluster_uid, namespaces and targets")
    cluster, namespaces, targets = scope["cluster_uid"], scope["namespaces"], scope["targets"]
    if not isinstance(cluster, str) or not cluster:
        raise QualificationRefused("scope.cluster_uid is required")
    if not isinstance(namespaces, list) or not namespaces or not all(isinstance(n, str) and n for n in namespaces):
        raise QualificationRefused("scope.namespaces must list at least one namespace")
    if not isinstance(targets, list) or not 1 <= len(targets) <= MAX_TARGETS:
        raise QualificationRefused(f"scope.targets must list 1..{MAX_TARGETS} targets")
    out = set()
    for t in targets:
        if not isinstance(t, dict) or set(t) != {"namespace", "name", "uid"} or \
                not all(isinstance(t[k], str) and t[k] for k in t) or t["namespace"] not in namespaces:
            raise QualificationRefused(f"a target needs namespace (in scope), name and uid: {t!r}")
        out.add((t["namespace"], t["name"], t["uid"]))
    return LabQualification(sha256=sha256_hex(data), source_tree=build.source_tree, image_digest=image_digest,
                            run_lock_sha256=doc["run_lock_sha256"], cluster_uid=cluster,
                            namespaces=frozenset(namespaces), targets=frozenset(out), not_before=nb, expires=exp)


def load(path: Optional[str], build: BuildInfo, now_s: int,
         image_digest: Optional[str] = None) -> Optional[LabQualification]:
    """None without a path; a verified qualification; or QualificationRefused."""
    if not path:
        return None
    try:
        with open(path, "rb") as fh:
            data = fh.read(1 << 16)
    except OSError as e:
        raise QualificationRefused(f"unreadable: {e}") from e
    return parse(data, build, now_s, image_digest)


def cluster_uid(reader) -> Optional[str]:
    """The cluster's identity: the kube-system namespace UID (None when it cannot be read: fail closed)."""
    try:
        ns = reader.get("/api/v1/namespaces/kube-system")
    except Exception:
        return None
    uid = ((ns or {}).get("metadata") or {}).get("uid") if isinstance(ns, dict) else None
    return uid if isinstance(uid, str) and uid else None


__all__ = ["BuildInfo", "LabQualification", "QualificationRefused", "cluster_uid", "load", "parse",
           "read_build_info"]
