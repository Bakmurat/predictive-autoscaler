"""Demonstration qualification: a narrowly scoped live demonstration, never an evaluation or a release.

A lab qualification (forecasting.qualification) records G0-G4 as passed. A demonstration asserts no such thing, so it
has its own document type, its own parser and its own, narrower rules. Like the lab qualification it replaces exactly
one input, the release gate (Global.replay_suite_passed), and nothing else: identity, settings, calibration, coverage,
availability, prospective evidence, selection and its hysteresis, the issuance deadline, caps, telemetry and the
operator's own verification and write guards all decide as always. An eligible answer authorizes nothing by itself: the
operator still needs its own verdict, Active mode and the RBAC write, so the document may be mounted while every
replica write is denied.

Two reviewed files, both mounted read-only:

  demonstration-qualification/v1   who may rely on it, where, for how long
    schema, source_tree, image_digest, contract_sha256, reviewers, not_before, expires, families,
    scope {cluster_uid, namespace, targets [{namespace, name, uid}]}
  demonstration-contract/v1        the running configuration it was reviewed for
    schema, checked {qualification {expires_not_after}, families, cadence {grid_seconds, update_interval_seconds},
    targets [{namespace, name, uid, min_replicas, max_replicas, capacity_per_replica, settings_sha256}]}, and
    optionally unchecked (any JSON object: the rest of the reviewed contract, hashed with the file and never
    interpreted here)

settings_sha256 is the target's autoscaler's resolved forecast settings fingerprint
(forecasting.contracts.ForecastSettings.fingerprint: risk, calibration, trust, predictive extra, grid, history,
horizon, families), so a reviewed trust or calibration setting cannot change under the same contract.
capacity_per_replica is compared as EXACT TEXT with spec.metrics.requests.capacityPerReplica ("5.5" is not "5.50"):
generate the contract from the admitted PredictiveAutoscaler object, never from a number.
Two more checked keys are RESERVED and not enforced by this build: calibration_policy_revision and
trust_policy_revision. A contract that carries either is refused, so nobody can believe they are compared before they
are; a later build makes them required and compares them, under the same schema name.

Neither file has a gates field: a demonstration qualification grants no G0-G5 pass and says nothing about them, and a
document that carries one is refused. The hash dependency runs one way: the qualification names the contract's sha256;
the contract states what a qualification must satisfy (its latest expiry, families and targets) and never the
qualification's hash.

It is honoured
  - only in an EVALUATION BUILD, only for the image digest the pod runs and only for the source tree it names (the
    three rules of the lab qualification, unchanged);
  - only with the contract it names: the mounted contract's sha256 must equal contract_sha256, the qualification's
    families and targets must equal the contract's, and it must expire no later than the contract allows;
  - only with at least two distinct reviewers and a validity window that is positive and at most 21 days;
  - only for one cluster UID (the kube-system namespace's UID, read live), ONE namespace and one or two targets, each
    by namespace, name AND UID;
  - only while BOTH mounted files still hold exactly the bytes admitted at start-up: they are read and hashed again
    at every authorization (no cache, so the bound is zero: the next authorization after a change sees it). A missing,
    unreadable, oversized or changed file withdraws the basis; nothing new is ever loaded without a restart, and the
    basis returns only if the mounted bytes are again exactly the admitted ones;
  - only for a request whose live configuration is the contract's: the autoscaler's families within the contract's,
    the 60 s grid and the 60 s update interval, the resolved forecast settings fingerprint, the target's replica
    bounds and its capacityPerReplica;
  - only for an answer whose valid_until is not after the expiry, and whose SELECTED family is one it lists
    (configuration names: seasonalNaive, statistical; a family this build does not know is never allowed);
  - never beside a lab qualification, and never when the release gates have passed (no silent precedence).
Anything malformed, out of scope or different from the contract fails closed: at start-up the service refuses to start;
per request the answer is simply not eligible, and the journal records why (mount_refusal(), refusal(), REFUSALS).
Withdrawing the qualification stops further authorization by the forecaster. It does not recall an answer the operator
has already received and cached for its validity window: revoking the scale permission is the emergency stop.
Every digest, tree, time, name and number is matched whole (fullmatch): no trailing newline or control character
passes. Reviewers are compared stripped and without regard to case.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from typing import FrozenSet, Mapping, Optional, Tuple

from forecasting import policy_sim as P
from forecasting.contracts import FAMILIES, FAMILY_MODELS, sha256_hex
from forecasting.qualification import BuildInfo, QualificationRefused, _ts

SCHEMA = "demonstration-qualification/v1"
CONTRACT_SCHEMA = "demonstration-contract/v1"
MAX_VALIDITY_SECONDS = 21 * 86400
MAX_TARGETS = 2
MAX_BYTES = 1 << 16
GRID_SECONDS = 60                    # the demonstration's cadence: one issuance per grid step, nothing else
UPDATE_INTERVAL_SECONDS = 60
# Why a request is not covered, in the order the checks run (the journal's release_gate.refused and the metric label).
REFUSALS = ("release", "qualification-file", "contract-file", "cluster", "namespace", "target", "validity",
            "families", "cadence", "settings", "bounds", "capacity", "family")
_KEYS = {"schema", "source_tree", "image_digest", "contract_sha256", "reviewers", "not_before", "expires", "families",
         "scope"}
_CONTRACT_KEYS = {"schema", "checked"}
_CHECKED_KEYS = {"qualification", "families", "cadence", "targets"}
# Reserved for the calibration and trust policy revisions of the bound identity: not enforced yet, so not accepted yet.
RESERVED_CHECKED_KEYS = ("calibration_policy_revision", "trust_policy_revision")
_TARGET_KEYS = {"namespace", "name", "uid"}
_CONTRACT_TARGET_KEYS = _TARGET_KEYS | {"min_replicas", "max_replicas", "capacity_per_replica", "settings_sha256"}
# Every pattern is used with fullmatch only: "$" would let a trailing newline through.
_CAPACITY = re.compile(r"[0-9]+(\.[0-9]+)?")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_TREE = re.compile(r"[0-9a-f]{40}")
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_LABEL = re.compile(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?")                               # a namespace
_SUBDOMAIN = re.compile(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*")   # a Deployment name
_UID = re.compile(r"[0-9A-Za-z]([-0-9A-Za-z._:]{0,126}[0-9A-Za-z])?")                      # an object UID


def _json(data: bytes, what: str):
    def pairs(items):
        out = {}
        for k, v in items:
            if k in out:
                raise QualificationRefused(f"{what}: the key {k!r} appears twice")
            out[k] = v
        return out
    try:
        return json.loads(data, object_pairs_hook=pairs)
    except ValueError as e:
        raise QualificationRefused(f"{what} is not JSON: {e}") from e


def _int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _whole(pattern, value, limit: Optional[int] = None) -> bool:
    return isinstance(value, str) and (limit is None or len(value) <= limit) and pattern.fullmatch(value) is not None


def _time(value, what: str) -> int:
    if not _whole(_TIME, value):
        raise QualificationRefused(f"{what} must be an RFC 3339 UTC time ending in Z (YYYY-MM-DDTHH:MM:SSZ)")
    return _ts(value, what)


def _families(value, where: str) -> FrozenSet[str]:
    if not isinstance(value, list) or not value or not all(isinstance(f, str) for f in value) or \
            len(set(value)) != len(value) or any(f not in FAMILIES for f in value):
        raise QualificationRefused(f"{where} must list distinct forecast families of {FAMILIES} (at least one)")
    return frozenset(value)


def _targets(value, keys, where: str):
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_TARGETS:
        raise QualificationRefused(f"{where} must list 1..{MAX_TARGETS} targets")
    for t in value:
        if not isinstance(t, dict) or set(t) != keys:
            raise QualificationRefused(f"a target of {where} needs exactly {sorted(keys)}: {t!r}")
        if not (_whole(_LABEL, t["namespace"]) and _whole(_SUBDOMAIN, t["name"], 253) and _whole(_UID, t["uid"])):
            raise QualificationRefused(f"a target of {where} needs a namespace (DNS-1123 label), a name (DNS-1123 "
                                       f"subdomain) and a uid: {t!r}")
    if len({(t["namespace"], t["name"]) for t in value}) != len(value) or len({t["uid"] for t in value}) != len(value):
        raise QualificationRefused(f"{where} must name each target and each uid once")
    return value


@dataclass(frozen=True)
class ContractTarget:
    namespace: str
    name: str
    uid: str
    min_replicas: int
    max_replicas: int
    capacity_per_replica: str            # the exact text of spec.metrics.requests.capacityPerReplica
    settings_sha256: str                 # ForecastSettings.fingerprint() of the target's autoscaler, as reviewed


@dataclass(frozen=True)
class DemonstrationContract:
    """The machine-checked part of a reviewed demonstration contract."""
    sha256: str
    expires_not_after: int
    families: FrozenSet[str]             # configuration names (spec.prediction.families)
    grid_seconds: int
    update_interval_seconds: int
    targets: Tuple[ContractTarget, ...]

    def target(self, namespace: str, name: str, uid: str) -> Optional[ContractTarget]:
        return next((t for t in self.targets if (t.namespace, t.name, t.uid) == (namespace, name, uid)), None)


def parse_contract(data: bytes) -> DemonstrationContract:
    doc = _json(data, "the demonstration contract")
    if not isinstance(doc, dict) or not _CONTRACT_KEYS <= set(doc) or set(doc) - _CONTRACT_KEYS - {"unchecked"} or \
            doc["schema"] != CONTRACT_SCHEMA:
        raise QualificationRefused(f"a {CONTRACT_SCHEMA} document needs exactly the keys {sorted(_CONTRACT_KEYS)} "
                                   f"(and optionally unchecked)")
    if "unchecked" in doc and not isinstance(doc["unchecked"], dict):
        raise QualificationRefused("the contract's unchecked part must be an object")
    checked = doc["checked"]
    reserved = sorted(set(checked) & set(RESERVED_CHECKED_KEYS)) if isinstance(checked, dict) else []
    if reserved:
        raise QualificationRefused(f"checked.{reserved[0]} is reserved: this build does not compare the calibration "
                                   f"or trust policy revision, so it does not accept a contract that states one")
    if not isinstance(checked, dict) or set(checked) != _CHECKED_KEYS:
        raise QualificationRefused(f"the contract's checked part needs exactly the keys {sorted(_CHECKED_KEYS)}")
    qual = checked["qualification"]
    if not isinstance(qual, dict) or set(qual) != {"expires_not_after"}:
        raise QualificationRefused("checked.qualification needs exactly expires_not_after")
    not_after = _time(qual["expires_not_after"], "checked.qualification.expires_not_after")
    families = _families(checked["families"], "checked.families")
    cadence = checked["cadence"]
    if not isinstance(cadence, dict) or set(cadence) != {"grid_seconds", "update_interval_seconds"} or \
            not all(_int(v) for v in cadence.values()):
        raise QualificationRefused("checked.cadence needs exactly the integers grid_seconds and "
                                   "update_interval_seconds")
    if (cadence["grid_seconds"], cadence["update_interval_seconds"]) != (GRID_SECONDS, UPDATE_INTERVAL_SECONDS):
        raise QualificationRefused(f"a demonstration runs only on the {GRID_SECONDS} s grid with a "
                                   f"{UPDATE_INTERVAL_SECONDS} s update interval")
    out = []
    for t in _targets(checked["targets"], _CONTRACT_TARGET_KEYS, "checked.targets"):
        lo, hi, cap = t["min_replicas"], t["max_replicas"], t["capacity_per_replica"]
        if not (_int(lo) and _int(hi) and 1 <= lo <= hi):
            raise QualificationRefused(f"a contract target needs integer replica bounds 1 <= min <= max: {t!r}")
        if not _whole(_CAPACITY, cap, 32) or \
                P.capacity_rpm({"metrics": {"requests": {"capacityPerReplica": cap}}}) is None:
            raise QualificationRefused(f"capacity_per_replica must be a plain decimal string above 0 that the policy "
                                       f"admits (requests/second per replica): {t!r}")
        if not _whole(_SHA256, t["settings_sha256"]):
            raise QualificationRefused(f"settings_sha256 must be the sha256 of the autoscaler's resolved forecast "
                                       f"settings: {t!r}")
        out.append(ContractTarget(t["namespace"], t["name"], t["uid"], lo, hi, cap, t["settings_sha256"]))
    return DemonstrationContract(sha256=sha256_hex(data), expires_not_after=not_after, families=families,
                                 grid_seconds=GRID_SECONDS, update_interval_seconds=UPDATE_INTERVAL_SECONDS,
                                 targets=tuple(out))


@dataclass(frozen=True)
class DemonstrationQualification:
    sha256: str
    source_tree: str
    image_digest: str
    contract: DemonstrationContract      # the contract it names, loaded and hash-verified
    cluster_uid: str
    namespace: str
    targets: FrozenSet[Tuple[str, str, str]]       # (namespace, name, uid)
    families: FrozenSet[str]             # configuration names (spec.prediction.families)
    not_before: int
    expires: int
    mounted: Optional[Tuple[str, str]] = None      # (qualification path, contract path) it was loaded from

    @property
    def contract_sha256(self) -> str:
        return self.contract.sha256

    def mount_refusal(self) -> Optional[str]:
        """None while both mounted files hold exactly the admitted bytes right now; otherwise which one does not
        (missing, unreadable, oversized or changed). Read and hashed on every call, through the path (a ConfigMap
        update swaps a symlink), never from a cache or a modification time. Nothing read here is ever admitted."""
        if self.mounted is None:
            return "qualification-file"
        for path, admitted, reason in ((self.mounted[0], self.sha256, "qualification-file"),
                                       (self.mounted[1], self.contract.sha256, "contract-file")):
            try:
                if sha256_hex(_read(path, reason)) != admitted:
                    return reason
            except QualificationRefused:
                return reason
        return None

    def allows(self, model: Optional[str]) -> bool:
        """The selected model family (wire name, e.g. seasonal_naive) is one of the listed families."""
        return isinstance(model, str) and model in {FAMILY_MODELS[f] for f in self.families if f in FAMILY_MODELS}

    def refusal(self, *, cluster_uid: Optional[str], autoscaler_namespace: str, namespace: str, name: str, uid: str,
                now_s: int, valid_until: int, spec: Mapping, families, sample_interval_seconds: int,
                settings_sha256: Optional[str], selected: Optional[str] = None) -> Optional[str]:
        """None when the demonstration basis holds for this request right now, otherwise the first reason of REFUSALS
        it does not: the live cluster, the autoscaler's namespace and its live target are in scope; it is valid now
        and at the answer's valid_until; the live configuration is the contract's (settings_sha256 is the live
        autoscaler's ForecastSettings.fingerprint()); and, once a family is selected (the model's wire name), that
        family is listed."""
        if cluster_uid is None or cluster_uid != self.cluster_uid:
            return "cluster"
        if autoscaler_namespace != self.namespace or namespace != self.namespace:
            return "namespace"
        target = self.contract.target(namespace, name, uid)
        if (namespace, name, uid) not in self.targets or target is None:
            return "target"
        if not self.not_before <= now_s < self.expires or valid_until > self.expires:
            return "validity"
        try:
            live = frozenset(families)
        except TypeError:
            return "families"
        if not live or not live <= self.contract.families or not live <= self.families:
            return "families"
        spec = spec if isinstance(spec, Mapping) else {}
        prediction = spec.get("prediction")
        update = prediction.get("updateIntervalSeconds") if isinstance(prediction, Mapping) else None
        if sample_interval_seconds != self.contract.grid_seconds or not _int(update) or \
                update != self.contract.update_interval_seconds:
            return "cadence"
        if not isinstance(settings_sha256, str) or settings_sha256 != target.settings_sha256:
            return "settings"
        lo, hi = spec.get("minReplicas"), spec.get("maxReplicas")
        if not (_int(lo) and _int(hi)) or (lo, hi) != (target.min_replicas, target.max_replicas):
            return "bounds"
        metrics = spec.get("metrics")
        requests = metrics.get("requests") if isinstance(metrics, Mapping) else None
        cap = requests.get("capacityPerReplica") if isinstance(requests, Mapping) else None
        if not (_int(cap) or isinstance(cap, str)) or str(cap) != target.capacity_per_replica or \
                P.capacity_rpm(spec) is None:
            return "capacity"
        if selected is not None and not self.allows(selected):
            return "family"
        return None

    def summary(self) -> dict:
        return {"sha256": self.sha256, "image_digest": self.image_digest, "contract_sha256": self.contract_sha256,
                "cluster_uid": self.cluster_uid, "namespace": self.namespace, "targets": len(self.targets),
                "families": sorted(self.families), "expires": self.expires}


def parse(data: bytes, contract_data: bytes, build: BuildInfo, now_s: int,
          image_digest: Optional[str]) -> DemonstrationQualification:
    if not build.evaluation_build:
        raise QualificationRefused("this is a release build: it never honours a demonstration qualification")
    if not _whole(_TREE, build.source_tree):
        raise QualificationRefused("the evaluation build records no source tree")
    if not _whole(_DIGEST, image_digest):
        raise QualificationRefused("the pod's image digest is unknown (run the image pinned by digest)")
    doc = _json(data, "the demonstration qualification")
    if isinstance(doc, dict) and "gates" in doc:
        raise QualificationRefused("a demonstration qualification carries no gates: it grants no G0-G5 pass")
    if not isinstance(doc, dict) or set(doc) != _KEYS or doc["schema"] != SCHEMA:
        raise QualificationRefused(f"a {SCHEMA} document needs exactly the keys {sorted(_KEYS)}")
    if doc["source_tree"] != build.source_tree:
        raise QualificationRefused("the qualification names another source tree than this build")
    if doc["image_digest"] != image_digest:
        raise QualificationRefused("the qualification names another image digest than this pod runs")
    if not _whole(_SHA256, doc["contract_sha256"]):
        raise QualificationRefused("contract_sha256 must be a sha256")
    reviewers = doc["reviewers"]
    if not isinstance(reviewers, list) or not all(isinstance(r, str) and r.strip() for r in reviewers) or \
            len({r.strip().casefold() for r in reviewers}) < 2:
        raise QualificationRefused("at least two distinct reviewers (executor and reviewer) are required")
    nb, exp = _time(doc["not_before"], "not_before"), _time(doc["expires"], "expires")
    if not nb < exp or exp - nb > MAX_VALIDITY_SECONDS:
        raise QualificationRefused("the validity window must be positive and at most 21 days")
    if now_s >= exp:
        raise QualificationRefused("the qualification has expired")
    families = _families(doc["families"], "families")
    scope = doc["scope"]
    if not isinstance(scope, dict) or set(scope) != {"cluster_uid", "namespace", "targets"}:
        raise QualificationRefused("scope needs exactly cluster_uid, namespace and targets")
    cluster, namespace = scope["cluster_uid"], scope["namespace"]
    if not _whole(_UID, cluster):
        raise QualificationRefused("scope.cluster_uid is required (the kube-system namespace's UID)")
    if not _whole(_LABEL, namespace):
        raise QualificationRefused("scope.namespace must name exactly one namespace (a DNS-1123 label)")
    targets = _targets(scope["targets"], _TARGET_KEYS, "scope.targets")
    if any(t["namespace"] != namespace for t in targets):
        raise QualificationRefused("every target of scope.targets must be in scope.namespace")
    named = frozenset((t["namespace"], t["name"], t["uid"]) for t in targets)
    # The contract it names: the mounted bytes, hash-verified, then compared field by field.
    if sha256_hex(contract_data) != doc["contract_sha256"]:
        raise QualificationRefused("the mounted demonstration contract is not the one the qualification names "
                                   "(contract_sha256 differs)")
    contract = parse_contract(contract_data)
    if exp > contract.expires_not_after:
        raise QualificationRefused("the qualification expires after the contract allows (expires_not_after)")
    if families != contract.families:
        raise QualificationRefused("the qualification's families differ from the contract's")
    if named != frozenset((t.namespace, t.name, t.uid) for t in contract.targets):
        raise QualificationRefused("the qualification's targets differ from the contract's")
    return DemonstrationQualification(sha256=sha256_hex(data), source_tree=build.source_tree,
                                      image_digest=image_digest, contract=contract, cluster_uid=cluster,
                                      namespace=namespace, targets=named, families=families, not_before=nb, expires=exp)


def _read(path: str, what: str) -> bytes:
    try:
        with open(path, "rb") as fh:
            data = fh.read(MAX_BYTES + 1)
    except OSError as e:
        raise QualificationRefused(f"{what} is unreadable: {e}") from e
    if len(data) > MAX_BYTES:
        raise QualificationRefused(f"{what} exceeds {MAX_BYTES} bytes")
    return data


def load(path: Optional[str], contract_path: Optional[str], build: BuildInfo, now_s: int,
         image_digest: Optional[str] = None) -> Optional[DemonstrationQualification]:
    """None without either path; a verified qualification with its verified contract, remembering the two paths so
    the mounted bytes can be checked again at every authorization; or QualificationRefused (one path without the other
    included)."""
    if not path and not contract_path:
        return None
    if not path or not contract_path:
        raise QualificationRefused("a demonstration qualification and its demonstration contract are mounted together "
                                   "or not at all")
    q = parse(_read(path, "the demonstration qualification"), _read(contract_path, "the demonstration contract"),
              build, now_s, image_digest)
    return replace(q, mounted=(path, contract_path))


__all__ = ["CONTRACT_SCHEMA", "ContractTarget", "DemonstrationContract", "DemonstrationQualification", "REFUSALS",
           "RESERVED_CHECKED_KEYS", "SCHEMA", "load", "parse", "parse_contract"]
