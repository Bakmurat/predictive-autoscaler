"""Write and check the two reviewed files of a demonstration qualification (forecasting.demonstration).

    python -m forecasting.demonstration_tool contract --pa PA.json [--pa PA2.json] --deployment DEP.json \
        [--deployment DEP2.json] --expires-not-after 2026-12-01T00:00:00Z --out contract.json
    python -m forecasting.demonstration_tool qualification --contract contract.json --source-tree TREE \
        --image-digest sha256:... --cluster-uid UID --reviewer A --reviewer B --not-before T --expires T \
        --out qualification.json
    python -m forecasting.demonstration_tool check --qualification qualification.json --contract contract.json \
        --source-tree TREE --image-digest sha256:... [--at T]

The contract is generated from the ADMITTED objects, never from numbers typed by hand: the PredictiveAutoscalers and
their target Deployments as `kubectl get -o json` printed them (read-only). Every target's autoscaler must be the ONE
validated forecast profile (contracts.VALIDATED_PROFILES: 60 s grid, an explicit 60 s update interval, minimum lead
600 s, risk 1000 bps, families [seasonalNaive], default calibration and trust settings), and its resolved settings must
carry this build's calibration and trust policy revisions. The settings fingerprint the contract records binds both
revisions and every other setting: a changed setting is a different contract. The revisions are also written, for the
reader, into the contract's unchecked part, which the service hashes with the file and never interprets.

The qualification is generated from the contract: its namespace, targets and families are the contract's, so the two
cannot disagree. Both files are written as canonical JSON (the bytes are what is hashed and mounted), and each is read
back through the service's own parser before it is written: a file this tool writes is one the service admits for the
given build and image, or nothing is written.

Nothing here talks to a cluster or decides anything live: the service checks the mounted files against the live
objects at every answer, and the cluster UID (the kube-system namespace's UID) is an input the executor reads.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Mapping, Optional, Sequence

from forecasting import demonstration as D
from forecasting import qualification as Q
from forecasting.contracts import (CALIBRATION_POLICY_REVISION, TRUST_POLICY_REVISION, ContractRefused, canonical_json,
                                   resolve_lead, resolve_settings, sha256_hex, validated_profile)

TOOL = "forecasting.demonstration_tool/v1"
# The one validated profile's lead (contracts.VALIDATED_PROFILES): spec.prediction.minLeadSeconds must state it.
DEMONSTRATION_LEAD_SECONDS = 600
UPDATE_INTERVAL_SECONDS = D.UPDATE_INTERVAL_SECONDS


class ToolRefused(Exception):
    pass


def _obj(doc, path: str) -> Mapping:
    cur = doc
    for key in path.split("."):
        if not isinstance(cur, Mapping) or key not in cur:
            raise ToolRefused(f"{path} is missing")
        cur = cur[key]
    return cur


def contract_target(pa: Mapping, deployment: Mapping) -> Dict:
    """One checked target of the contract from an admitted PredictiveAutoscaler and its target Deployment."""
    ns, name = _obj(pa, "metadata.namespace"), _obj(pa, "metadata.name")
    where = f"PredictiveAutoscaler {ns}/{name}"
    if pa.get("kind") not in (None, "PredictiveAutoscaler") or deployment.get("kind") not in (None, "Deployment"):
        raise ToolRefused(f"{where}: the inputs must be a PredictiveAutoscaler and a Deployment")
    spec = _obj(pa, "spec")
    t_ns = _obj(spec, "targetDeployment").get("namespace") or ns
    t_name = _obj(spec, "targetDeployment.name")
    if t_ns != ns:
        raise ToolRefused(f"{where}: its target is in another namespace ({t_ns})")
    if (_obj(deployment, "metadata.namespace"), _obj(deployment, "metadata.name")) != (t_ns, t_name):
        raise ToolRefused(f"{where}: the Deployment given is not its target {t_ns}/{t_name}")
    uid = _obj(deployment, "metadata.uid")
    status_uid = ((pa.get("status") or {}).get("metricSource") or {}).get("targetUID")
    if status_uid is not None and status_uid != uid:
        raise ToolRefused(f"{where}: its status names target UID {status_uid}, the Deployment given has {uid}: "
                          "read both objects again")
    if spec.get("mode", "Recommend") != "Recommend":
        raise ToolRefused(f"{where}: a contract is written while the autoscaler is in Recommend mode")
    pred = spec.get("prediction") if isinstance(spec.get("prediction"), Mapping) else {}
    if pred.get("updateIntervalSeconds") != UPDATE_INTERVAL_SECONDS or isinstance(pred.get("updateIntervalSeconds"), bool):
        raise ToolRefused(f"{where}: spec.prediction.updateIntervalSeconds must be stated as {UPDATE_INTERVAL_SECONDS} "
                          "(the default is not enough: the contract binds what the object says)")
    if pred.get("minLeadSeconds") != DEMONSTRATION_LEAD_SECONDS or isinstance(pred.get("minLeadSeconds"), bool):
        raise ToolRefused(f"{where}: spec.prediction.minLeadSeconds must be {DEMONSTRATION_LEAD_SECONDS}")
    try:
        settings = resolve_settings(pa)
    except ContractRefused as e:
        raise ToolRefused(f"{where}: its forecast settings do not resolve: {e}") from e
    # The profile is decided by the lead the operator SELECTED (status.prediction.lead, for the object's current
    # generation), not by the minimum: a startup budget can make the selected lead longer than 600 s, and then the
    # configuration is not the validated one and could never become eligible.
    lead, why = resolve_lead(pa, settings, _obj(pa, "metadata.generation"))
    if lead is None:
        raise ToolRefused(f"{where}: no valid selected lead for its current generation ({why}): wait for the operator "
                          "to publish one, then read the object again")
    if lead.lead_seconds != DEMONSTRATION_LEAD_SECONDS:
        raise ToolRefused(f"{where}: the operator selected a lead of {lead.lead_seconds} s, the validated profile's is "
                          f"{DEMONSTRATION_LEAD_SECONDS} s")
    if validated_profile(settings, lead.lead_seconds) is None:
        raise ToolRefused(f"{where}: its settings are not the validated profile (60 s grid and update interval, "
                          f"lead {DEMONSTRATION_LEAD_SECONDS} s, risk 1000 bps, [seasonalNaive], default calibration "
                          "and trust)")
    canonical = settings.canonical()["settings"]
    if canonical["calibration"]["method"] != CALIBRATION_POLICY_REVISION or \
            canonical["trust"]["policy"] != TRUST_POLICY_REVISION:
        raise ToolRefused(f"{where}: its settings carry calibration {canonical['calibration']['method']!r} and trust "
                          f"{canonical['trust']['policy']!r}, not this build's {CALIBRATION_POLICY_REVISION!r} and "
                          f"{TRUST_POLICY_REVISION!r}")
    cap = (((spec.get("metrics") or {}).get("requests") or {}).get("capacityPerReplica"))
    if not isinstance(cap, str):
        raise ToolRefused(f"{where}: spec.metrics.requests.capacityPerReplica must be set (its exact text is bound)")
    lo, hi = spec.get("minReplicas"), spec.get("maxReplicas")
    return {"namespace": ns, "name": t_name, "uid": uid, "min_replicas": lo, "max_replicas": hi,
            "capacity_per_replica": cap, "settings_sha256": settings.fingerprint()}


def _families_of(pas: Sequence[Mapping]) -> List[str]:
    fams = {tuple(sorted(resolve_settings(pa).families)) for pa in pas}
    if len(fams) != 1:
        raise ToolRefused(f"the autoscalers list different families: {sorted(fams)}")
    return list(next(iter(fams)))


def write_contract(pas: Sequence[Mapping], deployments: Sequence[Mapping], expires_not_after: str,
                   generated_at: str) -> bytes:
    if not 1 <= len(pas) <= D.MAX_TARGETS or len(deployments) != len(pas):
        raise ToolRefused(f"give 1..{D.MAX_TARGETS} autoscalers and exactly one Deployment for each")
    by_name = {(_obj(d, "metadata.namespace"), _obj(d, "metadata.name")): d for d in deployments}
    targets, autoscalers = [], []
    for pa in pas:
        ns = _obj(pa, "metadata.namespace")
        t = _obj(pa, "spec.targetDeployment")
        dep = by_name.get((t.get("namespace") or ns, t.get("name")))
        if dep is None:
            raise ToolRefused(f"no Deployment given for {ns}/{_obj(pa, 'metadata.name')}'s target")
        targets.append(contract_target(pa, dep))
        autoscalers.append({"namespace": ns, "name": _obj(pa, "metadata.name"), "uid": _obj(pa, "metadata.uid"),
                            "generation": _obj(pa, "metadata.generation")})
    if len({t["namespace"] for t in targets}) != 1:
        raise ToolRefused("a demonstration has ONE namespace")
    doc = {"schema": D.CONTRACT_SCHEMA,
           "checked": {"qualification": {"expires_not_after": expires_not_after}, "families": _families_of(pas),
                       "cadence": {"grid_seconds": D.GRID_SECONDS, "update_interval_seconds": UPDATE_INTERVAL_SECONDS},
                       "targets": targets},
           "unchecked": {"tool": TOOL, "generated_at": generated_at, "lead_seconds": DEMONSTRATION_LEAD_SECONDS,
                         "calibration_policy_revision": CALIBRATION_POLICY_REVISION,
                         "trust_policy_revision": TRUST_POLICY_REVISION,
                         "validated_profile": validated_profile(resolve_settings(pas[0]),
                                                                DEMONSTRATION_LEAD_SECONDS).evidence,
                         "autoscalers": autoscalers,
                         "note": "settings_sha256 binds both revisions and every forecast setting; the service "
                                 "compares it with the live autoscaler at every answer"}}
    data = _bounded(canonical_json(doc), "the contract")
    try:
        D.parse_contract(data)
    except Q.QualificationRefused as e:
        raise ToolRefused(f"the service would refuse this contract: {e}") from e
    return data


def write_qualification(contract: bytes, *, source_tree: str, image_digest: str, cluster_uid: str,
                        reviewers: Sequence[str], not_before: str, expires: str, now_s: int) -> bytes:
    try:
        c = D.parse_contract(contract)
    except Q.QualificationRefused as e:
        raise ToolRefused(f"the contract is not admissible: {e}") from e
    doc = json.loads(contract)
    targets = [{"namespace": t["namespace"], "name": t["name"], "uid": t["uid"]} for t in doc["checked"]["targets"]]
    namespaces = {t["namespace"] for t in targets}
    data = _bounded(canonical_json({"schema": D.SCHEMA, "source_tree": source_tree, "image_digest": image_digest,
                           "contract_sha256": c.sha256, "reviewers": list(reviewers), "not_before": not_before,
                           "expires": expires, "families": doc["checked"]["families"],
                           "scope": {"cluster_uid": cluster_uid, "namespace": next(iter(namespaces)),
                                     "targets": targets}}), "the qualification")
    check(data, contract, source_tree=source_tree, image_digest=image_digest, now_s=now_s)
    return data


def check(qualification: bytes, contract: bytes, *, source_tree: str, image_digest: str, now_s: int) -> Dict:
    """The service's own admission of the two files for this build and image at `now_s`, or ToolRefused. The size
    limit of the mounted files (D.MAX_BYTES) applies here too: the service reads at most that many bytes."""
    _bounded(qualification, "the qualification")
    _bounded(contract, "the contract")
    try:
        q = D.parse(qualification, contract, Q.BuildInfo(source_tree, True), now_s, image_digest)
    except Q.QualificationRefused as e:
        raise ToolRefused(f"the service would refuse these files: {e}") from e
    return {**q.summary(), "contract_targets": [t.__dict__ for t in q.contract.targets],
            "not_before": q.not_before}


def _bounded(data: bytes, what: str) -> bytes:
    if len(data) > D.MAX_BYTES:
        raise ToolRefused(f"{what} is {len(data)} bytes; the forecaster reads a mounted file of at most "
                          f"{D.MAX_BYTES} bytes")
    return data


def _read_json(path: str) -> Mapping:
    with open(path, "rb") as fh:
        return json.load(fh)


def _write(path: str, data: bytes) -> None:
    if os.path.exists(path):
        raise ToolRefused(f"{path} exists: a reviewed file is never overwritten, choose a new name")
    with open(path, "xb") as fh:
        fh.write(data)
    print(f"{path}: {len(data)} bytes, sha256 {sha256_hex(data)}")


def _now(text: Optional[str]) -> int:
    return Q._ts(text, "--at") if text else int(time.time())


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m forecasting.demonstration_tool", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("contract")
    c.add_argument("--pa", action="append", required=True)
    c.add_argument("--deployment", action="append", required=True)
    c.add_argument("--expires-not-after", required=True)
    c.add_argument("--out", required=True)
    q = sub.add_parser("qualification")
    for name in ("contract", "source-tree", "image-digest", "cluster-uid", "not-before", "expires", "out"):
        q.add_argument(f"--{name}", required=True)
    q.add_argument("--reviewer", action="append", required=True)
    q.add_argument("--at", help="check admission at this time instead of now (YYYY-MM-DDTHH:MM:SSZ)")
    k = sub.add_parser("check")
    for name in ("qualification", "contract", "source-tree", "image-digest"):
        k.add_argument(f"--{name}", required=True)
    k.add_argument("--at")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "contract":
            stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            _write(a.out, write_contract([_read_json(p) for p in a.pa], [_read_json(p) for p in a.deployment],
                                         a.expires_not_after, stamp))
        elif a.cmd == "qualification":
            with open(a.contract, "rb") as fh:
                contract = fh.read()
            _write(a.out, write_qualification(contract, source_tree=a.source_tree, image_digest=a.image_digest,
                                              cluster_uid=a.cluster_uid, reviewers=a.reviewer,
                                              not_before=a.not_before, expires=a.expires, now_s=_now(a.at)))
        else:
            with open(a.qualification, "rb") as fh:
                qual = fh.read()
            with open(a.contract, "rb") as fh:
                contract = fh.read()
            print(json.dumps(check(qual, contract, source_tree=a.source_tree, image_digest=a.image_digest,
                                   now_s=_now(a.at)), indent=1, sort_keys=True))
    except (ToolRefused, Q.QualificationRefused, OSError, ValueError, KeyError) as e:
        print(f"refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
