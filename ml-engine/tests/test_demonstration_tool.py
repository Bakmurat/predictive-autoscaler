"""forecasting.demonstration_tool: the two reviewed files are generated from the admitted objects, only for the one
validated profile, and only if the service's own parser admits them."""

import copy
import json

import pytest

from forecasting import demonstration as D
from forecasting import demonstration_tool as T
from forecasting.contracts import (CALIBRATION_POLICY_REVISION, TRUST_POLICY_REVISION, canonical_json,
                                   resolve_settings, sha256_hex)
from tests import demonstration_helpers as H

NS = "demo"
NOT_BEFORE, EXPIRES, NOT_AFTER = H.ts(H.NOW - 3600), H.ts(H.NOW + 14 * 86400), H.ts(H.NOW + 20 * 86400)


def pa(name="c28-demo-pa-n", target="c28-demo-pa-n", uid="pa-uid-1", dep_uid="dep-uid-1", lead=600, generation=2,
       **prediction):
    pred = {"sampleIntervalSeconds": 60, "updateIntervalSeconds": 60, "minLeadSeconds": 600,
            "families": ["seasonalNaive"], **prediction}
    return {"apiVersion": "autoscaling.devkuban.com/v1alpha1", "kind": "PredictiveAutoscaler",
            "metadata": {"namespace": NS, "name": name, "uid": uid, "generation": 2},
            "spec": {"targetDeployment": {"name": target, "namespace": NS}, "mode": "Recommend",
                     "minReplicas": 1, "maxReplicas": 12, "metrics": {"requests": {"capacityPerReplica": "17.5"}},
                     "prediction": {k: v for k, v in pred.items() if v is not None}},
            "status": {"metricSource": {"targetUID": dep_uid},
                       "prediction": {"lead": {"seconds": lead, "startupDelaySeconds": 420,
                                               "actuationAllowanceSeconds": 30, "method": "configured",
                                               "sampleCount": 0, "observedGeneration": generation}}}}


def dep(name="c28-demo-pa-n", uid="dep-uid-1"):
    return {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"namespace": NS, "name": name, "uid": uid}}


def two():
    return ([pa(), pa("c28-demo-pa-r", "c28-demo-pa-r", "pa-uid-2", "dep-uid-2")],
            [dep(), dep("c28-demo-pa-r", "dep-uid-2")])


def contract(pas=None, deps=None):
    p, d = two()
    return T.write_contract(pas or p, deps or d, NOT_AFTER, "2026-11-12T00:00:00Z")


def qualification(c, **over):
    kw = dict(source_tree=H.TREE, image_digest=H.DIGEST, cluster_uid="kube-system-uid", reviewers=["claude", "codex"],
              not_before=NOT_BEFORE, expires=EXPIRES, now_s=H.NOW)
    kw.update(over)
    return T.write_qualification(c, **kw)


def refused(fn, text):
    with pytest.raises(T.ToolRefused) as e:
        fn()
    assert text in str(e.value), str(e.value)


def test_the_two_files_are_admitted_by_the_service_and_bind_what_was_reviewed():
    c = contract()
    q = qualification(c)
    assert c == canonical_json(json.loads(c)) and q == canonical_json(json.loads(q))      # canonical bytes
    adm = D.parse(q, c, H.EVAL, H.NOW, H.DIGEST)                                            # the service's admission
    assert adm.contract_sha256 == sha256_hex(c) and adm.namespace == NS
    assert adm.targets == {(NS, "c28-demo-pa-n", "dep-uid-1"), (NS, "c28-demo-pa-r", "dep-uid-2")}
    assert adm.families == {"seasonalNaive"} and adm.cluster_uid == "kube-system-uid"
    doc = json.loads(c)
    t = doc["checked"]["targets"][0]
    assert t["settings_sha256"] == resolve_settings(pa()).fingerprint() and t["capacity_per_replica"] == "17.5"
    assert (t["min_replicas"], t["max_replicas"]) == (1, 12)
    u = doc["unchecked"]
    assert u["calibration_policy_revision"] == CALIBRATION_POLICY_REVISION
    assert u["trust_policy_revision"] == TRUST_POLICY_REVISION and u["lead_seconds"] == 600
    canonical = resolve_settings(pa()).canonical()["settings"]          # the fingerprint itself binds both revisions
    assert canonical["calibration"]["method"] == CALIBRATION_POLICY_REVISION
    assert canonical["trust"]["policy"] == TRUST_POLICY_REVISION
    assert T.check(q, c, source_tree=H.TREE, image_digest=H.DIGEST, now_s=H.NOW)["targets"] == 2


@pytest.mark.parametrize("change, text", [
    ({"updateIntervalSeconds": None}, "updateIntervalSeconds must be stated as 60"),
    ({"updateIntervalSeconds": 300}, "updateIntervalSeconds must be stated as 60"),
    ({"minLeadSeconds": None}, "minLeadSeconds must be 600"),
    ({"minLeadSeconds": 900}, "minLeadSeconds must be 600"),
    ({"families": ["seasonalNaive", "statistical"]}, "not the validated profile"),
    ({"sampleIntervalSeconds": 300}, "no valid selected lead"),     # 600 s cannot cover startup + a 300 s step
    ({"risk": {"miscoverageBps": 500}}, "not the validated profile"),
    ({"trust": {"minShadowHours": 2}}, "not the validated profile"),
    ({"calibration": {"etaBps": 11}}, "not the validated profile"),
])
def test_only_the_validated_profile_gets_a_contract(change, text):
    pas, deps = two()
    pas[0] = pa(**change)
    refused(lambda: contract(pas, deps), text)


def test_the_objects_must_belong_together():
    pas, deps = two()
    refused(lambda: contract(pas, [dep(uid="other"), deps[1]]), "names target UID dep-uid-1")
    refused(lambda: contract(pas, [deps[1], deps[1]]), "no Deployment given")
    refused(lambda: contract(pas[:1] * 3, deps * 2), "1..2 autoscalers")
    other = copy.deepcopy(pas[1])
    other["metadata"]["namespace"] = other["spec"]["targetDeployment"]["namespace"] = "elsewhere"
    d2 = dep("c28-demo-pa-r", "dep-uid-2")
    d2["metadata"]["namespace"] = "elsewhere"
    refused(lambda: contract([pas[0], other], [deps[0], d2]), "ONE namespace")
    active = copy.deepcopy(pas[0])
    active["spec"]["mode"] = "Active"
    refused(lambda: contract([active, pas[1]], deps), "Recommend mode")
    nocap = copy.deepcopy(pas[0])
    nocap["spec"]["metrics"]["requests"] = {"targetRPS": 10}
    refused(lambda: contract([nocap, pas[1]], deps), "capacityPerReplica must be set")


def test_a_qualification_the_service_would_refuse_is_never_written():
    c = contract()
    refused(lambda: qualification(c, reviewers=["claude", "Claude "]), "two distinct reviewers")
    refused(lambda: qualification(c, expires=H.ts(H.NOW + 22 * 86400)), "at most 21 days")
    refused(lambda: qualification(c, expires=H.ts(H.NOW + 21 * 86400)), "")               # beyond expires_not_after
    refused(lambda: qualification(c, image_digest="sha256:short"), "")
    refused(lambda: qualification(c, now_s=H.NOW + 15 * 86400), "expired")
    with pytest.raises(T.ToolRefused):
        T.check(qualification(c), contract(), source_tree="b" * 40, image_digest=H.DIGEST, now_s=H.NOW)


def test_a_reviewed_file_is_never_overwritten(tmp_path):
    out = tmp_path / "contract.json"
    out.write_bytes(b"{}")
    with pytest.raises(T.ToolRefused):
        T._write(str(out), contract())
    assert out.read_bytes() == b"{}"


def test_the_command_line_writes_both_files_and_checks_them(tmp_path, capsys):
    pas, deps = two()
    paths = {}
    for i, (p, d) in enumerate(zip(pas, deps)):
        paths[f"pa{i}"], paths[f"dep{i}"] = tmp_path / f"pa{i}.json", tmp_path / f"dep{i}.json"
        paths[f"pa{i}"].write_text(json.dumps(p))
        paths[f"dep{i}"].write_text(json.dumps(d))
    c, q = tmp_path / "contract.json", tmp_path / "qualification.json"
    assert T.main(["contract", "--pa", str(paths["pa0"]), "--pa", str(paths["pa1"]), "--deployment", str(paths["dep0"]),
                   "--deployment", str(paths["dep1"]), "--expires-not-after", NOT_AFTER, "--out", str(c)]) == 0
    at = H.ts(H.NOW)
    assert T.main(["qualification", "--contract", str(c), "--source-tree", H.TREE, "--image-digest", H.DIGEST,
                   "--cluster-uid", "kube-system-uid", "--reviewer", "claude", "--reviewer", "codex",
                   "--not-before", NOT_BEFORE, "--expires", EXPIRES, "--at", at, "--out", str(q)]) == 0
    assert T.main(["check", "--qualification", str(q), "--contract", str(c), "--source-tree", H.TREE,
                   "--image-digest", H.DIGEST, "--at", at]) == 0
    out = capsys.readouterr().out
    assert sha256_hex(c.read_bytes()) in out and '"targets": 2' in out
    assert T.main(["check", "--qualification", str(q), "--contract", str(c), "--source-tree", H.TREE,
                   "--image-digest", "sha256:" + "e" * 64, "--at", at]) == 1


def test_the_lead_the_operator_selected_must_be_600():
    pas, deps = two()
    pas[0] = pa(lead=900)                                   # minLeadSeconds 600, but a longer startup budget won
    refused(lambda: contract(pas, deps), "selected a lead of 900 s")
    pas[0] = pa(generation=1)                               # selected for an older generation
    refused(lambda: contract(pas, deps), "no valid selected lead")
    nolead = pa()
    del nolead["status"]["prediction"]
    pas[0] = nolead
    refused(lambda: contract(pas, deps), "no valid selected lead")


def test_files_larger_than_the_forecaster_reads_are_never_written_or_passed():
    c = contract()
    q = qualification(c)
    assert len(c) <= D.MAX_BYTES and len(q) <= D.MAX_BYTES
    refused(lambda: T.check(q + b" " * (D.MAX_BYTES - len(q) + 1), c, source_tree=H.TREE, image_digest=H.DIGEST,
                            now_s=H.NOW), "at most 65536 bytes")
    refused(lambda: T.check(q, c + b" " * (D.MAX_BYTES - len(c) + 1), source_tree=H.TREE, image_digest=H.DIGEST,
                            now_s=H.NOW), "at most 65536 bytes")
    exact = q + b" " * (D.MAX_BYTES - len(q))               # exactly at the limit: admitted (JSON allows the spaces)
    assert len(exact) == D.MAX_BYTES
    T.check(exact, c, source_tree=H.TREE, image_digest=H.DIGEST, now_s=H.NOW)
    refused(lambda: qualification(c, reviewers=["claude", "codex"] + [f"r{i:05d}" * 20 for i in range(600)]),
            "at most 65536 bytes")                          # a qualification that would grow past the limit
