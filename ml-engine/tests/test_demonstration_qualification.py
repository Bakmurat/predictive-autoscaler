"""A demonstration qualification is honoured only by the evaluation build and image it names, only with the
hash-verified contract it names, and only for its cluster, one namespace, at most two target UIDs, the contract's live
configuration and the families it lists, while valid; anything else fails closed. It records no gate and is not a lab
qualification. Mirrors test_lab_qualification.py, refusal for refusal."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting import contracts as c  # noqa: E402
from forecasting import demonstration as D  # noqa: E402
from forecasting import qualification as Q  # noqa: E402
from tests.demonstration_helpers import DIGEST, EVAL, NOW, TREE, contract, contract_doc, pair, qualification, \
    target  # noqa: E402

SPEC = {"minReplicas": 1, "maxReplicas": 12, "metrics": {"requests": {"capacityPerReplica": "5.5"}},
        "prediction": {"updateIntervalSeconds": 60}}


def parse(q=None, cd=None, build=EVAL, now=NOW, digest=DIGEST):
    default_q, default_c = pair()
    return D.parse(default_q if q is None else q, default_c if cd is None else cd, build, now, digest)


def live(d, **over):
    """refusal() for a request that is fully in scope unless `over` says otherwise."""
    kw = dict(cluster_uid="cluster-uid", autoscaler_namespace="demo", namespace="demo", name="web", uid="dep-uid",
              now_s=NOW, valid_until=NOW + 600, spec=SPEC, families=("seasonalNaive",), sample_interval_seconds=60)
    kw.update(over)
    return d.refusal(**kw)


def test_a_complete_reviewed_pair_is_honoured_for_exactly_its_scope():
    d = parse()
    assert live(d) is None
    assert live(d, cluster_uid="other-cluster") == "cluster"
    assert live(d, cluster_uid=None) == "cluster"                          # cluster unknown: fail closed
    assert live(d, autoscaler_namespace="shop") == "namespace"             # autoscaler elsewhere
    assert live(d, namespace="shop") == "namespace"
    assert live(d, uid="recreated-uid") == "target"                        # a recreated Deployment
    assert live(d, name="api") == "target"
    assert live(d, now_s=d.expires) == "validity"                          # expired
    assert live(d, now_s=d.not_before - 1) == "validity"
    assert live(d, valid_until=d.expires + 1) == "validity"                # the answer would outlive it
    assert live(d, now_s=d.expires - 1, valid_until=d.expires) is None
    s = d.summary()
    assert len(d.sha256) == 64 and s["targets"] == 1 and s["families"] == ["seasonalNaive"]
    assert s["contract_sha256"] == d.contract.sha256 == c.sha256_hex(contract()) and "gates" not in s


def test_two_targets_in_one_namespace_are_the_widest_scope():
    targets = [target(), target(name="api", uid="api-uid", max_replicas=6, capacity_per_replica="12")]
    q, cd = pair(contract_kw={"targets": targets},
                 scope={"cluster_uid": "cluster-uid", "namespace": "demo",
                        "targets": [{k: t[k] for k in ("namespace", "name", "uid")} for t in targets]})
    d = D.parse(q, cd, EVAL, NOW, DIGEST)
    assert live(d) is None and live(d, name="api", uid="api-uid") == "bounds"      # each target has its own contract
    api = {**SPEC, "maxReplicas": 6, "metrics": {"requests": {"capacityPerReplica": 12}}}
    assert live(d, name="api", uid="api-uid", spec=api) is None
    assert live(d, name="api", uid="dep-uid", spec=api) == "target"                # name and uid belong together


@pytest.mark.parametrize("build,why", [(Q.BuildInfo(TREE, False), "release build"),
                                       (Q.BuildInfo("unknown", True), "no source tree"),
                                       (Q.BuildInfo("c" * 40, True), "another source tree")])
def test_only_the_named_evaluation_build_honours_it(build, why):
    with pytest.raises(Q.QualificationRefused, match=why):
        parse(build=build)


@pytest.mark.parametrize("running,why", [(None, "digest is unknown"), ("latest", "digest is unknown"),
                                         ("sha256:" + "e" * 64, "another image digest")])
def test_only_the_named_image_honours_it(running, why):
    with pytest.raises(Q.QualificationRefused, match=why):
        parse(digest=running)


def _scope(**over):
    return {"scope": {"cluster_uid": "u", "namespace": "demo",
                      "targets": [{"namespace": "demo", "name": "web", "uid": "dep-uid"}], **over}}


@pytest.mark.parametrize("over,why", [
    ({"schema": "demonstration-qualification/v2"}, "exactly the keys"),
    ({"schema": "lab-qualification/v1"}, "exactly the keys"),                         # never a lab document
    ({"benchmark": True}, "exactly the keys"),                                        # no extra switches
    ({"asserts": "nothing about release gates"}, "exactly the keys"),                 # no free-text statement field
    ({"run_lock_sha256": "b" * 64}, "exactly the keys"),
    ({"gates": {g: "pass" for g in ("G0", "G1", "G2", "G3", "G4")}}, "carries no gates"),
    ({"gates": {}}, "carries no gates"),
    ({"gates": None}, "carries no gates"),
    ({"contract_sha256": "nope"}, "contract_sha256"),
    ({"contract_sha256": "B" * 64}, "contract_sha256"),
    ({"contract_sha256": "b" * 64}, "not the one the qualification names"),          # a hash alone enforces nothing
    ({"reviewers": ["executor"]}, "two distinct reviewers"),
    ({"reviewers": ["same", "same"]}, "two distinct reviewers"),
    ({"reviewers": ["same", " same "]}, "two distinct reviewers"),
    ({"reviewers": ["executor", "reviewer", ""]}, "two distinct reviewers"),
    ({"reviewers": ["executor", 7]}, "two distinct reviewers"),
    ({"reviewers": "executor,reviewer"}, "two distinct reviewers"),
    ({"expires": "2026-11-09T00:00:00Z", "not_before": "2026-11-01T00:00:00Z"}, "expired"),
    ({"expires": "2026-11-30T00:00:00Z"}, "at most 21 days"),                         # 22 days
    ({"expires": "2026-12-08T00:00:00Z"}, "at most 21 days"),                         # a lab-sized window
    ({"expires": "2026-11-07T00:00:00Z"}, "positive"),
    ({"expires": "2026-11-28T00:00:00+00:00"}, "ending in Z"),
    ({"not_before": 1794000000}, "ending in Z"),
    ({"families": []}, "families"),
    ({"families": ["prophet"]}, "families"),                                          # unknown: fail closed
    ({"families": ["seasonal_naive"]}, "families"),                                   # the wire name is not a family
    ({"families": ["seasonalNaive", "seasonalNaive"]}, "families"),
    ({"families": "seasonalNaive"}, "families"),
    ({"families": ["seasonalNaive", "statistical"]}, "families differ from the contract"),
    (_scope(cluster_uid=""), "cluster_uid"),
    (_scope(namespace=""), "exactly one namespace"),
    (_scope(namespace=["demo"]), "exactly one namespace"),
    ({"scope": {"cluster_uid": "u", "namespaces": ["demo"],
                "targets": [{"namespace": "demo", "name": "web", "uid": "dep-uid"}]}}, "exactly"),   # the lab shape
    (_scope(targets=[]), "targets"),
    (_scope(targets=[{"namespace": "demo", "name": n, "uid": n} for n in ("a", "b", "c")]), "1..2 targets"),
    (_scope(targets=[{"namespace": "shop", "name": "web", "uid": "dep-uid"}]), "in scope.namespace"),
    (_scope(targets=[{"namespace": "demo", "name": "web"}]), "uid"),
    (_scope(targets=[{"namespace": "demo", "name": "web", "uid": ""}]), "uid"),
    (_scope(targets=[{"namespace": "demo", "name": "web", "uid": "dep-uid", "any": True}]), "exactly"),
    (_scope(targets=[{"namespace": "demo", "name": "web", "uid": "u1"},
                     {"namespace": "demo", "name": "web", "uid": "u2"}]), "each target and each uid once"),
    (_scope(targets=[{"namespace": "demo", "name": "web", "uid": "u1"},
                     {"namespace": "demo", "name": "api", "uid": "u1"}]), "each target and each uid once"),
    (_scope(targets=[{"namespace": "demo", "name": "web", "uid": "other-uid"}]), "targets differ from the contract"),
    (_scope(targets=[{"namespace": "demo", "name": "api", "uid": "dep-uid"}]), "targets differ from the contract"),
    (_scope(everything=True), "exactly"),
])
def test_anything_incomplete_or_wider_is_refused(over, why):
    q, cd = pair(**over)
    with pytest.raises(Q.QualificationRefused, match=why):
        D.parse(q, cd, EVAL, NOW, DIGEST)


@pytest.mark.parametrize("data,why", [
    (b"not json", "not JSON"), (b"[]", "exactly the keys"), (b"null", "exactly the keys"),
    (b'{"schema": "demonstration-qualification/v1", "schema": "x"}', "appears twice")])
def test_a_qualification_that_is_not_one_plain_object_is_refused(data, why):
    with pytest.raises(Q.QualificationRefused, match=why):
        D.parse(data, contract(), EVAL, NOW, DIGEST)


def _contract_with(**checked):
    doc = contract_doc()
    doc["checked"].update(checked)
    return json.dumps(doc).encode()


def _without(key):
    doc = contract_doc()
    del doc["checked"][key]
    return json.dumps(doc).encode()


@pytest.mark.parametrize("cd,why", [
    (b"not json", "not JSON"),
    (json.dumps(contract_doc(schema="demonstration-contract/v2")).encode(), "exactly the keys"),
    (json.dumps(contract_doc(gates={"G0": "pass"})).encode(), "exactly the keys"),           # no gate here either
    (json.dumps(contract_doc(qualification_sha256="b" * 64)).encode(), "exactly the keys"),  # the hash chain is one-way
    (json.dumps(contract_doc(unchecked=["images"])).encode(), "unchecked part must be an object"),
    (json.dumps({"schema": D.CONTRACT_SCHEMA}).encode(), "exactly the keys"),
    (json.dumps({"schema": D.CONTRACT_SCHEMA, "checked": []}).encode(), "checked part needs exactly"),
    (_contract_with(images={"forecaster": DIGEST}), "checked part needs exactly"),            # checked stays small
    (_without("cadence"), "checked part needs exactly"),
    (_contract_with(qualification={}), "expires_not_after"),
    (_contract_with(qualification={"expires_not_after": "2026-11-28T00:00:00Z", "sha256": "b" * 64}),
     "expires_not_after"),
    (_contract_with(qualification={"expires_not_after": "soon"}), "expires_not_after"),
    (_contract_with(qualification={"expires_not_after": "2026-11-27T23:59:59Z"}), "expires after the contract allows"),
    (_contract_with(families=[]), "checked.families"),
    (_contract_with(families=["seasonal_naive"]), "checked.families"),
    (_contract_with(families=["seasonalNaive", "statistical"]), "families differ from the contract"),
    (_contract_with(cadence={"grid_seconds": 300, "update_interval_seconds": 60}), "only on the 60 s grid"),
    (_contract_with(cadence={"grid_seconds": 60, "update_interval_seconds": 300}), "60 s update interval"),
    (_contract_with(cadence={"grid_seconds": 60, "update_interval_seconds": 30}), "60 s update interval"),
    (_contract_with(cadence={"grid_seconds": 60}), "checked.cadence"),
    (_contract_with(cadence={"grid_seconds": 60, "update_interval_seconds": "60"}), "checked.cadence"),
    (_contract_with(cadence={"grid_seconds": True, "update_interval_seconds": 60}), "checked.cadence"),
    (_contract_with(cadence={"grid_seconds": 60.0, "update_interval_seconds": 60}), "checked.cadence"),
    (_contract_with(targets=[]), "1..2 targets"),
    (_contract_with(targets=[target(name=n, uid=n) for n in ("a", "b", "c")]), "1..2 targets"),
    (_contract_with(targets=[{"namespace": "demo", "name": "web", "uid": "dep-uid"}]), "needs exactly"),
    (_contract_with(targets=[target(image="x")]), "needs exactly"),
    (_contract_with(targets=[target(uid="")]), "needs exactly"),
    (_contract_with(targets=[target(), target()]), "each target and each uid once"),
    (_contract_with(targets=[target(uid="other-uid")]), "targets differ from the contract"),
    (_contract_with(targets=[target(), target(name="api", uid="api-uid")]), "targets differ from the contract"),
    (_contract_with(targets=[target(min_replicas=0)]), "replica bounds"),
    (_contract_with(targets=[target(min_replicas=13)]), "replica bounds"),
    (_contract_with(targets=[target(max_replicas="12")]), "replica bounds"),
    (_contract_with(targets=[target(max_replicas=True)]), "replica bounds"),
    (_contract_with(targets=[target(capacity_per_replica=5.5)]), "capacity_per_replica"),
    (_contract_with(targets=[target(capacity_per_replica=5)]), "capacity_per_replica"),
    (_contract_with(targets=[target(capacity_per_replica="0")]), "capacity_per_replica"),
    (_contract_with(targets=[target(capacity_per_replica="-1")]), "capacity_per_replica"),
    (_contract_with(targets=[target(capacity_per_replica="5k")]), "capacity_per_replica"),
    (_contract_with(targets=[target(capacity_per_replica="1e3")]), "capacity_per_replica"),
    (_contract_with(targets=[target(capacity_per_replica="2000000")]), "capacity_per_replica"),
    (_contract_with(targets=[target(capacity_per_replica="")]), "capacity_per_replica"),
])
def test_the_named_contract_is_loaded_verified_and_complete(cd, why):
    # The qualification names exactly these bytes, so every refusal here is the contract's own content.
    with pytest.raises(Q.QualificationRefused, match=why):
        D.parse(qualification(cd), cd, EVAL, NOW, DIGEST)


def test_the_contract_is_bound_by_its_hash_and_never_names_the_qualification():
    q, cd = pair()
    d = D.parse(q, cd, EVAL, NOW, DIGEST)
    assert d.contract_sha256 == c.sha256_hex(cd) and d.contract.expires_not_after == d.expires
    for other in (cd + b" ", contract(targets=[target(max_replicas=24)]), b""):      # any other bytes: another contract
        with pytest.raises(Q.QualificationRefused, match="not the one the qualification names"):
            D.parse(q, other, EVAL, NOW, DIGEST)
    # The rest of the reviewed contract rides along, hashed and uninterpreted: it changes the hash, never the checks.
    wider = contract(unchecked={"images": {"forecaster": DIGEST}, "bounds": "1..99", "gates": "none asserted"})
    with pytest.raises(Q.QualificationRefused, match="not the one the qualification names"):
        D.parse(q, wider, EVAL, NOW, DIGEST)
    w = D.parse(qualification(wider), wider, EVAL, NOW, DIGEST)
    assert w.contract.targets == d.contract.targets and w.contract_sha256 != d.contract_sha256
    assert live(w) is None and live(w, spec={**SPEC, "maxReplicas": 99}) == "bounds"
    # One-way dependency: the qualification's digest appears nowhere in the contract it names.
    assert d.sha256.encode() not in cd and b"qualification_sha256" not in cd


@pytest.mark.parametrize("over,why", [
    ({"families": ("seasonalNaive", "statistical")}, "families"),          # the autoscaler may forecast more
    ({"families": ()}, "families"),
    ({"families": None}, "families"),
    ({"sample_interval_seconds": 300}, "cadence"),
    ({"spec": {**SPEC, "prediction": {"updateIntervalSeconds": 300}}}, "cadence"),
    ({"spec": {**SPEC, "prediction": {}}}, "cadence"),                     # never a default: it must be set
    ({"spec": {**SPEC, "prediction": {"updateIntervalSeconds": True}}}, "cadence"),
    ({"spec": {**SPEC, "prediction": None}}, "cadence"),
    ({"spec": {**SPEC, "maxReplicas": 13}}, "bounds"),
    ({"spec": {**SPEC, "minReplicas": 2}}, "bounds"),
    ({"spec": {k: v for k, v in SPEC.items() if k != "minReplicas"}}, "bounds"),
    ({"spec": {**SPEC, "maxReplicas": "12"}}, "bounds"),
    ({"spec": {**SPEC, "metrics": {"requests": {"capacityPerReplica": "5.6"}}}}, "capacity"),
    ({"spec": {**SPEC, "metrics": {"requests": {"capacityPerReplica": "5.50"}}}}, "capacity"),   # the exact text
    ({"spec": {**SPEC, "metrics": {"requests": {"capacityPerReplica": 5.5}}}}, "capacity"),
    ({"spec": {**SPEC, "metrics": {"requests": {"capacityPerReplica": "5.5", "targetRPS": 6}}}}, "capacity"),
    ({"spec": {**SPEC, "metrics": {"requests": {"targetRPS": 5}}}}, "capacity"),
    ({"spec": {**SPEC, "metrics": {}}}, "capacity"),
    ({"spec": {**SPEC, "metrics": None}}, "capacity"),
    ({"spec": None}, "cadence"),
    ({"selected": "statistical"}, "family"),
    ({"selected": "lstm"}, "family"),                                      # a family the document cannot list
    ({"selected": "seasonalNaive"}, "family"),                             # the selected family is a wire name
    ({"selected": ""}, "family"),
])
def test_a_request_whose_live_configuration_differs_from_the_contract_is_not_covered(over, why):
    d = parse()
    assert live(d) is None and live(d, selected="seasonal_naive") is None
    assert live(d, **over) == why and why in D.REFUSALS


def test_the_first_failing_check_names_the_refusal():
    d = parse()
    everything_wrong = dict(cluster_uid="x", autoscaler_namespace="shop", namespace="shop", name="api", now_s=d.expires,
                            families=("statistical",), sample_interval_seconds=300, spec={}, selected="statistical")
    order = []
    for key, good in (("cluster_uid", "cluster-uid"), ("autoscaler_namespace", "demo"), ("namespace", "demo"),
                      ("name", "web"), ("now_s", NOW), ("families", ("seasonalNaive",)),
                      ("sample_interval_seconds", 60), ("spec", SPEC), ("selected", "seasonal_naive")):
        order.append(live(d, **everything_wrong))
        everything_wrong[key] = good
    assert order == ["cluster", "namespace", "namespace", "target", "validity", "families", "cadence", "cadence",
                     "family"] and live(d, **everything_wrong) is None
    assert set(order) | {"release", "bounds", "capacity"} == set(D.REFUSALS)


def test_the_configuration_name_and_the_wire_name_of_a_family_agree():
    # spec.prediction.families: [seasonalNaive] is what the document lists; seasonal_naive is what a bundle selects and
    # what the wire's model_family carries. The document is checked against the selected wire name through the one map.
    pa = {"spec": {"prediction": {"families": ["seasonalNaive"]}}}
    settings = c.resolve_settings(pa)
    assert settings.families == ("seasonalNaive",) and set(settings.families) <= set(c.FAMILIES)
    assert c.FAMILY_MODELS == {"seasonalNaive": "seasonal_naive", "statistical": "statistical"}
    assert set(c.FAMILY_MODELS) == set(c.FAMILIES) and set(c.FAMILY_MODELS.values()) <= set(c.MODEL_FAMILIES)
    d = parse()
    assert d.families == frozenset(settings.families)
    assert d.allows("seasonal_naive") and not d.allows("seasonalNaive") and not d.allows("statistical")
    assert not d.allows("lstm") and not d.allows(None) and not d.allows("")           # unknown: fail closed
    both_c = contract(families=("seasonalNaive", "statistical"))
    both = D.parse(qualification(both_c, families=["statistical", "seasonalNaive"]), both_c, EVAL, NOW, DIGEST)
    assert both.allows("seasonal_naive") and both.allows("statistical") and not both.allows("lstm")


def test_load_and_build_info(tmp_path):
    assert D.load(None, None, EVAL, NOW) is None and D.load("", "", EVAL, NOW) is None
    q, cd = pair()
    qf, cf = tmp_path / "q.json", tmp_path / "c.json"
    qf.write_bytes(q)
    cf.write_bytes(cd)
    for paths in ((str(qf), None), (None, str(cf)), (str(qf), "")):                   # both or neither
        with pytest.raises(Q.QualificationRefused, match="together or not at all"):
            D.load(*paths, EVAL, NOW, DIGEST)
    with pytest.raises(Q.QualificationRefused, match="qualification is unreadable"):
        D.load(str(tmp_path / "missing.json"), str(cf), EVAL, NOW, DIGEST)
    with pytest.raises(Q.QualificationRefused, match="contract is unreadable"):
        D.load(str(qf), str(tmp_path / "missing.json"), EVAL, NOW, DIGEST)
    assert D.load(str(qf), str(cf), EVAL, NOW, DIGEST).cluster_uid == "cluster-uid"
    with pytest.raises(Q.QualificationRefused, match="not the one the qualification names"):
        D.load(str(qf), str(qf), EVAL, NOW, DIGEST)                                    # the same file twice
    big = tmp_path / "big.json"
    big.write_bytes(cd + b" " * D.MAX_BYTES)
    with pytest.raises(Q.QualificationRefused, match="exceeds"):
        D.load(str(qf), str(big), EVAL, NOW, DIGEST)
    with pytest.raises(Q.QualificationRefused, match="release build"):
        D.load(str(qf), str(cf), Q.BuildInfo(TREE, False), NOW, DIGEST)


def test_a_lab_document_is_never_a_demonstration_document_and_the_reverse():
    from tests import test_lab_qualification as lab
    q, cd = pair()
    with pytest.raises(Q.QualificationRefused, match="carries no gates"):
        D.parse(lab.doc(), cd, EVAL, NOW, DIGEST)
    with pytest.raises(Q.QualificationRefused, match="exactly the keys"):
        Q.parse(q, EVAL, NOW, DIGEST)
    assert Q.parse(lab.doc(), EVAL, NOW, DIGEST).run_lock_sha256 == "b" * 64           # the lab parser is untouched


def test_the_app_refuses_to_start_with_a_qualification_it_cannot_honour(tmp_path, monkeypatch):
    from api import statistical as app
    from forecasting import service as S
    from tests import test_lab_qualification as lab
    q, cd = pair(scope={"cluster_uid": "cluster-uid", "namespace": "demo",
                        "targets": [{"namespace": "demo", "name": "web", "uid": "dep-uid"}]})
    qf, cf, lf = tmp_path / "q.json", tmp_path / "c.json", tmp_path / "lab.json"
    qf.write_bytes(q)
    cf.write_bytes(cd)
    lf.write_bytes(lab.doc())
    env = {"PROMETHEUS_URL": "http://prom", "FORECAST_STATE_DIR": str(tmp_path / "state"), "FORECAST_FIT_MODE": "off",
           "FORECAST_DEMONSTRATION_QUALIFICATION": str(qf), "FORECAST_DEMONSTRATION_CONTRACT": str(cf),
           "FORECAST_IMAGE_DIGEST": DIGEST}
    with pytest.raises(app.Refused, match="release build"):            # this checkout is no evaluation build
        app.default_service(env)
    monkeypatch.setattr(Q, "read_build_info", lambda path=Q.BUILD_INFO_PATH: EVAL)
    monkeypatch.setattr(app.time, "time", lambda: NOW)
    svc = app.default_service(env)
    try:
        h = svc.health()
        assert h["demonstration_qualification"]["cluster_uid"] == "cluster-uid" and h["lab_qualification"] is None
        assert h["demonstration_qualification"]["contract_sha256"] == c.sha256_hex(cd)
        assert h["build"]["evaluation_build"] is True and h["qualification_basis"] == "demonstration"
        assert h["release_gates_passed"] is False                        # the release gate itself never moves
    finally:
        svc.close()
    with pytest.raises(app.Refused, match="another image digest"):
        app.default_service({**env, "FORECAST_IMAGE_DIGEST": "sha256:" + "e" * 64})
    for missing in ("FORECAST_DEMONSTRATION_QUALIFICATION", "FORECAST_DEMONSTRATION_CONTRACT"):
        with pytest.raises(app.Refused, match="together or not at all"):
            app.default_service({k: v for k, v in env.items() if k != missing})
    cf.write_bytes(contract(targets=[target(max_replicas=24)]))          # the mounted contract was replaced
    with pytest.raises(app.Refused, match="not the one the qualification names"):
        app.default_service(env)
    cf.write_bytes(cd)
    # At most one of the two documents, whichever would have been valid alone.
    with pytest.raises(app.Refused, match="at most one of the two documents"):
        app.default_service({**env, "FORECAST_LAB_QUALIFICATION": str(lf)})
    lab_only = {k: v for k, v in env.items() if "DEMONSTRATION" not in k}
    svc = app.default_service({**lab_only, "FORECAST_LAB_QUALIFICATION": str(lf)})
    try:
        assert svc.health()["qualification_basis"] == "lab" and svc.health()["demonstration_qualification"] is None
    finally:
        svc.close()
    # No silent precedence: once the release gates have passed, a demonstration document stops the service.
    monkeypatch.setattr(S, "RELEASE_GATES_PASSED", True)
    with pytest.raises(app.Refused, match="release gates have passed"):
        app.default_service(env)
    monkeypatch.setattr(S, "RELEASE_GATES_PASSED", False)
    svc = app.default_service(lab_only)
    try:
        h = svc.health()
        assert h["demonstration_qualification"] is None and h["qualification_basis"] == "none"
    finally:
        svc.close()


def test_the_basis_metric_is_bounded_and_shows_the_expiry():
    from prometheus_client import CollectorRegistry, generate_latest
    from forecasting.qualification_metrics import BASES, QualificationMetrics
    state = {"status": ("none", {})}
    m = QualificationMetrics(lambda: state["status"], D.REFUSALS)
    reg = CollectorRegistry()
    reg.register(m)

    def series():
        out = {}
        for line in generate_latest(reg).decode().splitlines():
            if line and not line.startswith("#"):
                name, value = line.rsplit(" ", 1)
                out[name] = float(value)
        return out
    p = "predictive_autoscaler_forecaster_"
    got = series()
    assert got == {f'{p}qualification_basis{{basis="{b}"}}': float(b == "none") for b in BASES}    # nothing else
    state["status"] = ("demonstration", {"demonstration": 1795910400})
    m.refused("cadence")
    m.refused("cadence")
    m.refused("family")
    got = series()
    assert got[f'{p}qualification_basis{{basis="demonstration"}}'] == 1 and sum(
        v for k, v in got.items() if "qualification_basis" in k) == 1
    assert got[f'{p}qualification_expires_timestamp_seconds{{document="demonstration"}}'] == 1795910400
    assert got[f'{p}demonstration_refusals_total{{reason="cadence"}}'] == 2
    assert got[f'{p}demonstration_refusals_total{{reason="family"}}'] == 1
    assert len([k for k in got if "refusals_total" in k]) == len(D.REFUSALS)
    with pytest.raises(ValueError):
        m.refused("anything else")                                          # never a free-form label
    state["status"] = ("none", {"demonstration": 1795910400})              # mounted and expired: both stay visible
    got = series()
    assert got[f'{p}qualification_basis{{basis="none"}}'] == 1
    assert got[f'{p}qualification_expires_timestamp_seconds{{document="demonstration"}}'] == 1795910400
    state["status"] = ("everything", {})
    with pytest.raises(ValueError):
        list(m.collect())
