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
from tests import demonstration_helpers as H  # noqa: E402
from tests.demonstration_helpers import DIGEST, EVAL, NOW, SETTINGS, TREE, contract, contract_doc, pair, \
    qualification, target  # noqa: E402

SPEC = {"minReplicas": 1, "maxReplicas": 12, "metrics": {"requests": {"capacityPerReplica": "5.5"}},
        "prediction": {"updateIntervalSeconds": 60}}


def parse(q=None, cd=None, build=EVAL, now=NOW, digest=DIGEST):
    default_q, default_c = pair()
    return D.parse(default_q if q is None else q, default_c if cd is None else cd, build, now, digest)


def live(d, **over):
    """refusal() for a request that is fully in scope unless `over` says otherwise."""
    kw = dict(cluster_uid="cluster-uid", autoscaler_namespace="demo", namespace="demo", name="web", uid="dep-uid",
              now_s=NOW, valid_until=NOW + 600, spec=SPEC, families=("seasonalNaive",), sample_interval_seconds=60,
              settings_sha256=SETTINGS)
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
    other = "0f" * 32                                                  # the second autoscaler's own reviewed settings
    targets = [target(), target(name="api", uid="api-uid", max_replicas=6, capacity_per_replica="12",
                                settings_sha256=other)]
    q, cd = pair(contract_kw={"targets": targets},
                 scope={"cluster_uid": "cluster-uid", "namespace": "demo",
                        "targets": [{k: t[k] for k in ("namespace", "name", "uid")} for t in targets]})
    d = D.parse(q, cd, EVAL, NOW, DIGEST)
    assert live(d) is None and live(d, name="api", uid="api-uid") == "settings"    # each target has its own contract
    assert live(d, name="api", uid="api-uid", settings_sha256=other) == "bounds"
    api = {**SPEC, "maxReplicas": 6, "metrics": {"requests": {"capacityPerReplica": 12}}}
    assert live(d, name="api", uid="api-uid", spec=api, settings_sha256=other) is None
    assert live(d, name="api", uid="dep-uid", spec=api, settings_sha256=other) == "target"   # name and uid together
    assert live(d, settings_sha256=other) == "settings"                            # never the other target's settings


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
    ({"reviewers": ["Alice", "alice"]}, "two distinct reviewers"),                   # compared without regard to case
    ({"reviewers": ["executor", "EXECUTOR\n"]}, "two distinct reviewers"),
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
    (_scope(namespace="Demo"), "exactly one namespace"),                              # a DNS-1123 label
    (_scope(namespace="demo.shop"), "exactly one namespace"),
    (_scope(namespace="demo,shop"), "exactly one namespace"),
    (_scope(namespace="*"), "exactly one namespace"),
    (_scope(cluster_uid="cluster uid"), "cluster_uid"),
    (_scope(targets=[{"namespace": "demo", "name": "web..api", "uid": "dep-uid"}]), "DNS-1123 subdomain"),
    (_scope(targets=[{"namespace": "demo", "name": "Web", "uid": "dep-uid"}]), "DNS-1123 subdomain"),
    (_scope(targets=[{"namespace": "demo", "name": "*", "uid": "dep-uid"}]), "DNS-1123 subdomain"),
    (_scope(targets=[{"namespace": "demo", "name": "web", "uid": "dep uid"}]), "and a uid"),
    (_scope(targets=[{"namespace": "demo", "name": "web", "uid": 7}]), "and a uid"),
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
    (_contract_with(targets=[target(uid="")]), "and a uid"),
    (_contract_with(targets=[target(name="web.-api")]), "DNS-1123 subdomain"),
    (_contract_with(targets=[target(namespace="Demo")]), "DNS-1123 label"),
    (_contract_with(targets=[{k: v for k, v in target().items() if k != "settings_sha256"}]), "needs exactly"),
    (_contract_with(targets=[target(settings_sha256="nope")]), "settings_sha256"),
    (_contract_with(targets=[target(settings_sha256="5E" * 32)]), "settings_sha256"),
    (_contract_with(targets=[target(settings_sha256=None)]), "settings_sha256"),
    (_contract_with(targets=[target(settings_sha256="sha256:" + "5e" * 32)]), "settings_sha256"),
    # Reserved for the policy revisions of the bound identity: not compared by this build, so not accepted by it.
    (_contract_with(calibration_policy_revision="calibration/v1"), "calibration_policy_revision is reserved"),
    (_contract_with(trust_policy_revision="trust/v1"), "trust_policy_revision is reserved"),
    (_contract_with(trust_policy_revision=None, calibration_policy_revision=None),
     "calibration_policy_revision is reserved"),
    (_contract_with(targets=[target(calibration_policy_revision="calibration/v1")]), "needs exactly"),
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
    ({"settings_sha256": "0f" * 32}, "settings"),                          # a reviewed trust or calibration setting
    ({"settings_sha256": None}, "settings"),
    ({"settings_sha256": SETTINGS + "\n"}, "settings"),
    ({"settings_sha256": SETTINGS.upper()}, "settings"),
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
                            families=("statistical",), sample_interval_seconds=300, settings_sha256="0f" * 32,
                            spec={"prediction": {"updateIntervalSeconds": 60}}, selected="statistical")
    order = []
    for key, good in (("cluster_uid", "cluster-uid"), ("autoscaler_namespace", "demo"), ("namespace", "demo"),
                      ("name", "web"), ("now_s", NOW), ("families", ("seasonalNaive",)),
                      ("sample_interval_seconds", 60), ("settings_sha256", SETTINGS),
                      ("spec", {**SPEC, "metrics": {}}), ("spec", SPEC), ("selected", "seasonal_naive")):
        order.append(live(d, **everything_wrong))
        everything_wrong[key] = good
    assert order == ["cluster", "namespace", "namespace", "target", "validity", "families", "cadence", "settings",
                     "bounds", "capacity", "family"] and live(d, **everything_wrong) is None
    # With the three checks that come before any request is looked at, that is the whole fixed enum, in order.
    assert D.REFUSALS == ("release", "qualification-file", "contract-file") + tuple(dict.fromkeys(order))


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


TAILS = ["\n", "\r", "\t", "\x00", " ", "\r\n", "\x0b", "\u2028"]


@pytest.mark.parametrize("tail", TAILS)
def test_no_trailing_control_character_passes_any_validation(tail):
    """Every digest, tree, time, name and number of both files is matched whole (fullmatch): a value followed by a
    newline, carriage return, tab, NUL or other whitespace is refused, as is one preceded by it."""
    def refused(why, q=None, cd=None, build=EVAL, digest=DIGEST):
        default_q, default_c = pair()
        with pytest.raises(Q.QualificationRefused, match=why):
            D.parse(default_q if q is None else q, default_c if cd is None else cd, build, NOW, digest)

    def with_contract(why, **checked):
        cd = _contract_with(**checked)
        refused(why, qualification(cd), cd)
    for bad in (lambda v: v + tail, lambda v: tail + v):
        # The build's tree and the pod's digest, even when the document repeats the same bytes.
        refused("no source tree", pair(source_tree=bad(TREE))[0], build=Q.BuildInfo(bad(TREE), True))
        refused("digest is unknown", pair(image_digest=bad(DIGEST))[0], digest=bad(DIGEST))
        refused("another source tree", pair(source_tree=bad(TREE))[0])
        refused("another image digest", pair(image_digest=bad(DIGEST))[0])
        refused("contract_sha256", pair(contract_sha256=bad(c.sha256_hex(contract())))[0])
        refused("ending in Z", pair(not_before=bad("2026-11-08T00:00:00Z"))[0])
        refused("ending in Z", pair(expires=bad("2026-11-28T00:00:00Z"))[0])
        refused("cluster_uid", pair(**_scope(cluster_uid=bad("u")))[0])
        refused("exactly one namespace", pair(**_scope(namespace=bad("demo")))[0])
        for key, why in (("namespace", "DNS-1123 label"), ("name", "DNS-1123 subdomain"), ("uid", "and a uid")):
            t = {"namespace": "demo", "name": "web", "uid": "dep-uid"}
            refused(why, pair(**_scope(targets=[{**t, key: bad(t[key])}]))[0])
            with_contract(why, targets=[target(**{key: bad(target()[key])})])
        refused("families", pair(families=[bad("seasonalNaive")])[0])
        with_contract("checked.families", families=[bad("seasonalNaive")])
        with_contract("capacity_per_replica", targets=[target(capacity_per_replica=bad("5.5"))])
        with_contract("capacity_per_replica", targets=[target(capacity_per_replica=bad("5"))])
        with_contract("settings_sha256", targets=[target(settings_sha256=bad(SETTINGS))])
        with_contract("expires_not_after", qualification={"expires_not_after": bad("2026-11-28T00:00:00Z")})
    # Inside a value as well: a digest or a number broken by a control character.
    with_contract("capacity_per_replica", targets=[target(capacity_per_replica="5" + tail + "5")])
    with_contract("settings_sha256", targets=[target(settings_sha256=SETTINGS[:32] + tail + SETTINGS[33:])])
    assert parse().contract.targets[0].capacity_per_replica == "5.5"          # and the clean document still parses


def test_the_mounted_files_are_checked_again_on_every_call(tmp_path):
    """mount_refusal() reads and hashes both files each time it is called: missing, unreadable, oversized or changed
    bytes are refused; nothing read there is ever admitted; the admitted bytes, once back, are accepted again."""
    q, cd = pair()
    assert D.parse(q, cd, EVAL, NOW, DIGEST).mount_refusal() == "qualification-file"   # never loaded from a mount
    d = H.load(tmp_path / "m", q, cd)
    qf, cf = d.mounted
    assert d.mount_refusal() is None and d.mounted == (qf, cf)
    other_q = qualification(cd, reviewers=["executor", "another reviewer"])           # a valid document, not admitted
    other_c = contract(unchecked={"revision": 2})
    changes = {"qualification-file": [(qf, None), (qf, other_q), (qf, q[:-1]), (qf, q + b"\n"), (qf, b""),
                                      (qf, q + b" " * D.MAX_BYTES), (qf, cd)],
               "contract-file": [(cf, None), (cf, other_c), (cf, cd[:-1]), (cf, cd + b"\n"), (cf, b""),
                                 (cf, cd + b" " * D.MAX_BYTES), (cf, q)]}
    for why, cases in changes.items():
        for path, data in cases:
            os.remove(path)
            if data is not None:
                with open(path, "wb") as fh:
                    fh.write(data)
            assert d.mount_refusal() == why, (why, data and data[:20])
            with open(path, "wb") as fh:                                              # the admitted bytes again
                fh.write(q if path == qf else cd)
            assert d.mount_refusal() is None
    for path, why in ((qf, "qualification-file"), (cf, "contract-file")):             # unreadable: a directory there
        good = open(path, "rb").read()
        os.remove(path)
        os.mkdir(path)
        assert d.mount_refusal() == why
        os.rmdir(path)
        with open(path, "wb") as fh:
            fh.write(good)
        if os.geteuid() != 0:                                                         # and no permission to read it
            os.chmod(path, 0)
            assert d.mount_refusal() == why
            os.chmod(path, 0o644)
        assert d.mount_refusal() is None
    os.remove(qf)
    os.remove(cf)
    assert d.mount_refusal() == "qualification-file"                                  # both gone: the first names it
    assert d.sha256 == c.sha256_hex(q) and d.contract_sha256 == c.sha256_hex(cd)      # the object never changed


def test_a_configmap_style_update_is_seen_without_a_modification_time(tmp_path):
    # The kubelet updates a ConfigMap volume by swapping the ..data symlink; the new files may carry any timestamp.
    q, cd = pair()
    root = tmp_path / "volume"
    first, second = root / "..2026_11_10_a", root / "..2026_11_10_b"
    H.mount(first, q, cd)
    H.mount(second, qualification(cd, reviewers=["executor", "another reviewer"]), cd)
    for name in os.listdir(first):
        st = os.stat(first / name)
        os.utime(second / name, ns=(st.st_atime_ns, st.st_mtime_ns))                  # identical times, same sizes
        os.symlink(os.path.join("..data", name), root / name)
    os.symlink(first.name, root / "..data")
    d = D.load(str(root / "demonstration-qualification.json"), str(root / "demonstration-contract.json"), EVAL, NOW,
               DIGEST)
    assert d.mount_refusal() is None
    assert os.path.getsize(first / "demonstration-qualification.json") != 0
    os.symlink(second.name, root / "..data.tmp")
    os.replace(root / "..data.tmp", root / "..data")                                  # the atomic swap
    assert d.mount_refusal() == "qualification-file"
    os.symlink(first.name, root / "..data.tmp")
    os.replace(root / "..data.tmp", root / "..data")
    assert d.mount_refusal() is None


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
    loaded = D.load(str(qf), str(cf), EVAL, NOW, DIGEST)
    assert loaded.cluster_uid == "cluster-uid" and loaded.mounted == (str(qf), str(cf))
    in_memory = D.parse(q, cd, EVAL, NOW, DIGEST)
    assert in_memory.mounted is None and loaded.sha256 == in_memory.sha256
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
        assert h["demonstration_qualification"]["withdrawn"] is None
        # The app's own service checks the mounted files again without a restart: here the contract is replaced.
        cf.write_bytes(contract(targets=[target(max_replicas=24)]))
        h = svc.health()
        assert h["demonstration_qualification"]["withdrawn"] == "contract-file" and h["qualification_basis"] == "none"
        assert h["demonstration_qualification"]["contract_sha256"] == c.sha256_hex(cd)      # nothing was hot-loaded
        cf.write_bytes(cd)
        assert svc.health()["qualification_basis"] == "demonstration"
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
