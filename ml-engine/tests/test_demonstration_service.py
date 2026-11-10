"""The demonstration basis in the statistical service, end to end (offline, like test_statistical_service.py): it stands
in for the release gate and nothing else; only inside its scope, its validity and its contract's configuration; only
for a selected family it lists; an answer issued under it is re-checked on every reuse; and it is never combined with
a lab qualification or with passed release gates."""

import math
import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.identity import LookupFailed  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting import demonstration as D  # noqa: E402
from forecasting import qualification as Q  # noqa: E402
from forecasting import service as S  # noqa: E402
from forecasting.clock import ManualClock  # noqa: E402
from tests import demonstration_helpers as H  # noqa: E402
from tests import test_statistical_service as base  # noqa: E402

T0, DAY = base.T0, base.DAY
G, LEAD, STARTUP = 60, 180, 60                 # the demonstration's 60 s grid; the shortest lead that still acts
N = 118                                        # requests of the shared run: trust is earned at about the 110th
LAST = T0 + (N - 1) * G
BUILD = Q.BuildInfo(H.TREE, True)
PREDICTION = {"horizonMinutes": 60, "sampleIntervalSeconds": G, "historyDays": 2, "updateIntervalSeconds": 60,
              "families": ["seasonalNaive"], "trust": base.FAST_TRUST,
              "calibration": {"minWindows": 16, "maxScores": 64, "minMiscoverageBps": 600}}


def demand(t):
    """Exactly daily-periodic demand with a deterministic ripple: the seasonal-naive forecast earns trust quickly."""
    d = t % DAY
    return round(20 + 10 * math.sin(2 * math.pi * d / DAY) + (d * 2654435761 % 997) / 997, 6)


def demo_pa(prediction=None, **spec):
    pa = base.pa_object(prediction=dict(PREDICTION, **(prediction or {})))
    pa["status"]["prediction"]["lead"].update(seconds=LEAD, startupDelaySeconds=STARTUP)
    pa["spec"].update({"minReplicas": 1, "maxReplicas": 12, "metrics": {"requests": {"capacityPerReplica": "5.5"}},
                       **spec})
    return pa


def documents(*, expires=T0 + 15 * DAY, not_before=T0 - DAY, name="web", uid="dep-uid", namespace="shop",
              cluster="cluster-uid", contract_kw=None, **over):
    """A verified demonstration qualification (with its contract) for the test autoscaler's target."""
    kw = {"targets": [H.target(namespace=namespace, name=name, uid=uid)], "expires_not_after": H.ts(expires),
          **(contract_kw or {})}
    q, cd = H.pair(contract_kw=kw, not_before=H.ts(not_before), expires=H.ts(expires),
                   scope={"cluster_uid": cluster, "namespace": namespace,
                          "targets": [{"namespace": namespace, "name": name, "uid": uid}]}, **over)
    return D.parse(q, cd, BUILD, T0, H.DIGEST)


def open_service(path, *, pa=None, cluster="cluster-uid", at=T0, backend=None, **cfg):
    pa = pa or demo_pa()
    if "demonstration_qualification" in cfg or "lab_qualification" in cfg:
        cfg.setdefault("build", BUILD)
    svc, clock = base.make(path, reader=base.ClusterReader(pa, cluster), clock=ManualClock(at * 1000),
                           backend=backend or base.Backend(demand), store_id="journal-1", **cfg)
    return svc, clock, pa, c.resolve_settings(pa).fingerprint()


def ask(svc, clock, origin, settings):
    clock.set(max(clock.now_ms(), origin * 1000 + 10_000))
    return svc.predict(base.body(origin, settings))


def drive(path, n=N, **cfg):
    svc, clock, pa, settings = open_service(path, **cfg)
    wires = [ask(svc, clock, T0 + i * G, settings) for i in range(n)]
    return svc, clock, pa, settings, wires


def gate(svc):
    return svc.store.issuances()[-1].payload()["release_gate"]


def refusals(svc):
    return {r: n for r, n in svc.qualification_metrics._refusals.items() if n}


@pytest.fixture(scope="module")
def trusted(tmp_path_factory):
    """One run under a long-lived demonstration qualification, far enough for eligible answers; tests reopen copies of
    its journal."""
    path = tmp_path_factory.mktemp("trusted") / "journal"
    doc = documents()
    svc, _clock, pa, settings, wires = drive(path, demonstration_qualification=doc)
    bundles = [iss.payload() for iss in svc.store.issuances()]
    svc.close()
    return {"path": path, "doc": doc, "pa": pa, "settings": settings, "wires": wires, "bundles": bundles}


def reopen(trusted, tmp_path, *, at=LAST + 20, **cfg):
    path = tmp_path / "journal"
    if not path.exists():
        shutil.copytree(trusted["path"], path)
    cfg.setdefault("pa", trusted["pa"])
    return open_service(path, at=at, **cfg)


def retry_last(svc, settings):
    return c.response_from_wire(svc.predict(base.body(LAST, settings)))


# --- it replaces the release gate and nothing else --------------------------------------------------------------------

def test_a_demonstration_qualification_stands_in_for_the_release_gate_and_nothing_else(trusted, tmp_path):
    doc = trusted["doc"]
    got = {"demonstration": (trusted["wires"], trusted["bundles"])}
    for name, cfg in (("plain", {}), ("other-target", {"demonstration_qualification": documents(uid="recreated")})):
        svc, *_rest, wires = drive(tmp_path / name, **cfg)
        got[name] = (wires, [iss.payload() for iss in svc.store.issuances()])
        svc.close()

    def strip(b):                       # what the release gate may change: its record, the final reason and readiness
        v = {**b["verdict"], "reason": None, "prediction_ready": None}
        return {**{k: x for k, x in b.items() if k != "release_gate"}, "verdict": v}
    plain = [strip(b) for b in got["plain"][1]]
    for name in got:                    # identical evidence, calibration, bounds, trust and selection in every run
        assert [strip(b) for b in got[name][1]] == plain, name
    assert got["plain"][1][-1]["selected"] == "seasonal_naive"            # naive trusted within the run
    assert got["plain"][1][-1]["release_gate"] == {"passed": False, "basis": None, "lab_qualification_sha256": None}
    passed = {"passed": True, "basis": "demonstration", "lab_qualification_sha256": None,
              "demonstration_qualification_sha256": doc.sha256, "demonstration_contract_sha256": doc.contract_sha256,
              "refused": None}
    assert got["demonstration"][1][-1]["release_gate"] == passed
    other = got["other-target"][1][-1]["release_gate"]
    assert (other["passed"], other["basis"], other["refused"]) == (False, None, "target")
    eligible = {name: c.response_from_wire(w[-1]).predictive_eligible for name, (w, _) in got.items()}
    assert eligible == {"demonstration": True, "plain": False, "other-target": False}
    assert c.response_from_wire(got["demonstration"][0][-1]).reason == "Ready"
    assert c.response_from_wire(got["plain"][0][-1]).reason == "ReplayGateMissing"
    assert c.response_from_wire(got["other-target"][0][-1]).reason == "ReplayGateMissing"
    # The wire contract with the operator is the same object in every run: the same keys, the same family names.
    assert {tuple(sorted(w)) for wires, _ in got.values() for w in wires} == {tuple(sorted(got["plain"][0][0]))}
    assert c.response_from_wire(got["demonstration"][0][-1]).model_family == "seasonal_naive"
    # Every runtime gate still decides. Before trust was earned the gate was already substituted (passed, basis
    # demonstration), and the run answered exactly as the plain one: never eligible, the same reasons.
    demo_b, demo_w = got["demonstration"][1], got["demonstration"][0]
    first_ready = next(i for i, b in enumerate(demo_b) if b["verdict"]["prediction_ready"])
    assert first_ready > 16 and all(b["release_gate"] == passed for b in demo_b[:first_ready])
    assert all(not c.response_from_wire(w).predictive_eligible for w in demo_w[:first_ready])
    assert [b["verdict"]["reason"] for b in demo_b[:first_ready]] == \
        [b["verdict"]["reason"] for b in got["plain"][1][:first_ready]]
    assert {"CalibrationInsufficient", "ShadowInsufficient"} <= {b["verdict"]["reason"] for b in demo_b[:first_ready]}


def test_a_failing_coverage_gate_still_blocks_eligibility(trusted, tmp_path):
    backend = base.Backend(lambda t: demand(t) * (3.0 if t > LAST else 1.0))       # demand leaves every bound behind
    svc, clock, _pa, settings = reopen(trusted, tmp_path, backend=backend, demonstration_qualification=trusted["doc"])
    answers = [c.response_from_wire(ask(svc, clock, LAST + i * G, settings)) for i in range(1, 30)]
    assert answers[0].predictive_eligible is True                                   # nothing has matured yet
    low = [a for a in answers if a.reason == "CoverageLow"]
    assert low and all(not a.predictive_eligible and a.trust_state == "Degraded" for a in low)
    assert answers[-1].reason == "CoverageLow" and gate(svc)["passed"] is True      # the document still covers it
    assert gate(svc)["basis"] == "demonstration" and refusals(svc) == {}


def test_a_failing_availability_gate_still_blocks_eligibility(trusted, tmp_path):
    backend = base.Backend(demand)
    svc, clock, _pa, settings = reopen(trusted, tmp_path, backend=backend, demonstration_qualification=trusted["doc"])
    backend.missing = {LAST + i * G for i in range(1, 9)}                           # eight minutes never measured
    answers = []
    for i in range(1, 30):
        try:
            answers.append(c.response_from_wire(ask(svc, clock, LAST + i * G, settings)))
        except S.ServiceError as e:
            assert e.reason == "DataUnavailable"                                    # no answer at all: not eligible
    gone = [a for a in answers if a.reason == "DataUnavailable"]
    assert gone and all(not a.predictive_eligible for a in gone)
    assert all(b["release_gate"]["passed"] for b in (i.payload() for i in svc.store.issuances()[-len(answers):]))


# --- scope: cluster, namespace, target name and UID -------------------------------------------------------------------

@pytest.mark.parametrize("doc_kw,cluster,why", [
    ({}, "another-cluster", "cluster"),
    ({}, LookupFailed("forbidden"), "cluster"),                       # the cluster UID cannot be read: fail closed
    ({"cluster": "another-cluster"}, "cluster-uid", "cluster"),
    ({"namespace": "demo"}, "cluster-uid", "namespace"),
    ({"name": "api"}, "cluster-uid", "target"),
    ({"uid": "recreated"}, "cluster-uid", "target"),
])
def test_outside_its_scope_the_gate_does_not_pass(tmp_path, doc_kw, cluster, why):
    svc, clock, _pa, settings = open_service(tmp_path, cluster=cluster,
                                             demonstration_qualification=documents(**doc_kw))
    wire = ask(svc, clock, T0, settings)
    g = gate(svc)
    assert (g["passed"], g["basis"], g["refused"]) == (False, None, why)
    assert g["demonstration_qualification_sha256"] and c.response_from_wire(wire).predictive_eligible is False
    assert refusals(svc) == {why: 1}


def test_a_stored_eligible_answer_is_not_reused_in_another_cluster(trusted, tmp_path):
    svc, _clock, _pa, settings = reopen(trusted, tmp_path, demonstration_qualification=trusted["doc"])
    assert retry_last(svc, settings).predictive_eligible is True          # the same document, the same cluster
    svc.close()
    moved, _clock, _pa, settings = reopen(trusted, tmp_path, cluster="another-cluster",
                                          demonstration_qualification=trusted["doc"])
    r = retry_last(moved, settings)
    assert r.issuance_id == c.response_from_wire(trusted["wires"][-1]).issuance_id and r.predictive_eligible is False
    assert refusals(moved) == {"cluster": 1}


# --- the contract: the live configuration must be the reviewed one ----------------------------------------------------

@pytest.mark.parametrize("change,why", [
    ({"spec": {"maxReplicas": 13}}, "bounds"),
    ({"spec": {"minReplicas": 2}}, "bounds"),
    ({"spec": {"metrics": {"requests": {"capacityPerReplica": "6"}}}}, "capacity"),
    ({"spec": {"metrics": {"requests": {"capacityPerReplica": "5.5", "targetRPS": 5}}}}, "capacity"),
    ({"spec": {"metrics": {}}}, "capacity"),
    ({"prediction": {"updateIntervalSeconds": 300}}, "cadence"),
    ({"prediction": {"updateIntervalSeconds": None}}, "cadence"),
    ({"prediction": {"sampleIntervalSeconds": 300}}, "cadence"),
    ({"prediction": {"families": ["seasonalNaive", "statistical"]}}, "families"),
])
def test_a_live_configuration_that_differs_from_the_contract_is_not_eligible(trusted, tmp_path, change, why):
    # A first issuance under the changed configuration records why the gate did not pass.
    pa = demo_pa(prediction=change.get("prediction"), **change.get("spec", {}))
    if why == "cadence" and pa["spec"]["prediction"]["sampleIntervalSeconds"] == 300:
        pa["status"]["prediction"]["lead"].update(seconds=1200, startupDelaySeconds=420)
    svc, clock, _pa, settings = open_service(tmp_path / "fresh", pa=pa, demonstration_qualification=trusted["doc"])
    wire = ask(svc, clock, T0, settings)
    g = gate(svc)
    assert (g["passed"], g["basis"], g["refused"]) == (False, None, why)
    assert g["demonstration_contract_sha256"] == trusted["doc"].contract_sha256
    assert c.response_from_wire(wire).predictive_eligible is False and refusals(svc) == {why: 1}
    svc.close()
    # And an answer that WAS eligible stops being so the moment the live autoscaler drifts from the contract (the
    # forecast settings, and with them the stored issuance, are unchanged by a policy or cadence field).
    if why in ("bounds", "capacity") or change.get("prediction", {}).get("updateIntervalSeconds", 60) != 60:
        drifted, _clock, _pa, settings = reopen(trusted, tmp_path, pa=pa, demonstration_qualification=trusted["doc"])
        assert settings == trusted["settings"]
        r = retry_last(drifted, settings)
        assert r.issuance_id == c.response_from_wire(trusted["wires"][-1]).issuance_id
        assert r.predictive_eligible is False and refusals(drifted) == {why: 1}
        assert drifted.store.issuances()[-1].payload()["release_gate"]["passed"] is True   # the record stays true


# --- families: only the selected family the document lists ------------------------------------------------------------

def test_the_selected_family_decides_at_every_answer(trusted, tmp_path):
    svc, _clock, _pa, settings = reopen(trusted, tmp_path, demonstration_qualification=trusted["doc"])
    b = trusted["bundles"][-1]
    bound = c.bind_request(base.body(LAST, settings), svc.reader, now_ms=svc.clock.now_ms())
    assert b["selected"] == "seasonal_naive" and svc._authorized(bound, b) is True
    # The same stored answer with any other selected family (a challenger, a family this document cannot list, a
    # configuration name instead of the wire name, or none) is never authorized: cached and replayed answers included.
    for family in ("statistical", "lstm", "seasonalNaive", "prophet", "", None):
        assert svc._authorized(bound, {**b, "selected": family}) is False, family
    assert refusals(svc) == {"family": 4}                       # the two empty selections never reach the gate
    wire = dict(svc._response(bound, b))
    assert wire["predictive_eligible"] is True and wire["model_family"] == "seasonal_naive"


def test_a_family_outside_the_list_is_never_eligible(tmp_path, monkeypatch):
    """The challenger (statistical) gets selected over a trusted seasonal-naive forecast while the document lists only
    seasonalNaive. The configuration checks would refuse this autoscaler outright (it forecasts a family the contract
    does not list, on another grid), so they are answered with the contract's own values here: the selected-family
    rule alone must hold, at issuance and at every reuse."""
    from forecasting import policy_sim as P
    real_costs = P.paired_costs

    def passing(**kw):                                              # the same evidence, costs that promote E1
        r = real_costs(**kw)
        return dict(r, shortage=[0.0, r["shortage"][1]], replica_time=[r["replica_time"][1]] * 2)
    monkeypatch.setattr(P, "paired_costs", passing)
    pa = base.policy_pa(updateIntervalSeconds=60)                   # both families (the default), bounds 1..12
    doc = documents(expires=T0 + 20 * DAY, contract_kw={"targets": [H.target(namespace="shop",
                                                                             capacity_per_replica="5")]})
    real_refusal = D.DemonstrationQualification.refusal
    monkeypatch.setattr(D.DemonstrationQualification, "refusal", lambda self, **kw: real_refusal(
        self, **{**kw, "families": tuple(self.families), "sample_interval_seconds": G}))
    stop = lambda b: b["selected"] == "statistical"                 # noqa: E731
    cfg = {"reader": base.ClusterReader(pa), "until": stop}
    svc, out = base.drive(tmp_path / "demo", pa, 130, build=BUILD, demonstration_qualification=doc, **cfg)
    _plain, plain_out = base.drive(tmp_path / "plain", pa, 130, **cfg)
    last_wire, last = out[-1]
    assert last["selected"] == "statistical" and len(out) > 20
    # Until a family was selected the document covered the request (scope, validity and contract all held).
    assert all(b["release_gate"]["basis"] == "demonstration" for _, b in out[:-1])
    # The challenger's answer: no release gate at all, recorded with the reason; never ready, never eligible.
    g = last["release_gate"]
    assert (g["passed"], g["basis"], g["refused"]) == (False, None, "family")
    assert g["demonstration_qualification_sha256"] == doc.sha256
    assert last["verdict"]["reason"] == "ReplayGateMissing" and last["verdict"]["prediction_ready"] is False
    r = c.response_from_wire(last_wire)
    assert r.model_family == "statistical" and r.predictive_eligible is False and r.reason == "ReplayGateMissing"
    assert refusals(svc) == {"family": 1}
    assert not any(c.response_from_wire(w).predictive_eligible for w, _ in out)
    # Deciding the gate again after selection changed nothing else: evidence, bounds, trust, hysteresis and selection
    # are those of a service without any document.
    strip = lambda b: {**{k: x for k, x in b.items() if k != "release_gate"},      # noqa: E731
                       "verdict": {**b["verdict"], "reason": None, "prediction_ready": None}}
    assert [strip(b) for _, b in out] == [strip(b) for _, b in plain_out]
    assert plain_out[-1][1]["verdict"]["reason"] == "ReplayGateMissing"
    # Even a stored bundle that claimed the gate passed is refused for this family at the answer.
    o = last["decision_origin"]
    bound = c.bind_request(base.body(o, c.resolve_settings(pa).fingerprint()), svc.reader, now_ms=o * 1000 + 10_000)
    def forged(d):
        return {**last, "verdict": {**last["verdict"], "reason": "Ready", "prediction_ready": True},
                "release_gate": {"passed": True, "basis": "demonstration", "lab_qualification_sha256": None,
                                 "demonstration_qualification_sha256": d.sha256,
                                 "demonstration_contract_sha256": d.contract_sha256, "refused": None}}
    assert svc._authorized(bound, forged(doc)) is False
    assert c.response_from_wire(svc._response(bound, forged(doc))).predictive_eligible is False
    svc.close()
    # The control: the list is the only thing in the way. A document (and contract) that lists both families
    # authorizes the very same answer.
    both = documents(expires=T0 + 20 * DAY, families=["seasonalNaive", "statistical"], contract_kw={
        "families": ("seasonalNaive", "statistical"),
        "targets": [H.target(namespace="shop", capacity_per_replica="5")]})
    wide, _ = base.make(tmp_path / "demo", reader=base.ClusterReader(pa), store_id="journal-1", build=BUILD,
                        clock=ManualClock(o * 1000 + 10_000), demonstration_qualification=both)
    assert wide._authorized(bound, forged(both)) is True and wide._authorized(bound, forged(doc)) is False
    assert c.response_from_wire(wide._response(bound, forged(both))).predictive_eligible is True


# --- validity: expiry while running, and an answer never outlives the document ----------------------------------------

def test_expiry_while_running_stops_eligibility_and_is_visible(tmp_path):
    valid_until = LAST + G - 30                                 # the last answer's valid_until (actuation allowance 30)
    doc = documents(expires=valid_until + 10)
    svc, clock, _pa, settings, wires = drive(tmp_path, demonstration_qualification=doc)
    b = svc.store.issuances()[-1].payload()
    assert b["valid_until"] == valid_until and c.response_from_wire(wires[-1]).predictive_eligible is True
    assert svc.health()["qualification_basis"] == "demonstration" and refusals(svc) == {}
    assert svc.qualification_status() == ("demonstration", {"demonstration": doc.expires})
    clock.set((doc.expires + 5) * 1000)                         # it expired; the origin is still open
    again = retry_last(svc, settings)
    assert again.issuance_id == c.response_from_wire(wires[-1]).issuance_id and again.predictive_eligible is False
    # Visible: the basis in force is none while the mounted document and its expiry stay reported, and the refusals
    # are counted by reason.
    assert svc.qualification_status() == ("none", {"demonstration": doc.expires})
    assert svc.health()["qualification_basis"] == "none" and svc.health()["demonstration_qualification"]["expires"] == \
        doc.expires
    assert refusals(svc) == {"validity": 1}
    nxt = c.response_from_wire(ask(svc, clock, LAST + G, settings))            # and every later answer
    g = gate(svc)
    assert nxt.predictive_eligible is False and nxt.reason == "ReplayGateMissing" and nxt.trust_state == "Trusted"
    assert (g["passed"], g["basis"], g["refused"]) == (False, None, "validity")
    from prometheus_client import CollectorRegistry, generate_latest
    reg = CollectorRegistry()
    reg.register(svc.qualification_metrics)
    text = generate_latest(reg).decode()
    assert 'predictive_autoscaler_forecaster_qualification_basis{basis="none"} 1.0' in text
    assert 'predictive_autoscaler_forecaster_qualification_basis{basis="demonstration"} 0.0' in text
    (line,) = [x for x in text.splitlines() if x.startswith(
        'predictive_autoscaler_forecaster_qualification_expires_timestamp_seconds{document="demonstration"}')]
    assert float(line.rsplit(" ", 1)[1]) == doc.expires
    assert 'predictive_autoscaler_forecaster_demonstration_refusals_total{reason="validity"} 2.0' in text


def test_a_demonstration_answer_never_outlives_its_qualification(tmp_path):
    # The validity must cover each forecast's whole validity window: valid now is not enough.
    svc, clock, _pa, settings = open_service(tmp_path / "short", demonstration_qualification=documents(expires=T0 + 20))
    ask(svc, clock, T0, settings)                               # answered at T0 + 10 s, before the expiry
    b = svc.store.issuances()[-1].payload()
    assert clock.now_ms() // 1000 < T0 + 20 < b["valid_until"]
    assert (b["release_gate"]["passed"], b["release_gate"]["refused"]) == (False, "validity")
    exact, clock, _pa, settings = open_service(tmp_path / "exact",
                                               demonstration_qualification=documents(expires=b["valid_until"]))
    ask(exact, clock, T0, settings)
    assert gate(exact)["passed"] is True                        # an expiry exactly at valid_until still covers it
    early, clock, _pa, settings = open_service(tmp_path / "early",
                                               demonstration_qualification=documents(not_before=T0 + 3600))
    ask(early, clock, T0, settings)
    assert gate(early)["refused"] == "validity" and early.qualification_status()[0] == "none"   # not valid yet


# --- reuse: removal, replacement and another basis invalidate stored answers ------------------------------------------

def test_eligibility_is_reauthorised_at_every_answer(trusted, tmp_path):
    doc, first = trusted["doc"], c.response_from_wire(trusted["wires"][-1])
    assert first.predictive_eligible is True
    same, _clock, _pa, settings = reopen(trusted, tmp_path, demonstration_qualification=doc)
    assert retry_last(same, settings).predictive_eligible is True                # the same document and contract
    same.close()
    replacements = {
        # removed: the same journal under a release (default) build without any document
        "removed": {},
        # replaced by another reviewed qualification of the same contract
        "another qualification": {"demonstration_qualification": documents(reviewers=["executor", "second reviewer"])},
        # replaced by a qualification of another contract (the checked part is the same; the bytes are not)
        "another contract": {"demonstration_qualification": documents(contract_kw={"unchecked": {"revision": 2}})},
        # a lab qualification for the same target does not inherit a demonstration's answers
        "lab instead": {"lab_qualification": base.lab(expires=T0 + 15 * DAY)},
    }
    for name, cfg in replacements.items():
        svc, _clock, _pa, settings = reopen(trusted, tmp_path, **cfg)
        r = retry_last(svc, settings)
        assert r.issuance_id == first.issuance_id and r.predictive_eligible is False, name
        stored = svc.store.issuances()[-1].payload()["release_gate"]
        assert stored["basis"] == "demonstration" and stored["demonstration_qualification_sha256"] == doc.sha256
        svc.close()
    other = replacements["another qualification"]["demonstration_qualification"]
    contract = replacements["another contract"]["demonstration_qualification"]
    assert other.sha256 != doc.sha256 and other.contract_sha256 == doc.contract_sha256
    assert contract.contract_sha256 != doc.contract_sha256 and contract.contract.targets == doc.contract.targets
    # The identity an answer is tied to is the basis, the document's digest AND the contract's digest.
    assert S._GATE_IDENTITY == ("basis", "lab_qualification_sha256", "demonstration_qualification_sha256",
                                "demonstration_contract_sha256")
    svc, _clock, _pa, settings = reopen(trusted, tmp_path, demonstration_qualification=doc)
    b = trusted["bundles"][-1]
    bound = c.bind_request(base.body(LAST, settings), svc.reader, now_ms=svc.clock.now_ms())
    assert svc._authorized(bound, b) is True
    for key in S._GATE_IDENTITY[2:]:
        forged = {**b, "release_gate": {**b["release_gate"], key: "0" * 64}}
        assert svc._authorized(bound, forged) is False, key
    assert svc._authorized(bound, {**b, "release_gate": {**b["release_gate"], "basis": "lab"}}) is False
    assert svc._authorized(bound, {**b, "release_gate": {**b["release_gate"], "passed": False}}) is False


def test_a_lab_answer_is_not_reused_under_a_demonstration_qualification(tmp_path):
    # The reverse direction, on the lab run's own grid: the stored basis is lab, the mounted document a demonstration.
    last = T0 + 89 * base.DT
    svc, _clock, pa, settings, wires = base._lab_run(tmp_path, 90, base.lab(expires=T0 + 15 * DAY))
    assert c.response_from_wire(wires[-1]).predictive_eligible is True
    svc.close()
    demo, _ = base.make(tmp_path, reader=base.ClusterReader(pa), store_id="journal-1", build=BUILD,
                        clock=ManualClock((last + 20) * 1000), demonstration_qualification=documents())
    r = c.response_from_wire(demo.predict(base.body(last, settings)))
    assert r.issuance_id == c.response_from_wire(wires[-1]).issuance_id and r.predictive_eligible is False
    assert demo.store.issuances()[-1].payload()["release_gate"]["basis"] == "lab"


# --- start-up: never beside a lab qualification, a release build or passed release gates ------------------------------

def test_a_demonstration_qualification_fails_closed_at_start(tmp_path, monkeypatch):
    doc = documents()
    with pytest.raises(Q.QualificationRefused, match="evaluation build it names"):   # a release build never takes one
        base.make(tmp_path / "release", demonstration_qualification=doc, build=Q.BuildInfo(H.TREE, False))
    with pytest.raises(Q.QualificationRefused, match="evaluation build it names"):   # nor another tree's build
        base.make(tmp_path / "tree", demonstration_qualification=doc, build=Q.BuildInfo("c" * 40, True))
    with pytest.raises(Q.QualificationRefused, match="evaluation build it names"):   # nor a build without a record
        base.make(tmp_path / "none", demonstration_qualification=doc)
    with pytest.raises(Q.QualificationRefused, match="at most one"):                 # mutually exclusive with a lab one
        base.make(tmp_path / "both", demonstration_qualification=doc, lab_qualification=base.lab(), build=BUILD)
    # No silent precedence: with the release gates passed, the document is refused instead of ignored.
    monkeypatch.setattr(S, "RELEASE_GATES_PASSED", True)
    with pytest.raises(Q.QualificationRefused, match="release gates have passed"):
        base.make(tmp_path / "released", demonstration_qualification=doc, build=BUILD)
    # Without any document the release basis is what it always was.
    svc, clock, _pa, settings = open_service(tmp_path / "release-only")
    ask(svc, clock, T0, settings)
    assert gate(svc) == {"passed": True, "basis": "release", "lab_qualification_sha256": None}
    assert svc.qualification_status() == ("release", {})
    svc.close()
    # A lab qualification keeps its existing precedence under passed release gates (unchanged by this document type).
    lab_svc, clock, _pa, settings = open_service(tmp_path / "lab-release", lab_qualification=base.lab())
    ask(lab_svc, clock, T0, settings)
    assert gate(lab_svc)["basis"] == "release"


def test_release_gates_passing_under_a_running_demonstration_fail_closed(tmp_path, monkeypatch):
    # The constant cannot change in a running process; if it ever could, the answer is "not this basis", never both.
    svc, clock, _pa, settings = open_service(tmp_path, demonstration_qualification=documents())
    monkeypatch.setattr(S, "RELEASE_GATES_PASSED", True)
    ask(svc, clock, T0, settings)
    g = gate(svc)
    assert (g["passed"], g["basis"], g["refused"]) == (False, None, "release")


def test_the_basis_is_served_on_the_metrics_endpoint(tmp_path):
    from fastapi.testclient import TestClient
    from api import statistical
    svc, clock, _pa, settings = open_service(tmp_path, demonstration_qualification=documents())
    ask(svc, clock, T0, settings)
    with TestClient(statistical.create_app({}, service_factory=lambda: svc)) as client:
        text = client.get("/metrics").text
        health = client.get("/health").json()
    assert 'predictive_autoscaler_forecaster_qualification_basis{basis="demonstration"} 1.0' in text
    assert 'predictive_autoscaler_forecaster_qualification_expires_timestamp_seconds{document="demonstration"}' in text
    assert "predictive_autoscaler_forecast_issued_total" in text                  # beside the per-autoscaler series
    assert health["qualification_basis"] == "demonstration"
    assert health["demonstration_qualification"]["families"] == ["seasonalNaive"]
