"""The demonstration params stage, its rendering and its admission audit (LIVE-DEMO-PLAN section 5)."""

import copy
import hashlib
import json

import pytest

from c28 import demo, frozen as F, render
from demo_helpers import changed, params, prod_like


def problems(p):
    return render.stage_problems(p, "demonstration")


def refused(p, text):
    found = problems(p)
    assert any(text in x for x in found), found
    with pytest.raises(render.ParamsError, match="demonstration stage"):
        render.validate_stage(p, "demonstration")
    with pytest.raises(render.ParamsError):
        demo.render(p)


# --- the stage validates what it consumes, and only that --------------------------------------------------------------

def test_the_kind_and_the_prod_shaped_params_pass_the_stage():
    for p in (params(), prod_like()):
        assert problems(p) == []
        render.validate_stage(p, "demonstration")


def test_the_stage_does_not_consume_the_experiment_only_fields():
    p = prod_like()
    assert not any(k in p for k in render.EXPERIMENT_ONLY)          # absent: no dummy value is needed
    assert not any(f.split(".")[0] in render.EXPERIMENT_ONLY for f in render.STAGE_FIELDS["demonstration"])
    before = json.dumps(demo.render(p), sort_keys=True)
    for junk in ({"c0_rps": "<measured c0>", "hpa_cpu_target_percent": "__HPA__", "keda": "<not used>"},
                 {"c0_rps": 99, "hpa_cpu_target_percent": 50, "keda": {"server_address": "http://x", "polling_interval_seconds": 1}}):
        q = dict(p, **junk)
        assert problems(q) == []                                     # placeholders or values: never read
        assert json.dumps(demo.render(q), sort_keys=True) == before  # and never rendered
    with pytest.raises(render.ParamsError):                          # the comparison experiment still needs them
        render.validate(p)


def test_passing_the_stage_says_nothing_about_the_experiment():
    assert render.full_validation_problems(prod_like())


@pytest.mark.parametrize("path, value", [
    ("demonstration.capacity_per_replica_rps", "<0.875 x the provisional rate>"),
    ("demonstration.schedule_origin_utc", "__ORIGIN__"),
    ("demonstration.reservation_ref.nodes.workload.uid", "<W node UID>"),
    ("demonstration.generator.remote_write_url", "__REMOTE_WRITE__"),
    ("fixture.cpu_request", "<calibrated>"),
    ("routes.metrics_upstream", "__VMSELECT__"),
    ("nodes.support", "<S node name>"),
    ("image", "__FIXTURE_IMAGE__"),
])
def test_placeholders_are_refused(path, value):
    refused(changed(prod_like(), path, value), "placeholder" if not path == "image" else "pinned by digest")


def test_the_prod_template_is_refused_until_filled():
    with open(demo_template()) as fh:
        t = json.load(fh)
    text = "; ".join(problems(t))
    for f in ("nodes.workload is still a placeholder", "reservation.taint_value is still a placeholder",
              "demonstration.capacity_per_replica_rps", "demonstration.schedule_origin_utc",
              "demonstration.reservation_ref.run_id"):
        assert f in text, text
    assert not any(k in t for k in render.EXPERIMENT_ONLY)


def demo_template():
    import os
    return os.path.join(os.path.dirname(__file__), "..", "..", "params", "demonstration.template.json")


@pytest.mark.parametrize("cadence", [{"grid_seconds": 60, "update_interval_seconds": 30},
                                     {"grid_seconds": 30, "update_interval_seconds": 60},
                                     {"grid_seconds": 30, "update_interval_seconds": 30},
                                     {"grid_seconds": 60, "update_interval_seconds": 120},
                                     {"grid_seconds": 60}, {"grid_seconds": 60.5, "update_interval_seconds": 60},
                                     {"grid_seconds": 60, "update_interval_seconds": 60, "extra": 1},
                                     {"grid_seconds": True, "update_interval_seconds": 60}, "60/60", None])
def test_any_cadence_but_60_60_is_refused(cadence):
    refused(changed(prod_like(), "demonstration.cadence", cadence), "demonstration.cadence must be exactly")


def test_the_rendered_cadence_and_lead_are_the_params_and_the_qualified_ones(monkeypatch):
    p = prod_like()
    for pa in demo.render(p)["pas"]:
        assert pa["spec"]["prediction"]["sampleIntervalSeconds"] == 60
        assert pa["spec"]["prediction"]["updateIntervalSeconds"] == 60
        assert pa["spec"]["prediction"]["minLeadSeconds"] == 600 == p["demonstration"]["min_lead_seconds"]
    # The comparison's frozen cadence drifting does not move the demonstration: it renders the params' (60/60/600).
    monkeypatch.setattr(F, "DECISION_INTERVAL_S", 30)
    monkeypatch.setattr(F, "SAMPLE_INTERVAL_S", 300)
    for pa in demo.render(p)["pas"]:
        pred = pa["spec"]["prediction"]
        assert (pred["sampleIntervalSeconds"], pred["updateIntervalSeconds"], pred["minLeadSeconds"]) == (60, 60, 600)
    # A frozen value OUTSIDE the qualified profile is caught by the rendering audit.
    for name, value, text in (("SETTLEMENT_S", 60, "prediction.settlementSeconds must be exactly 120"),
                              ("GRACE_S", 300, "prediction.outcomeGraceSeconds must be exactly 600"),
                              ("MISCOVERAGE_BPS", 500, "prediction.risk must be exactly")):
        with monkeypatch.context() as m:
            m.setattr(F, name, value)
            with pytest.raises(render.ParamsError, match=text):
                demo.render(p)


@pytest.mark.parametrize("value", [0, 60, 180, 540, 660, 900, "600", 600.0, True, None, KeyError])
def test_any_minimum_lead_but_600_is_refused(value):
    text = "is missing min_lead_seconds" if value is KeyError else "demonstration.min_lead_seconds must be 600"
    refused(changed(prod_like(), "demonstration.min_lead_seconds", value), text)


# --- the qualified forecast profile, judged by the forecaster's own contracts (R3 item 2) --------------------------

def contracts():
    import os
    import sys
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "ml-engine")
    if os.path.abspath(root) not in sys.path:
        sys.path.insert(0, os.path.abspath(root))
    from forecasting import contracts as C
    return C


def profile_of(C, pa):
    """The allowlist entry the rendered object's settings resolve to at the 600 s lead (None: not a qualified
    configuration; a refused object is not one either)."""
    try:
        return C.validated_profile(C.resolve_settings(pa), 600)
    except C.ContractRefused:
        return None


def test_both_rendered_predictiveautoscalers_resolve_to_the_one_qualified_profile():
    C = contracts()
    assert len(C.VALIDATED_PROFILES) == 1
    want = C.VALIDATED_PROFILES[0]
    assert (want.sample_interval_seconds, want.update_interval_seconds, want.lead_seconds, want.miscoverage_bps,
            want.families) == (60, 60, 600, 1000, ("seasonalNaive",))
    for p in (prod_like(), params()):
        pas = {o["metadata"]["labels"][demo.DEMO_ROLE_LABEL]: o for o in demo.render(p)["pas"]}
        assert sorted(pas) == ["forecast", "reactive"]
        for role, pa in pas.items():
            pred = pa["spec"]["prediction"]
            # Explicit in the object: the grid, the update interval, the minimum lead and the families.
            assert (pred["sampleIntervalSeconds"], pred["updateIntervalSeconds"], pred["minLeadSeconds"],
                    pred["families"]) == (60, 60, 600, ["seasonalNaive"])
            assert "calibration" not in pred and "trust" not in pred       # NO override: the defaults apply
            s = C.resolve_settings(pa)
            assert (s.calibration, s.trust) == (C.CalibrationSettings(), C.TrustSettings())
            assert (s.min_lead_seconds, C.minimum_lead_seconds(s)) == (600, 600)
            assert C.validated_profile(s, 600) is want, role
            # The lead the operator selects for this object IS the qualified one: startup profile s20 alone would
            # give 180 s (startup 30 + decision 60 + allowance 15 + one grid step), which was never qualified.
            st = pa["spec"]["behavior"]["startup"]
            lead = C.automatic_lead_seconds(st["minimumDelaySeconds"], s.update_interval_seconds,
                                            st["actuationAllowanceSeconds"], s.sample_interval_seconds,
                                            s.min_lead_seconds)
            assert lead == 600 and C.validated_profile(s, lead) is want
            short = C.automatic_lead_seconds(st["minimumDelaySeconds"], 60, st["actuationAllowanceSeconds"], 60, 0)
            assert short == 180 and C.validated_profile(s, short) is None
        # The reactive reference is the same object with the predictive extra an explicit zero.
        f, r = pas["forecast"]["spec"]["prediction"], pas["reactive"]["spec"]["prediction"]
        assert r["predictiveExtra"] == {"maxReplicas": 0, "maxPercent": 0}
        assert {k: v for k, v in f.items() if k != "predictiveExtra"} == \
            {k: v for k, v in r.items() if k != "predictiveExtra"}


@pytest.mark.parametrize("edit", [
    lambda pred: pred.update(sampleIntervalSeconds=300),
    lambda pred: pred.update(updateIntervalSeconds=30),
    lambda pred: pred.update(updateIntervalSeconds=300),
    lambda pred: pred.pop("updateIntervalSeconds") and pred.update(updateIntervalSeconds=120),
    lambda pred: pred.update(families=["seasonalNaive", "statistical"]),
    lambda pred: pred.update(risk={"miscoverageBps": 500}),
    lambda pred: pred.update(trust={"minShadowHours": 24}),
    lambda pred: pred.update(trust={"acuteConsecutiveMisses": 6}),
    lambda pred: pred.update(calibration={"minWindows": 64}),
    lambda pred: pred.update(calibration={"etaBps": 100}),
    lambda pred: pred.update(settlementSeconds=60),
    lambda pred: pred.update(outcomeGraceSeconds=300),
    lambda pred: pred.update(engine="legacyLstm"),
])
def test_a_rendered_object_with_any_qualified_setting_changed_is_not_the_profile(edit):
    C = contracts()
    p = prod_like()
    for pa in demo.render(p)["pas"]:
        assert profile_of(C, pa) is C.VALIDATED_PROFILES[0]
        changed_pa = copy.deepcopy(pa)
        edit(changed_pa["spec"]["prediction"])
        assert profile_of(C, changed_pa) is None
        # ... and the stage's own audit refuses to render it.
        fams = pa["spec"]["prediction"]["families"]
        assert demo.qualified_prediction_problems(changed_pa["spec"]["prediction"], fams)
        assert demo.qualified_prediction_problems(pa["spec"]["prediction"], fams) == []


@pytest.mark.parametrize("lead", [0, 60, 180, 540, 660, 1200])
def test_another_minimum_lead_is_not_the_qualified_profile(lead):
    """minLeadSeconds decides the lead the operator selects: with any other value the selected lead is not 600 s, and
    the configuration at that lead is not on the allowlist. (Absent is 0.)"""
    C = contracts()
    pa = copy.deepcopy(demo.render(prod_like())["pas"][0])
    pred = pa["spec"]["prediction"]
    pred["minLeadSeconds"] = lead
    if lead == 0:
        del pred["minLeadSeconds"]
    s = C.resolve_settings(pa)
    st = pa["spec"]["behavior"]["startup"]
    selected = C.automatic_lead_seconds(st["minimumDelaySeconds"], s.update_interval_seconds,
                                        st["actuationAllowanceSeconds"], s.sample_interval_seconds, s.min_lead_seconds)
    assert selected == max(180, lead) != 600
    assert C.validated_profile(s, selected) is None
    assert any("minLeadSeconds" in x for x in demo.qualified_prediction_problems(pred, pred["families"]))


def test_the_audit_refuses_a_rendering_outside_the_qualified_profile(monkeypatch):
    p = prod_like()
    real = demo.qualified_pa
    for edit, text in ((lambda pred: pred.update(calibration={"minWindows": 128, "maxScores": 512, "retentionDays": 7}),
                        "prediction.calibration must not be rendered"),
                       (lambda pred: pred.update(trust={}), "prediction.trust must not be rendered"),
                       (lambda pred: pred.pop("minLeadSeconds"), "minLeadSeconds"),
                       (lambda pred: pred.update(minLeadSeconds=300), "minLeadSeconds"),
                       (lambda pred: pred.update(historyDays=7), "prediction.historyDays must not be rendered"),
                       (lambda pred: pred.update(somethingNew=1), "settings outside the qualified profile")):
        def altered(params, pa, edit=edit):
            out = real(params, pa)
            edit(out["spec"]["prediction"])
            return out
        monkeypatch.setattr(demo, "qualified_pa", altered)
        with pytest.raises(render.ParamsError, match=text):
            demo.render(p)


@pytest.mark.parametrize("targets, text", [
    ([{"name": "c28-demo-pa-n", "role": "forecast", "families": ["seasonalNaive"]}], "exactly two targets"),
    ([{"name": "c28-demo-pa-n", "role": "forecast", "families": ["seasonalNaive"]},
      {"name": "c28-demo-pa-r", "role": "reactive"}, {"name": "c28-demo-pa-x", "role": "reactive"}], "exactly two targets"),
    ([{"name": "c28-demo-pa-n", "role": "forecast", "families": ["seasonalNaive"]},
      {"name": "c28-demo-pa-r", "role": "forecast", "families": ["seasonalNaive"]}], "roles forecast and reactive"),
    ([{"name": "c28-demo-pa-n", "role": "reactive"}, {"name": "c28-demo-pa-r", "role": "reactive"}],
     "roles forecast and reactive"),
    ([{"name": "c28-demo-pa-n", "role": "forecast", "families": ["seasonalNaive"]},
      {"name": "c28-demo-pa-n", "role": "reactive"}], "two different names"),
    ([{"name": "demo-pa-n", "role": "forecast", "families": ["seasonalNaive"]},
      {"name": "c28-demo-pa-r", "role": "reactive"}], "must be a c28-* DNS label"),
    ([{"name": "c28-pa-n-s20", "role": "forecast", "families": ["seasonalNaive"]},
      {"name": "c28-demo-pa-r", "role": "reactive"}], "comparison arm's"),
    ([{"name": "c28-ingress", "role": "forecast", "families": ["seasonalNaive"]},
      {"name": "c28-demo-pa-r", "role": "reactive"}], "run-wide object's name"),
    ([{"name": "c28-demo-pa-n", "role": "forecast", "families": ["seasonalNaive", "statistical"]},
      {"name": "c28-demo-pa-r", "role": "reactive"}], "families must be exactly"),
    ([{"name": "c28-demo-pa-n", "role": "forecast", "families": []},
      {"name": "c28-demo-pa-r", "role": "reactive"}], "families must be exactly"),
    ([{"name": "c28-demo-pa-n", "role": "forecast"}, {"name": "c28-demo-pa-r", "role": "reactive"}],
     "is missing families"),
    ([{"name": "c28-demo-pa-n", "role": "forecast", "families": ["seasonalNaive"]},
      {"name": "c28-demo-pa-r", "role": "reactive", "families": ["statistical"]}], "unknown fields families"),
])
def test_exactly_two_targets_with_the_two_roles(targets, text):
    refused(changed(prod_like(), "demonstration.targets", targets), text)


@pytest.mark.parametrize("profile", ["s90", "s20,s90", ["s20"], ["s20", "s90"], "", None])
def test_one_profile_only(profile):
    refused(changed(prod_like(), "demonstration.profile", profile), "one startup profile s20")


@pytest.mark.parametrize("path, value, text", [
    ("demonstration.bounds", {"min": 1, "max": 16}, "bounds must be exactly"),
    ("demonstration.bounds", {"min": 2, "max": 12}, "bounds must be exactly"),
    ("demonstration.bounds", {"min": 1}, "bounds must be exactly"),
    ("demonstration.shadow_replicas", 13, "within the bounds 1..12"),
    ("demonstration.shadow_replicas", 0, "within the bounds 1..12"),
    ("demonstration.shadow_replicas", 12.0, "within the bounds 1..12"),
    ("demonstration.shadow_replicas", True, "within the bounds 1..12"),
])
def test_bounds_and_shadow_replicas(path, value, text):
    refused(changed(prod_like(), path, value), text)


@pytest.mark.parametrize("n", [1, 6, 12])
def test_shadow_replicas_within_the_bounds_are_rendered(n):
    p = changed(prod_like(), "demonstration.shadow_replicas", n)
    deps = [o for o in demo.render(p)["targets"] if o["kind"] == "Deployment"]
    assert [d["spec"]["replicas"] for d in deps] == [n, n]


@pytest.mark.parametrize("path, value, text", [
    ("demonstration.capacity_per_replica_rps", 0, "must be a positive number"),
    ("demonstration.capacity_per_replica_rps", -1.5, "must be a positive number"),
    ("demonstration.capacity_per_replica_rps", "17.5", "must be a positive number"),
    ("demonstration.capacity_per_replica_rps", True, "must be a positive number"),
    ("demonstration.capacity_label", "calibrated", 'must be the literal "provisional"'),
    ("demonstration.capacity_label", KeyError, "is missing capacity_label"),
    ("demonstration.capacity_source_calibration", "revision 4", "must name the source calibration id"),
    ("demonstration.capacity_source_calibration", KeyError, "is missing capacity_source_calibration"),
    ("demonstration.peak_multiple", 10, "peak_multiple must be 9"),
    ("demonstration.schedule_origin_utc", "2026-11-12T06:00:00Z", "must be a UTC midnight"),
    ("demonstration.schedule_origin_utc", "2026-11-12T00:00:00+00:00", "must be a UTC midnight"),
    ("demonstration.schedule_origin_utc", "2026-02-30T00:00:00Z", "must be a UTC midnight"),
    ("demonstration.schedule_origin_utc", 1793491200, "must be a UTC midnight"),
    ("demonstration.program.kind", "scored", "program.kind must be ordinary"),
    ("demonstration.program.seed", 201, "must not be a tuning or confirmation seed"),
    ("demonstration.program.seed", 103, "must not be a tuning or confirmation seed"),
    ("demonstration.program.seed", 180, "the schedule's seeds 180..207 include [201, 202, 203, 204, 205]"),   # a LATER day
    ("demonstration.program.seed", 74, "the schedule's seeds 74..101 include [101]"),                # the last day only
    ("demonstration.program.seed", 0, "program.seed must be a positive integer"),
    ("demonstration.program.seed", 2**31 - 1, "program.seed must be a positive integer"),
    ("demonstration.program.days", 0, "program.days must be an integer in 1..28"),
    ("demonstration.program.days", 29, "program.days must be an integer in 1..28"),
    ("demonstration.program.days", "28", "program.days must be an integer in 1..28"),
    ("demonstration.program.days", True, "program.days must be an integer in 1..28"),
    ("demonstration.program.days", KeyError, "demonstration.program is missing days"),
    ("demonstration.run_id", "c28-prod-1", "demonstration.run_id must be the params' run_id"),
    ("demonstration.extra_field", 1, "unknown fields extra_field"),
])
def test_block_fields(path, value, text):
    refused(changed(prod_like(), path, value), text)


@pytest.mark.parametrize("path, value, text", [
    ("demonstration.generator.image", "grafana/k6:0.57.0", "must be pinned by digest"),
    ("demonstration.generator.image", "grafana/k6@sha256:" + "0" * 64, "must be the reviewed k6 image"),
    ("demonstration.generator.k6_version", "0.58.0", "k6_version must be 0.57.0"),
    ("demonstration.generator.segment_hours", 6, "whole number of days"),
    ("demonstration.generator.segment_hours", 25, "whole number of days"),
    ("demonstration.generator.remote_write_url", "http://user:pw@vm:8428/api/v1/write", "without user-info"),
    ("demonstration.generator.remote_write_url", "vm:8428", "must be an http(s) URL"),
    ("demonstration.generator.resources.memory_request", "1Gi", "memory_request must be positive and at most"),
    ("demonstration.generator.resources.cpu_limit", "lots", "is not a CPU quantity"),
    ("demonstration.generator.replicas", 2, "unknown fields replicas"),
])
def test_generator_fields(path, value, text):
    refused(changed(prod_like(), path, value), text)


def test_a_mirrored_repository_with_the_reviewed_digest_is_accepted():
    p = changed(prod_like(), "demonstration.generator.image", "harbor.example/mirror/k6@" + demo.GENERATOR_DIGEST)
    assert problems(p) == []


# --- the reservation reference ---------------------------------------------------------------------------------------

def test_the_demonstration_occupies_another_runs_reservation_by_reference():
    p = prod_like()
    assert p["run_id"] == "c28-demo-1" and p["reservation"]["taint_value"] == "c28-prod-1"
    assert problems(p) == []
    # The reservation stage itself is unchanged: there the taint value must still be the run's own id.
    assert "reservation.taint_value must be the run_id" in render.stage_problems(p, "reservation")
    objs = demo.render_all(p)
    for o in objs:
        for t in render.pod_templates(o):
            assert t["spec"]["tolerations"] == [{"key": "c28.devkuban.com/reserved", "operator": "Equal",
                                                 "value": "c28-prod-1", "effect": "NoSchedule"}]
            assert t["metadata"]["labels"][render.RUN_LABEL] == "c28-demo-1"


@pytest.mark.parametrize("path, value, text", [
    ("reservation.taint_value", "c28-demo-1", "reservation.taint_value must be demonstration.reservation_ref.taint_value"),
    ("demonstration.reservation_ref.taint_value", "other", "taint_value must be the owner's run id"),
    ("demonstration.reservation_ref.run_id", "c28-prod-2", "taint_value must be the owner's run id"),
    ("demonstration.reservation_ref.nodes.workload.name", "worker-x", "nodes.workload.name must be nodes.workload"),
    ("demonstration.reservation_ref.nodes.support.name", "worker-w", "nodes.support.name must be nodes.support"),
    ("demonstration.reservation_ref.nodes.support.uid", "not-a-uid", "must be the reserved node's UID"),
    ("demonstration.reservation_ref.nodes.support.uid", "aaaaaaaa-0000-4000-8000-000000000001", "UIDs must differ"),
    ("demonstration.reservation_ref.nodes.workload", {"name": "worker-w"}, "is missing uid"),
    ("demonstration.reservation_ref", KeyError, "is missing reservation_ref"),
    ("demonstration.reservation_ref.nodes", {"workload": {"name": "worker-w", "uid": "aaaaaaaa-0000-4000-8000-000000000001"}},
     "is missing support"),
    ("reservation.taint_key", "not a key", "qualified name"),
])
def test_the_reservation_reference_must_match(path, value, text):
    refused(changed(prod_like(), path, value), text)


def test_residency_is_required_for_the_stage():
    refused(changed(prod_like(), "residency", KeyError), "residency is required")


# --- rendering --------------------------------------------------------------------------------------------------------

def by_kind(objs):
    out = {}
    for o in objs:
        out.setdefault(o["kind"], []).append(o["metadata"]["name"])
    return {k: sorted(v) for k, v in out.items()}


def test_rendered_objects():
    p = prod_like()
    parts = demo.render(p)
    assert by_kind(parts["common"]) == {
        "PriorityClass": ["c28-demo-fixture"], "ResourceQuota": ["c28-demo-fixture-quota"],
        "ServiceAccount": ["c28-ingress"], "Role": ["c28-ingress"], "RoleBinding": ["c28-ingress"],
        "Deployment": ["c28-forecast-route", "c28-ingress", "c28-metrics-route"],
        "Service": ["c28-forecast-route", "c28-ingress", "c28-metrics-route"],
        "ConfigMap": ["c28-forecast-route-faults", "c28-metrics-route-faults"]}
    assert by_kind(parts["targets"]) == {"ConfigMap": ["c28-demo-pa-n-faults", "c28-demo-pa-r-faults"],
                                         "Deployment": ["c28-demo-pa-n", "c28-demo-pa-r"],
                                         "Service": ["c28-demo-pa-n", "c28-demo-pa-r"]}
    assert by_kind(parts["pas"]) == {"PredictiveAutoscaler": ["c28-demo-pa-n", "c28-demo-pa-r"]}
    assert parts["writers"] == []
    assert by_kind(parts["generator"]) == {"ConfigMap": ["c28-demo-generator"], "Service": ["c28-demo-generator"],
                                           "StatefulSet": ["c28-demo-generator"]}
    for o in demo.flat(parts):
        assert o["metadata"]["name"].startswith("c28-")
        assert o["metadata"]["labels"][render.RUN_LABEL] == "c28-demo-1"
        if o["kind"] == "ConfigMap" and o["metadata"]["name"].endswith("-faults"):
            assert o["data"] == {"faults.json": "{}"}
    ingress = [o for o in parts["common"] if o["kind"] == "Deployment" and o["metadata"]["name"] == "c28-ingress"][0]
    env = {e["name"]: e["value"] for e in ingress["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["UPSTREAMS"] == "c28-demo-pa-n=http://c28-demo-pa-n.demo.svc,c28-demo-pa-r=http://c28-demo-pa-r.demo.svc"


@pytest.mark.parametrize("scrape, kind", [("VMPodScrape", "VMPodScrape"), ("PodMonitor", "PodMonitor")])
def test_scrape_objects_and_route_policies_under_the_run_label(scrape, kind):
    p = prod_like()
    p["scrape"], p["release_network_policies"] = scrape, True
    common = demo.render(p)["common"]
    assert by_kind(common)[kind] == ["c28-ingress"]
    assert by_kind(common)["NetworkPolicy"] == ["c28-forecast-route-to-forecaster", "c28-release-to-routes"]
    assert all(o["metadata"]["labels"][render.RUN_LABEL] == "c28-demo-1" for o in common)


def experiment_params():
    """Complete experiment params on the same fixture, nodes and reservation, for comparing the renderings."""
    p = prod_like()
    p.pop("demonstration")
    p["run_id"] = p["reservation"]["taint_value"] = "c28-prod-1"
    p.update({"c0_rps": 20, "hpa_cpu_target_percent": 60,
              "keda": {"server_address": render.metrics_query_url(p), "polling_interval_seconds": 15}})
    render.validate(p)
    return p


def test_targets_are_rendered_exactly_as_the_s20_arms():
    """The Deployment's pod spec (image by digest, the candidate's resources, probes, placement, env) and the Service
    are the arm rendering's own, apart from the target's name, the run label, the demo role label and the replicas."""
    p, e = prod_like(), experiment_params()
    parts = demo.render(p)
    for name, arm in (("c28-demo-pa-n", "PA-N"), ("c28-demo-pa-r", "PA-R")):
        ref_cm, ref_dep, ref_svc, _ = render.render_arm(e, arm, "s20")
        dep = [o for o in parts["targets"] if o["kind"] == "Deployment" and o["metadata"]["name"] == name][0]
        assert dep["spec"]["replicas"] == 12 and ref_dep["spec"]["replicas"] == render.FIXED_REPLICAS
        text = json.dumps(dep, sort_keys=True).replace(name, render.target_name(arm, "s20"))
        norm = json.loads(text)
        norm["spec"]["replicas"] = ref_dep["spec"]["replicas"]
        for md in (norm["metadata"], norm["spec"]["template"]["metadata"]):
            assert md["labels"].pop(demo.DEMO_ROLE_LABEL, None) in (("forecast" if arm == "PA-N" else "reactive"), None)
            md["labels"][render.RUN_LABEL] = "c28-prod-1"
        assert norm == ref_dep
        c = dep["spec"]["template"]["spec"]["containers"][0]
        assert c["image"] == p["image"] and c["resources"] == render._resources(p["fixture"])
        assert {x["name"]: x["value"] for x in c["env"]}["STARTUP_DELAY_SECONDS"] == "20"


def test_pas_reuse_the_arm_rendering_forecast_path_and_reactive_reference():
    p, e = prod_like(), experiment_params()
    pas = {o["metadata"]["name"]: o for o in demo.render(p)["pas"]}
    n, r = pas["c28-demo-pa-n"]["spec"], pas["c28-demo-pa-r"]["spec"]
    for spec in (n, r):
        assert spec["mode"] == "Recommend"
        assert (spec["minReplicas"], spec["maxReplicas"]) == (1, 12)
        assert spec["prediction"]["engine"] == "statistical" and spec["prediction"]["enabled"] is True
        assert spec["prediction"]["families"] == ["seasonalNaive"]
        assert spec["metrics"]["requests"]["capacityPerReplica"] == "17.5"
        assert spec["behavior"]["startup"]["minimumDelaySeconds"] == F.PROFILE_STARTUP_BUDGET["s20"]
    # The reactive reference: the predictive extra is an EXPLICIT zero, exactly as the PA-R arm is rendered.
    ref_r = render.render_arm(e, "PA-R", "s20")[-1]["spec"]
    assert r["prediction"]["predictiveExtra"] == {"maxReplicas": 0, "maxPercent": 0} == \
        ref_r["prediction"]["predictiveExtra"]
    assert "predictiveExtra" in r["prediction"]                      # present, not left to a default
    ref_n = render.render_arm(e, "PA-N", "s20")[-1]["spec"]
    assert n["prediction"]["predictiveExtra"] == ref_n["prediction"]["predictiveExtra"] == \
        {"maxReplicas": F.EXTRA_MAX_REPLICAS, "maxPercent": F.EXTRA_MAX_PERCENT}
    # Apart from the extra, the two PAs differ only in their target.
    a = json.loads(json.dumps(n).replace("c28-demo-pa-n", "T"))
    b = json.loads(json.dumps(r).replace("c28-demo-pa-r", "T"))
    a["prediction"].pop("predictiveExtra"), b["prediction"].pop("predictiveExtra")
    assert a == b
    # And apart from capacity, families and the qualified profile (the explicit minimum lead; no calibration
    # override, so the defaults apply), the forecast path is the PA-N arm's spec.
    x = json.loads(json.dumps(ref_n).replace("c28-pa-n-s20", "c28-demo-pa-n"))
    x["metrics"]["requests"]["capacityPerReplica"] = "17.5"
    assert "minLeadSeconds" not in x["prediction"] and "calibration" in x["prediction"]
    x["prediction"]["minLeadSeconds"] = 600
    del x["prediction"]["calibration"]
    assert x == n


def test_the_arm_rendering_itself_is_unchanged_by_the_refactor():
    """render_all of the kind smoke params is byte for byte what origin/main 44f65cf rendered before render.py was
    split into target_objects / pa_object / common_objects (the digest was computed on that tree)."""
    with open(demo_template().replace("demonstration.template.json", "kind-smoke.json")) as fh:
        p = json.load(fh)
    objs = render.render_all(p)
    assert len(objs) == 53
    assert hashlib.sha256(json.dumps(objs, sort_keys=True).encode()).hexdigest() == \
        "a41f26b17f971955e12ece23853a256872ef1fbaefa969c1fb43db0f66aedc57"


def test_every_pod_is_sidecar_free_least_privilege_and_placed():
    p = prod_like()
    objs = demo.render_all(p)
    render.audit_sidecar_free(objs)
    render.audit_rbac(objs)
    for o in objs:
        for t in render.pod_templates(o):
            name = o["metadata"]["name"]
            role = "workload" if name in ("c28-ingress", "c28-demo-pa-n", "c28-demo-pa-r") else "support"
            assert render._role(t, p) == role, name
            assert t["spec"]["automountServiceAccountToken"] is False


def test_the_generator_is_one_statefulset_with_one_replica_on_the_support_node():
    """At most one generator pod: the StatefulSet's single pod is always <name>-0, and its replacement is created
    only after the old pod object is gone (a Deployment creates the replacement of a deleted pod at once)."""
    p = prod_like()
    cm, svc, dep = demo.render(p)["generator"]
    assert (dep["kind"], dep["apiVersion"]) == ("StatefulSet", "apps/v1") and demo.generator_pod_name() == "c28-demo-generator-0"
    spec = dep["spec"]
    assert spec["replicas"] == 1 and spec["podManagementPolicy"] == "OrderedReady"
    assert "strategy" not in spec and spec["updateStrategy"] == {"type": "RollingUpdate"}
    assert spec["serviceName"] == svc["metadata"]["name"] == "c28-demo-generator"
    assert svc["kind"] == "Service" and svc["spec"] == {"clusterIP": "None", "selector": spec["selector"]["matchLabels"]}
    assert spec["selector"]["matchLabels"].items() <= spec["template"]["metadata"]["labels"].items()
    assert "restartPolicy" not in spec["template"]["spec"]                 # Always: the next segment in the same pod
    assert not [o for part in demo.render(p).values() for o in part
                if o["kind"] in ("Deployment", "Job", "Pod", "DaemonSet", "CronJob", "ReplicaSet")
                and o["metadata"]["name"].startswith("c28-demo-generator")]
    pod = spec["template"]["spec"]
    c = pod["containers"][0]
    assert c["image"] == "grafana/k6@" + demo.GENERATOR_DIGEST
    assert "command" not in c and c["args"][0] == "run" and c["args"][-1] == "/plan/c28-demo.js"
    for flag in ("--no-summary", "--no-thresholds", "--no-usage-report", "--quiet"):
        assert flag in c["args"]
    assert c["args"][c["args"].index("--out") + 1] == "experimental-prometheus-rw"
    env = {e["name"]: e for e in c["env"]}
    assert env["POD_UID"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid"
    assert env["K6_PROMETHEUS_RW_SERVER_URL"]["value"] == p["demonstration"]["generator"]["remote_write_url"]
    assert c["resources"] == {"requests": {"cpu": "250m", "memory": "256Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}
    assert pod["volumes"] == [{"name": "plan", "configMap": {"name": "c28-demo-generator"}}]
    assert set(cm["data"]) == {"c28-demo.js", "config.json"}
    assert cm["data"]["c28-demo.js"] == demo.script_text()
    cfg = json.loads(cm["data"]["config.json"])
    assert cfg == demo.generator_config(p)
    ann = spec["template"]["metadata"]["annotations"]
    assert ann["c28.devkuban.com/generator-config"] == cfg["digest"]
    assert ann["c28.devkuban.com/generator-script"] == hashlib.sha256(cm["data"]["c28-demo.js"].encode()).hexdigest()


@pytest.mark.parametrize("edit, text", [
    (lambda g: g[2]["spec"].update(replicas=2), "exactly one replica"),
    (lambda g: g[2]["spec"].update(replicas=0), "exactly one replica"),
    (lambda g: g[2].update(kind="Deployment"), "must be a StatefulSet"),
    (lambda g: g.append(dict(copy.deepcopy(g[2]), kind="Deployment")), "exactly one generator workload"),
    (lambda g: g.append(json.loads(json.dumps(g[2]).replace("c28-demo-generator", "c28-demo-generator-b"))),
     "exactly one generator workload"),
    (lambda g: g.append({"apiVersion": "v1", "kind": "Pod",
                         "metadata": dict(copy.deepcopy(g[2]["spec"]["template"]["metadata"]),
                                          name="c28-demo-generator-x", namespace=g[2]["metadata"]["namespace"]),
                         "spec": copy.deepcopy(g[2]["spec"]["template"]["spec"])}), "exactly one generator workload"),
    (lambda g: g[2]["spec"].update(podManagementPolicy="Parallel"), "podManagementPolicy OrderedReady"),
    (lambda g: g.pop(1), "needs its headless Service"),
    (lambda g: g[1]["spec"].pop("clusterIP"), "needs its headless Service"),
    (lambda g: g[2]["spec"]["template"]["spec"].update(restartPolicy="Never"), "restartPolicy Always"),
])
def test_the_render_never_allows_two_generator_pods(monkeypatch, edit, text):
    """The rendering audit: one StatefulSet with one replica and nothing else that carries a pod template."""
    real = demo.generator_objects

    def altered(params, *smoke):
        objs = real(params, *smoke)
        edit(objs)
        return objs
    monkeypatch.setattr(demo, "generator_objects", altered)
    with pytest.raises(render.ParamsError, match=text):
        demo.render(prod_like())


def test_the_generator_config_is_the_finite_schedule_of_28_days_from_one_origin():
    from c28 import schedule
    p = prod_like()
    cfg = demo.generator_config(p)
    assert cfg["schema"] == "c28-demo-generator/v2" == demo.GENERATOR_CONFIG_SCHEMA
    assert cfg["origin_utc"] == "2026-11-01T00:00:00Z" and cfg["origin_ms"] == 1793491200000
    assert cfg["origin_ms"] % 86_400_000 == 0
    assert cfg["targets"] == ["c28-demo-pa-n", "c28-demo-pa-r"] and cfg["base_url"] == "http://c28-ingress.demo.svc"
    # 28 predeclared, consecutive seeds from the base seed; one row of 1,440 counts per day; a finite length.
    assert cfg["days"] == 28 == demo.DEMONSTRATION_DAYS and cfg["seeds"] == list(range(301, 329)) == demo.schedule_seeds(p)
    assert not set(cfg["seeds"]) & set(F.TUNING_SEEDS + F.CONFIRMATION_SEEDS)
    assert cfg["schedule_minutes"] == 28 * 1440 and cfg["schedule_end_ms"] == cfg["origin_ms"] + 28 * 86_400_000
    assert cfg["schedule_end_ms"] == demo.schedule_end_ms(p)
    table = cfg["per_minute_days"]
    assert len(table) == 28 and all(len(row) == 1440 for row in table) and table == demo.day_table(p)
    for d in (0, 1, 13, 27):
        us = schedule.program("ordinary", 301 + d)
        assert table[d] == [round(u * 17.5 * 60) for u in us] == demo.day_plan(p, d)
        # The same conversion as the comparison's k6 plan: iterations per minute for the same capacity and seed.
        plan = schedule.k6_plan("ordinary", 301 + d, 17.5, "x")
        assert table[d] == [s["target"] for s in plan["stages"][0::2]]
    assert all(a != b for a, b in zip(table, table[1:]))                   # unequal adjacent days
    assert cfg["timeout_ms"] == 2000 and cfg["segment_minutes"] == 1440 and cfg["label"] == "demonstration"
    assert cfg["digest"] == schedule.digest(cfg) and demo.config_problems(cfg) == []
    assert cfg["program"] == {"kind": "ordinary", "seed": 301, "days": 28, "capacity_per_replica_rps": "17.5",
                              "capacity_label": "provisional", "capacity_source_calibration": "c28cal-0000000000"}
    peak = max(map(max, table))
    assert peak <= 9 * 1.05 * 17.5 * 60 + 1 and peak > 9 * 17.5 * 60 * 0.95
    smoke = demo.generator_config(p, smoke_segment_minutes=3)
    assert smoke["label"] == "smoke" and smoke["segment_minutes"] == 3 and smoke["per_minute_days"] == table
    assert smoke["digest"] != cfg["digest"]


def test_the_digest_binds_every_day_every_seed_the_origin_and_the_schema():
    """Whatever is changed in the schedule, the digest changes: through the params (another base seed, day count or
    origin) and in the configuration itself (one count of one day, one seed, the origin, the schema)."""
    from c28 import schedule
    p = prod_like()
    cfg = demo.generator_config(p)
    seen = {cfg["digest"]}
    for path, value in (("demonstration.program.seed", 302), ("demonstration.program.seed", 300),
                        ("demonstration.schedule_origin_utc", "2026-11-02T00:00:00Z"),
                        ("demonstration.capacity_per_replica_rps", 17.6)):
        other = demo.generator_config(changed(p, path, value))
        assert other["digest"] not in seen, path
        seen.add(other["digest"])
    assert demo.generator_config(changed(p, "demonstration.program.seed", 302))["per_minute_days"][0] == \
        cfg["per_minute_days"][1]                                           # the table really moved by one day
    fewer = demo.generator_config(changed(p, "demonstration.program.days", 27), smoke_segment_minutes=3)
    assert fewer["digest"] not in seen and fewer["days"] == 27 and len(fewer["per_minute_days"]) == 27
    for day in (0, 1, 14, 27):                                              # ANY day, not only the first
        for minute in (0, 719, 1439):
            edit = copy.deepcopy(cfg)
            edit["per_minute_days"][day][minute] += 1
            d = schedule.digest(edit)
            assert d not in seen, (day, minute)
            seen.add(d)
            assert demo.config_problems(edit) == ["the digest differs"]
    for i in (0, 9, 27):
        edit = copy.deepcopy(cfg)
        edit["seeds"][i] += 1000
        assert schedule.digest(edit) not in seen and "the digest differs" in demo.config_problems(edit)
        seen.add(schedule.digest(edit))
    swapped = copy.deepcopy(cfg)
    swapped["per_minute_days"][3], swapped["per_minute_days"][4] = cfg["per_minute_days"][4], cfg["per_minute_days"][3]
    assert schedule.digest(swapped) not in seen                             # the ORDER of the days is bound too
    for field, value in (("origin_ms", cfg["origin_ms"] + 86_400_000), ("origin_utc", "2026-11-02T00:00:00Z"),
                         ("schema", "c28-demo-generator/v1"), ("schedule_end_ms", cfg["schedule_end_ms"] + 60_000),
                         ("days", 27), ("label", "smoke")):
        assert schedule.digest(dict(cfg, **{field: value})) not in seen, field
        assert demo.config_problems(dict(cfg, **{field: value})), field


def test_a_demonstration_has_exactly_28_days_and_fewer_render_only_as_a_smoke():
    p = changed(prod_like(), "demonstration.program.days", 7)
    assert problems(p) == []                                                # the params stage allows 1..28
    with pytest.raises(render.ParamsError, match="exactly 28 days"):
        demo.generator_config(p)
    with pytest.raises(render.ParamsError, match="exactly 28 days"):
        demo.render(p)
    smoke = demo.generator_config(p, smoke_segment_minutes=3)
    assert (smoke["label"], smoke["days"], smoke["seeds"]) == ("smoke", 7, list(range(301, 308)))
    assert smoke["schedule_end_ms"] == smoke["origin_ms"] + 7 * 86_400_000 and demo.config_problems(smoke) == []
    relabelled = dict(smoke, label="demonstration")
    assert "a demonstration's schedule has exactly 28 days" in demo.config_problems(relabelled)
    one = demo.generator_config(changed(prod_like(), "demonstration.program.days", 1), smoke_segment_minutes=3)
    assert one["seeds"] == [301] and len(one["per_minute_days"]) == 1


def test_generator_sizing_states_the_arithmetic_against_the_peak_and_the_timeout():
    p = prod_like()
    s = demo.generator_sizing(p)
    table = demo.day_table(p)
    peak = max(max(day) for day in table)                                   # the WHOLE table, not one day
    assert s["peak_per_minute_per_target"] == peak and s["vus"] == 3 and s["timeout_s"] == 2
    assert s["days"] == 28 and s["seeds"] == [301, 328] and s["table_entries"] == 28 * 1440
    assert max(table[s["peak_day"]]) == peak and s["peak_seed"] == 301 + s["peak_day"]
    import math
    assert s["max_inflight_per_target"] == math.ceil(peak / 60 * 2 * 1.5) + 10
    assert s["memory_estimate_bytes"] == demo.ASSUMED_BASE_BYTES + 3 * demo.ASSUMED_VU_BYTES + \
        2 * s["max_inflight_per_target"] * demo.ASSUMED_INFLIGHT_BYTES + 3 * 28 * 1440 * demo.ASSUMED_TABLE_ENTRY_BYTES
    assert "not measured" in s["memory_estimate"]
    assert s["memory_estimate_bytes"] <= s["memory_limit_bytes"]
    refused(changed(p, "demonstration.generator.resources",
                    {"cpu_request": "250m", "cpu_limit": "1", "memory_request": "64Mi", "memory_limit": "128Mi"}),
            "below the estimated need at the peak rate")


def test_sizing_uses_the_maximum_across_all_days_not_the_first_day(monkeypatch):
    """A table whose highest minute is on a LATER day: admission (peak, max_inflight, the memory estimate and the
    configuration's max_inflight) follows that day, perturbation included."""
    p = prod_like()
    real = demo.day_table(p)
    first_day_peak = max(real[0])
    natural = demo.generator_sizing(p)
    assert natural["peak_per_minute_per_target"] == max(map(max, real)) >= first_day_peak

    def later_peak(params):
        table = [list(day) for day in real]
        table[19][1083] = 3 * first_day_peak                                # day 19 holds the highest minute by far
        return table
    monkeypatch.setattr(demo, "day_table", later_peak)
    s = demo.generator_sizing(p)
    assert (s["peak_per_minute_per_target"], s["peak_day"], s["peak_seed"]) == (3 * first_day_peak, 19, 320)
    import math
    assert s["max_inflight_per_target"] == math.ceil(3 * first_day_peak / 60 * 2 * 1.5) + 10
    assert s["max_inflight_per_target"] > 2.5 * natural["max_inflight_per_target"]
    assert s["memory_estimate_bytes"] > natural["memory_estimate_bytes"]
    # The admission follows it: the 512Mi limit that covers the first day's peak no longer covers the table's.
    need = s["memory_estimate_bytes"]
    assert natural["memory_estimate_bytes"] <= natural["memory_limit_bytes"] < need
    assert any("below the estimated need at the peak rate" in x for x in problems(p))
    with pytest.raises(render.ParamsError, match="below the estimated need at the peak rate"):
        demo.generator_config(p)
    ok = changed(p, "demonstration.generator.resources",
                 {"cpu_request": "250m", "cpu_limit": "1", "memory_request": "256Mi", "memory_limit": str(need)})
    assert problems(ok) == []
    assert demo.generator_config(ok)["max_inflight"] == s["max_inflight_per_target"]   # the script's bound too


def test_the_generators_configmap_holds_the_whole_table_within_the_api_limit():
    p = prod_like()
    cm = [o for o in demo.render(p)["generator"] if o["kind"] == "ConfigMap"][0]
    size = sum(len(k) + len(v.encode()) for k, v in cm["data"].items())
    assert 150_000 < size < demo.CONFIGMAP_LIMIT_BYTES * 3 // 4            # 28 x 1,440 counts and the script
    cfg = json.loads(cm["data"]["config.json"])
    assert len(cfg["per_minute_days"]) == 28 and cfg == demo.generator_config(p)


# --- admission: the residency audit -----------------------------------------------------------------------------------

def test_residency_counts_both_targets_at_max_with_overlap_and_everything_on_s():
    p = prod_like()
    rep = demo.residency_report(p)
    assert rep["problems"] == [] and rep["limit_fraction"] == F.RESIDENCY_LIMIT_FRACTION
    w = {c["name"]: c for c in rep["nodes"]["workload"]["contributions"]}
    s = {c["name"]: c for c in rep["nodes"]["support"]["contributions"]}
    for t in ("c28-demo-pa-n", "c28-demo-pa-r"):
        assert w[f"Deployment/{t}"]["pods"] == 12                         # max replicas, not the shadow count
        assert w[f"Deployment/{t} overlap"]["pods"] == 2                  # rollout/termination overlap, each target
    assert w["Deployment/c28-ingress"]["pods"] == 1 and "node system pods" in w
    assert set(s) == {"Deployment/c28-metrics-route", "Deployment/c28-forecast-route",
                      "StatefulSet/c28-demo-generator", "operator (chart)", "forecaster (chart)", "node system pods"}
    assert "generator Jobs" not in s                                       # the rendered generator, counted once
    assert s["StatefulSet/c28-demo-generator"]["per_pod_cpu"] == "250m"
    assert s["forecaster (chart)"]["per_pod_cpu"] == "1000m" and s["operator (chart)"]["per_pod_cpu"] == "100m"
    total = sum(c["cpu_m"] for c in w.values())
    assert rep["nodes"]["workload"]["total"]["cpu_m"] == total == 2 * 14 * 50 + 100 + 300
    # The shadow count never lowers the audit: it always counts the bounds' maximum.
    low = demo.residency_report(changed(p, "demonstration.shadow_replicas", 1))
    assert low["nodes"]["workload"]["total"] == rep["nodes"]["workload"]["total"]


def test_residency_refuses_above_75_percent_on_either_node():
    p = prod_like()
    p["residency"]["workload"]["allocatable"]["cpu"] = "2"            # 1800m of 2000m = 90 %
    rep = demo.residency_report(p)
    assert any(x.startswith("workload node worker-w: CPU requests 1800m are 90.0%") for x in rep["problems"])
    with pytest.raises(render.ParamsError, match="residency budget exceeded"):
        demo.render_all(p)
    q = prod_like()
    q["residency"]["support"]["allocatable"]["memory"] = "1Gi"
    assert any(x.startswith("support node worker-s: memory requests") for x in demo.residency_report(q)["problems"])


def test_residency_reads_the_final_chart_values():
    p = prod_like()
    chart = render.chart_values(demo.release_values(p), {"forecaster": {"statistical": {"resources": {
        "requests": {"cpu": "250m", "memory": "256Mi"}}}}})
    rep = demo.residency_report(p, chart=chart)
    assert any("forecaster requests 250m/256Mi, below its declared budget" in x for x in rep["problems"])
    big = render.chart_values(demo.release_values(p), {"operator": {"replicas": 1, "resources": {
        "requests": {"cpu": "6", "memory": "128Mi"}}}})
    assert any(x.startswith("support node worker-s: CPU requests") for x in demo.residency_report(p, chart=big)["problems"])


def test_release_values_for_the_stage_need_no_experiment_field():
    p = prod_like()
    v = demo.release_values(p)
    assert v["prometheus"]["url"] == "http://c28-metrics-route.pa-eval-system.svc:8481"
    assert v["operator"]["watchNamespaces"] == ["demo"] and v["forecaster"]["engine"] == "statistical"
    with pytest.raises(render.ParamsError):
        render.eval_release_values(p)                                  # the experiment's validation still refuses


# --- the Active configuration's values (validated, never produced) ----------------------------------------------------

@pytest.mark.parametrize("rbac, text", [
    ({"scaleWrite": False}, None),
    ({"scaleWrite": False, "scaleTargets": []}, None),
    ({"scaleWrite": False, "scaleTargets": ["c28-demo-pa-n"]}, "must be empty while operator.rbac.scaleWrite is false"),
    ({"scaleWrite": True, "scaleTargets": ["c28-demo-pa-n", "c28-demo-pa-r"]}, None),
    ({"scaleWrite": True, "scaleTargets": ["c28-demo-pa-r", "c28-demo-pa-n"]}, None),
    ({"scaleWrite": True, "scaleTargets": []}, "scaleTargets is empty"),
    ({"scaleWrite": True}, "scaleTargets is empty"),
    ({"scaleWrite": True, "scaleTargets": None}, "scaleTargets is empty"),
    ({"scaleWrite": True, "scaleTargets": ["c28-demo-pa-n"]}, "must equal the two target names"),
    ({"scaleWrite": True, "scaleTargets": ["c28-demo-pa-n", "c28-demo-pa-r", "macaron"]}, "must equal the two target names"),
    ({"scaleWrite": True, "scaleTargets": ["c28-demo-pa-n", "c28-demo-pa-n"]}, "must equal the two target names"),
    ({"scaleWrite": True, "scaleTargets": ["c28-demo-pa-n", "c28-ingress"]}, "must equal the two target names"),
    ({"scaleWrite": True, "scaleTargets": "c28-demo-pa-n,c28-demo-pa-r"}, "scaleTargets is empty"),
    ({"scaleWrite": "true", "scaleTargets": ["c28-demo-pa-n", "c28-demo-pa-r"]}, "must be an explicit boolean"),
    ({}, "must be an explicit boolean"),
])
def test_active_configuration_values(rbac, text):
    found = demo.active_values_problems({"operator": {"rbac": copy.deepcopy(rbac)}}, prod_like())
    assert (found == []) if text is None else any(text in x for x in found), found


# --- the command line -------------------------------------------------------------------------------------------------

def cli(tmp_path, capsys, p, *argv):
    from c28.__main__ import main
    path = tmp_path / "params.json"
    path.write_text(json.dumps(p))
    rc = main([argv[0], "--params", str(path), *argv[1:]])
    return rc, capsys.readouterr().out


def test_cli_validate_render_residency_and_plan(tmp_path, capsys):
    import yaml
    p = prod_like()
    rc, out = cli(tmp_path, capsys, p, "validate", "--stage", "demonstration")
    assert rc == 0 and json.loads(out) == {"stage": "demonstration", "problems": []}
    rc, out = cli(tmp_path, capsys, changed(p, "demonstration.cadence.grid_seconds", 30), "validate", "--stage",
                  "demonstration")
    assert rc == 1 and "demonstration.cadence must be exactly" in json.loads(out)["problems"][0]
    rc, out = cli(tmp_path, capsys, p, "render", "--stage", "demonstration")
    objs = list(yaml.safe_load_all(out))
    assert rc == 0 and objs == json.loads(json.dumps(demo.render_all(p)))
    rc, out = cli(tmp_path, capsys, p, "render", "--stage", "demonstration", "--part", "generator")
    assert [o["kind"] for o in yaml.safe_load_all(out)] == ["ConfigMap", "Service", "StatefulSet"]
    rc, out = cli(tmp_path, capsys, p, "residency", "--stage", "demonstration")
    assert rc == 0 and json.loads(out)["problems"] == []
    tight = prod_like()
    tight["residency"]["workload"]["allocatable"]["cpu"] = "2"
    rc, out = cli(tmp_path, capsys, tight, "residency", "--stage", "demonstration")
    assert rc == 1 and json.loads(out)["problems"]
    rc, out = cli(tmp_path, capsys, p, "demo-plan")
    assert rc == 0 and json.loads(out) == demo.generator_config(p)
    rc, out = cli(tmp_path, capsys, p, "demo-sizing")
    assert json.loads(out) == demo.generator_sizing(p)
    rc, out = cli(tmp_path, capsys, p, "release-values", "--stage", "demonstration")
    assert yaml.safe_load(out) == demo.release_values(p)


def test_cli_refuses_the_demonstration_params_as_an_experiment(tmp_path, capsys):
    from c28.__main__ import main
    path = tmp_path / "params.json"
    path.write_text(json.dumps(prod_like()))
    with pytest.raises(render.ParamsError, match="missing parameters: c0_rps"):
        main(["render", "--params", str(path)])
    with pytest.raises(render.ParamsError, match="belong to --stage demonstration"):
        main(["render", "--params", str(path), "--part", "pas"])


def test_cli_smoke_segment_is_labelled_and_only_for_the_generator(tmp_path, capsys):
    import yaml
    from c28.__main__ import main
    p = prod_like()
    rc, out = cli(tmp_path, capsys, p, "demo-plan", "--smoke-segment-minutes", "3")
    cfg = json.loads(out)
    assert (cfg["label"], cfg["segment_minutes"]) == ("smoke", 3) and cfg == demo.generator_config(p, 3)
    rc, out = cli(tmp_path, capsys, p, "render", "--stage", "demonstration", "--part", "generator",
                  "--smoke-segment-minutes", "3")
    cm = list(yaml.safe_load_all(out))[0]
    assert json.loads(cm["data"]["config.json"])["label"] == "smoke"
    with pytest.raises(render.ParamsError, match="render only --part generator"):
        main(["render", "--params", str(tmp_path / "params.json"), "--stage", "demonstration",
              "--smoke-segment-minutes", "3"])


def test_cli_smoke_origin_puts_the_smoke_at_a_chosen_time_of_the_schedules_day(tmp_path, capsys):
    """--smoke-origin-utc (only with a smoke segment): the origin on any whole minute, so a kind smoke can sit at the
    daily peak whatever the wall clock says. A demonstration can never use it."""
    import yaml
    from c28.__main__ import main
    p = prod_like()
    rc, out = cli(tmp_path, capsys, p, "demo-plan", "--smoke-segment-minutes", "3", "--smoke-origin-utc",
                  "2026-11-10T13:20:00Z")
    cfg = json.loads(out)
    assert rc == 0 and cfg == demo.generator_config(p, 3, 1794316800000)
    assert (cfg["label"], cfg["origin_utc"], cfg["origin_ms"]) == ("smoke", "2026-11-10T13:20:00Z", 1794316800000)
    assert cfg["origin_ms"] % 86_400_000 != 0 and demo.config_problems(cfg) == []
    rc, out = cli(tmp_path, capsys, p, "render", "--stage", "demonstration", "--part", "generator",
                  "--smoke-segment-minutes", "3", "--smoke-origin-utc", "2026-11-10T13:20:00Z")
    cm = list(yaml.safe_load_all(out))[0]
    assert json.loads(cm["data"]["config.json"]) == cfg
    params_file = str(tmp_path / "params.json")
    for argv, text in ((["demo-plan", "--params", params_file, "--smoke-origin-utc", "2026-11-10T13:20:00Z"],
                        "smoke-only option"),
                       (["demo-plan", "--params", params_file, "--smoke-segment-minutes", "3", "--smoke-origin-utc",
                         "2026-11-10T13:20:30Z"], "whole minute"),
                       (["demo-plan", "--params", params_file, "--smoke-segment-minutes", "3", "--smoke-origin-utc",
                         "yesterday"], "must be YYYY-MM-DDTHH:MM:SSZ"),
                       (["render", "--params", params_file, "--stage", "demonstration", "--smoke-origin-utc",
                         "2026-11-10T13:20:00Z"], "render only --part generator"),
                       (["render", "--params", params_file, "--stage", "demonstration", "--part", "generator",
                         "--smoke-origin-utc", "2026-11-10T13:20:00Z"], "smoke-only option")):
        with pytest.raises(render.ParamsError, match=text):
            main(argv)
