"""Fixture and arm manifests: placement, exclusive writers, frozen arm configuration (manifest sections 3, 4, 11)."""

import copy
import json
import os

import pytest

from c28 import frozen as F
from c28 import render, schedule

PARAMS = os.path.join(os.path.dirname(__file__), "..", "..", "params", "kind-smoke.json")


@pytest.fixture
def params():
    with open(PARAMS) as fh:
        return json.load(fh)


def by(objs, kind):
    return [o for o in objs if o["kind"] == kind]


def test_capacity_is_0875_c0_as_a_decimal():
    assert render.capacity_rps(8) == "7"
    assert render.capacity_rps(40) == "35"
    assert render.capacity_rps(13.3) == "11.638"


def test_every_arm_and_profile_has_its_own_target_and_exactly_one_writer(params):
    objs = render.render_all(params)
    deps = [d for d in by(objs, "Deployment") if render.ARM_LABEL in d["metadata"]["labels"]]
    assert len(deps) == len(F.ARMS) * len(F.PROFILES) == 10
    assert len({d["metadata"]["name"] for d in deps}) == 10
    assert len(by(objs, "HorizontalPodAutoscaler")) == 2 and len(by(objs, "ScaledObject")) == 2
    assert len(by(objs, "PredictiveAutoscaler")) == 6
    render.audit_rendered(objs)


def test_fixture_pods_tolerate_only_the_run_taint_and_require_the_workload_node(params):
    for d in by(render.render_all(params), "Deployment"):
        spec = d["spec"]["template"]["spec"]
        assert spec["tolerations"] == [{"key": "c28.devkuban.com/reserved", "operator": "Equal", "value": "c28-smoke",
                                        "effect": "NoSchedule"}]
        terms = spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
        exprs = {e["key"]: e["values"] for e in terms[0]["matchExpressions"]}
        role = "support" if d["metadata"]["name"].endswith("-route") else "workload"
        assert exprs == {"c28.devkuban.com/role": [role], "kubernetes.io/hostname": [params["nodes"][role]]}
        for c in spec["containers"]:
            assert c["image"] == params["image"] and "@sha256:" in c["image"]


def test_arm_templates_are_identical_apart_from_identity_and_startup(params):
    objs = render.render_all(params)
    deps = {d["metadata"]["name"]: d for d in by(objs, "Deployment") if render.ARM_LABEL in d["metadata"]["labels"]}

    def strip(d):
        t = copy.deepcopy(d["spec"]["template"])
        t["metadata"]["labels"] = {}
        c = t["spec"]["containers"][0]
        c["env"] = [e for e in c["env"] if e["name"] not in ("TARGET", "STARTUP_DELAY_SECONDS")]
        t["spec"]["volumes"][0]["configMap"]["name"] = ""
        return t
    ref = strip(deps["c28-h-cpu-s20"])
    for name, d in deps.items():
        assert strip(d) == ref, name
        env = {e["name"]: e["value"] for e in d["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert env["STARTUP_DELAY_SECONDS"] == str(F.PROFILES[name.rsplit("-", 1)[1]])
        assert d["spec"]["replicas"] == 2 and d["spec"]["template"]["spec"]["priorityClassName"] == "c28-fixture"


def test_baselines_and_pa_arms_carry_the_frozen_configuration(params):
    objs = render.render_all(params)
    c = render.capacity_rps(params["c0_rps"])
    for h in by(objs, "HorizontalPodAutoscaler"):
        s = h["spec"]
        assert (s["minReplicas"], s["maxReplicas"]) == (1, 12)
        assert s["metrics"][0]["resource"]["target"] == {"type": "Utilization", "averageUtilization": 60}
        sd = s["behavior"]["scaleDown"]
        assert sd["stabilizationWindowSeconds"] == 300 and sd["selectPolicy"] == "Max"
        assert {(p["type"], p["value"], p["periodSeconds"]) for p in sd["policies"]} == {("Percent", 10, 120), ("Pods", 2, 120)}
        assert s["behavior"]["scaleUp"]["stabilizationWindowSeconds"] == 0
    for so in by(objs, "ScaledObject"):
        s = so["spec"]
        t = s["triggers"][0]
        assert t["type"] == "prometheus" and t["metricType"] == "AverageValue"
        assert t["metadata"]["threshold"] == c and t["metadata"]["ignoreNullValues"] == "false"
        assert t["metadata"]["query"] == render.rps_query(so["metadata"]["name"])
        assert (s["minReplicaCount"], s["maxReplicaCount"]) == (1, 12)
        assert s["advanced"]["horizontalPodAutoscalerConfig"]["behavior"] == render.scale_down_behavior()
    for pa in by(objs, "PredictiveAutoscaler"):
        s, arm = pa["spec"], pa["metadata"]["labels"][render.ARM_LABEL]
        assert s["mode"] == "Recommend", "Active is the executor's gated step, never rendered"
        assert s["metrics"]["requests"]["capacityPerReplica"] == c
        assert s["metrics"]["requests"]["source"]["query"] == \
            'sum(rate(c28_ingress_requests_total{target="{{ .Name }}"}[60s]))'
        pred = s["prediction"]
        assert pred["engine"] == "statistical" and pred["sampleIntervalSeconds"] == 60
        assert pred["risk"] == {"miscoverageBps": 1000}
        assert pred["calibration"] == {"minWindows": 128, "maxScores": 512, "retentionDays": 7}
        assert pred["predictiveExtra"] == ({"maxReplicas": 0, "maxPercent": 0} if arm == "pa-r"
                                           else {"maxReplicas": 2, "maxPercent": 50})
        assert s["behavior"]["scaleDown"]["headroomPercent"] == 0
        budget = 30 if pa["metadata"]["name"].endswith("s20") else 120
        assert s["behavior"]["startup"] == {"fallbackDelaySeconds": budget, "minimumDelaySeconds": budget,
                                            "actuationAllowanceSeconds": 15}
        assert (pa["metadata"].get("annotations", {}).get(render.FAMILY_RESTRICTION) == "seasonal_naive") == (arm == "pa-n")


def test_the_quota_is_scoped_to_the_run_priority_class(params):
    objs = render.render_common(params)
    (pc,) = by(objs, "PriorityClass")
    assert pc["preemptionPolicy"] == "Never" and pc["globalDefault"] is False
    (q,) = by(objs, "ResourceQuota")
    assert q["spec"]["scopeSelector"]["matchExpressions"] == [
        {"scopeName": "PriorityClass", "operator": "In", "values": ["c28-fixture"]}]
    ingress = next(d for d in by(objs, "Deployment") if d["metadata"]["name"] == "c28-ingress")
    ups = {e["name"]: e["value"] for e in ingress["spec"]["template"]["spec"]["containers"][0]["env"]}["UPSTREAMS"]
    assert len(ups.split(",")) == 10 and "c28-pa-e-s90=http://c28-pa-e-s90.demo.svc" in ups


@pytest.mark.parametrize("change, message", [
    (lambda p: p.update(image="localhost/c28-fixture:latest"), "digest"),
    (lambda p: p["nodes"].update(support=p["nodes"]["workload"]), "must differ"),
    (lambda p: p["nodes"].update(workload="<W node>"), "nodes.workload"),
    (lambda p: p.update(hpa_cpu_target_percent=65), "tuning candidates"),
    (lambda p: p.update(c0_rps=0), "c0_rps"),
    (lambda p: p.pop("quota"), "missing"),
])
def test_invalid_parameters_are_refused(params, change, message):
    change(params)
    with pytest.raises(render.ParamsError, match=message):
        render.render_all(params)


def test_the_prodcluster_template_cannot_be_rendered_unresolved():
    with open(os.path.join(os.path.dirname(PARAMS), "prodcluster.template.json")) as fh:
        with pytest.raises(render.ParamsError):
            render.render_all(json.load(fh))


def test_a_second_writer_is_refused_at_render_and_in_the_live_audit(params):
    objs = render.render_all(params)
    extra = {"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
             "metadata": {"name": "intruder", "namespace": "demo"},
             "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "c28-pa-n-s20"}}}
    with pytest.raises(render.ParamsError, match="exactly one writer"):
        render.audit_rendered(objs + [extra])
    expected = render.expected_writers(params)
    live = [o for o in objs if o["kind"] in ("HorizontalPodAutoscaler", "PredictiveAutoscaler")]
    # KEDA's generated HPA stands for its ScaledObject; the ScaledObject itself is listed too.
    for so in by(objs, "ScaledObject"):
        live.append(so)
        live.append({"kind": "HorizontalPodAutoscaler", "metadata": {"name": f"keda-hpa-{so['metadata']['name']}",
                     "namespace": "demo", "ownerReferences": [{"kind": "ScaledObject", "name": so["metadata"]["name"],
                                                               "controller": True}]},
                     "spec": {"scaleTargetRef": {"kind": "Deployment", "name": so["metadata"]["name"]}}})
    assert render.audit_live(expected, live) == []
    legacy = {"kind": "PredictiveAutoscaler", "apiVersion": "autoscaler.example.com/v1alpha1",
              "metadata": {"name": "old", "namespace": "other"},
              "spec": {"targetDeployment": {"name": "c28-pa-e-s90", "namespace": "demo"}}}
    probs = render.audit_live(expected, live + [extra, legacy])
    assert len(probs) == 2 and any("intruder" in p for p in probs) and any("old" in p for p in probs)
    missing = [o for o in live if o["metadata"]["name"] != "c28-pa-r-s20"]
    assert any("c28-pa-r-s20" in p and "none" in p for p in render.audit_live(expected, missing))


def test_fault_configmaps_carry_the_block_schedule(params):
    from datetime import datetime, timezone
    objs = render.render_all(params)
    faults = schedule.fault_timetable(datetime(2026, 11, 20, tzinfo=timezone.utc))
    cms = {c["metadata"]["name"]: json.loads(c["data"]["faults.json"])
           for c in render.with_faults(objs, faults, ["c28-pa-n-s20"])}
    assert set(cms) == {"c28-metrics-route-faults", "c28-forecast-route-faults", "c28-pa-n-s20-faults"}
    assert cms["c28-metrics-route-faults"]["outages"] == [{"start": "2026-11-20T12:15:00Z", "end": "2026-11-20T12:30:00Z"}]
    assert cms["c28-pa-n-s20-faults"]["slow_starts"][0]["extra_startup_seconds"] == 120
