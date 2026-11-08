"""Sidecar-free evaluation pods, required calibrated fixture resources and the W/S residency budget (75 % of each
node's allocatable, CPU and memory separately)."""

import copy
import json
import os
import shutil
import subprocess

import pytest

from c28 import __main__ as cli
from c28 import frozen as F
from c28 import render

PARAMS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "params")
MI = 2 ** 20


def load(name):
    with open(os.path.join(PARAMS_DIR, name)) as fh:
        return json.load(fh)


@pytest.fixture
def params():
    return load("kind-smoke.json")


def resident(p, fixture_cpu="50m", fixture_mem="32Mi", w=("4000m", "7.75Gi"), w_sys=("400m", "512Mi"),
             s=("4000m", "7.75Gi"), s_sys=("300m", "512Mi"), **extra):
    """Kind-smoke params with a fixture size and a residency section (W = 4000m / 7.75Gi unless given)."""
    p = copy.deepcopy(p)
    p["fixture"].update(cpu_request=fixture_cpu, cpu_limit=fixture_cpu, memory_request=fixture_mem,
                        memory_limit=fixture_mem)
    p["residency"] = {"workload": {"allocatable": {"cpu": w[0], "memory": w[1]},
                                   "system_requests": {"cpu": w_sys[0], "memory": w_sys[1]}},
                      "support": {"allocatable": {"cpu": s[0], "memory": s[1]},
                                  "system_requests": {"cpu": s_sys[0], "memory": s_sys[1]}},
                      "support_extra": [], **extra}
    return p


def contribution(rep, role, name):
    return [c for c in rep["nodes"][role]["contributions"] if c["name"] == name]


# --- sidecar-free -----------------------------------------------------------------------------------------------------

def test_every_rendered_pod_template_carries_the_sidecar_opt_out(params):
    objs = render.render_all(params)
    templates = [(o, t) for o in objs for t in render.pod_templates(o)]
    names = sorted(o["metadata"]["name"] for o, _ in templates)
    # the ten fixture arms, the ingress and the two fault routes: every pod the renderer produces
    assert len(templates) == 13 and "c28-ingress" in names and "c28-forecast-route" in names
    for o, t in templates:
        assert t["metadata"]["annotations"][render.SIDECAR_ANNOTATION] == "false", o["metadata"]["name"]
        assert t["metadata"]["labels"][render.SIDECAR_LABEL] == "false", o["metadata"]["name"]
        assert render.SIDECAR_ANNOTATION not in (o["metadata"].get("annotations") or {})   # on the pod, not the owner
        assert render.SIDECAR_LABEL not in o["metadata"]["labels"]
        assert render.SIDECAR_LABEL not in o["spec"]["selector"].get("matchLabels", {})
    assert not any(o["kind"] == "Namespace" for o in objs)                                 # the namespace stays as is


def test_the_release_values_keep_operator_and_forecaster_sidecar_free(params):
    v = render.eval_release_values(params)
    for k in ("operator", "forecaster"):
        assert v[k]["podAnnotations"] == {"sidecar.istio.io/inject": "false"}
        assert v[k]["podLabels"] == {"sidecar.istio.io/inject": "false"}


@pytest.mark.parametrize("victim", ["c28-pa-n-s90", "c28-ingress", "c28-metrics-route"])
def test_the_audit_refuses_a_pod_template_without_the_opt_out(params, victim):
    objs = render.render_all(params)
    dep = next(o for o in objs if o["kind"] == "Deployment" and o["metadata"]["name"] == victim)
    del dep["spec"]["template"]["metadata"]["annotations"][render.SIDECAR_ANNOTATION]
    with pytest.raises(render.ParamsError, match=f"Deployment/{victim}"):
        render.audit_sidecar_free(objs)
    # on the Deployment's own metadata it does not reach the pods: still refused
    dep["metadata"]["annotations"] = {render.SIDECAR_ANNOTATION: "false"}
    with pytest.raises(render.ParamsError, match="sidecar-free"):
        render.audit_sidecar_free(objs)
    dep["spec"]["template"]["metadata"]["annotations"][render.SIDECAR_ANNOTATION] = "true"
    with pytest.raises(render.ParamsError):
        render.audit_sidecar_free(objs)


def test_the_audit_needs_the_label_as_well_as_the_annotation(params):
    objs = render.render_all(params)
    dep = next(o for o in objs if o["kind"] == "Deployment" and o["metadata"]["name"] == "c28-h-rps-s20")
    del dep["spec"]["template"]["metadata"]["labels"][render.SIDECAR_LABEL]       # the annotation is still there
    with pytest.raises(render.ParamsError, match=r"Deployment/c28-h-rps-s20 \(labels\)"):
        render.audit_sidecar_free(objs)


def test_unexpected_injection_is_found_in_regular_and_init_containers():
    def pod(name, containers, inits=()):
        return {"metadata": {"name": name, "namespace": "demo"},
                "spec": {"containers": [{"name": c} for c in containers],
                         "initContainers": [{"name": c} for c in inits]}}
    pods = [pod("clean", ["fixture"]), pod("proxied", ["fixture", "istio-proxy"]), pod("init", ["k6"], ["istio-init"])]
    problems = render.unexpected_injection(pods)
    assert len(problems) == 2 and "demo/proxied" in problems[0] and "istio-init" in problems[1]
    assert render.unexpected_injection(pods[:1]) == []


def test_the_audit_covers_jobs_cronjobs_and_pods():
    job = {"kind": "Job", "metadata": {"name": "gen"}, "spec": {"template": {"metadata": {}, "spec": {}}}}
    cron = {"kind": "CronJob", "metadata": {"name": "hist"},
            "spec": {"jobTemplate": {"spec": {"template": {"metadata": {"annotations": {}}, "spec": {}}}}}}
    pod = {"kind": "Pod", "metadata": {"name": "p", "annotations": dict(render.SIDECAR_FREE),
                                       "labels": dict(render.SIDECAR_FREE)}, "spec": {}}
    with pytest.raises(render.ParamsError, match=r"Job/gen \(labels and annotations\), CronJob/hist"):
        render.audit_sidecar_free([job, cron, pod])
    render.audit_sidecar_free([pod])


# --- calibrated fixture resources -------------------------------------------------------------------------------------

@pytest.mark.parametrize("key", ["cpu_request", "cpu_limit", "memory_request", "memory_limit"])
def test_placeholder_or_missing_fixture_resources_are_refused(params, key):
    template = load("prodcluster.template.json")
    p = copy.deepcopy(params)
    p["fixture"][key] = template["fixture"][key]            # the prodcluster template's placeholder
    with pytest.raises(render.ParamsError, match=f"fixture.{key} is still a placeholder.*never invented or lowered"):
        render.render_all(p)
    del p["fixture"][key]
    with pytest.raises(render.ParamsError, match=f"fixture.{key} is missing"):
        render.render_all(p)


def test_the_prodcluster_template_keeps_requiring_reviewed_fixture_and_node_values(params):
    template = load("prodcluster.template.json")
    for k in ("cpu_request", "cpu_limit", "memory_request", "memory_limit"):
        assert str(template["fixture"][k]).startswith("<")
    for role in ("workload", "support"):
        for part in ("allocatable", "system_requests"):
            assert all(str(v).startswith("<") for v in template["residency"][role][part].values())
    # resolved everywhere else, the fixture placeholders alone still refuse the template
    p = copy.deepcopy(params)
    p["fixture"].update({k: template["fixture"][k] for k in ("cpu_request", "cpu_limit", "memory_request",
                                                              "memory_limit")})
    with pytest.raises(render.ParamsError, match="placeholder"):
        render.validate(p)
    render.validate(params)                                  # the kind smoke keeps its own values


def test_fixture_request_above_its_limit_is_refused(params):
    params["fixture"]["cpu_request"] = "200m"
    with pytest.raises(render.ParamsError, match="at most fixture.cpu_limit"):
        render.validate(params)


# --- residency budget -------------------------------------------------------------------------------------------------

def test_quantities():
    assert render.cpu_millicores("250m") == 250 and render.cpu_millicores("1") == 1000
    assert render.cpu_millicores("0.5") == 500
    assert render.memory_bytes("7.75Gi") == int(7.75 * 1024) * MI and render.memory_bytes("1k") == 1000
    for bad in ("1Mi", "x", ""):
        with pytest.raises(render.ParamsError):
            render.cpu_millicores(bad)
    with pytest.raises(render.ParamsError):
        render.memory_bytes("100m")


def test_residency_passes_under_75_percent_and_reports_every_contribution(params):
    rep = render.check_residency(resident(params))
    w = rep["nodes"]["workload"]
    # 9 idle arms x 2 x 50m + loaded 12 x 50m + overlap 2 x 50m + ingress 100m + system 400m = 2100m of 4000m
    assert w["total"]["cpu_m"] == 2100 and w["fraction"]["cpu"] == pytest.approx(0.525)
    assert w["total"]["memory_bytes"] == (9 * 2 * 32 + 12 * 32 + 2 * 32 + 64 + 512) * MI
    assert sum(c["pods"] for c in w["contributions"] if c.get("note") == "idle arm") == 9 * 2
    assert [c["pods"] for c in w["contributions"] if "maxReplicas" in c.get("note", "")] == [F.MAX_REPLICAS]
    s = rep["nodes"]["support"]
    # two routes 2 x 20m + operator 100m + forecaster 1000m + generator 500m + system 300m
    assert s["total"]["cpu_m"] == 1940
    assert contribution(rep, "support", "forecaster (chart)")[0]["cpu_m"] == 1000    # 1 CPU / 512Mi, from the chart
    assert contribution(rep, "support", "forecaster (chart)")[0]["memory_bytes"] == 512 * MI
    assert rep["problems"] == []


def test_exactly_75_percent_passes(params):
    # evaluation on W is 1700m, so 1300m of system requests is exactly 3000m = 75 % of 4000m
    assert render.check_residency(resident(params, w_sys=("1300m", "512Mi")))["nodes"]["workload"]["fraction"]["cpu"] \
        == pytest.approx(0.75)
    with pytest.raises(render.ParamsError, match="CPU"):
        render.check_residency(resident(params, w_sys=("1301m", "512Mi")))


def test_cpu_over_budget_is_refused_on_its_own(params):
    p = resident(params, fixture_cpu="100m")                 # 3300m + 400m = 92.5 % of W
    rep = render.residency_report(p)
    assert len(rep["problems"]) == 1 and "workload node" in rep["problems"][0] and "CPU" in rep["problems"][0]
    assert "92.5%" in rep["problems"][0] and "idle arm" not in rep["problems"][0]
    assert "Deployment/c28-h-cpu-s20 overlap 2x(100m" in rep["problems"][0]           # contributions listed
    with pytest.raises(render.ParamsError, match="residency budget exceeded.*CPU requests 3700m"):
        render.render_all(p)


def test_memory_over_budget_is_refused_on_its_own(params):
    p = resident(params, fixture_mem="180Mi")                # 32 x 180Mi + 64Mi + 512Mi = 79.8 % of 7.75Gi
    rep = render.residency_report(p)
    assert len(rep["problems"]) == 1 and "memory requests" in rep["problems"][0]
    assert rep["nodes"]["workload"]["fraction"]["cpu"] < 0.75
    with pytest.raises(render.ParamsError, match="memory requests"):
        render.render_all(p)


def test_support_node_is_checked_too(params):
    rep = render.residency_report(resident(params, s=("2000m", "1.5Gi")))
    assert [p.split(":")[0] for p in rep["problems"]] == ["support node pa-c28-smoke-worker2"] * 2   # CPU and memory


def test_overlap_is_counted_and_at_least_one_pod(params):
    one = render.residency_report(resident(params, overlap_pods=1))["nodes"]["workload"]["total"]["cpu_m"]
    three = render.residency_report(resident(params, overlap_pods=3))["nodes"]["workload"]["total"]["cpu_m"]
    assert three - one == 2 * 50
    default = render.residency_report(resident(params))
    assert default["overlap_pods"] == F.SCALE_DOWN_PODS == 2
    assert contribution(default, "workload", "Deployment/c28-h-cpu-s20 overlap")[0]["pods"] == 2
    with pytest.raises(render.ParamsError, match="overlap_pods"):
        render.validate(resident(params, overlap_pods=0))


def test_idle_arm_parking_is_refused_until_a_mechanism_exists(params):
    with pytest.raises(render.ParamsError, match="parking"):
        render.validate(resident(params, idle_arm_replicas=1))
    base = render.residency_report(resident(params))["nodes"]["workload"]["total"]["cpu_m"]
    higher = render.residency_report(resident(params, idle_arm_replicas=3))["nodes"]["workload"]["total"]["cpu_m"]
    assert higher - base == 9 * 50                           # a conservative higher idle count is allowed


def test_generators_operator_and_extras_are_budgeted(params):
    p = resident(params, generator={"concurrent": 3, "cpu_request": "500m", "memory_request": "256Mi"},
                 forecaster={"cpu_request": "2", "memory_request": "1Gi"},
                 support_extra=[{"name": "recorder", "replicas": 1, "cpu_request": "200m", "memory_request": "256Mi"}])
    rep = render.residency_report(p)
    assert contribution(rep, "support", "generator Jobs")[0]["cpu_m"] == 1500
    assert contribution(rep, "support", "recorder")[0]["memory_bytes"] == 256 * MI
    assert rep["nodes"]["support"]["total"]["cpu_m"] == 40 + 100 + 2000 + 1500 + 200 + 300


@pytest.mark.parametrize("change, message", [
    (lambda r: r["workload"]["allocatable"].update(cpu="<W allocatable CPU>"), "placeholder"),
    (lambda r: r["support"].pop("system_requests"), "support.system_requests"),
    (lambda r: r.update(support_extra="<list them>"), "support_extra"),
    (lambda r: r["workload"]["allocatable"].update(memory="lots"), "memory quantity"),
])
def test_unresolved_residency_is_refused(params, change, message):
    p = resident(params)
    change(p["residency"])
    with pytest.raises(render.ParamsError, match=message):
        render.render_all(p)


def test_residency_cli(params, tmp_path, capsys):
    good, bad = tmp_path / "good.json", tmp_path / "bad.json"
    good.write_text(json.dumps(resident(params)))
    bad.write_text(json.dumps(resident(params, fixture_cpu="100m")))
    assert cli.main(["residency", "--params", str(good)]) == 0
    assert json.loads(capsys.readouterr().out)["nodes"]["workload"]["total"]["cpu_m"] == 2100
    assert cli.main(["residency", "--params", str(bad)]) == 1
    nores = tmp_path / "kind.json"
    nores.write_text(json.dumps(params))
    with pytest.raises(render.ParamsError, match="no residency section"):
        cli.main(["residency", "--params", str(nores)])


# --- support pods are counted from the final chart values -----------------------------------------------------------

@pytest.mark.parametrize("override, message", [
    ({"operator": {"cpu_request": "0", "memory_request": "0Mi"}}, "residency.operator must request"),
    ({"forecaster": {"cpu_request": "0", "memory_request": "0Mi"}}, "at least 1 CPU / 512Mi"),
    ({"forecaster": {"cpu_request": "1", "memory_request": "256Mi"}}, "at least 1 CPU / 512Mi"),
])
def test_under_budgeted_support_components_are_refused(params, override, message):
    with pytest.raises(render.ParamsError, match=message):
        render.render_all(resident(params, **override))


def test_the_release_requests_its_declared_budgets_and_accounting_reads_them(params):
    p = resident(params, forecaster={"cpu_request": "2", "memory_request": "1Gi"},
                 operator={"cpu_request": "200m", "memory_request": "300Mi"})
    v = render.eval_release_values(p)
    assert v["forecaster"]["statistical"]["resources"] == {"requests": {"cpu": "2", "memory": "1Gi"},
                                                           "limits": {"cpu": "2", "memory": "1Gi"}}   # limits raised
    assert v["operator"]["resources"] == {"requests": {"cpu": "200m", "memory": "300Mi"}, "limits": {"memory": "300Mi"}}
    rep = render.residency_report(p)
    assert contribution(rep, "support", "forecaster (chart)")[0]["cpu_m"] == 2000
    assert contribution(rep, "support", "operator (chart)")[0]["memory_bytes"] == 300 * MI


def test_chart_values_requesting_less_than_the_budget_are_refused(params):
    p = resident(params)
    smaller = {"forecaster": {"statistical": {"resources": {"requests": {"cpu": "250m", "memory": "256Mi"}}}}}
    chart = render.chart_values(render.eval_release_values(p), smaller)      # e.g. an overlay that drops the request
    rep = render.residency_report(p, chart=chart)
    assert any("forecaster requests 250m/256Mi, below its declared budget 1000m/512Mi" in x for x in rep["problems"])
    with pytest.raises(render.ParamsError, match="must not train"):
        render.residency_report(p, chart=render.chart_values(render.eval_release_values(p),
                                                             {"training": {"targets": [{"name": "x"}]}}))


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_chart_rendering_matches_the_accounting_and_is_sidecar_free(params, tmp_path):
    p = resident(params, forecaster={"cpu_request": "1500m", "memory_request": "768Mi"})
    overlay = render._merge(render.eval_release_values(p), {"networkPolicy": {"enabled": False}})
    f = tmp_path / "values.yaml"
    import yaml
    f.write_text(yaml.safe_dump(overlay))
    chart = os.path.dirname(render.CHART_VALUES)
    out = subprocess.run(["helm", "template", "pa-eval", chart, "-n", "pa-eval-system", "-f", str(f)],
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    docs = [d for d in yaml.safe_load_all(out.stdout) if d]
    pods = [d for d in docs if render.pod_templates(d)]
    assert sorted(d["metadata"]["name"].rsplit("-", 1)[1] for d in pods) == ["forecaster", "operator"]
    render.audit_sidecar_free(pods)                             # label and annotation reach the chart's pods
    rendered = {d["metadata"]["name"].rsplit("-", 1)[1]: (d["spec"].get("replicas", 1),
                                                          *render._pod_requests(d["spec"]["template"])) for d in pods}
    model = {name: (replicas, cpu, mem) for name, replicas, cpu, mem in render.chart_pods(render.chart_values(overlay))}
    assert rendered == model and model["forecaster"][1:] == (1500, 768 * MI)
