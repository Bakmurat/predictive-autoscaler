"""The chart's optional demonstration dashboard (charts/predictive-autoscaler/dashboards/demonstration.json): two
targets side by side, the forecast path (A) and the reactive reference (B), from demand to service quality.

Pins what cannot be seen by rendering the chart (test_chart.py covers the ConfigMap): that the JSON loads and follows
the overview's conventions, that every query goes through the data source variable and the selected installation,
namespace and targets, that every metric it reads exists (in the operator, the forecasting service, the evaluation
fixture and its load harness, or one of the named standard exporters), that the two sides are exact mirrors on the
same axes, and that a stand-in for a quantity no metric exports is called a proxy. No Grafana is involved: how the
panels look is not tested here."""

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS = ROOT / "charts" / "predictive-autoscaler" / "dashboards"
DEMO, OVERVIEW = DASHBOARDS / "demonstration.json", DASHBOARDS / "overview.json"
METRICS_GO = ROOT / "k8s-operator" / "controllers" / "metrics.go"
EVAL = ROOT / "deploy" / "eval-c28"
OPERATOR_JOB = 'job="${installation}-operator-metrics", pa_install_namespace="$install_namespace"'
FORECASTER_JOB = 'job="${installation}-forecaster", pa_install_namespace="$install_namespace"'
AXIS = "(axis) "

# Metrics of standard exporters (not defined in this repository), by where they come from.
KUBE_STATE_METRICS = {"kube_deployment_spec_replicas", "kube_deployment_status_replicas_ready"}
CADVISOR = {"container_cpu_cfs_throttled_periods_total", "container_cpu_cfs_periods_total"}
KUBELET = {"kubelet_volume_stats_used_bytes", "kubelet_volume_stats_capacity_bytes"}
CONTROLLER_RUNTIME = {"controller_runtime_reconcile_total", "controller_runtime_reconcile_errors_total"}
K6_REMOTE_WRITE = {"k6_http_reqs_total", "k6_dropped_iterations_total"}


@pytest.fixture(scope="module")
def dashboard():
    return json.loads(DEMO.read_text())


def data_panels(dashboard):
    return [p for p in dashboard["panels"] if p["type"] not in ("text", "row")]


def expressions(dashboard):
    return ([t["expr"] for p in data_panels(dashboard) for t in p["targets"]]
            + [a["expr"] for a in dashboard["annotations"]["list"]])


def metric_names(expr):
    return set(re.findall(r"(?<![$\w])([a-zA-Z_:][a-zA-Z0-9_:]*)\s*\{", expr))


def selectors(expr):
    """(metric, label matchers) of every selector; a ${variable} is not a selector."""
    return re.findall(r"(?<![$\w])([a-zA-Z_:][a-zA-Z0-9_:]*)\s*\{([^{}]*(?:\$\{\w+\}[^{}]*)*)\}", expr)


def operator_metrics():
    return set(re.findall(r'Name:\s*"(predictive_autoscaler_[a-z0-9_]+)"', METRICS_GO.read_text()))


def operator_labels(name):
    m = re.search(rf'Name: "{name}",\s*Help: "[^"]*",\s*\}},\s*\[\]string\{{([^}}]*)\}}', METRICS_GO.read_text())
    return tuple(re.findall(r'"(\w+)"', m.group(1)))


def forecaster_metrics():
    from forecasting.metrics import METRIC_NAMES
    return set(METRIC_NAMES)


def fixture_metrics():
    """The series names the evaluation fixture and its ingress write (a histogram writes _bucket, _sum and _count)."""
    text = "".join(f.read_text() for f in (EVAL / "fixture").glob("*.go") if not f.name.endswith("_test.go"))
    names = set(re.findall(r"\bc28_[a-z0-9_]+", text))
    return names | {n + "_bucket" for n in names if n.endswith("_seconds")}


def pairs(dashboard):
    a = {p["title"][len("A · $target_a · "):]: p for p in dashboard["panels"] if p["title"].startswith("A · $target_a · ")}
    b = {p["title"][len("B · $target_b · "):]: p for p in dashboard["panels"] if p["title"].startswith("B · $target_b · ")}
    assert a and set(a) == set(b), set(a) ^ set(b)
    return [(name, a[name], b[name]) for name in a]


# --- conventions ---------------------------------------------------------------------------------------------------

def test_it_loads_and_follows_the_overviews_conventions(dashboard):
    overview = json.loads(OVERVIEW.read_text())
    assert dashboard["schemaVersion"] == overview["schemaVersion"]
    assert dashboard["uid"] == "pa-demonstration" != overview["uid"]
    assert dashboard["title"] == "Predictive Autoscaler — Demonstration" != overview["title"]
    names = {v["name"]: v for v in dashboard["templating"]["list"]}
    assert list(names) == ["datasource", "install_namespace", "installation", "namespace", "target_a", "target_b",
                           "capacity_per_replica"]
    theirs = {v["name"]: v for v in overview["templating"]["list"]}
    for shared in ("datasource", "install_namespace", "installation"):          # the same installation selection
        assert names[shared] == theirs[shared], shared
    assert all(not names[n]["multi"] and not names[n]["includeAll"] for n in ("namespace", "target_a", "target_b"))
    assert names["capacity_per_replica"]["type"] == "textbox" and names["capacity_per_replica"]["query"] == "0"
    ids = [p["id"] for p in dashboard["panels"]]
    assert len(ids) == len(set(ids)) and "__inputs" not in dashboard


def test_no_hard_coded_data_source(dashboard):
    def walk(node):
        if isinstance(node, dict):
            if "datasource" in node:
                yield node["datasource"]
            for v in node.values():
                yield from walk(v)
        elif isinstance(node, list):
            for v in node:
                yield from walk(v)
    refs = list(walk(dashboard))
    assert len(refs) > 100
    assert all(r == {"type": "prometheus", "uid": "${datasource}"} for r in refs), [r for r in refs if not isinstance(r, dict)]
    for p in data_panels(dashboard):                    # every data panel and every one of its queries
        assert p["datasource"]["uid"] == "${datasource}", p["title"]
        assert p["targets"] and all(t["datasource"]["uid"] == "${datasource}" for t in p["targets"]), p["title"]
    assert all(a["datasource"]["uid"] == "${datasource}" for a in dashboard["annotations"]["list"])
    for v in dashboard["templating"]["list"]:
        assert v["type"] != "query" or v["datasource"]["uid"] == "${datasource}", v["name"]


def test_every_selector_is_limited_by_variables_and_no_namespace_is_hard_coded(dashboard):
    queries = expressions(dashboard) + [v["query"]["query"] for v in dashboard["templating"]["list"]
                                        if v["type"] == "query" and v["name"] not in ("install_namespace", "installation")]
    seen = 0
    for q in queries:
        assert q.count("{") - q.count("${") == len(selectors(q)), q            # every selector was understood
        for metric, labels in selectors(q):
            seen += 1
            for value in re.findall(r'\bnamespace\s*(?:=~|!~|!=|=)\s*"([^"]*)"', labels):
                assert value in ("$namespace", "$install_namespace"), q
            if metric.startswith("predictive_autoscaler_forecast_"):
                assert labels.startswith(FORECASTER_JOB + ', namespace="$namespace"'), q
            elif metric.startswith("predictive_autoscaler_"):
                assert labels.startswith(OPERATOR_JOB), q
                assert metric == "predictive_autoscaler_condition" and "label_values" in q \
                    or ', namespace="$namespace"' in labels, q
            elif metric.startswith("controller_runtime_"):
                assert labels == OPERATOR_JOB, q
            elif metric == "up":
                assert 'pa_install_namespace="$install_namespace"' in labels and "${installation}-" in labels, q
            elif metric.startswith("k6_"):                                       # remote-written: the plan's own tag
                assert re.match(r'c28_target="\$target_[ab]"', labels), q
            elif metric.startswith(("c28_", "container_")):
                assert labels.startswith('namespace="$namespace", '), q
            elif "${installation}" in labels:                                    # the installation's own objects
                assert labels.startswith('namespace="$install_namespace", '), q
            else:
                assert labels.startswith('namespace="$namespace", deployment="$target_'), q
            if metric.startswith(("predictive_autoscaler_", "c28_", "k6_", "container_")) and "label_values" not in q:
                assert "$target_a" in labels or "$target_b" in labels, q
    assert seen > 150


def test_every_metric_exists(dashboard):
    """Operator and forecasting-service metrics by their registered names, the fixture's by what its code writes, the
    generator's by the harness's own queries; everything else is one of the named standard exporters' metrics."""
    used = set().union(*(metric_names(e) for e in expressions(dashboard)))
    ours = {m for m in used if m.startswith("predictive_autoscaler_")}
    assert len(ours) >= 15 and ours <= operator_metrics() | forecaster_metrics(), ours - operator_metrics() - forecaster_metrics()
    assert not operator_metrics() & forecaster_metrics()
    fixture = {m for m in used if m.startswith("c28_")}
    assert len(fixture) >= 6 and fixture <= fixture_metrics(), fixture - fixture_metrics()
    k6 = {m for m in used if m.startswith("k6_")}
    harness = (EVAL / "harness" / "c28" / "collect.py").read_text()
    assert k6 == K6_REMOTE_WRITE and all(m + "{" in harness for m in k6)
    standard = KUBE_STATE_METRICS | CADVISOR | KUBELET | CONTROLLER_RUNTIME | {"up"}
    assert used - ours - fixture - k6 == standard
    # the labels the queries select on are the ones the code registers
    assert {operator_labels("predictive_autoscaler_" + m) for m in (
        "current_rpm", "predicted_rpm", "desired_replicas", "predicted_replicas", "actual_needed_replicas")} == {
        ("application", "namespace")}
    assert operator_labels("predictive_autoscaler_scale_events_total") == ("application", "namespace", "direction")
    assert {operator_labels("predictive_autoscaler_" + m) for m in ("predictive_extra_replicas", "predictive_capped")} == {
        ("namespace", "autoscaler")}
    assert operator_labels("predictive_autoscaler_prediction_gate") == ("namespace", "autoscaler", "phase")
    assert operator_labels("predictive_autoscaler_condition") == ("namespace", "autoscaler", "type", "status")
    from forecasting.metrics import BASE_LABELS
    assert BASE_LABELS == ("namespace", "autoscaler", "model")


def test_the_trust_phases_and_conditions_are_the_operators(dashboard):
    phases = re.search(r"v2Phases\s*=\s*\[\]string\{([^}]*)\}",
                       (ROOT / "k8s-operator" / "controllers" / "forecast_v2.go").read_text()).group(1)
    phases = re.findall(r'"(\w+)"', phases)
    assert phases == ["Collecting", "Calibrating", "Shadow", "Trusted", "Degraded"]
    conditions = re.search(r"conditionMetricTypes = \[\]string\{([^}]*)\}", METRICS_GO.read_text(), re.S).group(1)
    for _, a, _ in pairs(dashboard):
        if a["type"] != "state-timeline":
            continue
        if "Trust state" in a["title"]:
            expr = a["targets"][0]["expr"]
            assert re.findall(r'(\d) \* sum\([^)]*phase="(\w+)"\}\)', expr) == [(str(i + 1), p) for i, p in enumerate(phases)]
            (mapping,) = a["fieldConfig"]["defaults"]["mappings"]
            assert {k: v["text"] for k, v in mapping["options"].items()} == {str(i + 1): p for i, p in enumerate(phases)}
        else:
            (types,) = set(re.findall(r'type=~"([^"]+)"', a["targets"][0]["expr"]))
            assert set(types.split("|")) <= set(re.findall(r'"(\w+)"', conditions))
            assert {"ScalingActive", "PredictionReady", "TelemetryAvailable", "ConflictDetected"} <= set(types.split("|"))


def test_no_function_that_differs_between_prometheus_and_victoriametrics(dashboard):
    # MetricsQL counts a series' first sample as a change (changes) and extrapolates differently (increase, delta).
    for e in expressions(dashboard):
        assert not re.search(r"\b(changes|increase|delta|idelta|resets|deriv|predict_linear)\s*\(", e), e
        assert "$__interval" not in e and "$__range" not in e, e
    assert any("$__rate_interval" in e for e in expressions(dashboard))


# --- two targets, side by side -------------------------------------------------------------------------------------

def swap(node):
    text = json.dumps(node).replace("$target_a", "$target_X").replace("$target_b", "$target_a").replace("$target_X", "$target_b")
    return json.loads(text)


def test_both_sides_are_exact_mirrors_on_one_time_axis(dashboard):
    listed = pairs(dashboard)
    assert len(listed) >= 12
    for name, a, b in listed:
        assert (a["gridPos"]["x"], a["gridPos"]["w"], b["gridPos"]["x"], b["gridPos"]["w"]) == (0, 12, 12, 12), name
        assert (a["gridPos"]["y"], a["gridPos"]["h"]) == (b["gridPos"]["y"], b["gridPos"]["h"]), name
        want = swap({k: v for k, v in a.items() if k not in ("id", "gridPos", "title")})
        assert {k: v for k, v in b.items() if k not in ("id", "gridPos", "title")} == want, name
        own = [t for t in a["targets"] if not t["legendFormat"].startswith(AXIS)]
        assert own and all("$target_a" in t["expr"] and "$target_b" not in t["expr"] for t in own), name
    assert dashboard["graphTooltip"] >= 1                                       # a shared crosshair
    assert not any(k in p for p in dashboard["panels"] for k in ("timeFrom", "timeShift", "hideTimeOverride"))


def test_paired_charts_share_one_y_range(dashboard):
    """Grafana scales each panel to its own data. A fixed range (a ratio from 0 to 1) is the same on both sides;
    otherwise each panel also carries the other target's series, undrawn and out of the legend and tooltip, so both
    panels scale to the same data."""
    checked = 0
    for name, a, _ in pairs(dashboard):
        if a["type"] != "timeseries":
            continue
        defaults = a["fieldConfig"]["defaults"]
        assert defaults["min"] == 0, name
        own = [t for t in a["targets"] if not t["legendFormat"].startswith(AXIS)]
        axis = [t for t in a["targets"] if t["legendFormat"].startswith(AXIS)]
        if "max" in defaults:
            assert not axis, name
            continue
        checked += 1
        assert [(AXIS + t["legendFormat"], swap(t["expr"])) for t in own] == [(t["legendFormat"], t["expr"]) for t in axis], name
        last = a["fieldConfig"]["overrides"][-1]                                 # the last override wins
        assert last["matcher"] == {"id": "byRegexp", "options": "/^\\(axis\\) /"}, name
        props = {p["id"]: p["value"] for p in last["properties"]}
        assert props["custom.lineWidth"] == 0 and props["custom.fillOpacity"] == 0, name
        assert props["custom.hideFrom"] == {"legend": True, "tooltip": True, "viz": False}, name
    assert checked >= 9


def test_a_series_keeps_its_colour_in_every_panel(dashboard):
    colours = {}
    for p in data_panels(dashboard):
        for o in p["fieldConfig"]["overrides"]:
            for prop in o["properties"]:
                if prop["id"] == "color":
                    colours.setdefault(json.dumps(o["matcher"], sort_keys=True), set()).add(prop["value"]["fixedColor"])
    assert len(colours) >= 20 and all(len(v) == 1 for v in colours.values()), colours


def test_scale_writes_are_annotated_per_target(dashboard):
    notes = {a["name"]: a for a in dashboard["annotations"]["list"]}
    assert set(notes) == {"A scaled up", "B scaled up", "A scaled down", "B scaled down"}
    assert notes["A scaled up"]["enable"] and notes["B scaled up"]["enable"]
    assert notes["A scaled up"]["iconColor"] != notes["B scaled up"]["iconColor"]
    for name, a in notes.items():
        side, direction = name[0].lower(), name.rsplit(" ", 1)[-1]
        assert f'application="$target_{side}", direction="{direction}"' in a["expr"], name
        # the counter's series is created by the first write, so the first write is a step from "absent", not from 0
        assert a["expr"].startswith("(sum(predictive_autoscaler_scale_events_total{") and " offset 1m) or " in a["expr"]
        assert a["expr"].endswith(") != 0") and a["step"] == "60s", name


# --- what it says about itself -------------------------------------------------------------------------------------

def test_the_first_panel_says_what_this_is_and_is_not(dashboard):
    first = dashboard["panels"][0]
    assert first["type"] == "text" and first["options"]["mode"] == "markdown" and first["gridPos"]["y"] == 0
    assert min(p["gridPos"]["y"] for p in dashboard["panels"]) == 0
    text = first["options"]["content"]
    for phrase in ("**synthetic**", "not real business demand", "**not a comparison result**",
                   "Nothing on this page is a scored or qualified outcome", "How to read the Scaling row",
                   "In Recommend mode the operator writes nothing", "no metric exists"):
        assert phrase in text, phrase
    assert len(text) < 3000
    # no figure and no outcome is claimed anywhere on the page
    prose = text + " ".join(p.get("description", "") for p in dashboard["panels"]) + dashboard["description"]
    assert not re.search(r"\b(outperform|better than|faster than|improv|reduc|saves?|proves?|production)\w*", prose, re.I)


def test_the_rows_follow_the_chain(dashboard):
    rows = [p["title"] for p in dashboard["panels"] if p["type"] == "row"]
    assert [r.split(" · ")[0] for r in rows] == ["1", "2", "3", "4", "5", "6"]
    for want, row in zip(("Demand", "Forecast", "Scaling", "Mode and safety", "Service quality", "Health"), rows):
        assert want in row, row
    assert all(p.get("description") for p in data_panels(dashboard))


def test_a_stand_in_is_called_a_proxy(dashboard):
    """Journal health and state size have no metric: the forecasting service's readiness and its state volume stand in,
    and say so. Nothing else on the page is a stand-in."""
    for p in data_panels(dashboard):
        stand_in = any("${installation}-forecast" in t["expr"] and not t["expr"].startswith("up{")
                       and "predictive_autoscaler_" not in t["expr"] for t in p["targets"])
        assert stand_in == ("proxy" in p["title"].lower()), p["title"]
        assert stand_in == p["description"].startswith("PROXY. No metric exports"), p["title"]
    assert sum("proxy" in p["title"].lower() for p in dashboard["panels"]) == 2


def test_the_capacity_line_needs_the_variable(dashboard):
    lines = [t["expr"] for p in data_panels(dashboard) for t in p["targets"] if "$capacity_per_replica" in t["expr"]]
    assert len(lines) == 4                                                       # A and B, each also as the other's axis
    for e in lines:
        assert re.fullmatch(r'max\(kube_deployment_status_replicas_ready\{namespace="\$namespace", deployment="\$target_[ab]"\}\)'
                            r' \* \$capacity_per_replica \* 60 and on\(\) \(vector\(\$capacity_per_replica\) > 0\)', e), e


def test_the_objective_lines_are_the_frozen_ones(dashboard):
    frozen = (EVAL / "harness" / "c28" / "frozen.py").read_text()
    p95, p99, err = map(float, re.search(r"SLO_P95_S, SLO_P99_S, SLO_ERROR_FRACTION = ([\d.]+), ([\d.]+), ([\d.]+)", frozen).groups())
    by_name = {name: a for name, a, _ in pairs(dashboard)}
    lat, errors = by_name["Latency at the ingress: p95 and p99"], by_name["Error fraction"]
    assert [s["value"] for s in lat["fieldConfig"]["defaults"]["thresholds"]["steps"]] == [None, p95, p99]
    assert [s["value"] for s in errors["fieldConfig"]["defaults"]["thresholds"]["steps"]] == [None, err]
    for p in (lat, errors):
        assert p["fieldConfig"]["defaults"]["custom"]["thresholdsStyle"]["mode"] == "dashed"
    assert all('outcome="success"' in t["expr"] and "histogram_quantile(0.9" in t["expr"] for t in lat["targets"])
    assert lat["fieldConfig"]["defaults"]["unit"] == "s" and errors["fieldConfig"]["defaults"]["unit"] == "percentunit"
