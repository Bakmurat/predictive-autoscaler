"""The built-in Help / User Guide (B10): docs/user-guide.md and the chart's optional Grafana "Help & Overview" dashboard.

Pins that the dashboard renders only when asked, that its first panel is the Help text and points at the guide, that it
uses only metrics the operator really exports (k8s-operator/controllers/metrics.go) and no hard-coded data source, and
that the guide and the Help panel cover every alert rule and every documented condition type and reason."""

import json
import re
from pathlib import Path

import pytest

from tests.test_chart import BASE, _merge, by_kind, helm, render  # noqa: F401  (render is a fixture)

ROOT = Path(__file__).resolve().parents[2]
GUIDE = ROOT / "docs" / "user-guide.md"
DASHBOARD = ROOT / "charts" / "predictive-autoscaler" / "dashboards" / "overview.json"
METRICS_GO = ROOT / "k8s-operator" / "controllers" / "metrics.go"
HELPERS = ROOT / "charts" / "predictive-autoscaler" / "templates" / "_helpers.tpl"
CONFIGURATION = ROOT / "docs" / "configuration.md"
INSTALL_SELECTOR = 'job="${installation}-operator-metrics", pa_install_namespace="$install_namespace"'
TITLE = "Predictive Autoscaler — Help & Overview"


def dashboard_configmaps(docs):
    return [d for d in by_kind(docs, "ConfigMap")
            if d["metadata"]["labels"].get("app.kubernetes.io/component") == "dashboards"]


def embedded(cm):
    (key,) = cm["data"]
    return key, json.loads(cm["data"][key])


@pytest.fixture(scope="module")
def dashboard():
    return json.loads(DASHBOARD.read_text())


def help_text(dashboard):
    return dashboard["panels"][0]["options"]["content"]


def operator_metric_names():
    return set(re.findall(r'Name:\s*"(predictive_autoscaler_[a-z0-9_]+)"', METRICS_GO.read_text()))


def alert_names():
    names = re.findall(r'"alert" "([A-Za-z]+)"', HELPERS.read_text())
    assert len(names) >= 7, names
    return names


# --- the chart -----------------------------------------------------------------------------------------------------

def test_the_dashboard_is_opt_in(render):
    assert not dashboard_configmaps(render())


def test_the_dashboard_configmap_carries_the_sidecar_label_and_valid_json(render):
    docs = render({"dashboards": {"enabled": True, "labels": {"team": "platform"},
                                  "annotations": {"grafana_folder": "Predictive Autoscaler"}}})
    (cm,) = dashboard_configmaps(docs)
    meta = cm["metadata"]
    assert meta["labels"]["grafana_dashboard"] == "1" and meta["labels"]["team"] == "platform"
    assert meta["annotations"] == {"grafana_folder": "Predictive Autoscaler"}
    assert meta["namespace"] == "pa-system"
    key, got = embedded(cm)
    assert key == f"{got['uid']}.json" and re.fullmatch(r"pa-help-[0-9a-f]{12}", got["uid"])
    assert got["title"] == f"{TITLE} (pa-system/pa)"
    current = {v["name"]: v["current"] for v in got["templating"]["list"]}
    assert current["install_namespace"]["value"] == "pa-system"
    assert current["installation"]["value"] == "pa-predictive-autoscaler"
    # apart from the release-derived identity and default selection, the shipped JSON is embedded unchanged
    want = json.loads(DASHBOARD.read_text())
    for d in (got, want):
        d.pop("uid"), d.pop("title")
        for v in d["templating"]["list"]:
            v.pop("current", None)
    assert got == want


def test_each_release_gets_its_own_dashboard_identity(tmp_path):
    values = _merge(BASE, {"dashboards": {"enabled": True}})
    ids = set()
    for release in ("pa", "pb"):
        (cm,) = dashboard_configmaps(helm(values, tmp_path, release=release))
        key, got = embedded(cm)
        ids.add((key, got["uid"], got["title"]))
    assert len({k for k, _, _ in ids}) == len({u for _, u, _ in ids}) == len({t for _, _, t in ids}) == 2


def test_the_sidecar_label_can_be_dropped_and_unknown_keys_are_rejected(render):
    (cm,) = dashboard_configmaps(render({"dashboards": {"enabled": True, "labels": {"grafana_dashboard": None,
                                                                                    "dashboards": "pa"}}}))
    assert "grafana_dashboard" not in cm["metadata"]["labels"] and cm["metadata"]["labels"]["dashboards"] == "pa"
    assert render({"dashboards": {"enabled": True, "folder": "x"}}, check=False).returncode != 0


def test_the_notes_point_at_the_guide_and_the_dashboard():
    notes = (ROOT / "charts" / "predictive-autoscaler" / "templates" / "NOTES.txt").read_text()
    assert "docs/user-guide.md" in notes
    assert TITLE in notes and ".Values.dashboards.enabled" in notes
    assert "in your checkout: docs/user-guide.md" in notes


# --- the dashboard -------------------------------------------------------------------------------------------------

def test_the_first_panel_is_the_help_text_linking_the_guide(dashboard):
    first = dashboard["panels"][0]
    assert first["type"] == "text" and first["title"] == "Help / User Guide"
    assert first["options"]["mode"] == "markdown"
    assert "docs/user-guide.md" in first["options"]["content"]
    assert "In your checkout: `docs/user-guide.md`" in first["options"]["content"]
    assert min(p["gridPos"]["y"] for p in dashboard["panels"]) == first["gridPos"]["y"] == 0
    assert any("docs/user-guide.md" in link["url"] for link in dashboard["links"])


def test_the_dashboard_identity(dashboard):
    assert dashboard["uid"] == "pa-help-overview" and dashboard["title"] == TITLE
    names = {v["name"]: v for v in dashboard["templating"]["list"]}
    assert names["datasource"]["type"] == "datasource" and names["datasource"]["query"] == "prometheus"
    assert {"install_namespace", "installation", "namespace", "autoscaler", "application"} <= set(names)


def test_every_query_is_limited_to_one_installation(dashboard):
    queries = [t["expr"] for p in dashboard["panels"] for t in p.get("targets", [])]
    queries += [v["query"]["query"] for v in dashboard["templating"]["list"]
                if v["type"] == "query" and v["name"] not in ("install_namespace", "installation")]
    assert len(queries) > 15
    for q in queries:
        selectors = len(re.findall(r"(?<!\$)\{", q))  # label selectors, not ${variables}
        if q.startswith("up{"):
            assert 'pa_install_namespace="$install_namespace"' in q and "${installation}-" in q, q
        else:
            assert q.count(INSTALL_SELECTOR) == selectors, q
    legends = [t["legendFormat"] for p in dashboard["panels"] if p["type"] == "timeseries" for t in p["targets"]]
    assert all("{{namespace}}/{{application}}" in lf or "{{pa_install_namespace}} {{job}}" in lf for lf in legends)


def test_every_metric_the_dashboard_uses_is_exported_by_the_operator(dashboard):
    used = set(re.findall(r"predictive_autoscaler_[a-z0-9_]*[a-z0-9]", json.dumps(dashboard)))
    exported = operator_metric_names()
    assert len(exported) >= 15, exported
    assert used and used <= exported, used - exported
    exprs = [t["expr"] for p in dashboard["panels"] for t in p.get("targets", [])]
    other = {m for e in exprs for m in re.findall(r"\b([a-z_:][a-z0-9_:]*)\s*\{", e)} - exported
    assert other <= {"up"}, other


def test_no_hard_coded_datasource_uid(dashboard):
    def walk(node):
        if isinstance(node, dict):
            ds = node.get("datasource")
            if isinstance(ds, dict):
                yield ds
            elif isinstance(ds, str):
                yield {"uid": ds}
            for v in node.values():
                yield from walk(v)
        elif isinstance(node, list):
            for v in node:
                yield from walk(v)
    refs = list(walk(dashboard))
    assert refs
    assert all(r.get("uid") in ("${datasource}", "$datasource") for r in refs), refs
    assert "__inputs" not in dashboard


# --- the guide -----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("alert", alert_names())
def test_every_alert_is_explained_in_the_guide_and_the_help_panel(dashboard, alert):
    assert alert in GUIDE.read_text()
    assert alert in help_text(dashboard)


def documented_condition_terms():
    row = next(line for line in CONFIGURATION.read_text().splitlines() if line.startswith("| `conditions` |"))
    terms = set(re.findall(r"`([A-Za-z]+)`", row)) - {"conditions"}
    assert {"Ready", "ForecastAvailable", "ScaleDownHeadroom", "ReplicaWriter", "GuardAborted"} <= terms, terms
    return sorted(terms)


@pytest.mark.parametrize("term", documented_condition_terms())
def test_every_documented_condition_type_and_reason_is_in_the_guide(term):
    assert re.search(rf"\b{term}\b", GUIDE.read_text()), term


def test_the_docs_index_and_the_readme_link_the_guide():
    index = (ROOT / "docs" / "README.md").read_text()
    rows = [line for line in index.splitlines() if line.startswith("| [")]
    assert rows[0].startswith("| [user-guide.md](user-guide.md)")
    assert "(docs/user-guide.md)" in "\n".join((ROOT / "README.md").read_text().splitlines()[:10])


def test_every_metric_the_guide_names_is_exported_by_the_operator():
    named = set(re.findall(r"predictive_autoscaler_[a-z0-9_]*[a-z0-9]", GUIDE.read_text()))
    assert named and named <= operator_metric_names(), named - operator_metric_names()


def test_the_guide_keeps_the_status_statement_and_its_links_resolve():
    text = GUIDE.read_text()
    assert "research prototype" in text and "no published" in text
    for target in re.findall(r"\]\(([^)#]+\.md)(?:#[^)]*)?\)", text):
        assert (GUIDE.parent / target).resolve().exists(), target
