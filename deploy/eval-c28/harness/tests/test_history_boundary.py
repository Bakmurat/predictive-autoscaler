"""c28.history_boundary without a VictoriaMetrics: the attempt's scrape object, the guarded query and the acceptance
proof's refusals on raw-sample shapes (the real-VM behaviour is test_history_boundary_vm)."""

import pytest

from c28 import history_boundary as H, render as R
from demo_helpers import params

N, R_ = "c28-demo-pa-n", "c28-demo-pa-r"


def vm_params():
    p = params()
    p["scrape"] = "VMPodScrape"
    return p


def test_the_attempts_scrape_keeps_renames_and_labels_only_the_ingress_counter():
    obj = H.history_scrape(vm_params(), 3)
    ep = obj["spec"]["podMetricsEndpoints"][0]
    assert obj["metadata"]["name"] == "c28-demo-history-3" and ep["path"] == "/metrics" and ep["interval"] == "15s"
    rules = ep["metricRelabelConfigs"]
    assert rules[0] == {"action": "keep", "sourceLabels": ["__name__"], "regex": "c28_ingress_requests_total"}
    assert rules[1]["replacement"] == "c28_demo_history_requests_total" and rules[1]["targetLabel"] == "__name__"
    assert rules[2] == {"action": "replace", "targetLabel": "c28_history_epoch", "replacement": "3"}
    with pytest.raises(R.ParamsError):
        H.history_scrape(params(), 1)                                   # annotations scrape: refused
    for bad in (0, 100, "1", True):
        with pytest.raises(R.ParamsError):
            H.history_scrape(vm_params(), bad)


def test_the_guarded_query_names_the_attempt_and_the_guard():
    q = H.guarded_query(2, 1_800_000_300)
    assert q == ('sum(rate(c28_demo_history_requests_total{target="{{ .Name }}",c28_history_epoch="2"}[60s]))'
                 ' and on() (vector(time()) >= 1800000300)')
    assert H.compiled_query(2, 1_800_000_300, N).count(f'target="{N}"') == 1 and H.guard_s(1000) == 1300


def series(t, start, end, step=15, attempt="1", **labels):
    return [{"metric": {"__name__": H.HISTORY_METRIC, "target": t, H.EPOCH_LABEL: attempt, **labels},
             "values": [[x, "1"] for x in range(start, end + 1, step)]}]


@pytest.mark.parametrize("edit, text", [
    (lambda s: s.__setitem__(R_, []), "no series"),
    (lambda s: s.__setitem__(R_, s[R_] * 2), "2 series"),
    (lambda s: s[R_][0]["metric"].update(c28_history_epoch="9"), "labels not as relabelled"),
    (lambda s: s[R_][0]["metric"].update(__name__="c28_ingress_requests_total"), "labels not as relabelled"),
])
def test_the_acceptance_refuses_wrong_shapes(edit, text):
    o = 1_800_000_000
    g = H.guard_s(o)
    s = {t: series(t, o + 30, g) for t in (N, R_)}
    assert H.acceptance(s, [N, R_], 1, o, g)["accepted"]
    edit(s)
    r = H.acceptance(s, [N, R_], 1, o, g)
    assert not r["accepted"] and any(text in x for x in r["problems"]), r["problems"]
