"""The installation's namespace policy (WATCH_NAMESPACES) in the forecaster: a request for a namespace this installation
does not serve is refused before any read of the Kubernetes API, any registration, any history fetch and any journal
write; a malformed list stops the service and is never read as "every namespace"; an unset list is cluster-wide."""

import hashlib
import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests.test_metric_contract as ident  # noqa: E402
import tests.test_statistical_service as svc_helpers  # noqa: E402
from api.identity import (  # noqa: E402
    AUTOSCALER_NAMESPACE_NOT_WATCHED, CLUSTER_WIDE, TARGET_NAMESPACE_NOT_WATCHED, ProvenanceRefused, WatchConfigError,
    WatchPolicy, parse_watch_namespaces, resolve_signal)
from forecasting import contracts as c  # noqa: E402
from forecasting.service import ForecastService, ServiceConfig, ServiceError  # noqa: E402

SHOP = WatchPolicy(frozenset({"shop"}))
OTHER = WatchPolicy(frozenset({"other"}))


class NoReads:
    """A reader that must never be asked: any read fails the test."""

    def __init__(self):
        self.paths = []

    def get(self, path):
        self.paths.append(path)
        raise AssertionError(f"a refused namespace must cost no read, but {path} was read")


# --- the configuration ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw", [None, "", "   ", "\n"])
def test_an_unset_or_blank_list_is_cluster_wide_as_for_the_operator(raw):
    policy = parse_watch_namespaces(raw)
    assert policy is CLUSTER_WIDE and not policy.restricted
    assert policy.allows("anything") and policy.describe() == "cluster-wide (every namespace)"


def test_a_list_restricts_to_exactly_its_namespaces():
    policy = parse_watch_namespaces("shop, payments")
    assert policy.restricted and policy.namespaces == frozenset({"shop", "payments"})
    assert policy.allows("shop") and policy.allows("payments")
    assert not policy.allows("kube-system") and not policy.allows("") and not policy.allows(None)
    assert not policy.allows("shop ") and not policy.allows(["shop"])
    assert policy.describe() == "restricted to payments, shop"
    assert parse_watch_namespaces("shop").namespaces == frozenset({"shop"})


@pytest.mark.parametrize("raw", ["shop,", ",shop", "shop,,payments", ",", "Shop", "shop;payments", "shop payments",
                                 "-shop", "shop-", "a" * 64, "*", 5, ["shop"]])
def test_a_malformed_list_is_an_error_never_an_unrestricted_policy(raw):
    with pytest.raises(WatchConfigError):
        parse_watch_namespaces(raw)


@pytest.mark.parametrize("namespaces", [frozenset(), {"shop"}, frozenset({"Shop"}), frozenset({""}), ["shop"], "shop"])
def test_a_restricted_policy_cannot_be_built_empty_or_malformed(namespaces):
    with pytest.raises(WatchConfigError):
        WatchPolicy(namespaces)


# --- the single enforcement point: api.identity.resolve_signal -----------------------------------------------------

def test_each_namespace_field_is_refused_on_its_own_before_any_read():
    cases = [  # (autoscaler namespace, target namespace, the stable message)
        ("other", "shop", AUTOSCALER_NAMESPACE_NOT_WATCHED),      # foreign autoscaler namespace, allowed target
        ("shop", "other", TARGET_NAMESPACE_NOT_WATCHED),          # allowed autoscaler namespace, foreign target
        ("other", "elsewhere", AUTOSCALER_NAMESPACE_NOT_WATCHED),  # both foreign
        ("kube-system", "kube-system", AUTOSCALER_NAMESPACE_NOT_WATCHED),
    ]
    for pa_ns, target_ns, message in cases:
        reader = NoReads()
        with pytest.raises(ProvenanceRefused) as e:
            resolve_signal(ident.request(autoscaler_namespace=pa_ns, namespace=target_ns), reader, SHOP)
        assert str(e.value) == message and reader.paths == []
    assert AUTOSCALER_NAMESPACE_NOT_WATCHED != TARGET_NAMESPACE_NOT_WATCHED


@pytest.mark.parametrize("target_ns", [5, "", "Shop", ["shop"], {"shop": 1}])
def test_a_target_namespace_that_is_not_a_watched_name_is_refused_before_any_read(target_ns):
    reader = NoReads()
    with pytest.raises(ProvenanceRefused) as e:
        resolve_signal(dict(ident.request(), namespace=target_ns), reader, SHOP)
    assert str(e.value) == TARGET_NAMESPACE_NOT_WATCHED and reader.paths == []


def test_the_watched_namespace_resolves_and_the_live_checks_still_run():
    reader = ident.FakeReader()
    sig = resolve_signal(ident.request(), reader, SHOP)
    assert (sig.namespace, sig.name) == ("shop", "web") and len(reader.paths) == 2
    with pytest.raises(ProvenanceRefused, match="UID differs"):          # the identity checks after the reads remain
        resolve_signal(ident.request(target_uid="another"), ident.FakeReader(), SHOP)
    # a request without the optional target namespace (the first API) is checked on the autoscaler's namespace, and
    # the target is in the autoscaler's namespace by the resolver's own rule
    assert resolve_signal(ident.request(namespace=None), ident.FakeReader(), SHOP).namespace == "shop"


def test_an_explicitly_unrestricted_policy_reads_as_before():
    reader = ident.FakeReader()
    assert resolve_signal(ident.request(), reader, CLUSTER_WIDE).namespace == "shop" and len(reader.paths) == 2
    reader = ident.FakeReader(pa="missing")                 # a foreign namespace is looked up (and here not found)
    with pytest.raises(ProvenanceRefused, match="not found"):
        resolve_signal(ident.request(autoscaler_namespace="other"), reader, CLUSTER_WIDE)
    assert len(reader.paths) == 1 and "/namespaces/other/" in reader.paths[0]


@pytest.mark.parametrize("watch", [None, frozenset({"shop"}), "shop", True])
def test_no_forecast_path_can_resolve_without_stating_the_policy(watch):
    reader = NoReads()
    with pytest.raises(TypeError):
        resolve_signal(ident.request(), reader, watch)
    with pytest.raises(TypeError):
        resolve_signal(ident.request(), reader)             # no default: the argument is required
    with pytest.raises(TypeError):
        c.bind_request(svc_helpers.body(svc_helpers.T0), reader, now_ms=svc_helpers.T0 * 1000)
    assert reader.paths == []


# --- the v2 request path and the service ----------------------------------------------------------------------------

def test_bind_request_refuses_each_field_before_any_read():
    now = svc_helpers.T0 * 1000 + 10_000
    for change, message in (({"autoscaler_namespace": "other"}, AUTOSCALER_NAMESPACE_NOT_WATCHED),
                            ({"namespace": "other"}, TARGET_NAMESPACE_NOT_WATCHED),
                            ({"autoscaler_namespace": "other", "namespace": "other"},
                             AUTOSCALER_NAMESPACE_NOT_WATCHED)):
        reader = NoReads()
        with pytest.raises(ProvenanceRefused) as e:
            c.bind_request(dict(svc_helpers.body(svc_helpers.T0), **change), reader, now_ms=now, watch=SHOP)
        assert str(e.value) == message and reader.paths == []
    assert c.bind_request(svc_helpers.body(svc_helpers.T0), svc_helpers.Reader(), now_ms=now,
                          watch=SHOP).signal.namespace == "shop"


def _tree(path):
    out = {}
    for root, _dirs, files in os.walk(path):
        for f in files:
            p = os.path.join(root, f)
            out[os.path.relpath(p, path)] = hashlib.sha256(open(p, "rb").read()).hexdigest()
    return out


@pytest.mark.parametrize("change,message", [
    ({"autoscaler_namespace": "other"}, AUTOSCALER_NAMESPACE_NOT_WATCHED),
    ({"namespace": "other"}, TARGET_NAMESPACE_NOT_WATCHED),
    ({"autoscaler_namespace": "other", "namespace": "other"}, AUTOSCALER_NAMESPACE_NOT_WATCHED)])
def test_the_service_refuses_with_no_read_no_registration_no_history_and_no_journal_change(tmp_path, change, message):
    reader, backend = NoReads(), svc_helpers.Backend()
    svc, clock = svc_helpers.make(tmp_path, reader=reader, backend=backend, watch=SHOP)
    try:
        clock.set(svc_helpers.T0 * 1000 + 10_000)
        before = _tree(tmp_path)
        with pytest.raises(ServiceError) as e:
            svc.predict(dict(svc_helpers.body(svc_helpers.T0), **change))
        assert (e.value.status, e.value.reason, e.value.message) == (422, "IdentityMismatch", message)
        assert reader.paths == [] and backend.calls == 0
        assert list(svc.metrics.slots()) == [] and _tree(tmp_path) == before
    finally:
        svc.close()


def test_the_service_serves_its_watched_namespace(tmp_path):
    svc, clock = svc_helpers.make(tmp_path, watch=SHOP)
    try:
        wire = svc_helpers.ask(svc, clock, svc_helpers.T0)
        assert wire["target_namespace"] == "shop" and len(list(svc.metrics.slots())) == 1
    finally:
        svc.close()


def test_the_binding_read_again_before_an_issuance_uses_the_same_policy(tmp_path):
    now = svc_helpers.T0 * 1000 + 10_000
    body = svc_helpers.body(svc_helpers.T0)
    bound = c.bind_request(body, svc_helpers.Reader(), now_ms=now, watch=CLUSTER_WIDE)
    allowed, _ = svc_helpers.make(tmp_path / "a", watch=SHOP)
    refused, _ = svc_helpers.make(tmp_path / "b", reader=NoReads(), watch=OTHER)
    try:
        assert allowed._still_bound(body, bound, now) is True
        assert refused._still_bound(body, bound, now) is False and refused.reader.paths == []
    finally:
        allowed.close()
        refused.close()


def test_the_service_needs_a_policy_object(tmp_path):
    with pytest.raises(ValueError, match="WatchPolicy"):
        ForecastService(ServiceConfig(prometheus_url="http://prom", state_dir=str(tmp_path), fit_mode="off",
                                      watch=frozenset({"shop"})), reader=svc_helpers.Reader())
    assert ServiceConfig(prometheus_url="http://prom", state_dir=str(tmp_path)).watch is CLUSTER_WIDE


# --- startup: the statistical forecaster and the first API -----------------------------------------------------------

def _env(tmp_path, **more):
    return {"PROMETHEUS_URL": "http://prom", "FORECAST_STATE_DIR": str(tmp_path / "state"),
            "FORECAST_FIT_MODE": "off", **more}


def test_the_statistical_forecaster_takes_the_policy_from_its_environment_once(tmp_path, caplog):
    from api import statistical as app
    with caplog.at_level("WARNING"):
        svc = app.default_service(_env(tmp_path, WATCH_NAMESPACES="shop,payments"))
    try:
        assert svc.config.watch == WatchPolicy(frozenset({"shop", "payments"}))
        assert "NAMESPACE POLICY: forecasts are restricted to payments, shop" in caplog.text
    finally:
        svc.close()
    caplog.clear()
    with caplog.at_level("WARNING"):
        svc = app.default_service(_env(tmp_path / "wide"))
    try:
        assert svc.config.watch is CLUSTER_WIDE
        assert "NAMESPACE POLICY: forecasts are cluster-wide (every namespace)" in caplog.text
    finally:
        svc.close()


@pytest.mark.parametrize("raw", ["shop,", "Shop", ","])
def test_the_statistical_forecaster_refuses_to_start_on_a_malformed_list(tmp_path, raw):
    from api import statistical as app
    with pytest.raises(app.Refused, match="namespace policy is refused"):
        app.default_service(_env(tmp_path, WATCH_NAMESPACES=raw))
    assert not (tmp_path / "state").exists()                 # refused before the state directory is touched


def test_the_first_api_passes_its_policy_to_the_resolver(monkeypatch):
    from api import main as api_main
    assert isinstance(api_main.WATCH_POLICY, WatchPolicy)
    reader = NoReads()
    monkeypatch.setattr(api_main, "kube_reader", reader)
    monkeypatch.setattr(api_main, "WATCH_POLICY", OTHER)
    with pytest.raises(ProvenanceRefused) as e:
        api_main._serve_forecast(ident.request(), 60)
    assert str(e.value) == AUTOSCALER_NAMESPACE_NOT_WATCHED and reader.paths == []


def test_the_first_api_refuses_to_start_on_a_malformed_list():
    env = {**os.environ, "WATCH_NAMESPACES": "shop,", "KUBECONFIG": "/dev/null"}
    r = subprocess.run([sys.executable, "-c", "import api.main"], env=env, capture_output=True, text=True,
                       cwd=os.path.join(os.path.dirname(__file__), ".."), timeout=300)
    assert r.returncode != 0 and "the namespace policy is refused" in r.stderr
