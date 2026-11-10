"""The statistical forecaster's autoscaler registrations are released (review R3-06): the limit is configurable and
refused with its own reason, and a slot is freed only on authoritative evidence that its autoscaler is gone (the live
lookup's definite 404) or replaced (another UID), or after the configured idle period. An API failure proves nothing
and frees nothing; a release frees the registration and the metric series, never the journal's durable evidence."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.identity import KubeReader, LookupFailed  # noqa: E402
from data.bounded_http import TransportFailure  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting import service as service_module  # noqa: E402
from forecasting.artifacts import Binding  # noqa: E402
from forecasting.service import ServiceConfig, ServiceError  # noqa: E402
from tests.test_statistical_service import DT, T0, body, make, pa_object  # noqa: E402

SWEEP_MS = service_module.SLOT_SWEEP_SECONDS * 1000


class Cluster:
    """A fake Kubernetes API for the autoscalers of namespace "shop": {name: uid} of the live ones (a missing name is
    a definite 404), `fail` {name: exception} for lookups that cannot be completed, and every lookup counted."""

    def __init__(self, live=None):
        self.live, self.fail, self.lookups = dict(live or {}), {}, []

    def get(self, path):
        if "predictiveautoscalers" not in path:
            return {"metadata": {"uid": "dep-uid"}}
        name = path.rsplit("/", 1)[1]
        self.lookups.append(name)
        if name in self.fail:
            raise self.fail[name]
        return pa_object(uid=self.live[name]) if name in self.live else None


def names(n):
    return {f"pa-{i}": f"uid-{i}" for i in range(n)}


def ask(svc, clock, origin, name, uid):
    clock.set(max(clock.now_ms(), origin * 1000 + 10_000))
    return svc.predict(dict(body(origin), autoscaler_name=name, autoscaler_uid=uid))


def refused(svc, clock, origin, name, uid):
    with pytest.raises(ServiceError) as e:
        ask(svc, clock, origin, name, uid)
    return e.value


def fill(tmp_path, n=8, **cfg):
    """A service whose n registrations are all taken by live autoscalers."""
    cluster = Cluster(names(n))
    svc, clock = make(tmp_path, reader=cluster, max_autoscalers=n, **cfg)
    for name, uid in cluster.live.items():
        ask(svc, clock, T0, name, uid)
    assert svc.metrics.registered() == sorted(("shop", name) for name in cluster.live)
    return svc, clock, cluster


def registered(svc):
    return {name for _ns, name in svc.metrics.registered()}


# --- the limit ----------------------------------------------------------------------------------------------------

def test_the_default_limit_is_eight_and_the_ninth_name_is_refused_with_its_own_reason(tmp_path):
    assert ServiceConfig("http://prom", str(tmp_path)).max_autoscalers == 8
    svc, clock, cluster = fill(tmp_path)
    cluster.live["pa-new"] = "uid-new"
    e = refused(svc, clock, T0, "pa-new", "uid-new")
    assert (e.status, e.reason) == (503, "AutoscalerLimitReached") and "8 autoscalers" in e.message
    assert registered(svc) == set(names(8))                     # every registered autoscaler is live: none released
    assert svc.metrics.refused == 1 and svc.metrics.released == {"not_found": 0, "replaced": 0, "idle": 0}


def test_the_limit_is_configurable(tmp_path):
    svc, clock, cluster = fill(tmp_path, n=3)
    cluster.live["pa-new"] = "uid-new"
    e = refused(svc, clock, T0, "pa-new", "uid-new")
    assert e.reason == "AutoscalerLimitReached" and "3 autoscalers" in e.message
    with pytest.raises(ValueError):
        make(tmp_path / "zero", max_autoscalers=0)
    with pytest.raises(ValueError):
        make(tmp_path / "negative", slot_idle_seconds=-1)


# --- authoritative release ----------------------------------------------------------------------------------------

def test_a_definite_not_found_releases_the_slot_and_a_new_name_is_admitted(tmp_path):
    svc, clock, cluster = fill(tmp_path)
    del cluster.live["pa-3"]                                    # deleted: its lookup now answers a definite 404
    cluster.live["pa-new"] = "uid-new"
    wire = ask(svc, clock, T0, "pa-new", "uid-new")
    assert c.response_from_wire(wire).autoscaler_name == "pa-new"
    assert registered(svc) == set(names(8)) - {"pa-3"} | {"pa-new"}
    assert svc.metrics.released == {"not_found": 1, "replaced": 0, "idle": 0} and svc.metrics.refused == 0


@pytest.mark.parametrize("failure", ["timeout", "http_500", "http_403", "http_401"])
def test_an_api_failure_never_releases_a_slot(tmp_path, failure):
    # The real reader over a fake transport: only its definite 404 is "gone"; a timeout, a 5xx and a refused read are
    # LookupFailed, which proves nothing about the autoscaler.
    (tmp_path / "sa").mkdir()
    (tmp_path / "sa" / "token").write_text("t")
    cluster, broken = Cluster(names(2)), set()

    def fetch(url, **_kw):
        path = url[len("https://kube"):]
        name = path.rsplit("/", 1)[1]
        if name in broken:
            if failure == "timeout":
                raise TransportFailure("deadline exceeded")
            return int(failure[-3:]), b'{"kind":"Status","status":"Failure"}'
        obj = cluster.get(path)
        if obj is None:
            return 404, b'{"kind":"Status","status":"Failure","reason":"NotFound","code":404}'
        return 200, json.dumps(obj).encode()

    svc, clock = make(tmp_path / "state", reader=KubeReader("https://kube", str(tmp_path / "sa"), fetch),
                      max_autoscalers=2)
    for name, uid in cluster.live.items():
        ask(svc, clock, T0, name, uid)
    cluster.live["pa-new"] = "uid-new"
    del cluster.live["pa-0"]                                    # really gone, but the API cannot say so
    broken.update({"pa-0", "pa-1"})
    e = refused(svc, clock, T0, "pa-new", "uid-new")
    assert (e.status, e.reason) == (503, "AutoscalerLimitReached")
    assert registered(svc) == {"pa-0", "pa-1"} and sum(svc.metrics.released.values()) == 0
    broken.clear()                                              # the API answers again: now the 404 is authoritative
    clock.advance(SWEEP_MS)
    ask(svc, clock, T0 + DT, "pa-new", "uid-new")
    assert registered(svc) == {"pa-1", "pa-new"} and svc.metrics.released["not_found"] == 1


def test_a_request_for_a_deleted_autoscaler_releases_its_own_slot(tmp_path):
    svc, clock, cluster = fill(tmp_path, n=2)
    del cluster.live["pa-1"]
    e = refused(svc, clock, T0 + DT, "pa-1", "uid-1")           # the operator's cache still had it
    assert (e.status, e.reason) == (422, "IdentityMismatch")
    assert registered(svc) == {"pa-0"} and svc.metrics.released["not_found"] == 1
    cluster.fail["pa-0"] = LookupFailed("api down")             # a failed lookup of the request's own autoscaler
    e = refused(svc, clock, T0 + DT, "pa-0", "uid-0")
    assert e.status == 503 and registered(svc) == {"pa-0"}


def test_a_full_registry_is_swept_at_most_once_per_interval(tmp_path):
    svc, clock, cluster = fill(tmp_path, n=2)
    cluster.live["pa-new"] = "uid-new"
    cluster.lookups.clear()
    assert refused(svc, clock, T0, "pa-new", "uid-new").reason == "AutoscalerLimitReached"
    assert cluster.lookups == ["pa-new", "pa-0", "pa-1"]        # its own lookup, then one per registered autoscaler
    cluster.lookups.clear()
    del cluster.live["pa-0"]
    clock.advance(SWEEP_MS - 1)
    assert refused(svc, clock, T0, "pa-new", "uid-new").reason == "AutoscalerLimitReached"
    assert cluster.lookups == ["pa-new"]                        # inside the interval: refused without another sweep
    clock.advance(1)
    ask(svc, clock, T0, "pa-new", "uid-new")
    assert registered(svc) == {"pa-1", "pa-new"}


def test_the_sweep_releases_a_replaced_autoscaler_by_its_uid(tmp_path):
    svc, clock, cluster = fill(tmp_path, n=2)
    cluster.live.update({"pa-0": "uid-0b", "pa-new": "uid-new"})     # pa-0 was deleted and created again
    ask(svc, clock, T0, "pa-new", "uid-new")
    assert registered(svc) == {"pa-1", "pa-new"} and svc.metrics.released["replaced"] == 1


# --- a re-created autoscaler --------------------------------------------------------------------------------------

def test_a_recreated_autoscaler_is_a_fresh_identity_with_none_of_the_old_evidence(tmp_path):
    cluster = Cluster({"web-pa": "uid-old"})
    svc, clock = make(tmp_path, reader=cluster, max_autoscalers=1)
    old = [c.response_from_wire(ask(svc, clock, T0 + i * DT, "web-pa", "uid-old")) for i in range(96)][-1]
    assert old.calibration_windows >= 16 and old.upper_window_max_rps is not None
    old_epoch = old.calibration_epoch
    cluster.live["web-pa"] = "uid-new"                          # the same name, another object
    origin = T0 + 96 * DT
    e = refused(svc, clock, origin, "web-pa", "uid-old")
    assert (e.status, e.reason) == (422, "IdentityMismatch")    # the old identity is no longer answered
    new = c.response_from_wire(ask(svc, clock, origin, "web-pa", "uid-new"))
    # Nothing of the old object is served: another epoch, no calibration scores, no bound, no evidence, no trust.
    assert new.autoscaler_uid == "uid-new" and new.calibration_epoch != old_epoch
    assert (new.calibration_windows, new.prospective_windows, new.upper_window_max_rps) == (0, 0, None)
    assert new.evidence_cutoff is None and new.gate_coverage_ratio is None and new.predictive_eligible is False
    assert new.trust_state != "Trusted"
    state = svc.store.latest_checkpoint(new.calibration_epoch).state
    assert all(m["calibration"]["scores"] == [] for m in state["models"].values())
    assert [i.key.decision_origin for i in svc.store.issuances(new.calibration_epoch)] == [origin]
    # The one slot now belongs to the new UID, and its metric series restarted with it.
    assert [(name, uid) for _ns, name, uid, _seen in svc.metrics.slots()] == [("web-pa", "uid-new")]
    assert svc.metrics.released == {"not_found": 0, "replaced": 1, "idle": 0}
    from prometheus_client import CollectorRegistry, generate_latest
    reg = CollectorRegistry()
    reg.register(svc.metrics)
    assert 'predictive_autoscaler_forecast_issued_total{autoscaler="web-pa",model="seasonal_naive",namespace="shop"}' \
           ' 1.0' in generate_latest(reg).decode()
    # The old object's durable evidence is still in the journal, under its own epoch.
    assert len(svc.store.issuances(old_epoch)) == 96 and svc.store.latest_checkpoint(old_epoch) is not None


def test_evidence_and_fit_artifacts_are_bound_to_the_autoscaler_uid():
    from tests.test_statistical_service import Reader

    def bound(uid):
        return c.bind_request(dict(body(T0), autoscaler_uid=uid), Reader(pa_object(uid=uid)), now_ms=T0 * 1000)

    a, b = bound("uid-old"), bound("uid-new")
    assert a.settings_sha256 == b.settings_sha256 and a.signal.sha256 == b.signal.sha256   # only the UID differs
    assert a.evidence_key != b.evidence_key
    assert Binding.for_signal(a.signal, a.settings).key() != Binding.for_signal(b.signal, b.settings).key()


# --- a release frees the registration, never the durable state ------------------------------------------------------

def test_a_released_slot_keeps_its_durable_state_and_resumes_exactly(tmp_path):
    control_cluster = Cluster({"web-pa": "uid-a"})
    control, cclock = make(tmp_path / "control", reader=control_cluster, store_id="journal-1")
    expected = [ask(control, cclock, T0 + i * DT, "web-pa", "uid-a") for i in range(60)]
    cluster = Cluster({"web-pa": "uid-a"})
    svc, clock = make(tmp_path / "released", reader=cluster, store_id="journal-1", max_autoscalers=1)
    got = [ask(svc, clock, T0 + i * DT, "web-pa", "uid-a") for i in range(30)]
    ek = c.response_from_wire(got[-1]).calibration_epoch
    before = (svc.store.issuances(ek), svc.store.outcomes_after(ek, 0), svc.store.latest_checkpoint(ek).state)
    # The API answers 404 for it once (and the request for it is refused), so its slot is released ...
    del cluster.live["web-pa"]
    assert refused(svc, clock, T0 + 29 * DT, "web-pa", "uid-a").status == 422
    assert svc.metrics.registered() == [] and svc.metrics.released["not_found"] == 1
    # ... and nothing durable went with it.
    assert (svc.store.issuances(ek), svc.store.outcomes_after(ek, 0), svc.store.latest_checkpoint(ek).state) == before
    # The same object (the same UID) is served again from exactly where it was: no evidence lost, none invented.
    cluster.live["web-pa"] = "uid-a"
    got += [ask(svc, clock, T0 + i * DT, "web-pa", "uid-a") for i in range(30, 60)]
    assert got == expected
    assert svc.store.latest_checkpoint(ek).state == control.store.latest_checkpoint(ek).state


# --- the idle lifecycle -------------------------------------------------------------------------------------------

def test_an_idle_slot_is_released_after_the_configured_period_without_any_lookup(tmp_path):
    cluster = Cluster({"pa-0": "uid-0", "pa-new": "uid-new"})
    svc, clock = make(tmp_path, reader=cluster, max_autoscalers=1, slot_idle_seconds=3600)
    ek = c.response_from_wire(ask(svc, clock, T0, "pa-0", "uid-0")).calibration_epoch
    cluster.fail["pa-0"] = LookupFailed("forbidden")            # e.g. its namespace is no longer readable: never a 404
    half = T0 + 6 * DT                                          # 30 minutes later: not idle long enough
    assert refused(svc, clock, half, "pa-new", "uid-new").reason == "AutoscalerLimitReached"
    assert registered(svc) == {"pa-0"}
    cluster.lookups.clear()
    late = T0 + 12 * DT                                         # one hour without a request for pa-0
    ask(svc, clock, late, "pa-new", "uid-new")
    assert registered(svc) == {"pa-new"} and svc.metrics.released == {"not_found": 0, "replaced": 0, "idle": 1}
    assert cluster.lookups == ["pa-new"]                        # released on the clock alone
    assert len(svc.store.issuances(ek)) == 1 and svc.store.latest_checkpoint(ek) is not None    # its journal stays


def test_the_default_idle_period_is_a_day_and_zero_turns_it_off(tmp_path):
    assert ServiceConfig("http://prom", str(tmp_path)).slot_idle_seconds == 86400
    cluster = Cluster({"pa-0": "uid-0", "pa-new": "uid-new"})
    svc, clock = make(tmp_path, reader=cluster, max_autoscalers=1, slot_idle_seconds=0)
    ask(svc, clock, T0, "pa-0", "uid-0")
    cluster.fail["pa-0"] = LookupFailed("api down")
    week = T0 + 7 * 86400
    assert refused(svc, clock, week, "pa-new", "uid-new").reason == "AutoscalerLimitReached"
    assert registered(svc) == {"pa-0"}


def test_a_request_keeps_its_own_slot_fresh(tmp_path):
    cluster = Cluster({"pa-0": "uid-0", "pa-1": "uid-1"})
    svc, clock = make(tmp_path, reader=cluster, max_autoscalers=2, slot_idle_seconds=3600)
    ask(svc, clock, T0, "pa-0", "uid-0")
    ask(svc, clock, T0, "pa-1", "uid-1")
    for i in range(1, 14):                                      # pa-0 keeps asking; pa-1 stops
        ask(svc, clock, T0 + i * DT, "pa-0", "uid-0")
    assert registered(svc) == {"pa-0"} and svc.metrics.released["idle"] == 1
