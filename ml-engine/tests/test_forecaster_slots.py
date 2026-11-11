"""The statistical forecaster's autoscaler registrations are released (review R3-06): the limit is configurable and
refused with its own reason, and a slot is freed only on authoritative evidence that its autoscaler is gone (the live
lookup's definite 404) or replaced (another UID), or after the configured idle period. An API failure proves nothing
and frees nothing; a release frees the registration and the metric series, never the journal's durable evidence."""

import json
import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.identity import API_GROUP, KubeReader, LookupFailed, definite_not_found  # noqa: E402
from data.bounded_http import TransportFailure  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting import service as service_module  # noqa: E402
from forecasting.artifacts import Binding  # noqa: E402
from forecasting.service import ServiceConfig, ServiceError  # noqa: E402
from tests.test_statistical_service import DT, T0, Backend, body, make, pa_object  # noqa: E402

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


def not_found(path, **details):
    """The API server's own 404 for the object `path` names (apierrors.NewNotFound), as bytes; `details` overrides."""
    parts = path.split("/")
    named = {"name": parts[-1], "group": parts[2], "kind": parts[-2]}
    return json.dumps({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
                       "message": f"{parts[-2]}.{parts[2]} \"{parts[-1]}\" not found", "reason": "NotFound",
                       "details": {**named, **details}, "code": 404}).encode()


# 404 answers that say nothing about the object: the path is not served (e.g. the CRD is not installed), a proxy's
# page, a cut-off body, and a NotFound about another object, resource or group.
INDEFINITE_404 = {
    "path_not_served_404": lambda path: json.dumps({
        "kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure", "reason": "NotFound",
        "message": "the server could not find the requested resource", "details": {}, "code": 404}).encode(),
    "proxy_page_404": lambda path: b"<html><body><h1>404 Not Found</h1></body></html>",
    "plain_text_404": lambda path: b"404 page not found\n",
    "cut_off_404": lambda path: not_found(path)[:-9],
    "no_details_404": lambda path: b'{"kind":"Status","status":"Failure","reason":"NotFound","code":404}',
    "other_name_404": lambda path: not_found(path, name="another"),
    "other_resource_404": lambda path: not_found(path, kind="deployments"),
    "other_group_404": lambda path: not_found(path, group="apps"),
}


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


@pytest.mark.parametrize("failure", ["timeout", "http_500", "http_403", "http_401", *INDEFINITE_404])
def test_an_api_failure_never_releases_a_slot(tmp_path, failure):
    # The real reader over a fake transport: only the API server's NotFound naming the object is "gone"; a timeout, a
    # 5xx, a refused read and any other 404 are LookupFailed, which proves nothing about the autoscaler.
    (tmp_path / "sa").mkdir()
    (tmp_path / "sa" / "token").write_text("t")
    cluster, broken = Cluster(names(2)), set()

    def fetch(url, **_kw):
        path = url[len("https://kube"):]
        name = path.rsplit("/", 1)[1]
        if name in broken:
            if failure == "timeout":
                raise TransportFailure("deadline exceeded")
            if failure in INDEFINITE_404:
                return 404, INDEFINITE_404[failure](path)
            return int(failure[-3:]), b'{"kind":"Status","status":"Failure"}'
        obj = cluster.get(path)
        if obj is None:
            return 404, not_found(path)
        return 200, json.dumps(obj).encode()

    svc, clock = make(tmp_path / "state", reader=KubeReader("https://kube", str(tmp_path / "sa"), fetch),
                      max_autoscalers=2)
    for name, uid in cluster.live.items():
        ask(svc, clock, T0, name, uid)
    cluster.live["pa-new"] = "uid-new"
    del cluster.live["pa-0"]                                    # really gone, but the API cannot say so
    broken.update({"pa-0", "pa-1"})
    e = refused(svc, clock, T0, "pa-new", "uid-new")            # the check of the full registry releases nothing
    assert (e.status, e.reason) == (503, "AutoscalerLimitReached")
    assert registered(svc) == {"pa-0", "pa-1"} and sum(svc.metrics.released.values()) == 0
    e = refused(svc, clock, T0, "pa-0", "uid-0")                # nor does the request for the autoscaler itself
    assert (e.status, e.reason) == (503, "IdentityMismatch")
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


# --- what counts as "not found" ------------------------------------------------------------------------------------

PA_PATH = f"/apis/{API_GROUP}/v1alpha1/namespaces/shop/predictiveautoscalers/web-pa"


def test_only_the_api_servers_not_found_for_that_object_is_definite():
    assert definite_not_found(PA_PATH, not_found(PA_PATH))
    dep = "/apis/apps/v1/namespaces/shop/deployments/web"
    assert definite_not_found(dep, not_found(dep))
    core = b'{"kind":"Status","status":"Failure","reason":"NotFound","details":{"name":"kube-system",' \
           b'"kind":"namespaces"},"code":404}'
    assert definite_not_found("/api/v1/namespaces/kube-system", core)        # the core group has no group detail
    assert not definite_not_found("/api/v1/namespaces/other", core)
    for make_body in INDEFINITE_404.values():
        assert not definite_not_found(PA_PATH, make_body(PA_PATH))
    for body_ in (b"", b"[]", b"null", not_found(PA_PATH).replace(b'"code": 404', b'"code": 410'),
                  not_found(PA_PATH).replace(b"NotFound", b"Gone"), not_found(PA_PATH).replace(b"Failure", b"Success")):
        assert not definite_not_found(PA_PATH, body_)
    # A path this module cannot name an object for (a list, a subresource) is never proven absent.
    assert not definite_not_found(PA_PATH + "/status", not_found(PA_PATH))
    assert not definite_not_found(PA_PATH.rsplit("/", 1)[0], not_found(PA_PATH))


@pytest.mark.parametrize("ca", ["not_a_certificate", "unreadable"])
def test_an_unusable_ca_file_is_a_failed_lookup_not_an_error(tmp_path, ca):
    (tmp_path / "token").write_text("t")
    if ca == "unreadable":
        (tmp_path / "ca.crt").mkdir()                           # open() fails whoever runs the test
    else:
        (tmp_path / "ca.crt").write_text("-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n")
    calls = []
    reader = KubeReader("https://kube", str(tmp_path), lambda *a, **k: calls.append(a) or (200, b"{}"))
    with pytest.raises(LookupFailed, match="CA unusable"):
        reader.get(PA_PATH)
    assert calls == []                                          # nothing was sent


# --- requests still in flight when their registration changes -----------------------------------------------------

def in_flight(tmp_path, cluster, name, uid):
    """A service with one registration and a request for (name, uid) held while it reads its history: (service, clock,
    resume, join) where resume() lets it finish and join() returns its answer or its ServiceError."""
    entered, go, result, backend = threading.Event(), threading.Event(), {}, Backend()

    def fetch(*a, **kw):
        if threading.current_thread().name == "held-request":
            entered.set()
            assert go.wait(10)
        return backend(*a, **kw)

    svc, clock = make(tmp_path, reader=cluster, backend=fetch, max_autoscalers=1)

    def held():
        try:
            result["answer"] = ask(svc, clock, T0, name, uid)
        except ServiceError as e:
            result["error"] = e

    thread = threading.Thread(target=held, name="held-request")
    thread.start()
    assert entered.wait(10)

    def join():
        go.set()
        thread.join(10)
        assert not thread.is_alive()
        return result

    return svc, clock, join


def test_an_old_request_never_overwrites_the_registration_that_replaced_it(tmp_path):
    cluster = Cluster({"web-pa": "old-uid"})
    svc, clock, join = in_flight(tmp_path, cluster, "web-pa", "old-uid")
    cluster.live["web-pa"] = "new-uid"                          # deleted and created again while the request waits
    ask(svc, clock, T0, "web-pa", "new-uid")
    before = svc.metrics.slots()
    assert [s[2] for s in before] == ["new-uid"] and svc.metrics.released["replaced"] == 1
    new_epoch = svc._uppers.get(("shop", "web-pa"), {}).get("epoch")
    result = join()                                             # the old request finishes last
    assert (result["error"].status, result["error"].reason) == (503, "IdentityMismatch")
    assert "in flight" in result["error"].message
    assert svc.metrics.slots() == before                        # still the new object's registration and series
    assert svc._uppers.get(("shop", "web-pa"), {}).get("epoch") == new_epoch
    ask(svc, clock, T0 + DT, "web-pa", "new-uid")               # and the new object simply carries on
    assert [s[2] for s in svc.metrics.slots()] == ["new-uid"]
    svc.close()


def test_a_stale_identity_lookup_never_replaces_the_newer_registration(tmp_path):
    # The old request's identity lookup succeeds (old UID); before it is admitted, the object is replaced and the new
    # one registers. The old request then meets a registration with another UID: it must not win it back on the
    # strength of a lookup that is older than the registration. The live object is read again and decides.
    cluster = Cluster({"web-pa": "old-uid"})
    svc, clock = make(tmp_path, reader=cluster, max_autoscalers=1)
    ask(svc, clock, T0, "web-pa", "old-uid")
    entered, go, result, real = threading.Event(), threading.Event(), {}, svc._admit

    def held_admit(bound, now_ms):
        if threading.current_thread().name == "held-request":
            entered.set()                                       # bound with the old UID, not admitted yet
            assert go.wait(10)
        return real(bound, now_ms)

    svc._admit = held_admit

    def held():
        try:
            result["answer"] = ask(svc, clock, T0 + DT, "web-pa", "old-uid")
        except ServiceError as e:
            result["error"] = e

    thread = threading.Thread(target=held, name="held-request")
    thread.start()
    assert entered.wait(10)
    cluster.live["web-pa"] = "new-uid"
    ask(svc, clock, T0 + DT, "web-pa", "new-uid")
    before = svc.metrics.slots()
    assert [s[2] for s in before] == ["new-uid"] and svc.metrics.released["replaced"] == 1
    go.set()
    thread.join(10)
    assert not thread.is_alive() and "answer" not in result
    assert (result["error"].status, result["error"].reason) == (422, "IdentityMismatch")
    assert svc.metrics.slots() == before and svc.metrics.released["replaced"] == 1     # untouched, its clock too
    # While the live object cannot be read, neither side is believed: the registration stays as it is.
    cluster.fail["web-pa"] = LookupFailed("deadline exceeded")
    svc._admit = real
    with pytest.raises(ServiceError) as e:
        svc._reserve("shop", "web-pa", "old-uid", clock.now_ms())
    assert (e.value.status, e.value.reason) == (503, "IdentityMismatch") and svc.metrics.slots() == before
    del cluster.fail["web-pa"]
    ask(svc, clock, T0 + 2 * DT, "web-pa", "new-uid")
    assert [s[2] for s in svc.metrics.slots()] == ["new-uid"]
    svc.close()


def test_an_old_request_never_brings_a_released_registration_back(tmp_path):
    cluster = Cluster({"pa-0": "uid-0"})
    svc, clock, join = in_flight(tmp_path, cluster, "pa-0", "uid-0")
    del cluster.live["pa-0"]                                    # deleted while its request waits ...
    cluster.live["pa-new"] = "uid-new"
    ask(svc, clock, T0, "pa-new", "uid-new")                    # ... and the full registry's check frees its slot
    assert registered(svc) == {"pa-new"} and svc.metrics.released["not_found"] == 1
    result = join()
    assert (result["error"].status, result["error"].reason) == (503, "IdentityMismatch")
    assert registered(svc) == {"pa-new"} and ("shop", "pa-0") not in svc._uppers and ("shop", "pa-0") not in svc._costs
    assert svc.metrics.counts()[0] == 1
    svc.close()


def test_a_publication_under_a_stale_token_is_dropped(tmp_path):
    svc, clock = make(tmp_path, reader=Cluster({"web-pa": "uid-1", "other": "uid-2"}), max_autoscalers=1)
    ask(svc, clock, T0, "web-pa", "uid-1")
    held = svc.metrics.reserve("shop", "web-pa", "uid-1", clock.now_ms())
    snap = svc.metrics._snaps[("shop", "web-pa")]
    assert svc.metrics.holds("shop", "web-pa", held.token) and svc.metrics.update("shop", "web-pa", snap, held.token)
    assert svc.metrics.release("shop", "web-pa", "not_found")
    assert not svc.metrics.holds("shop", "web-pa", held.token)
    assert svc.metrics.update("shop", "web-pa", snap, held.token) is False and svc.metrics.registered() == []
    again = svc.metrics.reserve("shop", "web-pa", "uid-1", clock.now_ms())     # the same object registers anew:
    assert again.token != held.token                                            # the old work stays shut out
    assert svc.metrics.update("shop", "web-pa", snap, held.token) is False
    svc.close()


# --- slow lookups and the check of a full registry ----------------------------------------------------------------

def test_slow_lookups_cannot_keep_the_check_from_a_later_registration(tmp_path, monkeypatch):
    svc, clock, cluster = fill(tmp_path, n=3)
    del cluster.live["pa-2"]                                    # gone; pa-0 and pa-1 answer only after 8 s, with nothing
    elapsed, real = [0.0], cluster.get

    def get(path):
        name = path.rsplit("/", 1)[1]
        if name in ("pa-0", "pa-1"):
            cluster.lookups.append(name)
            elapsed[0] += 8.0
            raise LookupFailed("deadline exceeded")
        return real(path)

    cluster.get = get
    cluster.lookups.clear()
    monkeypatch.setattr(service_module.time, "monotonic", lambda: elapsed[0])
    now = clock.now_ms()
    assert svc._sweep_slots(now + SWEEP_MS) is False
    assert cluster.lookups == ["pa-0", "pa-1"] and registered(svc) == {"pa-0", "pa-1", "pa-2"}   # out of time
    assert svc._sweep_slots(now + 2 * SWEEP_MS) is True         # continues after pa-1 instead of starting over
    assert cluster.lookups[2:] == ["pa-2", "pa-0", "pa-1"] and registered(svc) == {"pa-0", "pa-1"}   # then wraps
    assert svc.metrics.released["not_found"] == 1
    monkeypatch.undo()
    svc.close()
