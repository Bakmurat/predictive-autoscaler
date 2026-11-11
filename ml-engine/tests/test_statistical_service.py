"""C2.6: the statistical forecasting service end to end (offline: a fake Kubernetes reader, a fake Prometheus backend,
a manual clock): coherent shadow responses, idempotent retries, calibration from matured windows, and restarts that keep
verified state or start over, never manufacturing trust."""

import hashlib
import json
import math
import os
import shutil
import sys
import urllib.parse

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.identity import CONTRACT, LookupFailed  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting.clock import ManualClock  # noqa: E402
from forecasting.service import ForecastService, ServiceConfig, ServiceError  # noqa: E402

DAY, DT, LEAD = 86400, 300, 1200
QUERY = 'sum(rate(istio_requests_total{destination_workload="web",destination_workload_namespace="shop"}[5m]))'
SHA = hashlib.sha256(QUERY.encode()).hexdigest()
T0 = 1_793_923_200 - 1_793_923_200 % DAY + 3 * DAY       # three days of history before the first request
PREDICTION = {"horizonMinutes": 60, "sampleIntervalSeconds": DT, "historyDays": 2,
              "calibration": {"minWindows": 16, "maxScores": 64, "minMiscoverageBps": 600}}  # 16: rank needs a >= 1/17


def pa_object(prediction=None, lead=True, uid="pa-uid"):
    pa = {"metadata": {"uid": uid, "generation": 3},
          "spec": {"targetDeployment": {"name": "web", "namespace": "shop"}, "prediction": prediction or PREDICTION},
          "status": {"metricSource": {"query": QUERY, "sha256": SHA, "observedGeneration": 3, "targetUID": "dep-uid",
                                      "contract": CONTRACT}}}
    if lead:
        pa["status"]["prediction"] = {"lead": {"seconds": LEAD, "startupDelaySeconds": 420,
                                               "actuationAllowanceSeconds": 30, "method": "configured",
                                               "sampleCount": 0, "observedGeneration": 3}}
    return pa


class Reader:
    def __init__(self, pa=None, fail=None):
        self.pa, self.fail = pa or pa_object(), fail

    def get(self, path):
        if self.fail:
            raise self.fail
        return self.pa if "predictiveautoscalers" in path else {"metadata": {"uid": "dep-uid"}}


def rps(ts):
    return round(20 + 10 * math.sin(2 * math.pi * (ts % DAY) / DAY) + 2 * math.sin(ts / 7000.0), 6)


class Backend:
    """A Prometheus query_range answering from rps(); `down` simulates an outage; `gap_after` stops data."""

    def __init__(self, value=None, on_fetch=None):
        self.down, self.gap_after, self.calls, self.value, self.on_fetch = False, None, 0, value or rps, on_fetch
        self.missing = set()                     # timestamps not (yet) delivered: a late receipt once removed

    def __call__(self, url, **kw):
        self.calls += 1
        if self.on_fetch:
            self.on_fetch()
        if self.down:
            from data.bounded_http import TransportFailure
            raise TransportFailure("connection refused")
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        assert q["query"] == QUERY
        step = int(q["step"].rstrip("s"))
        vals = [[t, str(self.value(t))] for t in range(int(q["start"]), int(q["end"]) + 1, step)
                if (self.gap_after is None or t <= self.gap_after) and t not in self.missing]
        res = [{"metric": {}, "values": vals}] if vals else []
        return 200, json.dumps({"status": "success", "data": {"resultType": "matrix", "result": res}}).encode()


def body(origin, settings=None):
    settings = settings or c.resolve_settings(pa_object()).fingerprint()
    return {"autoscaler_namespace": "shop", "autoscaler_name": "web-pa", "autoscaler_uid": "pa-uid",
            "autoscaler_generation": 3, "target_uid": "dep-uid", "metric_query_sha256": SHA, "contract": CONTRACT,
            "application": "web", "namespace": "shop", "forecast_contract": c.FORECAST_CONTRACT,
            "decision_origin": c.format_ts(origin), "settings_sha256": settings}


def make(tmp_path, *, reader=None, clock=None, backend=None, fit_mode="off", **cfg):
    clock = clock or ManualClock(T0 * 1000)
    svc = ForecastService(ServiceConfig(prometheus_url="http://prom", state_dir=str(tmp_path), fit_mode=fit_mode, **cfg),
                          reader=reader or Reader(), fetch=backend or Backend(), clock=clock)
    return svc, clock


def ask(svc, clock, origin):
    clock.set(max(clock.now_ms(), origin * 1000 + 10_000))
    return svc.predict(body(origin))


def run(svc, clock, n, start=T0):
    return [ask(svc, clock, start + i * DT) for i in range(n)]


# --- one request ------------------------------------------------------------------------------------------------

def test_a_request_gets_a_coherent_shadow_forecast(tmp_path):
    svc, clock = make(tmp_path)
    wire = ask(svc, clock, T0)
    r = c.response_from_wire(wire)                     # strict decoding and coherence
    assert r.model_family == "seasonal_naive" and r.unit == "requests/second"
    assert r.target_timestamps == tuple(T0 + k * DT for k in range(1, 5)) and r.window_end == T0 + LEAD
    assert r.point_rps == tuple(rps(t - DAY) for t in r.target_timestamps)
    assert r.upper_window_max_rps is None and r.calibration_windows == 0      # cold start: no bound yet
    assert r.predictive_eligible is False and r.trust_state in ("Calibrating", "Collecting", "Shadow")
    assert r.input_end == T0 - DT                       # the settlement watermark at origin + 10 s
    assert r.calibration_epoch == c.bind_request(body(T0), Reader(), now_ms=T0 * 1000 + 10_000).evidence_key


def test_a_retry_returns_the_same_issuance(tmp_path):
    svc, clock = make(tmp_path)
    first = ask(svc, clock, T0)
    clock.advance(30_000)
    again = svc.predict(body(T0))
    assert again == first and len(svc.store.issuances()) == 1


def test_refusals_and_unavailability(tmp_path):
    svc, clock = make(tmp_path)
    clock.set(T0 * 1000 + 10_000)
    for bad, status in ((dict(body(T0), autoscaler_uid="other"), 422), (dict(body(T0), history=[]), 422),
                        (body(T0 - DT), 422)):                # another PA, caller history, a stale origin
        with pytest.raises(ServiceError) as e:
            svc.predict(bad)
        assert e.value.status == status
    down, _ = make(tmp_path / "d", reader=Reader(fail=LookupFailed("api down")), clock=clock)
    with pytest.raises(ServiceError) as e:
        down.predict(body(T0))
    assert e.value.status == 503
    nolead, _ = make(tmp_path / "l", reader=Reader(pa_object(lead=False)), clock=clock)
    with pytest.raises(ServiceError) as e:
        nolead.predict(body(T0))
    assert (e.value.status, e.value.reason) == (503, "LeadInsufficient")
    backend = Backend()
    outage, _ = make(tmp_path / "o", backend=backend, clock=clock)
    backend.down = True
    with pytest.raises(ServiceError) as e:
        outage.predict(body(T0))
    assert (e.value.status, e.value.reason) == (503, "DataUnavailable")
    backend.down, backend.gap_after = False, T0 - 3600              # the newest data is an hour old
    with pytest.raises(ServiceError) as e:
        outage.predict(body(T0))
    assert (e.value.status, e.value.reason) == (503, "DataUnavailable")
    assert outage.store.issuances() == []


def test_the_registration_limit_answers_503(tmp_path):
    class TwoPAs(Reader):                                  # both autoscalers are live (tests/test_forecaster_slots.py
        def get(self, path):                               # covers the releases)
            if path.endswith("/predictiveautoscalers/api-pa"):
                return pa_object(uid="pa-2")
            return super().get(path)

    svc, clock = make(tmp_path, reader=TwoPAs(), max_autoscalers=1)
    ask(svc, clock, T0)
    b = dict(body(T0 + DT), autoscaler_name="api-pa", autoscaler_uid="pa-2")
    clock.set((T0 + DT) * 1000 + 10_000)
    with pytest.raises(ServiceError) as e:
        svc.predict(b)
    assert (e.value.status, e.value.reason) == (503, "AutoscalerLimitReached")
    assert svc.metrics.registered() == [("shop", "web-pa")]


# --- evidence over time -------------------------------------------------------------------------------------------

def test_matured_windows_calibrate_the_bound_and_feed_the_metrics(tmp_path):
    svc, clock = make(tmp_path)
    out = [c.response_from_wire(w) for w in run(svc, clock, 96)]          # 8 hours
    assert out[0].upper_window_max_rps is None
    bounded = [r for r in out if r.upper_window_max_rps is not None]
    assert bounded and bounded[0].calibration_windows >= 16                # 16 cohort windows, then a bound
    assert all(r.predictive_eligible is False for r in out)               # release gates not passed
    ek = out[-1].calibration_epoch
    assert len(svc.store.outcomes_after(ek, 0)) > 80                      # windows matured as time went on
    from prometheus_client import CollectorRegistry, generate_latest
    reg = CollectorRegistry()
    reg.register(svc.metrics)
    text = generate_latest(reg).decode()
    assert 'predictive_autoscaler_forecast_issued_total{autoscaler="web-pa",model="seasonal_naive",namespace="shop"}' \
           ' 96.0' in text
    assert 'predictive_autoscaler_forecast_calibration_windows{autoscaler="web-pa",model="seasonal_naive"' in text


# --- restarts: keep verified state, or start over -----------------------------------------------------------------

def test_a_restart_continues_exactly_where_it_stopped(tmp_path):
    whole, wclock = make(tmp_path / "whole", store_id="journal-1")
    expected = run(whole, wclock, 60)
    first, clock = make(tmp_path / "split", store_id="journal-1")
    got = run(first, clock, 30)
    first.close()
    second, _ = make(tmp_path / "split", clock=clock, store_id="journal-1")   # the same volume after a restart
    got += run(second, clock, 30, start=T0 + 30 * DT)
    assert [json.dumps(w, sort_keys=True) for w in got] == [json.dumps(w, sort_keys=True) for w in expected]
    ek = c.response_from_wire(got[-1]).calibration_epoch
    assert second.store.latest_checkpoint(ek).state == whole.store.latest_checkpoint(ek).state


def test_a_corrupt_journal_answers_503_and_issues_nothing(tmp_path):
    svc, clock = make(tmp_path)
    run(svc, clock, 5)
    svc.close()
    path = tmp_path / "journal.sqlite"
    raw = bytearray(path.read_bytes())
    raw[0:16] = b"\0" * 16
    path.write_bytes(bytes(raw))
    broken, _ = make(tmp_path, clock=clock)
    assert broken.health()["state"] == "unavailable" and broken.store is None
    with pytest.raises(ServiceError) as e:
        ask(broken, clock, T0 + 5 * DT)
    assert (e.value.status, e.value.reason) == (503, "StateUnavailable")


def test_a_lost_journal_starts_over_and_manufactures_no_trust(tmp_path):
    svc, clock = make(tmp_path)
    before = c.response_from_wire(run(svc, clock, 96)[-1])
    assert before.calibration_windows >= 16
    svc.close()
    for name in os.listdir(tmp_path):                                    # the volume was lost
        p = tmp_path / name
        shutil.rmtree(p) if p.is_dir() else p.unlink()
    fresh, _ = make(tmp_path, clock=clock)
    after = c.response_from_wire(ask(fresh, clock, T0 + 96 * DT))
    assert after.calibration_windows == 0 and after.upper_window_max_rps is None
    assert after.prospective_windows == 0 and after.trust_state != "Trusted" and after.predictive_eligible is False
    assert after.calibration_epoch == before.calibration_epoch           # same identity, evidence rebuilt from zero


# --- the statistical family ------------------------------------------------------------------------------------

def test_a_fresh_fit_adds_the_statistical_candidate(tmp_path):
    # (A noise-free smooth series is refused by the AR stability check, near a unit root; real traffic is noisy.)
    pred = dict(PREDICTION, historyDays=8)
    reader = Reader(pa_object(prediction=pred))
    clock = ManualClock((T0 + 6 * DAY) * 1000)
    noisy = lambda t: round(rps(t) + ((t * 2654435761) % 997) / 997 - 0.5, 6)   # deterministic noise  # noqa: E731
    svc = ForecastService(ServiceConfig(prometheus_url="http://prom", state_dir=str(tmp_path), fit_mode="inline"),
                          reader=reader, fetch=Backend(noisy), clock=clock)
    origin = T0 + 6 * DAY
    clock.set(origin * 1000 + 10_000)
    settings = c.resolve_settings(pa_object(prediction=pred)).fingerprint()
    wire = svc.predict(body(origin, settings))
    [iss] = svc.store.issuances()
    assert iss.models == ("seasonal_naive", "statistical")
    stat = iss.payload()["candidates"]["statistical"]
    assert stat["recipe"] == "e1-product/v1" and stat["artifact_sha256"] and stat["fit_cutoff"] == origin - DT
    assert c.response_from_wire(wire).model_family == "seasonal_naive"     # nothing trusted: naive reported


def test_retention_keeps_the_journal_bounded_without_losing_evidence(tmp_path):
    svc, clock = make(tmp_path, retention_seconds=2 * 3600, compact_every_seconds=1800)
    out = [c.response_from_wire(w) for w in run(svc, clock, 72)]          # 6 hours with a 2-hour retention
    ek = out[-1].calibration_epoch
    origins = [i.key.decision_origin for i in svc.store.issuances(ek)]
    assert len(origins) < 40 and max(origins) == T0 + 71 * DT               # old windows retired, the new ones kept
    summary = svc.store.retention_summary(ek)
    assert summary["issued"] + len(origins) == 72                          # every issuance counted, kept or retired
    assert out[-1].calibration_windows == max(r.calibration_windows for r in out)   # calibration lost nothing


# --- Codex review on #25: timing, durable trust, one artifact read, atomic registration, readiness, maximum state ---

def test_a_late_issuance_is_refused_never_backdated(tmp_path):
    # Codex repro: a backend delayed 310 s still committed a forecast after its first target, stamped with the start time.
    clock = ManualClock(T0 * 1000)
    svc, _ = make(tmp_path, clock=clock, backend=Backend(on_fetch=lambda: clock.advance(310_000)))
    with pytest.raises(ServiceError) as e:
        ask(svc, clock, T0)
    assert (e.value.status, e.value.reason) == (503, "LeadInsufficient") and svc.store.issuances() == []
    clock2 = ManualClock(T0 * 1000)                       # 100 s late but still before first target - A: issued
    slow, _ = make(tmp_path / "slow", clock=clock2, backend=Backend(on_fetch=lambda: clock2.advance(100_000)))
    r = c.response_from_wire(ask(slow, clock2, T0))
    assert r.issued_at_ms == (T0 + 110) * 1000 and r.input_received_through_ms == (T0 + 110) * 1000   # actual times


def test_a_retry_is_identical_even_after_another_window_matured(tmp_path):
    svc, clock = make(tmp_path)
    run(svc, clock, 12)
    origin = T0 + 12 * DT
    first = ask(svc, clock, origin)                        # at origin + 10 s
    ek = c.response_from_wire(first).calibration_epoch
    matured = len(svc.store.outcomes_after(ek, 0))
    clock.set((origin + 250) * 1000)                       # a window closes at origin + 120 s in between
    again = svc.predict(body(origin))
    assert len(svc.store.outcomes_after(ek, 0)) > matured and again == first


class Crash(Exception):
    pass


@pytest.mark.parametrize("boundary", ["before_issuance", "after_issuance"])
def test_a_crash_at_either_boundary_recovers_exactly(tmp_path, monkeypatch, boundary):
    whole, wclock = make(tmp_path / "whole", store_id="journal-1")
    expected = run(whole, wclock, 40)
    svc, clock = make(tmp_path / "crash", store_id="journal-1")
    got = run(svc, clock, 20)
    if boundary == "before_issuance":                      # the trust checkpoint is written, the issuance is not
        monkeypatch.setattr(svc.store, "get_or_issue", lambda *a, **k: (_ for _ in ()).throw(Crash()))
    else:                                                  # the issuance is committed, the final checkpoint is not
        calls, real = [], svc.store.write_checkpoint

        def flaky(*a, **k):
            calls.append(1)
            if len(calls) == 2:
                raise Crash()
            return real(*a, **k)
        monkeypatch.setattr(svc.store, "write_checkpoint", flaky)
    with pytest.raises(Crash):
        ask(svc, clock, T0 + 20 * DT)
    ek = c.response_from_wire(got[-1]).calibration_epoch
    trust = svc.store.latest_checkpoint(ek).state["trust"]
    assert trust and set(trust) >= {"seasonal_naive"}       # the trust transition was durable before the issuance
    restarted, _ = make(tmp_path / "crash", clock=clock, store_id="journal-1")     # a new process on the same volume
    got += run(restarted, clock, 20, start=T0 + 20 * DT)    # the crashed origin is retried first
    assert got == expected
    assert restarted.store.latest_checkpoint(ek).state == whole.store.latest_checkpoint(ek).state


def test_concurrent_new_autoscalers_cannot_exceed_the_limit(tmp_path):
    # Codex repro: with max_autoscalers=1, two new autoscalers both committed issuances.
    import threading
    barrier, together = threading.Barrier(2, timeout=10), set()

    class TwoPAs:
        def get(self, path):
            name = path.rsplit("/", 1)[1]
            if "predictiveautoscalers" in path:
                if name not in together:                   # (the refused one's check of the full registry reads the
                    together.add(name)                     # admitted autoscaler again, alone)
                    barrier.wait()                         # both requests pass identity together
                return pa_object(uid="pa-1" if name == "web-pa" else "pa-2")
            return {"metadata": {"uid": "dep-uid"}}

    svc, clock = make(tmp_path, reader=TwoPAs(), max_autoscalers=1)
    clock.set(T0 * 1000 + 10_000)
    results = {}

    def go(name, uid):
        try:
            results[name] = svc.predict(dict(body(T0), autoscaler_name=name, autoscaler_uid=uid))
        except ServiceError as e:
            results[name] = e.status
    threads = [threading.Thread(target=go, args=a) for a in (("web-pa", "pa-1"), ("api-pa", "pa-2"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert sorted(isinstance(v, dict) for v in results.values()) == [False, True]
    assert 503 in results.values() and len(svc.store.issuances()) == 1 and len(svc.metrics.registered()) == 1


def test_the_forecast_and_its_digest_come_from_one_artifact_read(tmp_path, monkeypatch):
    from forecasting.artifacts import ArtifactStore
    monkeypatch.setattr(ArtifactStore, "load", lambda *a, **k: (_ for _ in ()).throw(AssertionError("split read")))
    pred = dict(PREDICTION, historyDays=8)
    clock = ManualClock((T0 + 6 * DAY) * 1000)
    noisy = lambda t: round(rps(t) + ((t * 2654435761) % 997) / 997 - 0.5, 6)   # noqa: E731
    svc = ForecastService(ServiceConfig(prometheus_url="http://prom", state_dir=str(tmp_path), fit_mode="inline"),
                          reader=Reader(pa_object(prediction=pred)), fetch=Backend(noisy), clock=clock)
    origin = T0 + 6 * DAY
    clock.set(origin * 1000 + 10_000)
    request = body(origin, c.resolve_settings(pa_object(prediction=pred)).fingerprint())
    svc.predict(request)
    stat = svc.store.issuances()[0].payload()["candidates"]["statistical"]
    from forecasting.artifacts import Binding
    bound = c.bind_request(request, Reader(pa_object(prediction=pred)), now_ms=clock.now_ms())
    sha, cutoff, data = svc.artifacts.current(Binding.for_signal(bound.signal, bound.settings, "e1-product/v1"))
    assert stat["artifact_sha256"] == sha == hashlib.sha256(data).hexdigest() and stat["fit_cutoff"] == cutoff


def test_readiness_follows_the_journal(tmp_path):
    svc, clock = make(tmp_path)
    assert svc.ready()
    ask(svc, clock, T0)
    svc.store._fail("disk full")
    assert not svc.ready() and svc.health()["state"] == "unavailable"


WORST_FLOAT = -1.2345678901234567e-05          # the longest repr a finite float takes


def max_state(models):
    """The largest epoch state the supported settings allow: a 1-minute cadence; per model a full 7-day hourly
    ledger with every counter large and every sum a worst-case float, a full day of pending issuances (older ones are
    dropped), 4096 calibration scores (calibration.maxScores' maximum) and the acute ring; paired evidence over 7 days
    with a full day of unpaired outcomes; hysteresis and selection state."""
    from forecasting import trust as T
    from forecasting.calibration import CalibrationPolicy, initial
    ek, now = "e" * 64, T0 + 7 * DAY
    out = {}
    for m in models:
        led = T.new_ledger(ek, m, f"{m}/v1", 60)
        for h in range(170):
            b = T._bucket(led, now - h * 3600)
            for f in b:
                b[f] = WORST_FLOAT if isinstance(b[f], float) else 10 ** 6
        led["pending"] = {str(now - i * 60): now - i * 60 + 1800 for i in range(1441)}
        led["recent"] = [1] * 4096
        led["totals"] = {k: 10 ** 9 for k in led["totals"]}
        cal = initial(CalibrationPolicy(3600, max_scores=4096, min_windows=128), ek, m, f"{m}/v1")
        cal["scores"] = [[now - i * 60, WORST_FLOAT] for i in range(4096)]
        out[m] = {"recipe": f"{m}/v1", "calibration": cal, "ledger": led}
    pairs = T.new_pairs(ek, "statistical", "statistical/v1", "seasonal_naive/v1")
    pairs["buckets"] = {str(now // 3600 - h): {"n": 10 ** 6, "chal": WORST_FLOAT, "base": WORST_FLOAT, "first": now,
                                                "last": now} for h in range(170)}
    pairs["pending"] = {str(now - i * 60): ["seasonal_naive", WORST_FLOAT, f"iss-{i:032x}"] for i in range(1441)}
    trust = {m: T.new_hysteresis(f"{m}/v1", ek) for m in models}
    trust[T.SELECTION] = {**T.new_selection(ek), "entry_since": {f"{m}:{m}/v1": now for m in models}}
    return {"schema": "statistical-service-state/v1", "epoch": ek, "last_outcome_seq": 10 ** 12,
            "last_issuance_seq": 10 ** 12, "models": out, "pairs": {"statistical": pairs}, "trust": trust}


def test_the_maximum_supported_state_fits_one_checkpoint(tmp_path):
    from forecasting.service import RECIPES
    from forecasting.store import MAX_CHECKPOINT_BYTES, ForecastStore
    state = max_state(tuple(RECIPES))                      # every family the service forecasts
    size = len(c.canonical_json(state))
    assert size < MAX_CHECKPOINT_BYTES, size
    three = len(c.canonical_json(max_state(("seasonal_naive", "statistical", "lstm"))))
    assert three < MAX_CHECKPOINT_BYTES, three             # also the trust layer's three families (an LSTM later)
    store = ForecastStore(str(tmp_path / "j.sqlite"), clock=ManualClock((T0 + 7 * DAY) * 1000))
    assert store.write_checkpoint("e" * 64, state, 10 ** 12).state == state


def test_an_oversize_state_is_a_clear_503_never_an_unhandled_error(tmp_path, monkeypatch):
    import forecasting.store as store_module
    svc, clock = make(tmp_path)
    ask(svc, clock, T0)
    monkeypatch.setattr(store_module, "MAX_CHECKPOINT_BYTES", 200)             # the state no longer fits
    with pytest.raises(ServiceError) as e:
        ask(svc, clock, T0 + DT)
    assert (e.value.status, e.value.reason) == (503, "StateUnavailable") and "cannot be journaled" in e.value.message
    assert len(svc.store.issuances()) == 1                                    # nothing issued without its state


def test_a_one_minute_sixty_step_bundle_fits(tmp_path):
    pred = {"horizonMinutes": 60, "sampleIntervalSeconds": 60, "historyDays": 2,
            "calibration": {"minWindows": 16, "maxScores": 64, "minMiscoverageBps": 600}}
    pa = pa_object(prediction=pred)
    pa["status"]["prediction"]["lead"]["seconds"] = 3600
    svc, clock = make(tmp_path, reader=Reader(pa))
    clock.set(T0 * 1000 + 10_000)
    r = c.response_from_wire(svc.predict(body(T0, c.resolve_settings(pa).fingerprint())))
    assert len(r.point_rps) == 60 and len(svc.store.issuances()[0].bundle) < 8 << 10


# --- C2.8a: same-policy costs, families and the lab qualification ---------------------------------------------------

@pytest.fixture(autouse=True)
def validated_configuration(request, monkeypatch):
    """These tests drive the service with short test settings. Whether a configuration is on the allowlist of
    validated profiles is a separate gate (contracts.validated_profile; tests/test_trust_monitor.py): here every test
    runs as a validated configuration, except the one that tests the refusal."""
    if not request.node.name.startswith("test_an_unvalidated_configuration"):
        import forecasting.service as service_module
        monkeypatch.setattr(service_module, "validated_profile", lambda s, lead: c.VALIDATED_PROFILES[0])


FAST_TRUST = {"minShadowHours": 1, "minProspectiveWindows": 4, "acuteRecentWindows": 4, "worstDayMinOutcomes": 1, "repromotionWindows": 4,
              "challengerMinPairs": 4, "challengerMinHours": 1, "repromotionHours": 0, "challengerLossRatioBps": 10000}
NOISY = lambda t: round(rps(t) + ((t * 2654435761) % 997) / 997 - 0.5, 6)   # noqa: E731  (E1 refuses a smooth AR)


def policy_pa(capacity="5", **pred):
    pa = pa_object(prediction=dict(PREDICTION, historyDays=8, trust=FAST_TRUST, **pred))
    if capacity is not None:
        pa["spec"].update(minReplicas=1, maxReplicas=12, metrics={"requests": {"capacityPerReplica": capacity}})
    return pa


def drive(tmp_path, pa, n, *, fit_mode="inline", reader=None, until=None, backend=None, **cfg):
    """n requests on the 5-minute grid from six days into the history; stops early once until(bundle) holds."""
    start = T0 + 6 * DAY
    clock = ManualClock(start * 1000)
    svc = ForecastService(ServiceConfig(prometheus_url="http://prom", state_dir=str(tmp_path), fit_mode=fit_mode,
                                        store_id="journal-1", **cfg),
                          reader=reader or Reader(pa), fetch=backend or Backend(NOISY), clock=clock)
    settings, out = c.resolve_settings(pa).fingerprint(), []
    for i in range(n):
        o = start + i * DT
        clock.set(max(clock.now_ms(), o * 1000 + 10_000))
        wire = svc.predict(body(o, settings))
        out.append((wire, svc.store.issuances()[-1].payload()))
        if until is not None and until(out[-1][1]):
            break
    return svc, out


def test_the_challenger_is_selected_only_on_bound_same_policy_costs(tmp_path, monkeypatch):
    from forecasting import policy_sim as P
    from forecasting import trust as T
    pa = policy_pa()
    svc, out = drive(tmp_path / "real", pa, 130)
    bundles = [b for _, b in out]
    assert not any(b["selected"] == "statistical" for b in bundles)
    last = bundles[-1]
    assert last["selected"] == "seasonal_naive" and {v["phase"] for v in last["verdict"]["candidates"].values()} == \
        {"Trusted"}                                                    # both trusted: only the costs keep E1 out
    o = last["decision_origin"]
    bound = c.bind_request(body(o, c.resolve_settings(pa).fingerprint()), Reader(pa), now_ms=o * 1000 + 10_000)
    params, _ = P.resolve_policy(bound.spec, bound.settings, bound.lead)
    assert last["policy_sha256"] == params.fingerprint()
    cost = last["costs"]["statistical"]
    assert cost["paired_decisions"] > 0 and cost["unknown_truth_seconds"] == 0
    assert cost["replica_time"][0] > 1.02 * cost["replica_time"][1]   # E1 buys its bound with replica-time
    _key, full = svc._costs[("shop", "web-pa")]
    # The controller saw, at each decision, the newest observation its issuance was computed from; the truth only
    # scores. Recompute the replay from the journal with those inputs: the stored costs are exactly that.
    obs = {ts: v for ts, v, _ in svc.store.observations("signal:" + last["evidence_key"], 0, 10 ** 10)}
    by_origin = {i.key.decision_origin: i.payload() for i in svc.store.issuances(last["evidence_key"])}
    lo, hi = (w - (last["window_end"] - last["decision_origin"]) for w in full["statistical"]["binding"]["window"])
    controller = {o: obs[b["input_end"]] for o, b in by_origin.items() if lo <= o <= hi}
    assert all(b["input_end"] < o for o, b in by_origin.items())                 # never the truth at o itself
    recomputed = P.paired_costs(
        epoch=last["evidence_key"], challenger=("statistical", "e1-product/v1"),
        baseline=("seasonal_naive", by_origin[lo]["candidates"]["seasonal_naive"]["recipe"]), params=params,
        truth_rps={t: v for t, v in obs.items() if t >= lo}, controller_rps=controller,
        challenger_upper={o: b["candidates"]["statistical"]["upper_window_max_rps"] for o, b in by_origin.items()
                          if "statistical" in b["candidates"] and
                          b["candidates"]["statistical"]["upper_window_max_rps"] is not None},
        baseline_upper={o: b["candidates"]["seasonal_naive"]["upper_window_max_rps"] for o, b in by_origin.items()
                        if b["candidates"]["seasonal_naive"]["upper_window_max_rps"] is not None},
        start=lo, end=hi, interval=DT, window_offset=last["window_end"] - last["decision_origin"])
    assert recomputed == full["statistical"]
    state = svc.store.latest_checkpoint(last["evidence_key"]).state
    pairs = state["pairs"]["statistical"]
    now_s = last["decision_origin"]
    assert T.costs_bound(full["statistical"], pairs, T.pair_sums(pairs, now_s), last["policy_sha256"])
    assert full["statistical"]["binding"]["window"] == cost["window"]

    real = P.paired_costs                                              # the same evidence, costs that pass

    def passing(**kw):
        r = real(**kw)
        return dict(r, shortage=[0.0, r["shortage"][1]], replica_time=[r["replica_time"][1]] * 2)
    monkeypatch.setattr(P, "paired_costs", passing)
    _svc, out = drive(tmp_path / "passing", pa, 130, until=lambda b: b["selected"] == "statistical")
    b = out[-1][1]
    assert b["selected"] == "statistical" and b["verdict"]["reason"] == "ReplayGateMissing"   # shadow: never eligible
    assert c.response_from_wire(out[-1][0]).predictive_eligible is False


def test_without_a_known_policy_the_challenger_is_never_promoted(tmp_path, monkeypatch):
    from forecasting import policy_sim as P
    monkeypatch.setattr(P, "paired_costs", lambda **kw: pytest.fail("no policy: costs are never simulated"))
    _svc, out = drive(tmp_path, policy_pa(capacity=None), 110)
    assert all(b["policy_sha256"] is None and b["costs"] == {} and b["selected"] != "statistical" for _, b in out)


def test_a_naive_only_autoscaler_never_forecasts_or_fits_e1(tmp_path):
    pa = policy_pa(families=["seasonalNaive"])
    svc, out = drive(tmp_path, pa, 3)
    assert all(iss.models == ("seasonal_naive",) for iss in svc.store.issuances())
    assert all(set(b["candidates"]) == {"seasonal_naive"} and b["costs"] == {} for _, b in out)
    art = tmp_path / "artifacts"
    assert not art.exists() or not any(p.is_file() for p in art.rglob("*"))   # no fit was even attempted


TREE = "a" * 40
DIGEST = "sha256:" + "d" * 64


def lab(target_uid="dep-uid", name="web", expires=T0 + 30 * DAY):
    from datetime import datetime, timezone
    from forecasting import qualification as Q
    ts = lambda s: datetime.fromtimestamp(s, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")   # noqa: E731
    doc = {"schema": "lab-qualification/v1", "source_tree": TREE, "image_digest": DIGEST, "run_lock_sha256": "b" * 64,
           "gates": {g: "pass" for g in ("G0", "G1", "G2", "G3", "G4")}, "reviewers": ["executor", "reviewer"],
           "not_before": ts(T0 - DAY), "expires": ts(expires),
           "scope": {"cluster_uid": "cluster-uid", "namespaces": ["shop"],
                     "targets": [{"namespace": "shop", "name": name, "uid": target_uid}]}}
    return Q.parse(json.dumps(doc).encode(), Q.BuildInfo(TREE, True), T0, DIGEST)


class ClusterReader(Reader):
    def __init__(self, pa, cluster="cluster-uid"):
        super().__init__(pa)
        self.cluster = cluster

    def get(self, path):
        if path == "/api/v1/namespaces/kube-system":
            if isinstance(self.cluster, Exception):
                raise self.cluster
            return {"metadata": {"uid": self.cluster}}
        return super().get(path)


def test_a_lab_qualification_stands_in_for_the_release_gate_and_nothing_else(tmp_path):
    from forecasting import qualification as Q
    pa = pa_object(prediction=dict(PREDICTION, trust=FAST_TRUST))
    build = Q.BuildInfo(TREE, True)
    runs = {"plain": {}, "lab": {"build": build, "lab_qualification": lab()},
            "other-target": {"build": build, "lab_qualification": lab(target_uid="recreated")},
            "other-name": {"build": build, "lab_qualification": lab(name="api")}}
    got = {}
    for name, cfg in runs.items():
        svc, clock = make(tmp_path / name, reader=ClusterReader(pa), store_id="journal-1", **cfg)
        settings = c.resolve_settings(pa).fingerprint()
        wires = []
        for i in range(90):
            clock.set(max(clock.now_ms(), (T0 + i * DT) * 1000 + 10_000))
            wires.append(svc.predict(body(T0 + i * DT, settings)))
        got[name] = (wires, [iss.payload() for iss in svc.store.issuances()])
    sha = lab().sha256

    def strip(b):                       # what the release gate may change: its record, the final reason and readiness
        v = {**b["verdict"], "reason": None, "prediction_ready": None}
        return {**{k: x for k, x in b.items() if k != "release_gate"}, "verdict": v}
    plain = [strip(b) for b in got["plain"][1]]
    for name in runs:                   # identical evidence, calibration, bounds, trust and selection in every run
        assert [strip(b) for b in got[name][1]] == plain, name
    assert got["plain"][1][-1]["selected"] == "seasonal_naive"            # naive trusted within the run
    assert got["plain"][1][-1]["release_gate"] == {"passed": False, "basis": None, "lab_qualification_sha256": None}
    assert got["lab"][1][-1]["release_gate"] == {"passed": True, "basis": "lab", "lab_qualification_sha256": sha}
    for name in ("other-target", "other-name"):
        assert got[name][1][-1]["release_gate"]["passed"] is False
    eligible = {name: c.response_from_wire(w[-1]).predictive_eligible for name, (w, _) in got.items()}
    assert eligible == {"plain": False, "lab": True, "other-target": False, "other-name": False}
    assert c.response_from_wire(got["lab"][0][-1]).reason == "Ready"
    assert c.response_from_wire(got["plain"][0][-1]).reason == "ReplayGateMissing"
    # Before trust was earned the lab run answered exactly as the plain one: never eligible, same reasons.
    first_ready = next(i for i, b in enumerate(got["lab"][1]) if b["verdict"]["prediction_ready"])
    assert first_ready > 0 and all(not c.response_from_wire(w).predictive_eligible for w in got["lab"][0][:first_ready])
    assert all(a["verdict"]["reason"] == b["verdict"]["reason"]
               for a, b in zip(got["lab"][1][:first_ready], got["plain"][1][:first_ready]))


def test_a_change_of_the_update_interval_starts_a_new_epoch_and_reuses_no_stored_answer(tmp_path):
    """The operator's update interval is bound into the settings fingerprint and the evidence key: after the live
    object changes it, the answer stored for the same decision origin is not reused, the old fingerprint is refused,
    and the evidence starts again in a new epoch (so trust earned at one cadence never serves another)."""
    reader = Reader(pa_object(prediction=dict(PREDICTION, updateIntervalSeconds=300)))
    svc, clock = make(tmp_path, reader=reader)
    old = c.resolve_settings(reader.pa).fingerprint()
    clock.set(T0 * 1000 + 10_000)
    first = c.response_from_wire(svc.predict(body(T0, old)))
    assert c.response_from_wire(svc.predict(body(T0, old))).issuance_id == first.issuance_id     # the stored answer
    reader.pa = pa_object(prediction=dict(PREDICTION, updateIntervalSeconds=600))
    new = c.resolve_settings(reader.pa).fingerprint()
    assert new != old
    with pytest.raises(ServiceError) as e:
        svc.predict(body(T0, old))                               # the object no longer has these settings
    assert e.value.status == 422
    second = c.response_from_wire(svc.predict(body(T0, new)))
    assert second.issuance_id != first.issuance_id and second.calibration_epoch != first.calibration_epoch
    assert second.prospective_windows == 0                       # nothing earned before counts in the new epoch
    svc.close()


def test_an_unvalidated_configuration_is_never_eligible(tmp_path):
    """The same lab-qualified run without the allowlist override: the naive is trusted and selected, the release
    gate's stand-in holds, every bound is issued, and the answer is not eligible: ConfigurationNotValidated."""
    from forecasting import qualification as Q
    pa = pa_object(prediction=dict(PREDICTION, trust=FAST_TRUST))
    svc, clock = make(tmp_path, reader=ClusterReader(pa), store_id="journal-1", build=Q.BuildInfo(TREE, True),
                      lab_qualification=lab())
    settings = c.resolve_settings(pa).fingerprint()
    assert c.validated_profile(c.resolve_settings(pa), LEAD) is None
    wire = None
    for i in range(90):
        clock.set(max(clock.now_ms(), (T0 + i * DT) * 1000 + 10_000))
        wire = svc.predict(body(T0 + i * DT, settings))
    last = svc.store.issuances()[-1].payload()
    assert last["selected"] == "seasonal_naive" and last["release_gate"]["passed"] is True
    assert last["verdict"]["candidates"]["seasonal_naive"]["trusted"] is True
    assert (last["verdict"]["reason"], last["verdict"]["prediction_ready"]) == ("ConfigurationNotValidated", False)
    r = c.response_from_wire(wire)
    assert (r.predictive_eligible, r.reason, r.trust_state) == (False, "ConfigurationNotValidated", "Trusted")
    assert r.upper_window_max_rps is not None                 # served and bounded as always


def test_a_lab_qualification_fails_closed(tmp_path):
    from forecasting import qualification as Q
    pa = pa_object(prediction=dict(PREDICTION, trust=FAST_TRUST))
    with pytest.raises(Q.QualificationRefused):                      # a release build never takes one
        make(tmp_path / "release", lab_qualification=lab(), build=Q.BuildInfo(TREE, False))
    with pytest.raises(Q.QualificationRefused):                      # nor another tree's evaluation build
        make(tmp_path / "tree", lab_qualification=lab(), build=Q.BuildInfo("c" * 40, True))
    for name, cluster in (("other-cluster", "another-cluster"), ("unreadable", LookupFailed("forbidden"))):
        svc, clock = make(tmp_path / name, reader=ClusterReader(pa, cluster), lab_qualification=lab(),
                          build=Q.BuildInfo(TREE, True))
        clock.set(T0 * 1000 + 10_000)
        svc.predict(body(T0, c.resolve_settings(pa).fingerprint()))
        assert svc.store.issuances()[-1].payload()["release_gate"]["passed"] is False, name
    late, clock = make(tmp_path / "expired", reader=ClusterReader(pa), lab_qualification=lab(),
                       build=Q.BuildInfo(TREE, True), clock=ManualClock((T0 + 31 * DAY) * 1000))
    clock.set((T0 + 31 * DAY) * 1000 + 10_000)
    late.predict(body(T0 + 31 * DAY, c.resolve_settings(pa).fingerprint()))
    assert late.store.issuances()[-1].payload()["release_gate"]["passed"] is False


def _lab_run(tmp_path, n, q, **cfg):
    from forecasting import qualification as Q
    pa = pa_object(prediction=dict(PREDICTION, trust=FAST_TRUST))
    svc, clock = make(tmp_path, reader=ClusterReader(pa), store_id="journal-1", build=Q.BuildInfo(TREE, True),
                      lab_qualification=q, **cfg)
    settings = c.resolve_settings(pa).fingerprint()
    wires = []
    for i in range(n):
        clock.set(max(clock.now_ms(), (T0 + i * DT) * 1000 + 10_000))
        wires.append(svc.predict(body(T0 + i * DT, settings)))
    return svc, clock, pa, settings, wires


def test_eligibility_is_reauthorised_at_every_answer(tmp_path):
    # Codex r1 on #27: a stored lab-qualified issuance must not stay eligible after expiry or removal.
    last = T0 + 89 * DT                                     # the run's last origin; its valid_until is last + 270
    svc, clock, pa, settings, wires = _lab_run(tmp_path / "s", 90, lab(expires=last + 280))
    assert c.response_from_wire(wires[-1]).predictive_eligible is True
    clock.set((last + 285) * 1000)                          # the qualification expired; the origin is still open
    again = c.response_from_wire(svc.predict(body(last, settings)))
    assert again.issuance_id == c.response_from_wire(wires[-1]).issuance_id and again.predictive_eligible is False
    svc.close()
    # The same journal reopened by a release (default) build without any qualification: the stored issuance says it
    # passed under the lab basis, but nothing authorises it now.
    plain, _ = make(tmp_path / "s", reader=ClusterReader(pa), store_id="journal-1", clock=ManualClock(
        (last + 20) * 1000))
    r = c.response_from_wire(plain.predict(body(last, settings)))
    assert r.issuance_id == again.issuance_id and r.predictive_eligible is False
    assert plain.store.issuances()[-1].payload()["release_gate"]["basis"] == "lab"       # the record stays true
    # Another qualification (another digest) does not inherit the first one's issuances either.
    from forecasting import qualification as Q
    other, _ = make(tmp_path / "s", reader=ClusterReader(pa), store_id="journal-1",
                    clock=ManualClock((last + 20) * 1000), build=Q.BuildInfo(TREE, True),
                    lab_qualification=lab(expires=last + 290))
    assert c.response_from_wire(other.predict(body(last, settings))).predictive_eligible is False


def test_a_lab_answer_never_outlives_its_qualification(tmp_path):
    last = T0 + 89 * DT
    svc, _clock, _pa, _s, wires = _lab_run(tmp_path, 90, lab(expires=last + 200))   # expires before valid_until
    b = svc.store.issuances()[-1].payload()
    assert b["valid_until"] > last + 200 and b["release_gate"]["passed"] is False
    assert c.response_from_wire(wires[-1]).predictive_eligible is False
    earlier = svc.store.issuances()[-2].payload()            # the previous origin's answer ends before the expiry
    assert earlier["valid_until"] <= last + 200 and earlier["release_gate"]["passed"] is True


def test_costs_follow_late_truth_receipts_and_survive_a_restart(tmp_path, monkeypatch):
    # Codex r2 on #27: the cost cache must be bound to every replay input, not just the pair count and span.
    pa = policy_pa()
    svc, out = drive(tmp_path, pa, 130, until=lambda b: bool(b["costs"]))
    last = out[-1][1]
    assert last["costs"], "the run reached the cost replay"
    o = last["decision_origin"]
    bound = c.bind_request(body(o, c.resolve_settings(pa).fingerprint()), Reader(pa), now_ms=o * 1000 + 10_000)
    state = svc.store.latest_checkpoint(last["evidence_key"]).state
    both = {"seasonal_naive": None, "statistical": None}
    lo, _hi = svc._costs[("shop", "web-pa")][1]["statistical"]["binding"]["window"]
    start = lo - (last["window_end"] - o)
    late = {start + 3 * DT, start + 4 * DT}                  # two truth samples not received yet
    real = svc.store.observations
    monkeypatch.setattr(svc.store, "observations", lambda series, a, b: [r for r in real(series, a, b)
                                                                         if r[0] not in late])
    sha, before = svc._policy_costs(bound, state, o, both)
    assert before["statistical"]["unknown_truth_seconds"] == 2 * DT
    replays = svc.cost_replays
    assert svc._policy_costs(bound, state, o, both)[1] == before and svc.cost_replays == replays   # cached
    monkeypatch.setattr(svc.store, "observations", real)    # they arrive; pairs, span and policy are unchanged
    _sha, after = svc._policy_costs(bound, state, o, both)
    assert svc.cost_replays == replays + 1 and after["statistical"]["unknown_truth_seconds"] == 0
    assert after != before
    # An issuance the journal no longer holds (compacted) is not replayed from memory either.
    seqs = svc.store.issuance_seqs
    monkeypatch.setattr(svc.store, "issuance_seqs", lambda ek, a, b: {k: v for k, v in seqs(ek, a, b).items()
                                                                      if k != start})
    assert svc._policy_costs(bound, state, o, both)[1] != after and svc.cost_replays == replays + 2
    monkeypatch.setattr(svc.store, "issuance_seqs", seqs)
    # A restarted service on the same journal computes exactly the same evidence from nothing.
    svc.close()
    again = ForecastService(ServiceConfig(prometheus_url="http://prom", state_dir=str(tmp_path), fit_mode="off",
                                          store_id="journal-1"), reader=Reader(pa), fetch=Backend(NOISY),
                            clock=ManualClock((o + 40) * 1000))
    assert again._policy_costs(bound, state, o, both) == (sha, after)
