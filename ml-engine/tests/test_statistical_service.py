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
                if self.gap_after is None or t <= self.gap_after]
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
    svc, clock = make(tmp_path, max_autoscalers=1)
    ask(svc, clock, T0)
    other = pa_object(uid="pa-2")
    svc.reader = Reader(other)
    b = dict(body(T0 + DT), autoscaler_name="api-pa", autoscaler_uid="pa-2")
    clock.set((T0 + DT) * 1000 + 10_000)
    with pytest.raises(ServiceError) as e:
        svc.predict(b)
    assert e.value.status == 503


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
    barrier = threading.Barrier(2, timeout=10)

    class TwoPAs:
        def get(self, path):
            name = path.rsplit("/", 1)[1]
            if "predictiveautoscalers" in path:
                barrier.wait()                             # both requests pass identity together
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
