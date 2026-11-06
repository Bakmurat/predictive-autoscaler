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

    def __init__(self, value=None):
        self.down, self.gap_after, self.calls, self.value = False, None, 0, value or rps

    def __call__(self, url, **kw):
        self.calls += 1
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
