"""The chronological replay runner: network-free, deterministic, issuing every forecast before its future is revealed.

The runner walks the decision origins of a dataset on its grid with a ManualClock (wall time = the origin). At each
tick it does exactly what the service does, in the same order (design C2, section 6, "Event ordering"):
  1. ingest the observations whose receipt time has come (journal: first receipt kept, later values are corrections);
  2. close the issued windows with window_end + settlement <= now, in window-end order (forecasting.maturation: truth
     counts only when received by the fixed deadline window_end + settlement + grace; excluded truth is unscorable);
  3. fold the new outcomes into the state exactly once (ForecastStore.recover: the checkpoint plus later outcomes);
  4. forecast from a VisibleHistory: the live reader's finalized history, i.e. observations received by now whose
     event time is at or before the settlement watermark floor((now - settle)/Δ)*Δ, minus the dataset's exclusions;
  5. commit the issuance for this origin (get_or_issue: a committed bundle is never recomputed);
  6. checkpoint the state with the tick it completed (replay_tick).
Excluded observations are still ingested into the journal (their receipts stay auditable); they are invisible to
forecasting and unavailable as truth, and their windows count as unscorable (reason excluded) in availability.
Every record goes through forecasting.store, so a replay journal and a live one have the same shape; issuance bundles
are labelled evidence_mode=replay and carry the dataset's manifest digest.

A replay can be interrupted anywhere and resumed with the same arguments: it restarts at the tick after the last
checkpointed one, and every step of a tick is idempotent (duplicate observations, committed outcomes and issuances are
returned, not rewritten), so the finished journal is identical to an uninterrupted run.

The forecasters here (last_value, daily_naive) are simple stand-ins with fixed recipes (the statistical kernels are in
forecasting.statistical). Each issuance carries the window bound from forecasting.calibration, computed at issue time
from the cohort scores that had matured by then (null until enough exist); each outcome is folded into the
calibration state exactly once, through the journal, so the level moves only for bounds actually issued.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from evaluation.datasets import Dataset
from forecasting import calibration as cal
from forecasting.clock import ManualClock
from forecasting.contracts import FORECAST_CONTRACT, canonical_json, sha256_hex
from forecasting.history import watermark
from forecasting.maturation import window_outcome
from forecasting.store import ForecastStore, IssuanceKey, Outcome

REPLAY_SCHEMA = "forecast-replay/v1"
ISSUANCE_SCHEMA = "forecast-issuance/v1"
DAY = 86400


class ReplayError(Exception):
    pass


@dataclass(frozen=True)
class ReplayConfig:
    first_origin: int
    last_origin: int
    lead_seconds: int
    settlement_seconds: int = 120
    outcome_grace_seconds: int = 600
    model_family: str = "seasonal_naive"
    model_recipe: str = "replay-skeleton/daily-naive/v1"
    miscoverage_bps: int = 1000
    eta_bps: int = 50
    min_miscoverage_bps: int = 100
    max_miscoverage_bps: int = 2500
    min_windows: int = 128
    max_scores: int = 512
    retention_seconds: int = 7 * 86400

    def policy(self) -> cal.CalibrationPolicy:
        return cal.CalibrationPolicy(self.lead_seconds, self.miscoverage_bps, self.eta_bps, self.min_miscoverage_bps,
                                     self.max_miscoverage_bps, self.min_windows, self.max_scores,
                                     self.retention_seconds)

    def check(self, interval: int) -> None:
        for name in ("first_origin", "last_origin", "lead_seconds"):
            v = getattr(self, name)
            if not isinstance(v, int) or isinstance(v, bool) or v % interval:
                raise ReplayError(f"{name} must lie on the {interval} s grid")
        if self.lead_seconds <= 0 or self.last_origin < self.first_origin:
            raise ReplayError("a positive lead and an ordered origin range are required")
        if self.settlement_seconds < 0 or self.outcome_grace_seconds < 0:
            raise ReplayError("settlement and grace are non-negative")


class VisibleHistory(Mapping):
    """event time -> RPS of the finalized history at `now_ms`: received by then, event time <= `through` (the settlement
    watermark) and not excluded. Read-only; nothing else is reachable."""

    def __init__(self, received: Mapping[int, Tuple[float, int]], now_ms: int, interval: int,
                 through: Optional[int] = None, excluded: Optional[Callable[[int], Optional[str]]] = None):
        self._received, self.now_ms, self.interval = MappingProxyType(received), now_ms, interval
        self.through, self._excluded = through, excluded

    def _visible(self, t) -> Optional[Tuple[float, int]]:
        got = self._received.get(t)
        if got is None or got[1] > self.now_ms or (self.through is not None and t > self.through):
            return None
        return None if self._excluded is not None and self._excluded(t) else got

    def __getitem__(self, t) -> float:
        got = self._visible(t)
        if got is None:
            raise KeyError(t)
        return got[0]

    def __iter__(self):
        return (t for t in sorted(self._received) if self._visible(t) is not None)

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def latest(self) -> Optional[int]:
        best = None
        for t in self._received:
            if (best is None or t > best) and self._visible(t) is not None:
                best = t
        return best

    def received_through_ms(self) -> Optional[int]:
        ats = [got[1] for t in self._received if (got := self._visible(t)) is not None]
        return max(ats) if ats else None


Forecaster = Callable[[VisibleHistory, int, Tuple[int, ...]], Optional[Sequence[float]]]


def last_value(view: VisibleHistory, origin: int, targets: Tuple[int, ...]) -> Optional[List[float]]:
    """Persistence: the newest received value for every target."""
    t = view.latest()
    return None if t is None else [view[t]] * len(targets)


def daily_naive(view: VisibleHistory, origin: int, targets: Tuple[int, ...]) -> Optional[List[float]]:
    """x(s - 1 day) for every target s; no forecast unless every lagged slot was received."""
    lagged = [view.get(s - DAY) for s in targets]
    return None if any(v is None for v in lagged) else lagged


@dataclass(frozen=True)
class ReplayResult:
    evidence_key: str
    settings_sha256: str
    issued: int                      # issuances committed by this run (a resumed run counts only its own)
    no_forecast: int                 # origins where the forecaster returned nothing
    scored: int
    unscorable: int                  # every unscorable window, excluded ones included (availability denominator)
    excluded: int                    # of which: truth inside a declared exclusion
    pending: int                     # issued windows still open at the end
    checkpoint: Optional[dict]
    journal_sha256: str


def _issue(store: ForecastStore, ek: str, settings_sha: str, dataset: Dataset, config: ReplayConfig,
           view: VisibleHistory, t: int, targets: Tuple[int, ...], points: Sequence[float], state: dict) -> int:
    points = [float(x) for x in points]
    if len(points) != len(targets) or not all(math.isfinite(x) and x >= 0 for x in points):
        raise ReplayError(f"the forecaster returned an invalid trajectory at {t}")
    key = IssuanceKey(ek, t, t, targets[-1], settings_sha)
    input_end, through = view.latest(), view.received_through_ms()
    base = max(points)
    upper, info = cal.upper_bound(state["calibration"], config.policy(), base, t)

    def build(issuance_id: str) -> dict:
        return {"schema": ISSUANCE_SCHEMA, "evidence_mode": "replay", "forecast_contract": FORECAST_CONTRACT,
                "issuance_id": issuance_id, "evidence_key": ek, "settings_sha256": settings_sha,
                "dataset_sha256": dataset.manifest_sha256, "model_family": config.model_family,
                "model_recipe": config.model_recipe, "decision_origin": t, "window_start": t,
                "window_end": targets[-1], "target_timestamps": list(targets),
                "sample_interval_seconds": view.interval, "point_rps": points, "base_window_max_rps": base,
                "upper_window_max_rps": upper, "nominal_miscoverage_bps": config.miscoverage_bps,
                "calibration": {**info, "in_cohort": cal.in_cohort(t, config.lead_seconds)},
                "issued_at_ms": view.now_ms, "input_end": input_end, "input_received_through_ms": through}
    _, created = store.get_or_issue(key, build, models=(config.model_family,))
    return int(created)


def initial_state(epoch: str, policy: cal.CalibrationPolicy) -> dict:
    return {"scored": 0, "unscorable": 0, "excluded": 0, "missing_truth": 0, "covered": 0, "missed": 0,
            "unbounded": 0, "abs_error_sum_rps": 0.0, "last_window_end": None,
            "calibration": cal.initial(policy, epoch)}


def fold(state: dict, o: Outcome, issuance, policy: cal.CalibrationPolicy) -> dict:
    """The checkpoint fold: counters over terminal outcomes and the calibration update, in journal order. Pure."""
    s, p = dict(state), o.payload()
    s["calibration"] = cal.apply(state["calibration"], cal.CalibrationRecord.from_journal(issuance, o), policy)
    s["last_window_end"] = o.window_end
    if o.status == "unscorable":
        s["unscorable"] += 1
        s[p["reason"]] += 1                      # excluded | missing_truth
        return s
    s["scored"] += 1
    s["abs_error_sum_rps"] = s["abs_error_sum_rps"] + p["abs_error_rps"]
    if p["covered"] is None:
        s["unbounded"] += 1
    elif p["covered"]:
        s["covered"] += 1
    else:
        s["missed"] += 1
    return s


def replay_keys(dataset: Dataset, config: ReplayConfig) -> Tuple[str, str]:
    """(evidence key, settings hash) of a replay: bound to the dataset digest and every replay parameter."""
    settings = sha256_hex(canonical_json({"schema": REPLAY_SCHEMA, "config": asdict(config),
                                          "interval_seconds": dataset.interval_seconds}))
    evidence = sha256_hex(canonical_json({"schema": REPLAY_SCHEMA, "dataset": dataset.manifest_sha256,
                                          "settings": settings, "forecast_contract": FORECAST_CONTRACT}))
    return evidence, settings


def run_replay(dataset: Dataset, config: ReplayConfig, store: ForecastStore, *,
               forecaster: Forecaster = daily_naive) -> ReplayResult:
    dt = dataset.interval_seconds
    config.check(dt)
    policy = config.policy()
    ek, settings_sha = replay_keys(dataset, config)
    series = "replay:" + dataset.manifest_sha256
    source = dataset.manifest["data_sha256"]
    arrivals = sorted(dataset.observations, key=lambda o: (o.available_at_ms, o.event_time))
    done = store.latest_checkpoint(ek)
    resume_from = done.state["replay_tick"] + dt if done else config.first_origin
    origins = range(resume_from, config.last_origin + 1, dt)
    clock = store.clock
    if not isinstance(clock, ManualClock):
        raise ReplayError("a replay store runs on a ManualClock")

    received: Dict[int, Tuple[float, int]] = {}
    nxt, issued, no_forecast = 0, 0, 0
    for t in origins:
        now_ms = t * 1000
        clock.set(now_ms)
        # 1. ingest what has arrived by now, in receipt order (one transaction)
        batch: List[Tuple[int, float, int]] = []
        while nxt < len(arrivals) and arrivals[nxt].available_at_ms <= now_ms:
            o = arrivals[nxt]
            received.setdefault(o.event_time, (o.value_rps, o.available_at_ms))
            batch.append((o.event_time, o.value_rps, o.available_at_ms))
            nxt += 1
        if batch:
            store.record_observations(series, batch, source)
        # 2. close windows that have settled, in window-end order
        for iss in store.open_issuances(ek, config.model_family, t - config.settlement_seconds):
            p = iss.payload()
            res = window_outcome(targets=p["target_timestamps"], base_rps=p["base_window_max_rps"],
                                 upper_rps=p["upper_window_max_rps"], received=received, now_ms=now_ms,
                                 settlement_s=config.settlement_seconds, grace_s=config.outcome_grace_seconds,
                                 excluded=dataset.excluded)
            if res is not None:
                store.commit_outcome(iss.issuance_id, config.model_family, *res)
        # 3. fold the new outcomes in exactly once (calibration, C2.4, will read this state before issuing)
        state, last, _ = store.recover(
            ek, lambda st, o: fold(st, o, store.get_issuance_by_id(o.issuance_id), policy),
            lambda: initial_state(ek, policy))
        # 4. forecast from what is visible now; 5. commit the issuance before anything later is revealed
        view = VisibleHistory(received, now_ms, dt, through=watermark(t, dt, config.settlement_seconds),
                              excluded=dataset.excluded)
        targets = tuple(t + k * dt for k in range(1, config.lead_seconds // dt + 1))
        points = forecaster(view, t, targets)
        if points is None:
            no_forecast += 1
        else:
            issued += _issue(store, ek, settings_sha, dataset, config, view, t, targets, points, state)
        # 6. the tick is complete
        store.write_checkpoint(ek, {**state, "replay_tick": t}, last)

    cp = store.latest_checkpoint(ek)
    totals = cp.state if cp else initial_state(ek, policy)
    pending = len(store.open_issuances(ek, config.model_family, config.last_origin + config.lead_seconds))
    return ReplayResult(evidence_key=ek, settings_sha256=settings_sha, issued=issued, no_forecast=no_forecast,
                        scored=totals["scored"], unscorable=totals["unscorable"], excluded=totals["excluded"],
                        pending=pending,
                        checkpoint=cp.state if cp else None, journal_sha256=store.journal_digest())
