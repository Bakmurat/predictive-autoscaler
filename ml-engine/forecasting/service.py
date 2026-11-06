"""The statistical forecasting service (forecast contract v2): one /v2/predict request, end to end.

For a request at decision origin t (all under one journal; nothing is wired to the operator before C2.7):
  1. bind the request to the live autoscaler (forecasting.contracts.bind_request: identity, settings hash, origin);
  2. read the signal's finalized RPS history (forecasting.history) and record it in the journal (first receipts);
  3. close the issued windows that have settled (forecasting.maturation, the fixed truth deadline);
  4. fold new issuances and outcomes into the epoch's state exactly once: per model, its calibration
     (forecasting.calibration) and its evidence ledger (forecasting.trust), plus the paired challenger evidence;
  5. forecast every available family from what is finalized (seasonal-naive; E1-product when a fresh, verified fit
     exists; a due fit is queued on the bounded worker, never run on the request path);
  6. evaluate trust (gates, hysteresis, selection) and commit ONE immutable issuance bundle holding every candidate,
     its calibrated bound and the verdict, before answering; a retry at the same origin returns that bundle;
  7. checkpoint the state and answer from the bundle (forecasting.contracts.V2Response, checked before it is sent).

This preview is shadow only: the release gates (G0-G5, C2.8) have not passed, so the verdict's PredictionReady and every
response's predictive_eligible are false whatever the evidence says. A journal that cannot be opened or written answers
503 (StateUnavailable) and never issues; a lost journal starts empty, so trust has to be earned again from nothing.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from api.identity import LookupFailed, ProvenanceRefused
from data.bounded_http import bounded_get
from forecasting import calibration as cal
from forecasting import trust as T
from forecasting.artifacts import ArtifactRefused, ArtifactStore, Binding, encode_e1, fit_payload
from forecasting.clock import ReceiptClock
from forecasting.contracts import FORECAST_CONTRACT, UNIT, BoundRequest, ContractRefused, V2Response, bind_request, \
    check_response
from forecasting.history import HistoryRefused, HistoryUnavailable, read_rps_history
from forecasting.maturation import candidate, issued_outcome
from forecasting.metrics import ForecastMetrics, RegistryFull, Snapshot, model_samples
from forecasting.statistical import RECIPE_E1, RECIPE_NAIVE, KernelSettings, KernelUnavailable, Series, fit_e1, \
    forecast_e1, seasonal_naive
from forecasting.store import DEFAULT_QUOTA_BYTES, ForecastStore, IssuanceKey, StoreError, StoreUnavailable

STATE_SCHEMA = "statistical-service-state/v1"
DAY = 86400
REFIT_SECONDS = 6 * 3600
RECIPES = {"seasonal_naive": RECIPE_NAIVE, "statistical": RECIPE_E1}
RELEASE_GATES_PASSED = False        # C2.8: until the G0-G5 evidence exists, nothing is PredictionReady


class ServiceError(Exception):
    def __init__(self, status: int, reason: str, message: str):
        super().__init__(message)
        self.status, self.reason, self.message = status, reason, message


@dataclass(frozen=True)
class ServiceConfig:
    prometheus_url: str
    state_dir: str
    quota_bytes: int = DEFAULT_QUOTA_BYTES
    fit_mode: str = "worker"             # worker (bounded fit processes) | inline (tests) | off
    max_autoscalers: int = 8
    store_id: Optional[str] = None       # a fixed journal identity (deterministic tests); None: random at creation
    retention_seconds: int = 7 * DAY     # issuances and outcomes kept online (observations: 28 days)
    compact_every_seconds: int = 3600


class ForecastService:
    def __init__(self, config: ServiceConfig, *, reader, fetch: Callable = bounded_get, clock=None, worker=None):
        if config.fit_mode not in ("worker", "inline", "off"):
            raise ValueError(f"unknown fit mode {config.fit_mode!r}")
        self.config, self.reader, self.fetch = config, reader, fetch
        self.clock = clock or ReceiptClock()
        os.makedirs(config.state_dir, exist_ok=True)
        self.store: Optional[ForecastStore] = None
        self.store_problem: Optional[str] = None
        try:
            self.store = ForecastStore(os.path.join(config.state_dir, "journal.sqlite"), clock=self.clock,
                                       quota_bytes=config.quota_bytes, store_id=config.store_id,
                                       retention_seconds=config.retention_seconds,
                                       observation_retention_seconds=max(config.retention_seconds, 28 * DAY))
        except StoreError as e:
            self.store_problem = str(e)
        self.artifacts = ArtifactStore(os.path.join(config.state_dir, "artifacts"))
        self.worker = worker
        if self.worker is None and config.fit_mode == "worker":
            from forecasting.worker import FitWorker
            self.worker = FitWorker(self.artifacts)
        self.metrics = ForecastMetrics(max_autoscalers=config.max_autoscalers)
        self._lock = threading.Lock()                   # one writer of the journal and the epoch states
        self._compacted_s: Optional[int] = None

    # --- status -------------------------------------------------------------------------------------------------

    @property
    def store_healthy(self) -> bool:
        return self.store is not None and self.store.healthy

    def health(self) -> Dict:
        problem = self.store_problem or (self.store.problem if self.store is not None else None)
        return {"status": "healthy", "state": "healthy" if self.store_healthy else "unavailable",
                "state_problem": problem, "forecast_contract": FORECAST_CONTRACT,
                "release_gates_passed": RELEASE_GATES_PASSED}

    def close(self) -> None:
        if self.worker is not None:
            self.worker.close()
        if self.store is not None:
            self.store.close()

    # --- the request --------------------------------------------------------------------------------------------

    def predict(self, body) -> Dict:
        now_ms = self.clock.now_ms()
        try:
            bound = bind_request(body, self.reader, now_ms=now_ms)
        except (ContractRefused, ProvenanceRefused) as e:
            raise ServiceError(422, "IdentityMismatch", str(e)) from e
        except LookupFailed as e:
            raise ServiceError(503, "IdentityMismatch", f"the autoscaler could not be read: {e}") from e
        sig = bound.signal
        if (sig.namespace, bound.autoscaler_name) not in self.metrics.registered() and \
                len(self.metrics.registered()) >= self.config.max_autoscalers:
            raise ServiceError(503, "StateUnavailable", "the service's registration limit is reached")
        if not self.store_healthy:
            raise ServiceError(503, "StateUnavailable", "the forecast journal is unavailable; no issuance")
        if bound.lead is None:
            raise ServiceError(503, "LeadInsufficient", bound.lead_problem)
        s = bound.settings
        try:
            grid = read_rps_history(self.config.prometheus_url, sig, now=now_ms / 1000,
                                    interval_s=s.sample_interval_seconds, history_seconds=s.history_days * DAY,
                                    settlement_s=s.settlement_seconds, fetch=self.fetch,
                                    receipt_ms=self.clock.now_ms)
        except HistoryRefused as e:
            raise ServiceError(422, "DataUnavailable", f"history refused: {e}") from e
        except HistoryUnavailable as e:
            raise ServiceError(503, "DataUnavailable", f"history unavailable: {e}") from e
        if not grid.is_fresh(now_ms / 1000, s.settlement_seconds):
            raise ServiceError(503, "DataUnavailable", "the newest finalized observation is too old")
        with self._lock:
            try:
                return self._issue(bound, grid, now_ms)
            except StoreUnavailable as e:
                raise ServiceError(503, "StateUnavailable", f"the forecast journal failed: {e}") from e

    def _issue(self, bound: BoundRequest, grid, now_ms: int) -> Dict:
        store, s, sig, lead = self.store, bound.settings, bound.signal, bound.lead
        ek, t, dt = bound.evidence_key, bound.decision_origin, s.sample_interval_seconds
        window = bound.window
        policy = cal.CalibrationPolicy.from_settings(s, lead.lead_seconds)
        series_key = "signal:" + ek
        store.record_observations(series_key, [(ts, v, at) for ts, v, at in zip(grid.timestamps, grid.values,
                                                                                grid.received_at_ms)
                                               if v is not None], grid.response_sha256)
        received = {ts: (v, at) for ts, v, at in store.observations(series_key, grid.start, grid.end)}
        # Close what has settled, then fold everything new into the state exactly once.
        now_s = now_ms // 1000
        for model in RECIPES:
            for iss in store.open_issuances(ek, model, now_s - s.settlement_seconds):
                res = issued_outcome(iss.payload(), model, received=received, now_ms=now_ms,
                                     settlement_s=s.settlement_seconds, grace_s=s.outcome_grace_seconds)
                if res is not None:
                    store.commit_outcome(iss.issuance_id, model, *res)
        state = self._recover(bound, policy)
        existing = store.get_issuance(IssuanceKey(ek, t, t, window.window_end, bound.settings_sha256))
        if existing is None:
            bundle = self._build(bound, grid, state, policy, now_ms)
        iss, created = store.get_or_issue(IssuanceKey(ek, t, t, window.window_end, bound.settings_sha256),
                                          lambda iid: {**bundle, "issuance_id": iid},
                                          models=tuple(sorted(bundle["candidates"])) if existing is None
                                          else existing.models)
        state = self._recover(bound, policy, state)
        cp = store.write_checkpoint(ek, state, state["last_outcome_seq"])
        if self._compacted_s is None or now_s - self._compacted_s >= self.config.compact_every_seconds:
            store.compact(now_s)          # only checkpoint-covered, fully matured windows leave the journal
            self._compacted_s = now_s
        self._export(bound, state, grid, now_s)
        return self._response(bound, iss.payload(), cp.payload_sha256)

    # --- state ----------------------------------------------------------------------------------------------------

    def _initial(self, ek: str) -> Dict:
        return {"schema": STATE_SCHEMA, "epoch": ek, "last_outcome_seq": 0, "last_issuance_seq": 0, "models": {},
                "trust": {}, "pairs": {"statistical": T.new_pairs(ek, "statistical", RECIPE_E1, RECIPE_NAIVE)}}

    def _model_state(self, state: Dict, model: str, recipe: str, policy, ek: str, dt: int) -> Dict:
        m = state["models"].get(model)
        if m is None or m["recipe"] != recipe:
            m = state["models"][model] = {"recipe": recipe, "calibration": cal.initial(policy, ek, model, recipe),
                                          "ledger": T.new_ledger(ek, model, recipe, dt)}
        return m

    def _recover(self, bound: BoundRequest, policy, state: Optional[Dict] = None) -> Dict:
        """The epoch's state: the latest checkpoint (or `state`) plus every later issuance, then every later outcome,
        each folded exactly once."""
        store, s, ek = self.store, bound.settings, bound.evidence_key
        if state is None:
            cp = store.latest_checkpoint(ek)
            state = dict(cp.state) if cp is not None else self._initial(ek)
        state = {**state, "models": {k: dict(v) for k, v in state["models"].items()}, "pairs": dict(state["pairs"])}
        dt = s.sample_interval_seconds
        for iss in store.issuances_after(ek, state["last_issuance_seq"]):
            b = iss.payload()
            for model in iss.models:
                c = candidate(b, model)
                m = self._model_state(state, model, c["recipe"], policy, ek, dt)
                m["ledger"] = T.note_issuance(
                    m["ledger"], seq=iss.seq, issuance_id=iss.issuance_id, epoch=ek, model=model, recipe=c["recipe"],
                    origin=iss.key.decision_origin, window_end=iss.key.window_end,
                    deadline_s=iss.key.window_end + s.settlement_seconds + s.outcome_grace_seconds,
                    lead_s=policy.lead_seconds, mode=b["evidence_mode"], bounded=c["upper_window_max_rps"] is not None)
            state["last_issuance_seq"] = iss.seq
        for o in store.outcomes_after(ek, state["last_outcome_seq"]):
            iss = store.get_issuance_by_id(o.issuance_id)
            rec = T.EvidenceRecord.from_journal(iss, o)
            m = self._model_state(state, o.model, rec.cal.recipe, policy, ek, dt)
            m["calibration"] = cal.apply(m["calibration"], rec.cal, policy)
            m["ledger"] = T.note_outcome(m["ledger"], rec, lead_s=policy.lead_seconds,
                                         nominal_miscoverage=policy.nominal_ratio,
                                         recent_n=s.trust.acute_recent_windows)
            state["pairs"]["statistical"] = T.note_pair(state["pairs"]["statistical"], rec,
                                                        lead_s=policy.lead_seconds,
                                                        nominal_miscoverage=policy.nominal_ratio)
            state["last_outcome_seq"] = o.seq
        return state

    # --- forecasting and the bundle ---------------------------------------------------------------------------------

    def _fit_for(self, bound: BoundRequest, series: Series, input_end: int, ks: KernelSettings):
        binding = Binding.for_signal(bound.signal, bound.settings, RECIPE_E1)
        try:
            fit = self.artifacts.load(binding, ks, input_end=input_end)
            sha = self.artifacts.current(binding)[0] if fit is not None else None
        except ArtifactRefused:
            fit, sha = None, None
        due = fit is None or input_end - fit.cutoff >= REFIT_SECONDS
        if due and input_end - series.start + ks.interval_seconds >= ks.min_history_days * DAY:
            if self.config.fit_mode == "inline":
                try:
                    data = encode_e1(fit_e1(series, input_end, ks), binding)
                    self.artifacts.promote(binding, data, ks)
                    fit = self.artifacts.load(binding, ks, input_end=input_end)
                    sha = self.artifacts.current(binding)[0]
                except (KernelUnavailable, ArtifactRefused, OSError):
                    pass
            elif self.config.fit_mode == "worker" and self.worker is not None:
                from forecasting.worker import FitRefused, QueueFull
                try:
                    self.worker.submit(binding, RECIPE_E1, fit_payload(series, input_end, ks, binding))
                except (QueueFull, FitRefused):
                    pass
        return fit, sha

    def _build(self, bound: BoundRequest, grid, state: Dict, policy, now_ms: int) -> Dict:
        s, lead, window, t = bound.settings, bound.lead, bound.window, bound.decision_origin
        dt = s.sample_interval_seconds
        series = Series.from_grid(grid)
        input_end = grid.latest_observed()
        ks = KernelSettings(dt)
        targets = window.target_timestamps
        points: Dict[str, Dict] = {}
        try:
            points["seasonal_naive"] = {"point_rps": seasonal_naive(series, input_end, targets, ks),
                                        "fit_cutoff": None, "artifact_sha256": None}
        except KernelUnavailable:
            pass
        fit, sha = self._fit_for(bound, series, input_end, ks)
        if fit is not None:
            try:
                points["statistical"] = {"point_rps": forecast_e1(fit, series, input_end, targets, ks)["points"],
                                         "fit_cutoff": fit.cutoff, "artifact_sha256": sha}
            except KernelUnavailable:
                pass
        if not points:
            raise ServiceError(503, "HistoryInsufficient", "no forecast family has the history it needs yet")
        first_obs = next(ts for ts, v in zip(grid.timestamps, grid.values) if v is not None)
        cands, inputs = {}, []
        for model, p in points.items():
            m = self._model_state(state, model, RECIPES[model], policy, bound.evidence_key, dt)
            base = max(p["point_rps"])
            upper, info = cal.upper_bound(m["calibration"], policy, base, t)
            cands[model] = {"recipe": RECIPES[model], "point_rps": [float(x) for x in p["point_rps"]],
                            "base_window_max_rps": base, "upper_window_max_rps": upper, "calibration": info,
                            "fit_cutoff": p["fit_cutoff"], "artifact_sha256": p["artifact_sha256"]}
            inputs.append(T.CandidateInputs(model, m["ledger"], upper is not None, info["n"], policy.min_windows,
                                            input_end - first_obs + dt, T.REQUIRED_HISTORY_SECONDS[model], True,
                                            None if p["fit_cutoff"] is None else input_end - p["fit_cutoff"]))
        g = T.Global(now_s=now_ms // 1000, lead_seconds=lead.lead_seconds, nominal_miscoverage=policy.nominal_ratio,
                     identity_ok=True, store_healthy=self.store_healthy, replay_suite_passed=RELEASE_GATES_PASSED,
                     startup_budget_valid=True)
        pairs = state["pairs"] if "seasonal_naive" in points else {}   # paired evidence needs its naive baseline
        verdict, new_h = T.evaluate(inputs, state["trust"], g, s.trust, pairs=pairs, costs=None)
        state["trust"] = new_h
        through = max(at for at, v in zip(grid.received_at_ms, grid.values) if v is not None)
        return {"schema": "forecast-issuance/v1", "evidence_mode": "prospective", "forecast_contract": FORECAST_CONTRACT,
                "evidence_key": bound.evidence_key, "settings_sha256": bound.settings_sha256,
                "decision_origin": t, "window_start": window.window_start, "window_end": window.window_end,
                "target_timestamps": list(targets), "sample_interval_seconds": dt,
                "lead_seconds": lead.lead_seconds, "startup_delay_seconds": lead.startup_delay_seconds,
                "actuation_allowance_seconds": lead.actuation_allowance_seconds,
                "valid_until": window.valid_until(lead), "issued_at_ms": now_ms, "input_end": input_end,
                "input_received_through_ms": min(through, now_ms), "nominal_miscoverage_bps": policy.nominal_bps,
                "candidates": cands, "selected": verdict.selected,
                "verdict": {"phase": verdict.phase, "reason": verdict.reason, "prediction_ready": verdict.prediction_ready,
                            "candidates": {m: {"trusted": v.trusted, "phase": v.phase, "reason": v.reason,
                                               "coverage_gate": v.coverage_gate, "availability": v.availability,
                                               "prospective_gate_bounds": v.prospective_gate_bounds,
                                               "evidence_cutoff":
                                                   state["models"][m]["ledger"]["last_prospective_outcome_s"]}
                                           for m, v in verdict.candidates.items()}}}

    def _response(self, bound: BoundRequest, b: Dict, checkpoint_sha: Optional[str]) -> Dict:
        sig = bound.signal
        reported = b["selected"] or ("seasonal_naive" if "seasonal_naive" in b["candidates"] else "statistical")
        c, v = b["candidates"][reported], b["verdict"]
        cv = v["candidates"][reported]
        state_phase = v["phase"] if b["selected"] else cv["phase"]
        avail = cv["availability"]
        r = V2Response(
            autoscaler_namespace=bound.autoscaler_namespace, autoscaler_name=bound.autoscaler_name,
            autoscaler_uid=sig.autoscaler_uid, autoscaler_generation=sig.generation, target_namespace=sig.namespace,
            target_name=sig.name, target_uid=sig.target_uid, metric_query_sha256=sig.sha256, contract=sig.contract,
            settings_sha256=bound.settings_sha256, forecast_contract=FORECAST_CONTRACT, unit=UNIT,
            issuance_id=b["issuance_id"], issued_at_ms=b["issued_at_ms"], input_end=b["input_end"],
            input_received_through_ms=b["input_received_through_ms"], fit_cutoff=c["fit_cutoff"],
            model_family=reported, model_recipe=c["recipe"], artifact_sha256=c["artifact_sha256"],
            calibration_epoch=bound.evidence_key, checkpoint_sha256=checkpoint_sha,
            decision_origin=b["decision_origin"], window_start=b["window_start"], window_end=b["window_end"],
            target_timestamps=tuple(b["target_timestamps"]), sample_interval_seconds=b["sample_interval_seconds"],
            lead_seconds=b["lead_seconds"], startup_delay_seconds=b["startup_delay_seconds"],
            actuation_allowance_seconds=b["actuation_allowance_seconds"], valid_until=b["valid_until"],
            point_rps=tuple(c["point_rps"]), base_window_max_rps=c["base_window_max_rps"],
            upper_window_max_rps=c["upper_window_max_rps"], nominal_miscoverage_bps=b["nominal_miscoverage_bps"],
            working_miscoverage_ratio=c["calibration"]["working_miscoverage"],
            calibration_windows=c["calibration"]["n"], prospective_windows=cv["prospective_gate_bounds"],
            gate_coverage_ratio=cv["coverage_gate"], missing_outcome_ratio=None if avail is None else 1.0 - avail,
            selected_candidate=reported, trust_state=state_phase, reason=v["reason"],
            predictive_eligible=bool(RELEASE_GATES_PASSED and v["prediction_ready"] and b["selected"]
                                     and c["upper_window_max_rps"] is not None),
            evidence_cutoff=cv["evidence_cutoff"])
        check_response(r, bound)
        return r.to_wire()

    def _export(self, bound: BoundRequest, state: Dict, grid, now_s: int) -> None:
        trusted = {m: h.get("trusted", False) for m, h in state["trust"].items() if m in RECIPES}
        failures = dict(self.worker.failures) if self.worker is not None else {}
        models = {}
        first = next((ts for ts, v in zip(grid.timestamps, grid.values) if v is not None), now_s)
        for model, m in state["models"].items():
            models[model] = model_samples(ledger=m["ledger"], calibration=m["calibration"],
                                          nominal_miscoverage=bound.settings.risk.miscoverage_bps / 10_000,
                                          trusted=trusted.get(model, False), history_seconds=max(0, now_s - first),
                                          now_s=now_s, fit_failures=failures if model == "statistical" else None)
        try:
            self.metrics.update(bound.signal.namespace, bound.autoscaler_name, Snapshot(bound.signal.autoscaler_uid,
                                                                                      models))
        except RegistryFull:
            pass

    def forget(self, namespace: str, autoscaler: str) -> None:
        """After confirmed deletion: drop the autoscaler's series (its journal epoch simply stops growing)."""
        self.metrics.remove(namespace, autoscaler)

