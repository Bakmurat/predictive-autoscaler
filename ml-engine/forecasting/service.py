"""The statistical forecasting service (forecast contract v2): one /v2/predict request, end to end.

For a request at decision origin t (all under one journal; nothing is wired to the operator before C2.7):
  1. bind the request to the live autoscaler (forecasting.contracts.bind_request: identity, settings hash, origin);
  2. read the signal's finalized RPS history (forecasting.history) and record it in the journal (first receipts);
  3. close the issued windows that have settled (forecasting.maturation, the fixed truth deadline);
  4. fold new issuances and outcomes into the epoch's state exactly once: per model, its calibration
     (forecasting.calibration) and its evidence ledger (forecasting.trust), plus the paired challenger evidence;
  5. forecast every available family from what is finalized (seasonal-naive; E1-product when a fresh, verified fit
     exists; a due fit is queued on the bounded worker, never run on the request path);
  6. evaluate trust (gates, hysteresis, selection) and checkpoint that state (the trust transition is durable before
     anything is issued);
  7. commit ONE immutable issuance bundle holding every candidate, its calibrated bound, the verdict and the digest of
     the checkpoint it was computed from, stamped with the clock read just before the commit; an issuance that could no
     longer act before its first target is refused instead (never backdated);
  8. fold the issuance into the state, checkpoint again, and answer from the bundle alone (a retry at the same origin
     returns the same bytes, whatever has matured since, except predictive_eligible, which re-checks the release-gate
     authorisation at every answer and can only turn false).
Recovery is exact at every boundary: a crash before the issuance leaves the trust checkpoint (a retry re-decides from
it); a crash after it leaves the issuance in the journal, folded in by the next request.

The challenger (E1-product) is selected over seasonal-naive only on bound same-policy costs (forecasting.policy_sim):
once the retained paired evidence is large and long enough, both forecasts are replayed through the operator's own v2
policy for this autoscaler (capacity, bounds, caps, headroom, startup delay, decision interval) over the decisions of
those pairs, with the issued bounds from the journal and the measured rates as truth; the result names its epoch,
models, recipes, window and policy fingerprint, and trust uses it only when all of them match.
spec.prediction.families restricts what is forecast at all (seasonalNaive only: E1 is never a candidate).

Predictive eligibility also needs a validated configuration (contracts.VALIDATED_PROFILES: grid, lead, risk and family
set the trust policy was qualified for, with the default calibration, trust and timing settings). Any other
configuration is forecast, calibrated, scored and reported exactly the same, and its verdict is never PredictionReady
(reason ConfigurationNotValidated), under a release gate or a lab qualification alike.

This preview is shadow only: the release gates (G0-G5, C2.8) have not passed, so the verdict's PredictionReady and every
response's predictive_eligible are false whatever the evidence says, except under a verified LAB qualification
(forecasting.qualification) in an evaluation build, for the named cluster, namespaces and target UIDs only: it stands in
for the release gate and nothing else. Every bundle records the basis (if any) it was issued under, and every ANSWER
re-checks it (a retry included): an expired, removed or changed qualification makes even a stored issuance
ineligible, and a lab-qualified issuance is eligible only if its valid_until is not after the qualification's expiry,
so a cached eligible answer never outlives it. A journal
that cannot be opened or written answers 503 (StateUnavailable) and never issues; a lost journal starts empty, so trust
has to be earned again from nothing.

A DEMONSTRATION qualification (forecasting.demonstration) is the second, separate basis: it records no gate, is bound
to a mounted and hash-verified demonstration contract, and stands in for the release gate only for its cluster, one
namespace and at most two target UIDs, only while both mounted files still hold the admitted bytes (read again at
every answer; nothing new is loaded without a restart), only while the live configuration is the contract's (the
resolved settings fingerprint included), and only for an answer whose SELECTED family it lists. That last rule is
decided after selection (the gate is first evaluated without it) and re-checked on every answer, like the document's
and the contract's digests. It is never combined with a lab qualification or with passed release gates: the service
refuses to start. Eligibility is an answer, not an action: nothing here assumes a replica write follows.

The service registers at most ServiceConfig.max_autoscalers (namespace, autoscaler) names at once (its metric series
and working memory are sized for them); one more is refused with 503 AutoscalerLimitReached. A registration is
released only on evidence, never because a lookup failed:
  - the Kubernetes API answers a definite 404 for the autoscaler (on its own request, or when a full registry is
    checked: every registered name is looked up, at most once per SLOT_SWEEP_SECONDS);
  - the live object has another UID: the old identity's registration ends and the new UID starts as a new, isolated
    identity (the evidence key, the journal epoch and the fit artifacts are all bound to the UID);
  - no request for it for ServiceConfig.slot_idle_seconds (a day by default; 0: never).
A timeout, a 5xx, a refused read (401/403) or a 404 that does not name the object (a missing API path, a proxy's page)
proves nothing and releases nothing. A release frees the registration, the metric series and the in-memory caches; the
journal keeps the autoscaler's evidence under its retention rules, so the same object (the same UID) registering again
continues exactly where it was.

A request works under the registration it was admitted to (forecasting.metrics.Reservation). If that registration is
replaced or released while the request is still reading history, the request is refused (503, retryable) before it
issues, and whatever it would publish is dropped: old work never overwrites a replacement, never brings a released
registration back and never refills its caches. The same holds before admission: a request bound with a UID
other than the registered one may carry the stale lookup, so the live object is read again and the registration is
replaced only for the live UID, and only the registration that was seen (compare and swap). A full registry is checked in name order from where the last check
stopped, so slow lookups cannot keep the check from ever reaching a later registration.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, replace
from typing import Callable, Dict, Mapping, Optional, Tuple

from api.identity import API_GROUP, API_VERSION, AutoscalerNotFound, LookupFailed, ProvenanceRefused
from data.bounded_http import bounded_get
from forecasting import calibration as cal
from forecasting import demonstration as D
from forecasting import policy_sim as P
from forecasting import qualification as Q
from forecasting import trust as T
from forecasting.artifacts import ArtifactRefused, ArtifactStore, Binding, encode_e1, fit_payload
from forecasting.clock import ReceiptClock
from forecasting.contracts import FAMILY_MODELS, FORECAST_CONTRACT, UNIT, BoundRequest, ContractRefused, V2Response, \
    bind_request, canonical_json, check_response, sha256_hex, validated_profile
from forecasting.history import HistoryRefused, HistoryUnavailable, read_rps_history
from forecasting.maturation import candidate, issued_outcome
from forecasting.metrics import ForecastMetrics, RegistryFull, ServiceMetrics, Snapshot, UidConflict, model_samples
from forecasting.qualification_metrics import QualificationMetrics
from forecasting.statistical import RECIPE_E1, RECIPE_NAIVE, KernelSettings, KernelUnavailable, Series, fit_e1, \
    forecast_e1, seasonal_naive
from forecasting.store import DEFAULT_QUOTA_BYTES, ForecastStore, IssuanceKey, StoreError, StoreUnavailable

STATE_SCHEMA = "statistical-service-state/v3"       # v3: the coverage monitor (ledger v3) and its alarm accounting
DAY = 86400
REFIT_SECONDS = 6 * 3600
RECIPES = {"seasonal_naive": RECIPE_NAIVE, "statistical": RECIPE_E1}
RELEASE_GATES_PASSED = False        # C2.8: until the G0-G5 evidence exists, nothing is PredictionReady
COST_HISTORY_SECONDS = 8 * DAY      # issued bounds kept in memory for cost replays (paired evidence: 7 days)
# What ties a stored answer to the basis it was issued under (absent keys compare as None: older bundles have fewer).
_GATE_IDENTITY = ("basis", "lab_qualification_sha256", "demonstration_qualification_sha256",
                  "demonstration_contract_sha256")
SLOT_SWEEP_SECONDS = 60             # a full registry is checked against the Kubernetes API at most this often
SLOT_SWEEP_BUDGET_SECONDS = 10      # ... and one check starts no further lookup after this long

logger = logging.getLogger("forecasting.service")


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
    max_autoscalers: int = 8             # (namespace, autoscaler) names registered at once; one more is refused
    slot_idle_seconds: int = DAY         # a registration without a request for this long is released; 0: never
    store_id: Optional[str] = None       # a fixed journal identity (deterministic tests); None: random at creation
    retention_seconds: int = 7 * DAY     # issuances and outcomes kept online (observations: 28 days)
    compact_every_seconds: int = 3600
    build: Q.BuildInfo = Q.BuildInfo("unknown", False)               # the image's build record
    lab_qualification: Optional[Q.LabQualification] = None          # verified by forecasting.qualification.load
    # verified, with its contract, by forecasting.demonstration.load; never beside a lab qualification
    demonstration_qualification: Optional[D.DemonstrationQualification] = None


class ForecastService:
    def __init__(self, config: ServiceConfig, *, reader, fetch: Callable = bounded_get, clock=None, worker=None):
        if config.fit_mode not in ("worker", "inline", "off"):
            raise ValueError(f"unknown fit mode {config.fit_mode!r}")
        if config.max_autoscalers < 1 or config.slot_idle_seconds < 0:
            raise ValueError("max_autoscalers must be at least 1 and slot_idle_seconds 0 (never) or more")
        q = config.lab_qualification
        if q is not None and not (config.build.evaluation_build and q.source_tree == config.build.source_tree):
            raise Q.QualificationRefused("a lab qualification is honoured only by the evaluation build it names")
        d = config.demonstration_qualification
        if d is not None and q is not None:
            raise Q.QualificationRefused("at most one of a lab qualification and a demonstration qualification")
        if d is not None and RELEASE_GATES_PASSED:
            raise Q.QualificationRefused("the release gates have passed: a demonstration qualification is refused "
                                         "(no silent precedence between release and demonstration)")
        if d is not None and not (config.build.evaluation_build and d.source_tree == config.build.source_tree):
            raise Q.QualificationRefused("a demonstration qualification is honoured only by the evaluation build it "
                                         "names")
        if d is not None and d.mounted is None:
            raise Q.QualificationRefused("a demonstration qualification is honoured only from its two mounted files "
                                         "(forecasting.demonstration.load): their bytes are checked at every answer")
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
        self.qualification_metrics = QualificationMetrics(self.qualification_status, D.REFUSALS)
        self.service_metrics = ServiceMetrics(self.metrics)
        self._lock = threading.Lock()                   # one writer of the journal and the epoch states
        self._compacted_s: Optional[int] = None
        self._cluster_uid: Optional[str] = None         # read once it succeeds (a pod never changes cluster)
        self._uppers: Dict = {}                         # (namespace, autoscaler) -> issued bounds of its epoch
        self._costs: Dict = {}                          # (namespace, autoscaler) -> (cache key, costs)
        self._sweep_lock = threading.Lock()             # one check of a full registry at a time
        self._swept_ms: Optional[int] = None            # when the last one started
        self._sweep_after: Optional[Tuple[str, str]] = None   # the last (namespace, autoscaler) a check looked up
        self.cost_replays = 0                           # cost simulations run (each one replays both families)

    # --- status -------------------------------------------------------------------------------------------------

    @property
    def store_healthy(self) -> bool:
        return self.store is not None and self.store.healthy

    def ready(self) -> bool:
        """Readiness: the journal can support issuance. Young history or untrusted models do not make it unready."""
        return self.store_healthy

    def health(self) -> Dict:
        problem = self.store_problem or (self.store.problem if self.store is not None else None)
        q, d = self.config.lab_qualification, self.config.demonstration_qualification
        return {"status": "healthy", "state": "healthy" if self.store_healthy else "unavailable",
                "state_problem": problem, "forecast_contract": FORECAST_CONTRACT,
                "release_gates_passed": RELEASE_GATES_PASSED,
                "build": {"source_tree": self.config.build.source_tree,
                          "evaluation_build": self.config.build.evaluation_build},
                "lab_qualification": None if q is None else q.summary(),
                "demonstration_qualification": None if d is None else {**d.summary(), "withdrawn": d.mount_refusal()},
                "qualification_basis": self.qualification_status()[0]}

    def qualification_status(self) -> Tuple[str, Dict[str, int]]:
        """(the release-gate basis in force for this process now: release, lab, demonstration or none; the mounted
        documents' expiries). A document's scope is checked per request; this is what the metric and /health show.
        A demonstration qualification whose mounted files no longer hold the admitted bytes is withdrawn: none."""
        now_s = self.clock.now_ms() // 1000
        docs = {"lab": self.config.lab_qualification, "demonstration": self.config.demonstration_qualification}
        valid = [k for k, doc in docs.items() if doc is not None and doc.not_before <= now_s < doc.expires
                 and (k != "demonstration" or doc.mount_refusal() is None)]
        basis = "release" if RELEASE_GATES_PASSED else valid[0] if valid else "none"
        return basis, {k: doc.expires for k, doc in docs.items() if doc is not None}

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
        except AutoscalerNotFound as e:
            for ns, name, uid, seen in self.metrics.slots():   # the live lookup's definite 404: it is gone, so the
                if (ns, name) == (e.namespace, e.name) and seen <= now_ms:     # registration it held then is released
                    self._release(ns, name, "not_found", uid=uid, seen_ms=seen)
            raise ServiceError(422, "IdentityMismatch", str(e)) from e
        except (ContractRefused, ProvenanceRefused) as e:
            raise ServiceError(422, "IdentityMismatch", str(e)) from e
        except LookupFailed as e:
            raise ServiceError(503, "IdentityMismatch", f"the autoscaler could not be read: {e}") from e
        sig = bound.signal
        token = self._admit(bound, now_ms)                     # atomic, before any expensive work
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
        if not grid.is_fresh(self.clock.now_ms() / 1000, s.settlement_seconds):
            raise ServiceError(503, "DataUnavailable", "the newest finalized observation is too old")
        with self._lock:
            # _forget (a replacement's or a release's cache cleanup) takes this lock too: a request that passes this
            # check finishes before the cleanup, and one that comes after it issues and caches nothing.
            if not self.metrics.holds(sig.namespace, bound.autoscaler_name, token):
                raise ServiceError(503, "IdentityMismatch", "the autoscaler's registration was replaced or released "
                                   "while this request was in flight; nothing is issued")
            try:
                return self._issue(bound, grid, self.clock.now_ms(), token)   # the time after the history and the lock
            except StoreUnavailable as e:
                raise ServiceError(503, "StateUnavailable", f"the forecast journal failed: {e}") from e
            except StoreError as e:
                # A record the journal refuses (e.g. a state or bundle above its size cap): this autoscaler is
                # unavailable with that reason, nothing is issued, and the error never escapes as a 500.
                raise ServiceError(503, "StateUnavailable", f"the forecast state cannot be journaled: {e}") from e

    def _issue(self, bound: BoundRequest, grid, now_ms: int, token: Optional[int] = None) -> Dict:
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
        key = IssuanceKey(ek, t, t, window.window_end, bound.settings_sha256)
        iss = store.get_issuance(key)
        if iss is None:
            self._refuse_late(bound, now_ms)
            bundle = self._build(bound, grid, state, policy)
            decided = store.write_checkpoint(ek, state, state["last_outcome_seq"])   # the trust transition, durably
            issued_at = self.clock.now_ms()
            self._refuse_late(bound, issued_at)
            bundle.update(issued_at_ms=issued_at, checkpoint_sha256=decided.payload_sha256)
            iss, _ = store.get_or_issue(key, lambda iid: {**bundle, "issuance_id": iid},
                                        models=tuple(sorted(bundle["candidates"])))
        state = self._recover(bound, policy, state)
        store.write_checkpoint(ek, state, state["last_outcome_seq"])
        if self._compacted_s is None or now_s - self._compacted_s >= self.config.compact_every_seconds:
            store.compact(now_s)          # only checkpoint-covered, fully matured windows leave the journal
            self._compacted_s = now_s
        self._export(bound, state, grid, now_s, token)
        return self._response(bound, iss.payload())

    @staticmethod
    def _refuse_late(bound: BoundRequest, now_ms: int) -> None:
        """A new issuance must still be able to act: now + A before the first target (design C2, section 5)."""
        first = bound.window.target_timestamps[0] * 1000
        if now_ms + bound.lead.actuation_allowance_seconds * 1000 >= first:
            raise ServiceError(503, "LeadInsufficient", "too late to issue for this decision origin; the next origin "
                                                        "will be served")

    # --- state ----------------------------------------------------------------------------------------------------

    def _initial(self, ek: str) -> Dict:
        return {"schema": STATE_SCHEMA, "epoch": ek, "last_outcome_seq": 0, "last_issuance_seq": 0, "models": {},
                "scale": None, "trust": {},
                "pairs": {"statistical": T.new_pairs(ek, "statistical", RECIPE_E1, RECIPE_NAIVE)}}

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
            if state.get("schema") != STATE_SCHEMA:
                # A checkpoint of another calibration method: its scores and its trust are never read as this one's.
                raise StoreError(f"the epoch's checkpoint has state schema {state.get('schema')!r}, not {STATE_SCHEMA}")
        state = {**state, "models": {k: dict(v) for k, v in state["models"].items()}, "pairs": dict(state["pairs"])}
        dt = s.sample_interval_seconds
        limits = T.coverage_limits(s.trust, policy.lead_seconds, policy.nominal_bps, len(s.families))
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
                                         nominal_miscoverage=policy.nominal_ratio, limits=limits)
            state["pairs"]["statistical"] = T.note_pair(state["pairs"]["statistical"], rec,
                                                        lead_s=policy.lead_seconds,
                                                        nominal_miscoverage=policy.nominal_ratio)
            state["last_outcome_seq"] = o.seq
        return state

    # --- forecasting and the bundle ---------------------------------------------------------------------------------

    def _fit_for(self, bound: BoundRequest, series: Series, input_end: int, ks: KernelSettings):
        binding = Binding.for_signal(bound.signal, bound.settings, RECIPE_E1)
        try:
            snap = self.artifacts.load_snapshot(binding, ks, input_end=input_end)   # one read: digest and bytes agree
        except ArtifactRefused:
            snap = None
        sha, fit = snap if snap is not None else (None, None)
        due = fit is None or input_end - fit.cutoff >= REFIT_SECONDS
        if due and input_end - series.start + ks.interval_seconds >= ks.min_history_days * DAY:
            if self.config.fit_mode == "inline":
                try:
                    data = encode_e1(fit_e1(series, input_end, ks), binding)
                    self.artifacts.promote(binding, data, ks)
                    sha, fit = self.artifacts.load_snapshot(binding, ks, input_end=input_end)
                except (KernelUnavailable, ArtifactRefused, OSError):
                    pass
            elif self.config.fit_mode == "worker" and self.worker is not None:
                from forecasting.worker import FitRefused, QueueFull
                try:
                    self.worker.submit(binding, RECIPE_E1, fit_payload(series, input_end, ks, binding))
                except (QueueFull, FitRefused):
                    pass
        return fit, sha

    @staticmethod
    def _scale_in_force(state: Dict, series: Series, input_end: int, origin: int, policy) -> Optional[cal.Scale]:
        """The calibration scale for an issuance at `origin` (forecasting.calibration): fitted from the finalized
        history at the first origin that has enough day-lag pairs, refitted at the first origin of each UTC day, and
        kept in the epoch's state in between. A refit without enough pairs keeps the scale in force."""
        cur, day = state.get("scale"), origin // DAY
        if cur is None or cur["params"] is None or cur["day"] != day:
            slots = DAY // series.interval
            levels, residuals = cal.season_pairs(series.values[:series.index(input_end) + 1].tolist(), slots)
            fit = cal.fit_scale(levels, residuals, policy, slots)
            if fit is not None:
                cur = {"day": day, "fit_origin": origin, "pairs": len(levels), "params": fit.to_wire()}
            elif cur is None or cur["params"] is None:
                cur = {"day": day, "fit_origin": None, "pairs": len(levels), "params": None}
            else:
                cur = {**cur, "day": day}
            state["scale"] = cur
        return None if cur["params"] is None else cal.Scale.from_wire(cur["params"])

    def _build(self, bound: BoundRequest, grid, state: Dict, policy) -> Dict:
        now_ms = self.clock.now_ms()
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
        fit, sha = (None, None)
        if "statistical" in self._models(bound):        # families: seasonalNaive only never fits or offers E1
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
        scale = self._scale_in_force(state, series, input_end, t, policy)
        cands, inputs = {}, []
        for model, p in points.items():
            m = self._model_state(state, model, RECIPES[model], policy, bound.evidence_key, dt)
            base = max(p["point_rps"])
            upper, info = cal.upper_bound(m["calibration"], policy, base, t, scale)
            cands[model] = {"recipe": RECIPES[model], "point_rps": [float(x) for x in p["point_rps"]],
                            "base_window_max_rps": base, "upper_window_max_rps": upper, "calibration": info,
                            "fit_cutoff": p["fit_cutoff"], "artifact_sha256": p["artifact_sha256"]}
            inputs.append(T.CandidateInputs(model, m["ledger"], upper is not None, info["n"], policy.min_windows,
                                            input_end - first_obs + dt, T.REQUIRED_HISTORY_SECONDS[model], True,
                                            None if p["fit_cutoff"] is None else input_end - p["fit_cutoff"]))
        gate = self._release_gate(bound, now_ms // 1000, window.valid_until(lead))
        policy_sha, costs = self._policy_costs(bound, state, now_ms // 1000, points)
        g = T.Global(now_s=now_ms // 1000, lead_seconds=lead.lead_seconds, nominal_miscoverage=policy.nominal_ratio,
                     identity_ok=True, store_healthy=self.store_healthy, replay_suite_passed=gate["passed"],
                     startup_budget_valid=True, policy_sha256=policy_sha,
                     outcome_deadline_seconds=s.settlement_seconds + s.outcome_grace_seconds,
                     families=len(s.families),
                     configuration_validated=validated_profile(s, lead.lead_seconds) is not None)
        pairs = state["pairs"] if "seasonal_naive" in points else {}   # paired evidence needs its naive baseline
        verdict, new_h = T.evaluate(inputs, state["trust"], g, s.trust, pairs=pairs, costs=costs)
        if gate["basis"] == "demonstration" and verdict.selected is not None:
            # A demonstration covers only the families it lists, and the family is known only now: decide the gate
            # again for the selected one. Outside the list this forecast has no release gate at all, so the verdict is
            # the one of a service without a qualification (selection and hysteresis never read the gate).
            decided = self._release_gate(bound, now_ms // 1000, window.valid_until(lead), verdict.selected)
            if not decided["passed"]:
                gate = decided
                verdict, new_h = T.evaluate(inputs, state["trust"], replace(g, replay_suite_passed=False), s.trust,
                                            pairs=pairs, costs=costs)
        state["trust"] = new_h
        through = max(at for at, v in zip(grid.received_at_ms, grid.values) if v is not None)   # actual receipts
        return {"schema": "forecast-issuance/v1", "evidence_mode": "prospective",
                "forecast_contract": FORECAST_CONTRACT,
                "evidence_key": bound.evidence_key, "settings_sha256": bound.settings_sha256,
                "decision_origin": t, "window_start": window.window_start, "window_end": window.window_end,
                "target_timestamps": list(targets), "sample_interval_seconds": dt,
                "lead_seconds": lead.lead_seconds, "startup_delay_seconds": lead.startup_delay_seconds,
                "actuation_allowance_seconds": lead.actuation_allowance_seconds,
                "valid_until": window.valid_until(lead), "input_end": input_end,
                "input_received_through_ms": through, "nominal_miscoverage_bps": policy.nominal_bps,
                "candidates": cands, "selected": verdict.selected, "release_gate": gate,
                "calibration_scale": {k: state["scale"][k] for k in ("fit_origin", "pairs")},
                "policy_sha256": policy_sha,
                "costs": {m: {"shortage": v["shortage"], "replica_time": v["replica_time"],
                              "window": v["binding"]["window"], "paired_decisions": v["paired_decisions"],
                              "decisions": v["decisions"], "unknown_truth_seconds": v["unknown_truth_seconds"],
                              "seconds": v["seconds"]}
                          for m, v in costs.items()},
                "verdict": {"phase": verdict.phase, "reason": verdict.reason,
                            "prediction_ready": verdict.prediction_ready,
                            "candidates": {m: {"trusted": v.trusted, "phase": v.phase, "reason": v.reason,
                                               "coverage_gate": v.coverage_gate, "availability": v.availability,
                                               "prospective_gate_bounds": v.prospective_gate_bounds,
                                               "coverage_failing": list(v.coverage_failing),
                                               "evidence_cutoff":
                                                   state["models"][m]["ledger"]["last_prospective_outcome_s"]}
                                           for m, v in verdict.candidates.items()}}}

    def _authorized(self, bound: BoundRequest, b: Dict) -> bool:
        """The bundle's release-gate basis still holds NOW, for this request: the same basis and the same qualification
        digest, re-checked on every answer (an issuance stored under a lab qualification is not eligible once it has
        expired, been removed or replaced). Under a demonstration qualification the same holds for its digest and its
        contract's, and the bundle's SELECTED family must be one the document lists: this is where eligibility is
        finally decided, for a first answer and for every stored one."""
        issued = b.get("release_gate") or {}
        if issued.get("passed") is not True or not b.get("selected"):
            return False
        now = self._release_gate(bound, self.clock.now_ms() // 1000, b["valid_until"], b["selected"])
        return now["passed"] and all(now.get(k) == issued.get(k) for k in _GATE_IDENTITY)

    def _response(self, bound: BoundRequest, b: Dict) -> Dict:
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
            calibration_epoch=bound.evidence_key, checkpoint_sha256=b["checkpoint_sha256"],
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
            predictive_eligible=bool(v["prediction_ready"] and b["selected"] and c["upper_window_max_rps"] is not None
                                     and self._authorized(bound, b)),
            evidence_cutoff=cv["evidence_cutoff"])
        check_response(r, bound)
        return r.to_wire()

    def _export(self, bound: BoundRequest, state: Dict, grid, now_s: int, token: Optional[int] = None) -> None:
        trusted = {m: h.get("trusted", False) for m, h in state["trust"].items() if m in RECIPES}
        failures = dict(self.worker.failures) if self.worker is not None else {}
        models = {}
        first = next((ts for ts, v in zip(grid.timestamps, grid.values) if v is not None), now_s)
        for model, m in state["models"].items():
            models[model] = model_samples(ledger=m["ledger"], calibration=m["calibration"],
                                          nominal_miscoverage=bound.settings.risk.miscoverage_bps / 10_000,
                                          trusted=trusted.get(model, False), history_seconds=max(0, now_s - first),
                                          now_s=now_s, fit_failures=failures if model == "statistical" else None)
        self.metrics.update(bound.signal.namespace, bound.autoscaler_name,
                            Snapshot(bound.signal.autoscaler_uid, models), token)

    # --- registrations ----------------------------------------------------------------------------------------------

    def _admit(self, bound: BoundRequest, now_ms: int) -> int:
        """Register the request's autoscaler and return its registration token, or refuse it at the limit (503
        AutoscalerLimitReached). Registrations idle for slot_idle_seconds are released first; a full registry is then
        checked against the API once (_sweep_slots). A registration held by another UID becomes the new object's, with
        nothing carried over."""
        ns, name, uid = bound.signal.namespace, bound.autoscaler_name, bound.signal.autoscaler_uid
        idle_ms = self.config.slot_idle_seconds * 1000
        if idle_ms:
            for s_ns, s_name, s_uid, seen in self.metrics.slots():
                if (s_ns, s_name) != (ns, name) and now_ms - seen >= idle_ms:
                    self._release(s_ns, s_name, "idle", uid=s_uid, seen_ms=seen)
        try:
            held = self._reserve(ns, name, uid, now_ms)
        except RegistryFull:
            try:
                if not self._sweep_slots(now_ms):
                    raise
                held = self._reserve(ns, name, uid, now_ms)
            except RegistryFull as e:
                self.metrics.count_refusal()
                raise ServiceError(503, "AutoscalerLimitReached", f"the service's registration limit is reached: {e}; "
                                   "a registration is released when its autoscaler is deleted, replaced or idle") from e
        if held.replaced is not None:
            self._forget(ns, name)
            logger.info("autoscaler %s/%s was replaced (UID %s, now %s): registered as a new identity", ns, name,
                        held.replaced, uid)
        return held.token

    def _reserve(self, ns: str, name: str, uid: str, now_ms: int):
        """Reserve the name for this request's UID. When it is registered to ANOTHER UID, the identity this request was
        bound with may be the stale one: its lookup can be older than the registration (the object was replaced and
        the new one registered while this request waited). So the live object is read again, and the registration is
        replaced only if the live UID still is this request's, and only the registration that was seen (compare and
        swap). A request whose UID is no longer the live object's is refused and changes nothing."""
        replacing = None
        for _ in range(3):
            try:
                return self.metrics.reserve(ns, name, uid, now_ms, replacing=replacing)
            except UidConflict as e:
                try:
                    pa = self.reader.get(f"/apis/{API_GROUP}/{API_VERSION}/namespaces/{ns}/predictiveautoscalers/"
                                         f"{name}")
                except LookupFailed as lf:
                    raise ServiceError(503, "IdentityMismatch", "the autoscaler could not be read again to decide "
                                       f"which object holds its registration: {lf}") from lf
                meta = pa.get("metadata") if isinstance(pa, dict) else None
                live = meta.get("uid") if isinstance(meta, dict) else None
                if live != uid:
                    raise ServiceError(422, "IdentityMismatch", "the autoscaler was replaced while this request was "
                                       "in flight (its UID is no longer the live object's); nothing is changed") from e
                replacing = e.held
        raise ServiceError(503, "IdentityMismatch", "the autoscaler's registration changed repeatedly while this "
                           "request was admitted; retry")

    def _sweep_slots(self, now_ms: int) -> bool:
        """The registry is full: look up the registered autoscalers and release those the API says are gone (a
        definite 404) or replaced (another UID). A lookup that could not be completed (timeout, 5xx, 401/403) or an
        unreadable object proves nothing and keeps its registration. One check at a time, at most one per
        SLOT_SWEEP_SECONDS, and none starts a lookup after SLOT_SWEEP_BUDGET_SECONDS: it goes in name order and the
        next check continues after the last name this one looked up, so every registration is reached within a
        bounded number of checks however slow the lookups are. True when a registration was released."""
        if not self._sweep_lock.acquire(blocking=False):
            return False
        try:
            if self._swept_ms is not None and now_ms - self._swept_ms < SLOT_SWEEP_SECONDS * 1000:
                return False
            self._swept_ms = now_ms
            started, freed = time.monotonic(), False
            slots = sorted(self.metrics.slots(), key=lambda slot: slot[:2])
            if self._sweep_after is not None:                   # continue after the last name looked up, then wrap
                slots = [x for x in slots if x[:2] > self._sweep_after] + \
                        [x for x in slots if x[:2] <= self._sweep_after]
            for ns, name, uid, seen in slots:
                if time.monotonic() - started > SLOT_SWEEP_BUDGET_SECONDS:
                    break
                self._sweep_after = (ns, name)
                try:
                    pa = self.reader.get(f"/apis/{API_GROUP}/{API_VERSION}/namespaces/{ns}/predictiveautoscalers/"
                                         f"{name}")
                except LookupFailed:
                    continue
                if pa is None:
                    reason = "not_found"
                else:
                    meta = pa.get("metadata") if isinstance(pa, dict) else None
                    live = meta.get("uid") if isinstance(meta, dict) else None
                    if not (isinstance(live, str) and live and uid and live != uid):
                        continue
                    reason = "replaced"
                freed = self._release(ns, name, reason, uid=uid, seen_ms=seen) or freed
            return freed
        finally:
            self._sweep_lock.release()

    def _release(self, namespace: str, autoscaler: str, reason: str, *, uid: Optional[str] = None,
                 seen_ms: Optional[int] = None) -> bool:
        """Free an autoscaler's registration and series (with uid/seen_ms: only if it still is that identity, with no
        request since). Its journal epoch simply stops growing: nothing durable is deleted."""
        if not self.metrics.release(namespace, autoscaler, reason, uid=uid, seen_ms=seen_ms):
            return False
        self._forget(namespace, autoscaler)
        logger.info("released the registration of autoscaler %s/%s (%s); its journal evidence is kept", namespace,
                    autoscaler, reason)
        return True

    def _forget(self, namespace: str, autoscaler: str) -> None:
        with self._lock:
            self._uppers.pop((namespace, autoscaler), None)
            self._costs.pop((namespace, autoscaler), None)

    # --- families, the release gate and same-policy costs -------------------------------------------------------------

    @staticmethod
    def _models(bound: BoundRequest) -> tuple:
        return tuple(FAMILY_MODELS[f] for f in bound.settings.families)

    def _release_gate(self, bound: BoundRequest, now_s: int, valid_until: int, selected: Optional[str] = None) -> Dict:
        """The release gate's basis for this request: the product release (RELEASE_GATES_PASSED), a lab qualification
        whose scope covers the live cluster, namespace and target right now and that is still valid at the answer's
        valid_until, or none. Only Global.replay_suite_passed reads it; every runtime gate is evaluated as always.
        A demonstration qualification is the only basis that reads `selected` (the selected model family, once known):
        see _demonstration_gate."""
        q = self.config.lab_qualification
        d = self.config.demonstration_qualification
        if d is not None:
            return self._demonstration_gate(d, bound, now_s, valid_until, selected)
        if RELEASE_GATES_PASSED:
            return {"passed": True, "basis": "release", "lab_qualification_sha256": None}
        if q is None:
            return {"passed": False, "basis": None, "lab_qualification_sha256": None}
        if self._cluster_uid is None:
            self._cluster_uid = Q.cluster_uid(self.reader)
        sig = bound.signal
        ok = q.covers(self._cluster_uid, bound.autoscaler_namespace, sig.namespace, sig.name, sig.target_uid, now_s) \
            and valid_until <= q.expires
        return {"passed": ok, "basis": "lab" if ok else None, "lab_qualification_sha256": q.sha256}

    def _demonstration_gate(self, d: D.DemonstrationQualification, bound: BoundRequest, now_s: int, valid_until: int,
                            selected: Optional[str]) -> Dict:
        """The gate under a demonstration qualification: passed only when both mounted files still hold the admitted
        bytes (read and hashed now, at every call), the live cluster, namespace and target are in its scope, it is
        valid now and at the answer's valid_until, the live autoscaler is configured as its contract says (families,
        cadence, resolved settings fingerprint, replica bounds, capacity) and, once a family is selected, that family
        is listed. The record names the document and its contract and, when it did not pass, the first reason."""
        if self._cluster_uid is None:
            self._cluster_uid = Q.cluster_uid(self.reader)
        sig = bound.signal
        why = "release" if RELEASE_GATES_PASSED else d.mount_refusal() or d.refusal(
            cluster_uid=self._cluster_uid, autoscaler_namespace=bound.autoscaler_namespace, namespace=sig.namespace,
            name=sig.name, uid=sig.target_uid, now_s=now_s, valid_until=valid_until, spec=bound.spec,
            families=bound.settings.families, sample_interval_seconds=bound.settings.sample_interval_seconds,
            settings_sha256=bound.settings.fingerprint(), selected=selected)
        if why is not None:
            self.qualification_metrics.refused(why)
        return {"passed": why is None, "basis": "demonstration" if why is None else None,
                "lab_qualification_sha256": None, "demonstration_qualification_sha256": d.sha256,
                "demonstration_contract_sha256": d.contract_sha256, "refused": why}

    def _issued_uppers(self, bound: BoundRequest, lo: int) -> Dict[int, tuple]:
        """{decision origin: (naive bound, E1 bound, input_end, seq)} of the epoch's issuances, folded incrementally from
        the journal and kept for COST_HISTORY_SECONDS. input_end is the newest observation the issuance was computed
        from: what the forecaster had received at that decision. Callers use only the entries the journal still holds
        (_retained), so a long-running process and a restarted one replay exactly the same inputs."""
        scope, ek = (bound.autoscaler_namespace, bound.autoscaler_name), bound.evidence_key
        u = self._uppers.get(scope)
        if u is None or u["epoch"] != ek:
            u = self._uppers[scope] = {"epoch": ek, "seq": 0, "by_origin": {}}
        for iss in self.store.issuances_after(ek, u["seq"]):
            b = iss.payload()
            pick = {m: candidate(b, m).get("upper_window_max_rps") for m in iss.models}
            u["by_origin"][iss.key.decision_origin] = (pick.get("seasonal_naive"), pick.get("statistical"),
                                                       b.get("input_end"), iss.seq)
            u["seq"] = iss.seq
        u["by_origin"] = {o: v for o, v in u["by_origin"].items() if o >= lo}
        return u["by_origin"]

    def _policy_costs(self, bound: BoundRequest, state: Dict, now_s: int,
                      points: Mapping) -> Tuple[Optional[str], Dict]:
        """(the fingerprint of this autoscaler's v2 policy or None, {"statistical": bound paired costs} or {}).
        Costs are computed only when they can matter: both families forecast now and the retained pairs already meet
        the count and span the promotion test requires. Missing capacity or bounds: no policy, so no promotion."""
        params, _why = P.resolve_policy(bound.spec, bound.settings, bound.lead)
        if params is None:
            return None, {}
        sha = params.fingerprint()
        t, pairs = bound.settings.trust, state["pairs"]["statistical"]
        if not {"seasonal_naive", "statistical"} <= set(points):
            return sha, {}
        ps = T.pair_sums(pairs, now_s)
        if ps["n"] < t.challenger_min_pairs or ps["span"] < t.challenger_min_hours * 3600:
            return sha, {}
        scope, ek = (bound.autoscaler_namespace, bound.autoscaler_name), bound.evidence_key
        offset = bound.window.window_end - bound.decision_origin
        c, dt = params.decision_interval_seconds, bound.settings.sample_interval_seconds
        start, end = ps["first"] - offset, ps["last"] - offset
        cached_uppers = self._issued_uppers(bound, now_s - COST_HISTORY_SECONDS)
        retained = self.store.issuance_seqs(ek, start, end)
        uppers = {o: u for o, u in cached_uppers.items() if retained.get(o) == u[3]}   # what the journal holds now
        lo = min([start] + [u[2] for u in uppers.values() if u[2] is not None])
        obs = {ts: v for ts, v, _at in self.store.observations("signal:" + ek, lo, end + c)}   # first receipts
        # The controller's input at decision o: the newest observation the issuance at o was computed from (it had
        # been received by then); no issuance, no measurement (the operator holds). The truth only scores.
        controller = {o: obs[u[2]] for o, u in uppers.items() if u[2] in obs}
        truth = {ts: v for ts, v in obs.items() if ts >= start}
        inputs = {"challenger_upper": {o: u[1] for o, u in uppers.items() if u[1] is not None},
                  "baseline_upper": {o: u[0] for o, u in uppers.items() if u[0] is not None},
                  "controller_rps": controller, "truth_rps": truth}
        # The cache is bound to EVERY replay input (bounds, controller inputs, truth, policy, window), so a late truth
        # receipt, a compaction or a restart can never leave stale costs: equal inputs give equal costs.
        digest = sha256_hex(canonical_json({k: sorted(v.items()) for k, v in inputs.items()}))
        key = (ek, sha, start, end, offset, digest)
        cached = self._costs.get(scope)
        if cached is not None and cached[0] == key:
            return sha, cached[1]
        costs = {"statistical": P.paired_costs(
            epoch=ek, challenger=("statistical", RECIPE_E1), baseline=("seasonal_naive", RECIPE_NAIVE), params=params,
            start=start, end=end, interval=dt, window_offset=offset, **inputs)}
        self.cost_replays += 1
        self._costs[scope] = (key, costs)
        return sha, costs

