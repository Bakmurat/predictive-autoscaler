from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import logging
import os
import json
import math
import sys
import gc
import asyncio
import functools
import copy
import hashlib
import time
import concurrent.futures
import threading
import subprocess
import psutil
import tensorflow as tf
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response
import joblib
from pathlib import Path

# Add parent directory to path to import models
sys.path.append(str(Path(__file__).parent.parent))

from models.lstm_model import LSTMForecastModel, asymmetric_mse  # noqa: F401 — registers custom loss for keras model loading
from data.victoriametrics_collector import VictoriaMetricsCollector
from api.accuracy import AccuracyTracker
from api.auth import AuthMiddleware, TokenReviewer, config_from_env
from api.identity import CONTRACT, KubeReader, LookupFailed, ProvenanceRefused, resolve_signal
from api.registry import ModelIncompatible, ModelRegistry, artifact_paths, load_record, model_key as registry_key
from data.history import HistoryRefused, HistoryUnavailable, query_history
from api.seasonal_experiment import SeasonalExperiment
from api.ensemble_experiment import EnsembleExperiment
from models import seasonal_ensemble

# Set up logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Prometheus metrics
PREDICTION_REQUESTS = Counter('ml_api_prediction_requests_total', 'Total prediction requests', ['application'])
PREDICTION_DURATION = Histogram('ml_api_prediction_duration_seconds', 'Time spent on predictions')
PREDICTION_ERRORS = Counter('ml_api_prediction_errors_total', 'Total prediction errors', ['error_type'])
MAPE_GAUGE = Gauge(
    'predictive_autoscaler_mape',
    'Traffic-weighted MAPE of predictions (rolling 24h)',
    ['application', 'namespace', 'metric_type']
)
ACCURACY_SCORED_GAUGE = Gauge(
    'predictive_autoscaler_accuracy_scored_forecasts',
    'Number of matured forecasts that entered the MAPE/MAE calculation (C-85: an error figure '
    'without this count cannot be told from "nothing measured")',
    ['application', 'namespace', 'metric_type']
)
COMPONENT_SCORED_GAUGE = Gauge(
    'predictive_autoscaler_component_accuracy_scored_forecasts',
    'Number of matured forecasts that entered the per-component MAPE calculation (C-85)',
    ['application', 'namespace', 'component']
)
MAE_GAUGE = Gauge(
    'predictive_autoscaler_mae',
    'Mean Absolute Error of predictions (rolling 24h)',
    ['application', 'namespace', 'metric_type']
)
MODEL_AGE_GAUGE = Gauge(
    'predictive_autoscaler_model_age_hours',
    'Age of the loaded model in hours',
    ['application', 'metric_type']
)
RSS_BYTES_GAUGE = Gauge(
    'predictive_autoscaler_ml_api_rss_bytes',
    'ML API process RSS memory in bytes'
)

PREDICTION_RPM_GAUGE = Gauge(
    'ml_api_prediction_rpm',
    'Per-component per-step RPM prediction value',
    ['application', 'namespace', 'component', 'step']
)

FLOOR_PCT_GAUGE = Gauge(
    'ml_api_floor_pct',
    'Safety floor percentage currently applied to predictions',
    ['application', 'namespace']
)

ENSEMBLE_MARGIN_GAUGE = Gauge(
    'ml_api_ensemble_margin_rpm',
    'capacity margin (the experiment\'s error quantile, q90 or q95) added to every served step of a seasonal-ensemble arm',
    ['application', 'namespace']
)

ENSEMBLE_MARGIN_SAMPLES_GAUGE = Gauge(
    'ml_api_ensemble_margin_samples',
    'Matured lead-window errors behind the seasonal-ensemble margin (zero margin below 30)',
    ['application', 'namespace']
)

COMPONENT_MAPE_GAUGE = Gauge(
    'ml_api_component_mape',
    'Per-component rolling MAPE (24h window)',
    ['application', 'namespace', 'component']
)

app = FastAPI(
    title="Predictive Autoscaler ML API",
    description="LSTM-based Machine Learning API for Kubernetes Predictive Autoscaling",
    version="3.8.0"
)

# Caller authentication (B5e): with FORECASTER_AUTH=tokenreview, protected paths need the operator's projected service
# account token, checked with TokenReview before the body is read. An unsafe configuration stops the service here.
auth_config = config_from_env()
if auth_config.enabled:
    app.add_middleware(AuthMiddleware, config=auth_config, reviewer=TokenReviewer(auth_config))

class LSTMPredictor:
    """LSTM-based predictor using real neural network models."""

    # Models older than this will be retrained
    MODEL_MAX_AGE_HOURS = 6

    def __init__(self):
        raw_experiment = os.getenv("SEASONAL_EXPERIMENT")
        self.seasonal_experiment = (SeasonalExperiment.parse(raw_experiment)
                                    if raw_experiment is not None else None)
        self.lstm_model = LSTMForecastModel(sequence_length=144)
        self.trained_models = {}
        self.model_train_times = {}  # Track when each model was trained
        self.model_file_mtimes = {}  # Track file mtime for disk reload detection
        self.model_meta = {}  # model_key -> provenance sidecar (lstm_<key>.meta.json) written by the trainer
        # C-47: a served model object carries per-request mutable state (last_sequence, input_timestamps,
        # seasonal_history); its registry record's lock serialises mutate-then-predict (api/registry.py).
        try:
            from data import gapfill as _gf
            self.validity_mask = _gf.load_mask(os.getenv("VALIDITY_MASK"))
        except Exception as e:
            logger.warning(f"validity mask not loaded: {e}")
            self.validity_mask = {"version": 0, "intervals": []}
        self.validation_metadata = {}  # model_key -> {status, mape, old_mape, timestamp}
        self._training_locks = {}  # Per-model locks to prevent concurrent training
        # Check for MODEL_DIR env var, then try container path, then local paths
        model_dir_env = os.getenv("MODEL_DIR")
        if model_dir_env:
            self.model_dir = Path(model_dir_env)
        elif os.path.exists("/app/models/trained"):
            self.model_dir = Path("/app/models/trained")
        else:
            # Local development - check both possible locations
            local_trained = Path(__file__).parent.parent / "models" / "trained"
            local_training = Path(__file__).parent.parent / "training" / "models"
            self.model_dir = local_trained if local_trained.exists() else local_training

        self.model_dir.mkdir(parents=True, exist_ok=True)

        # Serving models are validated, pinned records (api/registry.py): identity, query hash, contract, autoscaler
        # and target UIDs, and the digest of the exact bytes loaded. The dicts above are a sandbox used only by
        # train_on_data (tests, offline experiments); serving never reads them.
        self.registry = ModelRegistry(cleanup=lambda rec: self._release_model_object(rec.model))
        self.record_stamps = {}   # key -> (artifact mtime, sidecar mtime) of the last DETERMINISTIC load outcome
        self._reload_locks = {}   # key -> lock: one reload per key at a time
        self._reload_guard = threading.Lock()
        self._reload_retry = {}   # key -> (not_before, backoff_s, stamp) after a transient load failure

        # Try to load pre-trained models
        self._load_pretrained_models()

        # Set initial RSS gauge after model loading
        RSS_BYTES_GAUGE.set(self._get_rss_bytes())

        # Cold start: no compatible model, trigger the training CronJob once. Only on explicit opt-in
        # (COLD_START_CRONJOB names the CronJob, set by the deployment manifest): an unset variable never reaches a
        # cluster, so a test or a local run cannot create Jobs wherever the developer's kubeconfig points.
        has_requests_model = bool(self.registry.snapshot())
        cold_start_cronjob = os.getenv("COLD_START_CRONJOB", "")
        if not has_requests_model and cold_start_cronjob:
            logger.info("No compatible model found, triggering cold-start training job")
            try:
                result = subprocess.run(
                    ["kubectl", "create", "job", f"--from=cronjob/{cold_start_cronjob}",
                     f"{cold_start_cronjob}-coldstart", "-n", os.getenv("POD_NAMESPACE", "ml-engine")],
                    capture_output=True, text=True, timeout=30
                )
                if result.returncode == 0:
                    logger.info(f"Cold-start job triggered successfully: {result.stdout.strip()}")
                else:
                    logger.warning(f"Cold-start job trigger failed (non-fatal): {result.stderr.strip()}")
            except Exception as e:
                logger.warning(f"Cold-start job trigger error (non-fatal): {e}")
    
    def _load_pretrained_models(self):
        """Load every compatible artifact into the registry. Artifacts without the metric-contract provenance
        (legacy `lstm_<app>_<metric>.pkl`, a missing or inconsistent sidecar) are not loaded: retrain."""
        try:
            model_files = sorted(self.model_dir.glob("lstm_*.pkl"))
        except Exception as e:
            logger.info(f"No pre-trained models found: {e}")
            return
        logger.info(f"Found {len(model_files)} model artifact(s) in {self.model_dir}")
        for path in model_files:
            stem = path.name[len("lstm_"):-len(".pkl")]
            if len(stem) == 32 and all(c in "0123456789abcdef" for c in stem):
                self._reload_if_changed(stem)
            else:
                logger.warning(f"Model artifact {path.name} not loaded: legacy name without the metric-contract "
                               f"provenance (retrain)")

    RELOAD_BACKOFF_S = (15.0, 300.0)   # first and maximum wait after a transient load failure

    def _reload_if_changed(self, key: str) -> None:
        """Load key's artifact into the registry when the artifact or its sidecar changed.

        One reload per key at a time: a concurrent caller does not wait, it serves the incumbent. A load whose files
        changed while it was loading is discarded (superseded), never installed over a newer pair. The record is
        validated before it replaces the incumbent (a failed load keeps the incumbent serving); a replaced record is
        released only after its last in-flight user unpins it."""
        with self._reload_guard:
            lock = self._reload_locks.setdefault(key, threading.Lock())
        if not lock.acquire(blocking=False):
            return
        try:
            self._reload_locked(key)
        finally:
            lock.release()

    def _artifact_stamp(self, key: str):
        pkl, meta = artifact_paths(self.model_dir, key)
        try:
            return pkl, (pkl.stat().st_mtime_ns, meta.stat().st_mtime_ns)
        except FileNotFoundError:
            return pkl, None

    def _reload_locked(self, key: str) -> None:
        pkl, stamp = self._artifact_stamp(key)
        if stamp is None or self.record_stamps.get(key) == stamp:
            return
        retry = self._reload_retry.get(key)
        if retry and retry[2] == stamp and time.monotonic() < retry[0]:
            return   # backing off a transient failure of this same pair
        try:
            rec = load_record(pkl)
        except ModelIncompatible as e:
            self.record_stamps[key] = stamp   # deterministic for this pair: retried when either file changes
            self._reload_retry.pop(key, None)
            logger.warning(f"Model artifact not loaded: {e}")
            return
        except Exception as e:
            first, cap = self.RELOAD_BACKOFF_S
            backoff = min(cap, retry[1] * 2) if retry and retry[2] == stamp else first
            self._reload_retry[key] = (time.monotonic() + backoff, backoff, stamp)
            logger.warning(f"Failed to load {pkl.name}: {e}; retrying in {backoff:.0f} s, the incumbent (if any) "
                           f"keeps serving")
            return
        if self._artifact_stamp(key)[1] != stamp:
            logger.info(f"{pkl.name} changed while it was loading; the newer pair is loaded on the next call")
            return
        self.record_stamps[key] = stamp
        self._reload_retry.pop(key, None)
        if LSTMForecastModel._is_old_model_format(rec.model):
            logger.warning(f"Old Dense(1) model in {pkl.name}: not loaded (retrain)")
            return
        self.registry.install(rec)
        RSS_BYTES_GAUGE.set(self._get_rss_bytes())
        logger.info(f"Loaded model {rec.namespace}/{rec.name} ({rec.key}): sha256={rec.artifact_sha256[:12]}, "
                    f"query={rec.meta['metric_query_sha256'][:12]}, cutoff={rec.meta.get('training_cutoff', 'unknown')}")

    def _get_rss_bytes(self) -> int:
        """Get current process RSS in bytes."""
        return psutil.Process().memory_info().rss

    def _cleanup_old_model(self, model_key: str):
        """Clean up old model memory before replacement.

        Sequence per user decision:
        1. del model.model (Keras Sequential -- dereferences TF graph objects)
        2. del self.trained_models[key] (LSTMForecastModel wrapper)
        3. tf.keras.backend.clear_session() (clears TF session -- cheap no-op if nothing)
        4. gc.collect() (force garbage collection of dereferenced objects)
        """
        rss_before = self._get_rss_bytes()

        old_model = self.trained_models.get(model_key)
        if old_model is not None:
            # Step 1: Delete the Keras Sequential model (TF graph objects)
            if hasattr(old_model, 'model') and old_model.model is not None:
                del old_model.model
            # Step 2: Remove from dict (drops LSTMForecastModel wrapper)
            del self.trained_models[model_key]

        # Step 3: Clear TF session (always safe -- no-op if nothing to clear)
        tf.keras.backend.clear_session()

        # Step 4: Force garbage collection
        gc.collect()

        rss_after = self._get_rss_bytes()
        RSS_BYTES_GAUGE.set(rss_after)
        logger.info(f"Cleaned up model {model_key}: RSS {rss_before / 1024 / 1024:.1f}MB -> "
                    f"{rss_after / 1024 / 1024:.1f}MB (delta: {(rss_after - rss_before) / 1024 / 1024:+.1f}MB)")

    def _release_model_object(self, detached) -> None:
        """Free a model object that has ALREADY been swapped out of the registry (C-47).

        _cleanup_old_model() frees whatever the registry currently holds, so it must never run
        before a replacement is proven loadable. This variant takes the detached incumbent, so
        the swap happens first and the service is never left without a model.
        """
        rss_before = self._get_rss_bytes()
        if detached is not None:
            if hasattr(detached, 'model') and detached.model is not None:
                del detached.model
            del detached
        # No tf.keras.backend.clear_session() here: it resets process-wide Keras/TF state that the other served
        # records still use. Dropping the references and collecting is the per-record part.
        gc.collect()
        rss_after = self._get_rss_bytes()
        RSS_BYTES_GAUGE.set(rss_after)
        logger.info(f"Released the previous model object: RSS {rss_before / 1024 / 1024:.1f}MB -> "
                    f"{rss_after / 1024 / 1024:.1f}MB (delta: {(rss_after - rss_before) / 1024 / 1024:+.1f}MB)")

    def _is_model_stale(self, model_key: str) -> bool:
        """Check if a model needs retraining."""
        if model_key not in self.model_train_times:
            return True
        age = (datetime.utcnow() - self.model_train_times[model_key]).total_seconds() / 3600
        is_stale = age > self.MODEL_MAX_AGE_HOURS
        if is_stale:
            logger.info(f"Model {model_key} is stale (age: {age:.1f}h > {self.MODEL_MAX_AGE_HOURS}h)")
        return is_stale
    
    def _get_model_age_hours(self, model_key: str) -> float:
        """Get the age of a loaded model in hours."""
        if model_key in self.model_train_times:
            return (datetime.utcnow() - self.model_train_times[model_key]).total_seconds() / 3600
        return -1.0

    def _update_validation_metadata(self, model_key, status, new_mape, old_mape=None, age_hours=None,
                                    decay_factor=None, scored=None):
        """Update validation metadata for a model after accept/reject decision."""
        self.validation_metadata[model_key] = {
            "status": status,
            "mape": round(new_mape, 2),
            "scored": scored,  # C-85: how many holdout sequences the validation mape rests on
            "old_mape": round(old_mape, 2) if old_mape is not None else None,
            "age_hours": round(age_hours, 1) if age_hours is not None else None,
            "decay_factor": round(decay_factor, 3) if decay_factor is not None else None,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def train_on_data(self, application: str, metric_data: List[Dict], metric_type: str = "cpu") -> Dict:
        """Train LSTM model on provided historical data."""
        try:
            # Convert metric data to DataFrame
            df = pd.DataFrame(metric_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            df.set_index('timestamp', inplace=True)

            # Ensure we have enough data (need at least sequence_length + some)
            min_points = 144 + 50  # sequence_length + buffer
            if len(df) < min_points:
                raise ValueError(f"Insufficient data: {len(df)} points (minimum {min_points} required)")

            # Create a fresh model with the correct sequence length
            new_model = LSTMForecastModel(sequence_length=144)

            # Train the model
            logger.info(f"Training LSTM on {len(df)} data points for {application}/{metric_type} "
                        f"(value range: {df['value'].min():.0f} - {df['value'].max():.0f})")
            training_result = new_model.train(df, target_column='value', epochs=50)

            # --- Validation gate: compare new model against existing ---
            model_key = f"{application}_{metric_type}"

            # Evaluate new model on holdout data
            holdout_start = int(0.8 * len(df))
            holdout_df = df.iloc[holdout_start:]
            new_metrics = new_model.evaluate(holdout_df, target_column='value')
            new_mape = new_metrics['mape']
            new_scored = new_metrics.get('sequences_scored')  # C-85

            old_mape = None
            age_h = None       # set below if existing model
            decay_factor = None  # set below if age-decay path (not 48h bypass)
            if model_key in self.trained_models:
                old_model = self.trained_models[model_key]
                old_metrics = old_model.evaluate(holdout_df, target_column='value')
                old_mape = old_metrics['mape']

                # Guard: if holdout is too small (inf mape), skip validation
                if old_mape == float('inf') or new_mape == float('inf'):
                    logger.warning(f"Holdout too small for {model_key}, skipping validation "
                                   f"(old_mape={old_mape}, new_mape={new_mape})")
                else:
                    # Compute age-decay threshold
                    age_hours = self._get_model_age_hours(model_key)
                    age_h = max(age_hours, 0)

                    # 48h staleness bypass: accept any retrain with MAPE < 100%
                    if age_h >= 48:
                        if new_mape >= 100:
                            logger.info(f"Model {model_key} rejected: new MAPE={new_mape:.2f}% >= 100% sanity floor "
                                        f"(age={age_h:.1f}h, staleness bypass denied)")
                            self._update_validation_metadata(model_key, "rejected", new_mape, old_mape,
                                                             age_hours=age_h, decay_factor=None, scored=new_scored)
                            self.model_train_times[model_key] = datetime.utcnow()
                            return training_result
                        logger.info(f"Model {model_key} accepted (staleness bypass): new MAPE={new_mape:.2f}% "
                                    f"(age={age_h:.1f}h, old MAPE={old_mape:.2f}%)")
                        # Fall through to model promotion below
                    else:
                        # Age-decay formula: threshold relaxes as model ages
                        decay_factor = 1.05 + 0.01 * age_h
                        threshold = old_mape * decay_factor
                        if new_mape > threshold:
                            logger.info(f"Model {model_key} rejected: new MAPE={new_mape:.2f}% > "
                                        f"threshold={threshold:.2f}% (old MAPE={old_mape:.2f}%, "
                                        f"age={age_h:.1f}h, decay_factor={decay_factor:.3f})")
                            self._update_validation_metadata(model_key, "rejected", new_mape, old_mape,
                                                             age_hours=age_h, decay_factor=decay_factor, scored=new_scored)
                            self.model_train_times[model_key] = datetime.utcnow()
                            return training_result
                        logger.info(f"Model {model_key} accepted: new MAPE={new_mape:.2f}% <= "
                                    f"threshold={threshold:.2f}% (old MAPE={old_mape:.2f}%, "
                                    f"age={age_h:.1f}h, decay_factor={decay_factor:.3f})")

                # Log scaler range change -- Phase 16 (INFRA-03): center_/scale_ for RobustScaler
                if hasattr(old_model, 'scaler') and old_model.scaler is not None:
                    try:
                        old_center, old_scale = old_model.scaler.center_[0], old_model.scaler.scale_[0]
                        new_center, new_scale = new_model.scaler.center_[0], new_model.scaler.scale_[0]
                        logger.info(f"Scaler range change for {model_key}: "
                                    f"center [{old_center:.2f} -> {new_center:.2f}], "
                                    f"scale [{old_scale:.2f} -> {new_scale:.2f}]")
                    except (AttributeError, IndexError):
                        pass
            else:
                logger.info(f"Model {model_key} accepted (bootstrap): MAPE={new_mape:.2f}%")

            # Update validation metadata (age_h/decay_factor set above if existing model)
            self._update_validation_metadata(model_key, "accepted", new_mape, old_mape,
                                             age_hours=age_h, decay_factor=decay_factor, scored=new_scored)

            # Clean up old model before promoting new one
            if model_key in self.trained_models:
                self._cleanup_old_model(model_key)

            # Promote model
            self.trained_models[model_key] = new_model
            self.model_train_times[model_key] = datetime.utcnow()

            # Re-derive last_sequence and raw_training_values from full training data
            values = df['value'].values
            new_model.raw_training_values = values.flatten()
            raw_tail = values[-new_model.sequence_length:].reshape(-1, 1)
            new_model.last_sequence = new_model.scaler.transform(raw_tail).flatten()
            logger.info(f"Re-scaled last_sequence for {model_key} using new scaler "
                        f"(center: {new_model.scaler.center_[0]:.2f}, scale: {new_model.scaler.scale_[0]:.2f})")

            # Sandbox only (tests, offline experiments): nothing is written to the shared model store, and serving
            # never reads these dicts. Serving models come from the training job with full provenance.
            return training_result

        except Exception as e:
            logger.error(f"Training failed: {e}")
            raise e
    
    def predict(self, application: str, metric_data: List[Dict], horizon_minutes: int = 60,
                metric_type: str = "requests", namespace: str = "default", signal=None,
                accuracy_app: Optional[str] = None) -> Dict:
        """Forecast for a resolved signal (the product path, api/identity.py) or, only when benchmark experiments are
        enabled, for a seasonal experiment arm.

        The model is one pinned registry record for the whole call: inference and every provenance field of the
        response come from that record, so a reload during the call can neither swap the model nor relabel the
        answer. There is no default model: without a compatible record the forecast is refused (422).
        """
        with PREDICTION_DURATION.time():
            try:
                if not metric_data:
                    raise ValueError("No metric data provided")
                experiment = getattr(self, "seasonal_experiment", None)
                if experiment and not experiment.matches(application, namespace, metric_type):
                    experiment = None
                if experiment is None and signal is None:
                    raise HTTPException(422, "forecast refused: no resolved metric source")
                if experiment:
                    key = registry_key(experiment.source_namespace, experiment.source_application, metric_type)
                    who = f"{experiment.source_namespace}/{experiment.source_application}"
                else:
                    key = registry_key(signal.namespace, signal.name, signal.metric)
                    who = f"{signal.namespace}/{signal.name}"
                if experiment is None:
                    self._reload_if_changed(key)   # the baseline owns checkpoint reload; an experiment arm never triggers it
                with self.registry.pin(key) as rec:
                    if rec is None:
                        raise HTTPException(422, f"forecast refused: ModelUnavailable: no compatible model for {who} "
                                                 f"(trained with the current metric query)")
                    if experiment is None:
                        why = rec.incompatibility(signal)
                        if why:
                            raise HTTPException(422, f"forecast refused: ModelQueryMismatch: {why}")
                    return self._predict_with_record(rec, application, namespace, metric_type, metric_data,
                                                     horizon_minutes, experiment, signal, accuracy_app or application)
            except Exception as e:
                PREDICTION_ERRORS.labels(error_type=type(e).__name__).inc()
                raise e

    def _predict_with_record(self, rec, application, namespace, metric_type, metric_data, horizon_minutes,
                             experiment, signal, accuracy_app) -> Dict:
        if len(metric_data) < 60:
            logger.warning(f"Limited data ({len(metric_data)} points), predictions may be less accurate")
        if experiment:
            # Never write request state into the shared source wrapper: forecast from a copy.
            with rec.lock:
                model = copy.copy(rec.model)
            model.pattern_weight_override = 1.0
        else:
            model = rec.model
        trained_dt = _parse_iso(rec.meta.get("trained_at"))
        age_hours = (datetime.utcnow() - trained_dt).total_seconds() / 3600 if trained_dt else -1.0
        if age_hours > self.MODEL_MAX_AGE_HOURS:
            logger.warning(f"Model {rec.key} is stale (age: {age_hours:.1f}h > {self.MODEL_MAX_AGE_HOURS}h), serving it anyway")
        MODEL_AGE_GAUGE.labels(application=application, metric_type=metric_type).set(age_hours if age_hours >= 0 else -1)

        # Inference input window (data/gapfill.py rule): the latest input must be a genuinely
        # observed, fresh sample; masked intervals are excluded; only bounded interior gaps are
        # filled and every fill is reported. An incomplete window is refused (the operator then
        # falls back to its reactive rule) instead of forecasting from a broken series.
        from data import gapfill
        pts = []
        for dpt in metric_data:
            try:
                pts.append((gapfill._ts(dpt["timestamp"]), float(dpt["value"])))
            except Exception:
                continue
        pts, mask_info = gapfill.apply_mask(pts, self.validity_mask, role="inference")
        window, inference_fill = gapfill.check_inference_window(
            pts, now=int(datetime.now(timezone.utc).timestamp()), sequence_length=model.sequence_length,
            forbidden=gapfill.mask_intervals(self.validity_mask))
        inference_fill["mask"] = {k: mask_info[k] for k in ("mask_version", "dropped_in_intervals")}
        # Update the model with the fresh window so that:
        # 1. last_sequence reflects current state (not training-time state)
        # 2. raw_training_values is time-aligned for historical pattern lookup
        all_values = np.array([v for _, v in window])
        window_timestamps = [datetime.utcfromtimestamp(t) for t, _ in window]
        forecast_origin = window_timestamps[-1]
        # C-44: the pattern lookup needs history indexed by timestamp; `window` is exactly
        # sequence_length points, so passing it as the seasonal history silently disabled
        # the pattern and made the blend a no-op.
        # D-88: the old `len(pts) > len(window)` gate withheld the history entirely when
        # the caller supplied exactly one window. That threw away a usable lookup: a
        # complete 144-point window spans 23h50m and already contains yesterday's value
        # for all six targets, because the targets lie in the FUTURE of the last
        # observation. Hand over whatever timestamped history exists and let the model
        # decide availability per step.
        seasonal_history = None
        if pts:
            seasonal_history = pd.Series(
                [v for _, v in pts],
                index=pd.DatetimeIndex([datetime.utcfromtimestamp(t) for t, _ in pts]),
            ).sort_index()
        # C-47: hold the per-key lock across mutate-then-predict. The model object carries
        # request-scoped state (last_sequence, input_timestamps, seasonal_history); without
        # this, two concurrent requests for the same application interleave their windows and
        # one forecast is produced from the other's data.
        with rec.lock:
            if hasattr(model, 'scaler') and model.scaler is not None and len(all_values) >= model.sequence_length:
                recent_scaled = model.scaler.transform(
                    all_values[-model.sequence_length:].reshape(-1, 1)
                ).flatten()
                model.last_sequence = recent_scaled
                logger.info(f"Updated model with fresh data: {len(all_values)} points "
                            f"(range: {all_values.min():.0f} - {all_values.max():.0f}); "
                            f"seasonal history: "
                            f"{0 if seasonal_history is None else len(seasonal_history)} points")

            # Make predictions (at 10-min data resolution)
            steps_ahead = horizon_minutes // 10  # Predictions every 10 minutes
            if steps_ahead < 1:
                steps_ahead = 1

            # Phase 14 (PRED-01, D-02): Use pre-floor (blended) MAPE for floor calculation
            # to break the safety floor feedback loop. Blended component is recorded
            # on each predict call and tracks pre-floor prediction accuracy.
            model.mape_for_floor = resolve_floor_mape(accuracy_app, namespace, metric_type)

            prediction_result = model.predict(
                steps_ahead=steps_ahead, confidence_level=0.95,
                origin=forecast_origin,                 # C-45: the last OBSERVED timestamp
                input_timestamps=window_timestamps,     # C-45: real calendar features
                seasonal_history=seasonal_history,      # C-44: a real second component
            )

        if experiment:
            comp = prediction_result.get("components") or {}
            pattern = comp.get("pattern") or []
            available = comp.get("pattern_available_per_step") or []
            weights = comp.get("pattern_weights") or []
            if (len(pattern) != steps_ahead or len(available) != steps_ahead
                    or len(weights) != steps_ahead or not all(available)
                    or any(v is None or not math.isfinite(float(v)) for v in pattern)
                    or any(w != 1.0 for w in weights)):
                raise HTTPException(422, "seasonal forecast refused: incomplete seasonal support")

        # Return flat predictions array (Go operator compatible)
        predicted_values = [round(float(v), 2) for v in prediction_result['predictions']]

        # C-86 / D-108: a SERVED step must be a number, or the forecast is refused.
        #
        # C-83's boundary sweep maps non-finite floats to null so the payload is valid
        # JSON. That is right for diagnostics and wrong here: the operator decodes
        # `predictions` into []float64 (predictiveautoscaler_controller.go:93) and
        # encoding/json leaves a null as the ZERO VALUE. A step the model could not
        # forecast would arrive as a forecast of zero requests per minute --
        # indistinguishable from a genuine quiet period, and able to drive a
        # scale-down. Refuse instead: 422 is the documented refusal the operator
        # already maps to forecastRefusedError and reactive fallback (C-17).
        unservable = [i for i, v in enumerate(predicted_values) if not math.isfinite(v)]
        if unservable:
            comp = prediction_result.get("components", {}) or {}
            raise HTTPException(
                status_code=422,
                detail=(
                    f"forecast refused: no finite value for step(s) "
                    f"{', '.join(str(i + 1) for i in unservable)} of {len(predicted_values)} "
                    f"(pattern_available_per_step="
                    f"{comp.get('pattern_available_per_step')}, "
                    f"network_finite_per_step={comp.get('network_finite_per_step')}, "
                    f"network_failed={comp.get('network_failed')}); "
                    f"serving null would be read downstream as a forecast of zero"
                ),
            )

        PREDICTION_REQUESTS.labels(application=application).inc()

        logger.info(f"Predictions for {application}/{metric_type}: "
                   f"min={min(predicted_values):.2f}, max={max(predicted_values):.2f}, "
                   f"confidence={prediction_result['confidence']:.3f}")

        meta, sha = rec.meta, rec.artifact_sha256
        trained_at = (trained_dt.isoformat() + "Z") if trained_dt else ""
        version = f"{rec.key}@{sha[:12]}"
        if experiment:
            version = f"{experiment.id}:{experiment.config_sha256}:{rec.key}@{sha}"
        inference_input_end = gapfill._iso(window[-1][0])
        response = {
            **({"experiment": experiment.provenance()} if experiment else {}),
            "application": application,
            "metric_type": metric_type,
            "model_version": version,
            "model_trained_at": trained_at,
            "training_cutoff": meta.get("training_cutoff") or "",
            "artifact_sha256": sha,
            "sequence_length": int(getattr(model, "sequence_length", 0) or 0),
            "inference_input_end": inference_input_end,
            "inference_window": {k: inference_fill[k] for k in ("window_slots", "imputed_in_window", "latest_observed", "latest_is_imputed", "gaps_filled", "mask")},
            "provenance": "sidecar",
            "predictions": predicted_values,
            # C-79: the accuracy queue keys every step on ITS target timestamp.
            "target_timestamps": list(prediction_result.get("target_timestamps") or []),
            "confidence": round(prediction_result['confidence'], 3),
            "model_name": f"lstm_{rec.key}",
            "model_identity": {"namespace": rec.namespace, "name": rec.name, "metric": rec.metric},
            "timestamp": datetime.utcnow().isoformat(),
            "horizon_minutes": horizon_minutes,
            "data_points_used": len(metric_data),
            "model_age_hours": round(age_hours, 2),
            "components": prediction_result.get("components", {}),
            "floor_pct": round(prediction_result.get("floor_pct", 0.0), 2)
        }
        if experiment is None:
            # The product attestation: the forecast was computed on exactly this compiled query. Experiment
            # arms never carry it, so the product operator never uses their forecasts.
            response["metric_query_sha256"] = signal.sha256
            response["contract"] = signal.contract
        return response

def _fmt_err(stats, unit=""):
    """'not measured' or the value -- never a bare 0.0 for an empty window (C-85)."""
    v = stats.get("value")
    return "not measured" if v is None else f"{v:.1f}{unit}"


def _json_safe(obj):
    """Recursively map non-finite floats to None so the response is JSON-compliant (C-83).

    FastAPI's JSONResponse uses json.dumps(allow_nan=False); a single NaN anywhere in the
    payload -- even an unused diagnostic -- became HTTP 400 "Out of range float values are
    not JSON compliant" and a finite seasonal forecast was refused. The model already emits
    None for its known diagnostic fields; this sweep is the guarantee at the boundary.
    """
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (float, np.floating)):
        return float(obj) if math.isfinite(obj) else None
    if isinstance(obj, np.ndarray):
        return _json_safe(obj.tolist())
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    return obj


def resolve_floor_mape(application: str, namespace: str, metric_type: str) -> float:
    """The error the percentile rule uses as its floor input, chosen on AVAILABILITY (C-88).

    Phase 14 (PRED-01, D-02) uses the pre-floor (blended) error so the safety floor does not
    feed on itself. The blended component may not have been scored yet, and the fallback for
    that is the overall error.

    The fallback used to test a zero sentinel -- `if mape == 0.0` -- which C-85 turned
    backwards when the getters started returning None for "not measured":

        component None (nothing scored)  -> `None == 0.0` is False -> overall IGNORED -> 0.0
        component 0.0  (measured, perfect) -> looked missing -> REPLACED by overall

    Both cases were wrong, and in opposite directions. A measured zero is evidence and
    survives; an unmeasured component is exactly what the fallback is for. When nothing has
    been scored anywhere the result is 0.0 -- the percentile rule's neutral input (no
    adjustment below 10), which is also its cold-start value -- never a fabricated error.
    """
    try:
        component = accuracy_tracker.get_component_mape(application, namespace, metric_type, "blended")
        if component is not None:
            return float(component)
        overall = accuracy_tracker.get_mape(application, namespace, metric_type)
        if overall is not None:
            return float(overall)
    except Exception as e:  # a tracker fault must not fail the forecast
        logger.warning(f"floor MAPE lookup failed, using the neutral input (non-fatal): {e}")
    return 0.0


def _set_error_gauge(gauge, stats, **labels):
    """Set a labelled error gauge from a stats dict, or REMOVE it when not measured (C-85).

    A gauge that reads 0.0 because nothing was scored is indistinguishable from a perfect
    score. An unmeasured value must be absent from the scrape, not zero.
    """
    if stats.get("measured") and stats.get("value") is not None:
        gauge.labels(**labels).set(float(stats["value"]))
    else:
        try:
            gauge.remove(*labels.values())
        except KeyError:
            pass


def _finite_or_none(value):
    """A finite float, or None for None/NaN/inf/unparseable. Used by the accuracy queue and
    the per-component gauges so an unavailable step is skipped, not fatal (C-79)."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _parse_iso(value):
    """ISO-8601 (with or without Z) -> naive UTC datetime, or None."""
    if not value or not isinstance(value, str):
        return None
    try:
        v = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _observation_timestamp(point):
    """Timestamp of an observation supplied by the caller, as naive UTC, or None (C-48).

    Accuracy scoring needs the observation's own time, not the moment the request arrived:
    a delayed sample must be matched to the forecast that targeted it.
    """
    if not isinstance(point, dict):
        return None
    raw = point.get("timestamp")
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(raw))
        except Exception:
            return None
    return _parse_iso(raw)


# Initialize predictor
# Benchmark experiment arms forecast one workload from another workload's history, so they can never attest the arm's
# own compiled query. They are refused unless explicitly enabled, and even then their responses carry no product
# attestation (metric_query_sha256/contract), so the product operator never uses them.
ALLOW_BENCHMARK_EXPERIMENTS = os.getenv("ALLOW_BENCHMARK_EXPERIMENTS") == "1"
if not ALLOW_BENCHMARK_EXPERIMENTS and (os.getenv("SEASONAL_EXPERIMENT") is not None
                                        or os.getenv("ENSEMBLE_EXPERIMENT") is not None):
    raise RuntimeError("SEASONAL_EXPERIMENT/ENSEMBLE_EXPERIMENT are benchmark tooling: set "
                       "ALLOW_BENCHMARK_EXPERIMENTS=1 to run them (their forecasts are never product-attested)")

predictor = LSTMPredictor()

# The product path: the autoscaler's compiled query, resolved from the Kubernetes API (api/identity.py) and read from the
# Prometheus-compatible endpoint with a bounded exchange (data/history.py).
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL") or os.getenv(
    "VICTORIA_METRICS_URL", "http://vmselect-vmst.monitoring.svc.cluster.local:8481/select/0/prometheus")
if not os.getenv("PROMETHEUS_URL") and os.getenv("VICTORIA_METRICS_URL"):
    logger.info("VICTORIA_METRICS_URL is deprecated; set PROMETHEUS_URL")
kube_reader = KubeReader()
# Admission: a request takes a slot before any Kubernetes or history work and keeps it until its worker thread has
# really finished (a cancelled or timed-out request still holds it); a full house is 503. Inference has its own,
# smaller bound inside the job.
FORECAST_ADMISSION = int(os.getenv("FORECAST_ADMISSION", "4"))
_admission = threading.BoundedSemaphore(FORECAST_ADMISSION)
_forecast_executor = concurrent.futures.ThreadPoolExecutor(max_workers=FORECAST_ADMISSION,
                                                           thread_name_prefix="forecast")
_inference_slots = threading.BoundedSemaphore(int(os.getenv("FORECAST_INFERENCE_SLOTS", "2")))
INFERENCE_WAIT_S = 20.0


class ServiceBusy(Exception):
    """No capacity for this forecast now (503)."""


def accuracy_scope(signal) -> str:
    """The accuracy tracker's key for one signal: errors, pending forecasts and the floor/blend feedback belong to one
    autoscaler, target object, compiled query and contract, never to the workload name alone."""
    ident = f"{signal.autoscaler_uid}|{signal.target_uid}|{signal.sha256}|{signal.contract}"
    return f"{signal.name}#{hashlib.sha256(ident.encode()).hexdigest()[:16]}"


def _serve_forecast(request: Dict, horizon_minutes: int):
    """The product forecast job (a worker thread of _forecast_executor, run under an admission slot)."""
    signal = resolve_signal(request, kube_reader)
    history = query_history(PROMETHEUS_URL, signal.query)
    if not _inference_slots.acquire(timeout=INFERENCE_WAIT_S):
        raise ServiceBusy("inference capacity busy")
    try:
        prediction = predictor.predict(signal.name, history, horizon_minutes, signal.metric, signal.namespace,
                                       signal=signal, accuracy_app=accuracy_scope(signal))
    finally:
        _inference_slots.release()
    return prediction, signal, history


async def _run_admitted(fn, *args, executor=None):
    """Run fn under an admission slot. The slot is released exactly once by the submitted future's done callback:
    when the work finishes, or when a cancelled request cancels it before it started. The awaiting coroutine never
    releases it, so a cancellation can neither leak a slot nor free one while its work still runs."""
    sem = _admission
    if not sem.acquire(blocking=False):
        raise HTTPException(503, "forecast unavailable: the service is at capacity")
    try:
        fut = (executor or _forecast_executor).submit(fn, *args)
    except BaseException:
        sem.release()   # never submitted, so no callback will release it
        raise
    fut.add_done_callback(lambda _f: sem.release())
    return await asyncio.wrap_future(fut)

# Initialize accuracy tracker
accuracy_tracker = AccuracyTracker()

# Seasonal-ensemble arms (Task 03 U-21, U-23): opt-in, malformed configuration fails at startup.
_raw_ensemble = os.getenv("ENSEMBLE_EXPERIMENT")
ensemble_experiments = EnsembleExperiment.parse_all(_raw_ensemble) if _raw_ensemble is not None else []
ENSEMBLE_HISTORY_HOURS = int(os.getenv("ENSEMBLE_HISTORY_HOURS", "360"))

# Initialize VictoriaMetrics collector
vm_url = os.getenv("VICTORIA_METRICS_URL", "http://vmselect-vmst.monitoring.svc.cluster.local:8481/select/0/prometheus")
vm_collector = VictoriaMetricsCollector(vm_url)

async def fetch_metrics_from_vm(application: str, namespace: str, metric_type: str, hours: int = 168) -> List[Dict]:
    """Fetch metrics from VictoriaMetrics for the given application."""
    try:
        logger.info(f"Fetching {metric_type} metrics from VictoriaMetrics for {namespace}/{application}")

        if metric_type == "requests":
            df = vm_collector.get_istio_request_rate(
                destination_workload=application,
                namespace=namespace,
                hours=hours
            )
            if df is not None and not df.empty:
                # Returns raw RPM (requests per minute) - no normalization
                # Operator handles conversion to replica count
                return [
                    {"timestamp": row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp']),
                     "value": float(row['value'])}
                    for _, row in df.iterrows()
                ]

        return []

    except Exception as e:
        logger.error(f"Failed to fetch metrics from VictoriaMetrics: {e}")
        return []

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "3.8.0",
        "timestamp": datetime.utcnow().isoformat(),
        "models_loaded": [f"{m['namespace']}/{m['name']}" for m in predictor.registry.snapshot()]
    }

@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint."""
    return {
        "status": "ready", 
        "model_type": "lstm",
        "trained_models": len(predictor.registry.snapshot())
    }

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/predict")
async def predict(request: Dict):
    """
    Forecast for a PredictiveAutoscaler's target from its compiled request-rate query.

    The operator sends a reference, never a query or a history:
    {
        "application": "web", "namespace": "shop", "metric_type": "requests", "horizon_minutes": 60,
        "autoscaler_name": "web", "autoscaler_namespace": "shop", "autoscaler_uid": "...",
        "autoscaler_generation": 3, "target_uid": "...", "metric_query_sha256": "...",
        "contract": "requests-per-second/v1"
    }
    422 = refused (provenance, no compatible model, history not one valid series, incomplete input window);
    503 = unavailable now (Kubernetes or metrics lookup failed, busy).
    """
    try:
        if not isinstance(request, dict):
            raise HTTPException(status_code=400, detail="The request must be a JSON object")
        application = request.get("application")
        if not application:
            raise HTTPException(status_code=400, detail="Application name is required")
        if "metric_data" in request:
            raise HTTPException(422, "forecast refused: caller-supplied history is not accepted (the service reads "
                                     "the autoscaler's compiled query itself)")
        metric_type = request.get("metric_type", "requests")
        horizon_minutes = request.get("horizon_minutes", 60)
        namespace = request.get("namespace", "default")

        if metric_type != "requests":
            raise HTTPException(status_code=400, detail="metric_type must be 'requests'")
        if not isinstance(horizon_minutes, int) or isinstance(horizon_minutes, bool) or not 10 <= horizon_minutes <= 1440:
            raise HTTPException(status_code=400, detail="horizon_minutes must be an integer between 10 and 1440")

        ensemble_experiment = next((e for e in ensemble_experiments
                                    if e.matches(application, namespace, metric_type)), None)
        if ensemble_experiment:
            return await predict_ensemble(ensemble_experiment, application, namespace, metric_type, horizon_minutes)

        experiment = getattr(predictor, "seasonal_experiment", None)
        if experiment and not experiment.matches(application, namespace, metric_type):
            experiment = None

        loop = asyncio.get_running_loop()
        if experiment:
            # Benchmark experiment arm (enabled explicitly): history of the configured source workload.
            metric_data = await fetch_metrics_from_vm(experiment.source_application, experiment.source_namespace,
                                                      metric_type)
            if not metric_data:
                raise HTTPException(status_code=400, detail=f"Could not fetch {metric_type} metrics for {application}")
            try:
                prediction = await loop.run_in_executor(None, functools.partial(
                    predictor.predict, application, metric_data, horizon_minutes, metric_type, namespace))
            except ValueError as e:
                raise HTTPException(status_code=422, detail=f"forecast refused: {e}")
            acc_app = application
        else:
            try:
                prediction, signal, metric_data = await _run_admitted(_serve_forecast, request, horizon_minutes)
            except (ProvenanceRefused, HistoryRefused) as e:
                raise HTTPException(status_code=422, detail=f"forecast refused: {e}")
            except (LookupFailed, HistoryUnavailable, ServiceBusy) as e:
                raise HTTPException(status_code=503, detail=f"forecast unavailable: {e}")
            except ValueError as e:
                # inference-window refusals; the operator falls back to reactive
                raise HTTPException(status_code=422, detail=f"forecast refused: {e}")
            application, namespace = signal.name, signal.namespace
            acc_app = accuracy_scope(signal)

        # --- ACCURACY TRACKING (C-48: timestamp-matched, matured targets only) ---
        # A forecast for t+10min becomes an error signal only once t+10min has passed and the
        # observation for THAT timestamp exists. The previous code compared the last stored
        # forecast against whatever arrived next -- typically 60 s later -- which is not a
        # forecast error at all, and it also fed that number back into the adaptive percentile.
        try:
            if metric_data and len(metric_data) > 0:
                current_actual = float(metric_data[-1].get("value", 0))
                observation_at = _observation_timestamp(metric_data[-1])

                if observation_at is not None and current_actual > 0:
                    matured = accuracy_tracker.take_matured(
                        acc_app, namespace, metric_type, observation_at)
                    for predicted in matured:
                        accuracy_tracker.record(acc_app, namespace, metric_type,
                                                predicted, current_actual)
                    if matured:
                        mape_st = accuracy_tracker.mape_stats(acc_app, namespace, metric_type)
                        mae_st = accuracy_tracker.mae_stats(acc_app, namespace, metric_type)
                        labels = dict(application=application, namespace=namespace, metric_type=metric_type)
                        _set_error_gauge(MAPE_GAUGE, mape_st, **labels)
                        _set_error_gauge(MAE_GAUGE, mae_st, **labels)
                        ACCURACY_SCORED_GAUGE.labels(**labels).set(mape_st["scored"])
                        logger.info(
                            f"Accuracy for {application}/{metric_type}: "
                            f"MAPE={_fmt_err(mape_st, '%')} ({mape_st['scored']} scored of "
                            f"{mape_st['recorded']} recorded), MAE={_fmt_err(mae_st)}; "
                            f"matured {len(matured)} forecast(s) whose target was {observation_at.isoformat()}, "
                            f"actual={current_actual:.1f}")

            # Queue every step of THIS forecast against its own target timestamp.
            targets = (prediction or {}).get("target_timestamps") or []
            preds = (prediction or {}).get("predictions") or []
            queued = {"final": 0, "lstm": 0, "pattern": 0, "blended": 0}
            skipped_unavailable = {"lstm": 0, "pattern": 0, "blended": 0}
            for step, value in enumerate(preds):
                if step >= len(targets):
                    break
                target_at = _parse_iso(targets[step])
                if target_at is None:
                    continue
                # C-79: a component may be unavailable at THIS step (a pattern step with no
                # previous-day observation is None; a failed network is NaN). float(None)
                # raised here and aborted the loop after the first forecast, so later steps
                # and components were never queued. Skip the unavailable value, keep going.
                if _finite_or_none(value) is not None:
                    accuracy_tracker.store_forecast(acc_app, namespace, metric_type,
                                                    target_at, float(value))
                    queued["final"] += 1
                for comp in ("lstm", "pattern", "blended"):
                    series = (prediction.get("components") or {}).get(comp)
                    if not isinstance(series, (list, tuple)) or step >= len(series):
                        continue
                    v = _finite_or_none(series[step])
                    if v is None:
                        skipped_unavailable[comp] += 1
                        continue
                    accuracy_tracker.store_forecast(acc_app, namespace, metric_type,
                                                    target_at, v, component=comp)
                    queued[comp] += 1
            if any(skipped_unavailable.values()):
                logger.info(f"Accuracy queue for {application}/{metric_type}: queued {queued}, "
                            f"skipped unavailable {skipped_unavailable}")
        except Exception as e:
            logger.warning(f"Accuracy tracking error (non-fatal): {e}")

        # --- PER-COMPONENT GAUGE UPDATES (D-04, D-05, D-06, D-07) ---
        try:
            components = prediction.get("components", {})
            if components:
                # Only the numeric per-step series are gauges. `components` also carries
                # descriptive fields (pattern_source, pattern_available, agreement,
                # pattern_weights) added with the C-44 fix; iterating those blindly would
                # emit one gauge per character of a string.
                for component_name in ("lstm", "pattern", "blended", "final"):
                    values = components.get(component_name)
                    if not isinstance(values, (list, tuple)):
                        continue
                    for i, val in enumerate(values):
                        labels = dict(application=application, namespace=namespace,
                                      component=component_name, step=str(i + 1))
                        v = _finite_or_none(val)
                        if v is None:
                            # C-79: float(None) aborted this loop mid-way and left the
                            # remaining gauges STALE from the previous issuance. An
                            # unavailable step must not keep showing last time's value:
                            # remove the labelled series so scrapes see its absence.
                            try:
                                PREDICTION_RPM_GAUGE.remove(*labels.values())
                            except KeyError:
                                pass
                            continue
                        PREDICTION_RPM_GAUGE.labels(**labels).set(v)

            floor_pct = prediction.get("floor_pct", 0.0)
            FLOOR_PCT_GAUGE.labels(
                application=application,
                namespace=namespace
            ).set(float(floor_pct))

            # Per-component MAPE tracking (C-48: score matured targets, never the value
            # that was just issued for a time that has not arrived).
            if components and metric_data and len(metric_data) > 0:
                current_actual = float(metric_data[-1].get("value", 0))
                observation_at = _observation_timestamp(metric_data[-1])
                if current_actual > 0 and observation_at is not None:
                    for comp_name in ["lstm", "pattern", "blended"]:
                        for predicted in accuracy_tracker.take_matured(
                                acc_app, namespace, metric_type, observation_at,
                                component=comp_name):
                            accuracy_tracker.record_component(
                                acc_app, namespace, metric_type,
                                comp_name, float(predicted), current_actual
                            )
                            comp_st = accuracy_tracker.component_mape_stats(
                                acc_app, namespace, metric_type, comp_name
                            )
                            clabels = dict(application=application, namespace=namespace,
                                           component=comp_name)
                            _set_error_gauge(COMPONENT_MAPE_GAUGE, comp_st, **clabels)
                            COMPONENT_SCORED_GAUGE.labels(**clabels).set(comp_st["scored"])
        except Exception as e:
            logger.warning(f"Component gauge update error (non-fatal): {e}")

        # Include MAPE for requests metric type in the response
        # The operator always wants requests MAPE regardless of which metric was predicted
        # C-85: the error figure travels with its provenance. `mape` is null -- never 0.0 --
        # when nothing has been scored; `mape_scored` and `mape_availability` say how much
        # evidence stands behind a value that IS present.
        try:
            mape_st = accuracy_tracker.mape_stats(acc_app, namespace, "requests")
            mae_st = accuracy_tracker.mae_stats(acc_app, namespace, "requests")
        except Exception:
            mape_st = {"value": None, "scored": 0, "recorded": 0, "availability": None, "measured": False}
            mae_st = dict(mape_st)
        accuracy = {
            "mape": mape_st["value"], "mape_measured": mape_st["measured"],
            "mape_scored": mape_st["scored"], "mape_recorded": mape_st["recorded"],
            "mape_availability": mape_st["availability"],
            "mae": mae_st["value"], "mae_measured": mae_st["measured"], "mae_scored": mae_st["scored"],
        }
        # C-83: the whole payload is swept for non-finite floats at the boundary.
        return JSONResponse(content=_json_safe({**prediction, **accuracy}))
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

def _naive_iso(value: str) -> str:
    """The seasonal-ensemble module writes 'Z' ISO strings; the API serves naive UTC like the LSTM."""
    return value[:-1] if value.endswith("Z") else value


async def predict_ensemble(experiment, application, namespace, metric_type, horizon_minutes):
    """Serve the experiment's forecaster (seasonal ensemble U-21/U-23, or the relative profile-AR
    forecaster R1) plus the experiment's margin.

    The served `predictions` already include the margin; `ensemble.raw` keeps the margin-free
    forecast for scoring. Any refusal is a 422, which the operator maps to reactive fallback.
    """
    if horizon_minutes != 60:
        raise HTTPException(422, "ensemble forecast refused: only a 60-minute horizon is supported")
    metric_data = await fetch_metrics_from_vm(experiment.source_application, experiment.source_namespace,
                                              metric_type, hours=ENSEMBLE_HISTORY_HOURS)
    if not metric_data:
        raise HTTPException(422, "ensemble forecast refused: no source history")
    from data import gapfill
    pts = []
    for dpt in metric_data:
        try:
            pts.append((gapfill._ts(dpt["timestamp"]), float(dpt["value"])))
        except Exception:
            continue
    pts, mask_info = gapfill.apply_mask(pts, predictor.validity_mask, role="benchmark")
    now_ts = int(datetime.now(timezone.utc).timestamp())
    loop = asyncio.get_event_loop()
    try:
        with PREDICTION_DURATION.time():
            result = await loop.run_in_executor(
                None, functools.partial(experiment.module.forecast, pts, now_ts,
                                        f"{experiment.source_namespace}/{experiment.source_application}",
                                        **experiment.forecast_kwargs()))
    except (seasonal_ensemble.ForecastUnavailable, ValueError) as e:
        PREDICTION_ERRORS.labels(error_type="EnsembleUnavailable").inc()
        raise HTTPException(422, f"forecast refused: {e}")
    served = [round(float(v), 2) for v in result["served"]]
    if not all(math.isfinite(v) for v in served):
        raise HTTPException(422, "forecast refused: non-finite ensemble step")
    targets = [_naive_iso(t) for t in result["target_timestamps"]]
    gen = result["generation"]
    PREDICTION_REQUESTS.labels(application=application).inc()

    # Accuracy queue: the served value (final) and the margin-free forecast (component "raw").
    try:
        last = metric_data[-1]
        current_actual = float(last.get("value", 0))
        observation_at = _observation_timestamp(last)
        if observation_at is not None and current_actual > 0:
            for predicted in accuracy_tracker.take_matured(application, namespace, metric_type, observation_at):
                accuracy_tracker.record(application, namespace, metric_type, predicted, current_actual)
            for predicted in accuracy_tracker.take_matured(application, namespace, metric_type,
                                                           observation_at, component="raw"):
                accuracy_tracker.record_component(application, namespace, metric_type, "raw",
                                                  float(predicted), current_actual)
            raw_st = accuracy_tracker.component_mape_stats(application, namespace, metric_type, "raw")
            clabels = dict(application=application, namespace=namespace, component="raw")
            _set_error_gauge(COMPONENT_MAPE_GAUGE, raw_st, **clabels)
            COMPONENT_SCORED_GAUGE.labels(**clabels).set(raw_st["scored"])
            mape_st = accuracy_tracker.mape_stats(application, namespace, metric_type)
            mae_st = accuracy_tracker.mae_stats(application, namespace, metric_type)
            labels = dict(application=application, namespace=namespace, metric_type=metric_type)
            _set_error_gauge(MAPE_GAUGE, mape_st, **labels)
            _set_error_gauge(MAE_GAUGE, mae_st, **labels)
            ACCURACY_SCORED_GAUGE.labels(**labels).set(mape_st["scored"])
        for step, target in enumerate(targets):
            target_at = _parse_iso(target)
            if target_at is None:
                continue
            accuracy_tracker.store_forecast(application, namespace, metric_type, target_at, served[step])
            accuracy_tracker.store_forecast(application, namespace, metric_type, target_at,
                                            float(result["raw"][step]), component="raw")
    except Exception as e:
        logger.warning(f"Ensemble accuracy tracking error (non-fatal): {e}")

    try:
        for name, values in ([(f"ensemble_{c}", v) for c, v in result["components"].items()]
                             + [("ensemble_raw", result["raw"]), ("final", served)]):
            for i, v in enumerate(values):
                PREDICTION_RPM_GAUGE.labels(application=application, namespace=namespace,
                                            component=name, step=str(i + 1)).set(float(v))
        ENSEMBLE_MARGIN_GAUGE.labels(application=application, namespace=namespace).set(result["margin"])
        ENSEMBLE_MARGIN_SAMPLES_GAUGE.labels(application=application, namespace=namespace).set(
            result["margin_samples"])
    except Exception as e:
        logger.warning(f"Ensemble gauge update error (non-fatal): {e}")

    boundary = gen["boundary"]
    age_hours = (datetime.now(timezone.utc) - datetime.fromisoformat(boundary.replace("Z", "+00:00"))
                 ).total_seconds() / 3600
    logger.info("ENSEMBLE_ISSUANCE " + json.dumps({
        "application": application, "namespace": namespace, "origin": result["origin"],
        "experiment": experiment.id, "forecaster": experiment.forecaster,
        "margin_quantile": experiment.margin_quantile,
        "margin_mode": experiment.margin_mode, "partial_rule": experiment.partial_rule,
        "served_components": result.get("served_components"),
        "raw": [round(v, 2) for v in result["raw"]], "margin": round(result["margin"], 2),
        "margin_samples": result["margin_samples"], "served": served, "generation": gen,
        "stale_generation": result["stale_generation"]}, sort_keys=True))
    payload = {
        "experiment": experiment.provenance(),
        "application": application,
        "metric_type": metric_type,
        "model_version": f"{experiment.id}:{experiment.config_sha256[:12]}:{experiment.module.VERSION}@{gen['fingerprint'][:12]}",
        "model_trained_at": _naive_iso(boundary),
        "training_cutoff": boundary,
        "artifact_sha256": gen["fingerprint"],
        "sequence_length": 0,
        "inference_input_end": result["origin"],
        "inference_window": {"mask": {k: mask_info[k] for k in ("mask_version", "dropped_in_intervals")},
                             "latest_observed": result["origin"]},
        "provenance": experiment.forecaster,
        "predictions": served,
        "target_timestamps": targets,
        "confidence": 0.95,
        "model_name": f"ensemble_{application}_{metric_type}",
        "timestamp": datetime.utcnow().isoformat(),
        "horizon_minutes": horizon_minutes,
        "data_points_used": len(pts),
        "model_age_hours": round(age_hours, 2),
        "ensemble": {**{k: result[k] for k in ("origin", "components", "raw", "margin", "margin_samples",
                                                 "generation", "stale_generation", "settings")},
                     **{k: result[k] for k in ("hw", "profile_ar") if k in result}},
    }
    return JSONResponse(content=_json_safe(payload))


@app.post("/train")
async def train_model(request: Dict):
    """Disabled: models are trained by the training job from the autoscaler's compiled query, with full provenance.
    Training from caller-supplied data would put a model without query provenance next to the served ones."""
    raise HTTPException(status_code=410, detail="training through the API is disabled; models come from the training "
                                                "job, which reads the autoscaler's compiled query")

@app.get("/models")
async def get_models():
    """The served models: identity and provenance from their validated sidecars (null = missing or non-finite)."""
    models_info = {}
    snapshot = predictor.registry.snapshot()
    for m in snapshot:
        trained = _parse_iso(m.get("trained_at"))
        age_hours = (datetime.utcnow() - trained).total_seconds() / 3600 if trained else None
        models_info[m["key"]] = {
            "namespace": m["namespace"], "name": m["name"], "metric": m["metric"],
            "artifact_sha256": m["artifact_sha256"], "metric_query_sha256": m["metric_query_sha256"],
            "trained_at": m.get("trained_at"),
            "provenance": m.get("provenance"),
            "scaler_range": m.get("scaler_range"),
            "age_hours": round(age_hours, 1) if age_hours is not None else None,
            "is_stale": age_hours > predictor.MODEL_MAX_AGE_HOURS if age_hours is not None else True,
        }
    return _json_safe({
        "model_type": "lstm",
        "trained_models": [f"{m['namespace']}/{m['name']}" for m in snapshot],
        "models": models_info,
        "model_directory": str(predictor.model_dir),
        "version": "3.8.0"
    })

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Predictive Autoscaler LSTM API",
        "version": "3.8.0",
        "model_type": "lstm",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "ready": "/ready", 
            "predict": "/predict (POST)",
            "train": "/train (POST)",
            "models": "/models",
            "metrics": "/metrics"
        },
        "supported_metrics": ["requests"]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
