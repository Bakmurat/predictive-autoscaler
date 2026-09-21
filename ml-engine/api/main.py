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

class LSTMPredictor:
    """LSTM-based predictor using real neural network models."""

    # Models older than this will be retrained
    MODEL_MAX_AGE_HOURS = 6

    def __init__(self):
        self.lstm_model = LSTMForecastModel(sequence_length=144)
        self.trained_models = {}
        self.model_train_times = {}  # Track when each model was trained
        self.model_file_mtimes = {}  # Track file mtime for disk reload detection
        self.model_meta = {}  # model_key -> provenance sidecar (lstm_<key>.meta.json) written by the trainer
        # C-47: a served model object carries per-request mutable state (last_sequence,
        # input_timestamps, seasonal_history). Two concurrent requests for the same key would
        # interleave their windows. One lock per model key serialises mutate-then-predict, and
        # the same lock makes a reload swap atomic with respect to inference.
        self._model_locks = {}
        self._model_locks_guard = threading.Lock()
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

        # Try to load pre-trained models
        self._load_pretrained_models()

        # Set initial RSS gauge after model loading
        RSS_BYTES_GAUGE.set(self._get_rss_bytes())

        # Cold start: no model on PVC, trigger immediate CronJob training
        has_requests_model = any(k.endswith("_requests") for k in self.trained_models)
        if not has_requests_model and os.getenv("MODEL_DIR"):
            logger.info("No requests model found on PVC, triggering cold-start training job")
            try:
                result = subprocess.run(
                    ["kubectl", "create", "job", "--from=cronjob/ml-training",
                     "ml-training-coldstart", "-n", "ml-engine"],
                    capture_output=True, text=True, timeout=30
                )
                if result.returncode == 0:
                    logger.info(f"Cold-start job triggered successfully: {result.stdout.strip()}")
                else:
                    logger.warning(f"Cold-start job trigger failed (non-fatal): {result.stderr.strip()}")
            except Exception as e:
                logger.warning(f"Cold-start job trigger error (non-fatal): {e}")
    
    def _read_meta(self, model_key: str) -> dict:
        """Read the trainer's provenance sidecar for a model (artifact hash, training cutoff, split
        boundaries). Missing or unreadable sidecar -> {} and the model is reported as provenance-unknown."""
        meta_path = self.model_dir / f"lstm_{model_key}.meta.json"
        try:
            with open(meta_path) as fh:
                meta = json.load(fh)
            if not isinstance(meta, dict):
                return {}
            return meta
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.warning(f"Unreadable provenance sidecar {meta_path}: {e}")
            return {}

    def _load_pretrained_models(self):
        """Load pre-trained LSTM models if available.

        Phase 16 (D-10, D-12): Detects old Dense(1) models and marks them
        for immediate retrain by setting train_time to datetime.min.
        """
        try:
            model_files = list(self.model_dir.glob("lstm_*.pkl"))
            logger.info(f"Found {len(model_files)} pre-trained models in {self.model_dir}")
            for model_file in model_files:
                try:
                    app_name = model_file.stem.replace("lstm_", "")
                    # Clean up existing model if reloading
                    if app_name in self.trained_models:
                        self._cleanup_old_model(app_name)
                    model = joblib.load(model_file)

                    # Phase 16 (D-10): Check if model is old Dense(1) format
                    if LSTMForecastModel._is_old_model_format(model):
                        logger.warning(f"Old Dense(1) model detected for {app_name} -- marking for retrain")
                        self.trained_models[app_name] = model
                        self.model_train_times[app_name] = datetime.min  # Force stale
                        continue

                    self.trained_models[app_name] = model
                    file_mtime = model_file.stat().st_mtime
                    self.model_file_mtimes[app_name] = file_mtime
                    meta = self._read_meta(app_name)
                    self.model_meta[app_name] = meta
                    # Training time from the trainer's sidecar; the file mtime is only the fallback
                    trained_at = _parse_iso(meta.get("trained_at")) or datetime.utcfromtimestamp(file_mtime)
                    self.model_train_times[app_name] = trained_at
                    age_hours = (datetime.utcnow() - trained_at).total_seconds() / 3600
                    logger.info(f"Loaded model for {app_name} (age: {age_hours:.1f}h, sha256={meta.get('artifact_sha256', 'unknown')[:12]}, "
                                f"training_cutoff={meta.get('training_cutoff', 'unknown')})")
                except Exception as e:
                    logger.warning(f"Failed to load {model_file}: {e}")
        except Exception as e:
            logger.info(f"No pre-trained models found: {e}")

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
        tf.keras.backend.clear_session()
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
    
    def _check_and_reload_model(self, model_key: str):
        """Check if model file on disk is newer than loaded model, and reload if so.

        Phase 16 (D-10, D-12): Also checks for old Dense(1) format and marks for retrain.
        """
        model_path = self.model_dir / f"lstm_{model_key}.pkl"
        if not model_path.exists():
            return

        current_mtime = model_path.stat().st_mtime
        last_mtime = self.model_file_mtimes.get(model_key, 0)

        if current_mtime > last_mtime:
            # C-47: load and validate the replacement BEFORE touching the incumbent. The previous
            # order freed the running model first and then logged "keeping old model" on failure,
            # which was false -- a bad artifact left the service with no model at all.
            try:
                # The trainer renames the artifact into place and writes the sidecar afterwards
                # (training/train_nginx_test.py), so a reload can catch a new artifact beside a
                # stale sidecar. Refuse a mismatched pair and retry on the next call.
                meta = self._read_meta(model_key)
                declared = (meta or {}).get("artifact_sha256")
                actual = self._file_sha256(model_path)
                if declared and actual and declared != actual:
                    logger.info(
                        f"Model {model_key}: artifact/sidecar mismatch "
                        f"(sidecar {declared[:12]}, file {actual[:12]}); publication is still in "
                        f"progress. Incumbent kept; will retry on the next request."
                    )
                    return

                candidate = joblib.load(model_path)

                # Phase 16 (D-10, D-12): Check for old Dense(1) format
                if LSTMForecastModel._is_old_model_format(candidate):
                    logger.warning(f"Old Dense(1) model on disk for {model_key} -- marking for retrain")
                    self.model_train_times[model_key] = datetime.min
                    self.model_file_mtimes[model_key] = current_mtime  # don't re-read the same bad file
                    return  # Don't load old format
            except Exception as e:
                logger.error(
                    f"Failed to load replacement model for {model_key} from disk: {e}. "
                    f"The incumbent is untouched and still serving."
                )
                return

            # The replacement is loaded and valid: swap it in, then release the old one.
            with self._model_lock(model_key):
                detached = self.trained_models.get(model_key)
                self.trained_models[model_key] = candidate
                self.model_meta[model_key] = meta
                self.model_train_times[model_key] = _parse_iso(meta.get("trained_at")) or datetime.utcfromtimestamp(current_mtime)
                self.model_file_mtimes[model_key] = current_mtime
            if detached is not None:
                self._release_model_object(detached)
            RSS_BYTES_GAUGE.set(self._get_rss_bytes())
            logger.info(
                f"Reloaded model {model_key} from disk (file updated); "
                f"artifact sha256={(actual or 'unknown')[:12]}"
            )

    def _model_lock(self, model_key: str):
        """Per-key lock serialising mutate-then-predict and the reload swap (C-47)."""
        with self._model_locks_guard:
            lock = self._model_locks.get(model_key)
            if lock is None:
                lock = threading.RLock()
                self._model_locks[model_key] = lock
            return lock

    @staticmethod
    def _file_sha256(path) -> str:
        """sha256 of a published artifact, for the sidecar consistency check (C-47)."""
        import hashlib
        h = hashlib.sha256()
        try:
            with open(path, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
        except Exception as e:
            logger.warning(f"could not hash {path}: {e}")
            return ""
        return h.hexdigest()

    def _get_model_age_hours(self, model_key: str) -> float:
        """Get the age of a loaded model in hours."""
        if model_key in self.model_train_times:
            return (datetime.utcnow() - self.model_train_times[model_key]).total_seconds() / 3600
        return -1.0

    def _update_validation_metadata(self, model_key, status, new_mape, old_mape=None, age_hours=None, decay_factor=None):
        """Update validation metadata for a model after accept/reject decision."""
        self.validation_metadata[model_key] = {
            "status": status,
            "mape": round(new_mape, 2),
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
                                                             age_hours=age_h, decay_factor=None)
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
                                                             age_hours=age_h, decay_factor=decay_factor)
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
                                             age_hours=age_h, decay_factor=decay_factor)

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

            # Save model to disk
            model_path = self.model_dir / f"lstm_{application}_{metric_type}.pkl"
            joblib.dump(new_model, model_path)
            logger.info(f"Model saved to {model_path}")

            return training_result

        except Exception as e:
            logger.error(f"Training failed: {e}")
            raise e
    
    def predict(self, application: str, metric_data: List[Dict], horizon_minutes: int = 60, metric_type: str = "cpu", namespace: str = "default") -> Dict:
        """Generate predictions using LSTM model.

        Returns predictions as flat float array for Go operator compatibility.
        The operator expects: {"predictions": [float, float, ...], "confidence": float}
        """

        with PREDICTION_DURATION.time():
            try:
                # Check if we have enough data
                if not metric_data:
                    raise ValueError("No metric data provided")

                if len(metric_data) < 60:
                    logger.warning(f"Limited data ({len(metric_data)} points), predictions may be less accurate")

                # Check if model file on disk has been updated (by CronJob)
                model_key = f"{application}_{metric_type}"
                self._check_and_reload_model(model_key)

                # Get the model
                model = self.trained_models.get(model_key, self.lstm_model)

                # Track model age
                age_hours = self._get_model_age_hours(model_key)
                if age_hours > self.MODEL_MAX_AGE_HOURS:
                    logger.warning(f"Model {model_key} is stale (age: {age_hours:.1f}h > {self.MODEL_MAX_AGE_HOURS}h), serving from disk anyway")

                # Update Prometheus gauge
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
                with self._model_lock(model_key):
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
                    try:
                        mape = accuracy_tracker.get_component_mape(application, namespace, metric_type, "blended")
                        if mape == 0.0:
                            # Fallback: blended component may not have enough entries yet (cold start)
                            mape = accuracy_tracker.get_mape(application, namespace, metric_type)
                    except Exception:
                        mape = 0.0
                    model.mape_for_floor = mape

                    prediction_result = model.predict(
                        steps_ahead=steps_ahead, confidence_level=0.95,
                        origin=forecast_origin,                 # C-45: the last OBSERVED timestamp
                        input_timestamps=window_timestamps,     # C-45: real calendar features
                        seasonal_history=seasonal_history,      # C-44: a real second component
                    )

                # Return flat predictions array (Go operator compatible)
                predicted_values = [round(float(v), 2) for v in prediction_result['predictions']]

                PREDICTION_REQUESTS.labels(application=application).inc()

                logger.info(f"Predictions for {application}/{metric_type}: "
                           f"min={min(predicted_values):.2f}, max={max(predicted_values):.2f}, "
                           f"confidence={prediction_result['confidence']:.3f}")

                meta = self.model_meta.get(model_key, {})
                sha = meta.get("artifact_sha256") or ""
                trained_dt = self.model_train_times.get(model_key)
                trained_at = (trained_dt.isoformat() + "Z") if trained_dt and trained_dt != datetime.min else ""
                if model_key not in self.trained_models:
                    version = f"{model_key}@untrained"
                elif sha:
                    version = f"{model_key}@{sha[:12]}"
                else:
                    version = f"{model_key}@mtime{int(self.model_file_mtimes.get(model_key, 0))}"
                inference_input_end = gapfill._iso(window[-1][0])
                return {
                    "application": application,
                    "metric_type": metric_type,
                    "model_version": version,
                    "model_trained_at": trained_at,
                    "training_cutoff": meta.get("training_cutoff") or "",
                    "artifact_sha256": sha,
                    "sequence_length": int(getattr(model, "sequence_length", 0) or 0),
                    "inference_input_end": inference_input_end,
                    "inference_window": {k: inference_fill[k] for k in ("window_slots", "imputed_in_window", "latest_observed", "latest_is_imputed", "gaps_filled", "mask")},
                    "provenance": "sidecar" if meta else "unknown",
                    "predictions": predicted_values,
                    # C-79: the accuracy queue keys every step on ITS target timestamp. This key
                    # was read at the queue loop but never written here, so `targets` was always
                    # empty and the loop broke at step 0: nothing was ever queued for scoring.
                    "target_timestamps": list(prediction_result.get("target_timestamps") or []),
                    "confidence": round(prediction_result['confidence'], 3),
                    "model_name": f"lstm_{application}_{metric_type}",
                    "timestamp": datetime.utcnow().isoformat(),
                    "horizon_minutes": horizon_minutes,
                    "data_points_used": len(metric_data),
                    "model_age_hours": round(age_hours, 2),
                    "components": prediction_result.get("components", {}),
                    "floor_pct": round(prediction_result.get("floor_pct", 0.0), 2)
                }

            except Exception as e:
                PREDICTION_ERRORS.labels(error_type=type(e).__name__).inc()
                raise e

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
predictor = LSTMPredictor()

# Initialize accuracy tracker
accuracy_tracker = AccuracyTracker()

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
        "models_loaded": [k for k in predictor.trained_models if k.endswith("_requests")]
    }

@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint."""
    return {
        "status": "ready", 
        "model_type": "lstm",
        "trained_models": len(predictor.trained_models)
    }

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/predict")
async def predict(request: Dict):
    """
    Generate predictions for a workload using LSTM.
    
    Expected request format:
    {
        "application": "nginx-test",
        "namespace": "default",
        "metric_type": "cpu",  // or "memory" or "requests"
        "metric_data": [...],  // Optional - will fetch from VictoriaMetrics if not provided
        "horizon_minutes": 60
    }
    """
    try:
        application = request.get("application")
        if not application:
            raise HTTPException(status_code=400, detail="Application name is required")
        
        metric_data = request.get("metric_data", [])
        metric_type = request.get("metric_type", "requests")
        horizon_minutes = request.get("horizon_minutes", 60)
        namespace = request.get("namespace", "default")

        if metric_type != "requests":
            raise HTTPException(status_code=400, detail="metric_type must be 'requests'")
        
        # If no metric_data provided, fetch from VictoriaMetrics
        if not metric_data:
            logger.info(f"No metric_data provided, fetching from VictoriaMetrics for {application}")
            metric_data = await fetch_metrics_from_vm(application, namespace, metric_type)
            if not metric_data:
                raise HTTPException(status_code=400, detail=f"Could not fetch {metric_type} metrics for {application}")
        
        logger.info(f"Generating LSTM prediction for {application} ({metric_type}) with {len(metric_data)} data points")

        # Run blocking ML work in a thread so health probes stay responsive
        loop = asyncio.get_event_loop()
        try:
            prediction = await loop.run_in_executor(
                None,
                functools.partial(predictor.predict, application, metric_data, horizon_minutes, metric_type, namespace)
            )
        except ValueError as e:
            # includes "Model must be trained" and inference-window refusals; the operator falls back to reactive
            raise HTTPException(status_code=422, detail=f"forecast refused: {e}")

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
                        application, namespace, metric_type, observation_at)
                    for predicted in matured:
                        accuracy_tracker.record(application, namespace, metric_type,
                                                predicted, current_actual)
                    if matured:
                        mape = accuracy_tracker.get_mape(application, namespace, metric_type)
                        mae = accuracy_tracker.get_mae(application, namespace, metric_type)
                        MAPE_GAUGE.labels(application=application, namespace=namespace, metric_type=metric_type).set(mape)
                        MAE_GAUGE.labels(application=application, namespace=namespace, metric_type=metric_type).set(mae)
                        logger.info(
                            f"Accuracy for {application}/{metric_type}: MAPE={mape:.1f}%, MAE={mae:.1f}, "
                            f"scored {len(matured)} forecast(s) whose target was {observation_at.isoformat()}, "
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
                    accuracy_tracker.store_forecast(application, namespace, metric_type,
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
                    accuracy_tracker.store_forecast(application, namespace, metric_type,
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
                                application, namespace, metric_type, observation_at,
                                component=comp_name):
                            accuracy_tracker.record_component(
                                application, namespace, metric_type,
                                comp_name, float(predicted), current_actual
                            )
                            comp_mape = accuracy_tracker.get_component_mape(
                                application, namespace, metric_type, comp_name
                            )
                            COMPONENT_MAPE_GAUGE.labels(
                                application=application,
                                namespace=namespace,
                                component=comp_name
                            ).set(comp_mape)
        except Exception as e:
            logger.warning(f"Component gauge update error (non-fatal): {e}")

        # Include MAPE for requests metric type in the response
        # The operator always wants requests MAPE regardless of which metric was predicted
        try:
            response_mape = accuracy_tracker.get_mape(application, namespace, "requests")
        except Exception:
            response_mape = 0.0

        return JSONResponse(content={**prediction, "mape": response_mape})
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/train")
async def train_model(request: Dict):
    """
    Train LSTM model on provided data.
    
    Expected request format:
    {
        "application": "nginx-test",
        "metric_type": "cpu",
        "metric_data": [
            {"timestamp": "2024-01-01T10:00:00Z", "value": 0.0462},
            ...
        ]
    }
    """
    try:
        application = request.get("application")
        if not application:
            raise HTTPException(status_code=400, detail="Application name is required")
        
        metric_data = request.get("metric_data", [])
        metric_type = request.get("metric_type", "cpu")
        
        if len(metric_data) < 120:
            raise HTTPException(status_code=400, detail="Minimum 120 data points required for training")
        
        logger.info(f"Training LSTM model for {application} ({metric_type})")

        # Run blocking training in a thread so health probes stay responsive
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            functools.partial(predictor.train_on_data, application, metric_data, metric_type)
        )
        
        return JSONResponse(content={
            "success": True,
            "application": application,
            "metric_type": metric_type,
            "training_result": result,
            "message": f"LSTM model trained successfully with {len(metric_data)} data points"
        })
        
    except ValueError as e:
        logger.error(f"Training validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Training error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/models")
async def get_models():
    """Get information about available trained models."""
    models_info = {}
    for model_key, model in predictor.trained_models.items():
        if not model_key.endswith("_requests"):
            continue
        train_time = predictor.model_train_times.get(model_key)
        age_hours = (datetime.utcnow() - train_time).total_seconds() / 3600 if train_time else None
        is_stale = age_hours > predictor.MODEL_MAX_AGE_HOURS if age_hours is not None else True

        scaler_range = None
        if hasattr(model, 'scaler') and model.scaler is not None:
            try:
                scaler_range = {
                    "center": float(model.scaler.center_[0]),
                    "scale": float(model.scaler.scale_[0])
                }
            except (AttributeError, IndexError):
                pass

        validation = predictor.validation_metadata.get(model_key, {})

        models_info[model_key] = {
            "validation_status": validation.get("status"),
            "validation_mape": validation.get("mape"),
            "validation_old_mape": validation.get("old_mape"),
            "validation_timestamp": validation.get("timestamp"),
            "validation_age_hours": validation.get("age_hours"),
            "validation_decay_factor": validation.get("decay_factor"),
            "scaler_range": scaler_range,
            "age_hours": round(age_hours, 1) if age_hours is not None else None,
            "is_stale": is_stale,
            "provenance": predictor.model_meta.get(model_key) or {"provenance": "unknown"},
        }

    return {
        "model_type": "lstm",
        "trained_models": [k for k in predictor.trained_models if k.endswith("_requests")],
        "models": models_info,
        "model_directory": str(predictor.model_dir),
        "version": "3.8.0"
    }

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