"""
Standalone LSTM model for time series forecasting.
No external dependencies on base classes.

Phase 16: Dense(6) multi-step output, cyclical time features, RobustScaler.
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from datetime import datetime, timedelta
import logging
import warnings

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)


import tensorflow as tf

try:
    _register = tf.keras.utils.register_keras_serializable(name="asymmetric_mse")
except AttributeError:
    _register = lambda fn: fn  # noqa: E731 -- fallback if TF version lacks this API


# Phase 16 (D-01): Number of direct output steps
STEPS_AHEAD = 6


def generate_time_features(timestamps):
    """Generate sin/cos features for hour-of-day and day-of-week.

    Args:
        timestamps: array of datetime objects or pd.DatetimeIndex

    Returns:
        np.ndarray of shape (len(timestamps), 4) -- [hour_sin, hour_cos, dow_sin, dow_cos]
    """
    hours = np.array([t.hour + t.minute / 60.0 for t in timestamps])
    dows = np.array([t.weekday() for t in timestamps])  # 0=Monday, 6=Sunday

    hour_sin = np.sin(2 * np.pi * hours / 24.0)
    hour_cos = np.cos(2 * np.pi * hours / 24.0)
    dow_sin = np.sin(2 * np.pi * dows / 7.0)
    dow_cos = np.cos(2 * np.pi * dows / 7.0)

    return np.column_stack([hour_sin, hour_cos, dow_sin, dow_cos])


def weighted_percentile(values, weights, percentile):
    """Compute weighted percentile of a list of values.

    Args:
        values: list/array of numeric values
        weights: list/array of corresponding weights (higher = more influence)
        percentile: desired percentile (0-100)

    Returns:
        Interpolated weighted percentile value.
    """
    values = np.asarray(values, dtype=float)
    weights = np.asarray(weights, dtype=float)

    if len(values) == 1:
        return float(values[0])

    # Sort by value
    sort_idx = np.argsort(values)
    sorted_vals = values[sort_idx]
    sorted_weights = weights[sort_idx]

    # Cumulative normalized weights
    cumulative = np.cumsum(sorted_weights)
    cumulative = (cumulative - 0.5 * sorted_weights) / cumulative[-1]

    # Interpolate at desired percentile
    target = percentile / 100.0
    return float(np.interp(target, cumulative, sorted_vals))


@_register
def asymmetric_mse(y_true, y_pred):
    """Weighted MSE: under-predictions penalized 2:1 vs over-predictions.

    Phase 16 (D-03): Handles (batch, 6) shaped tensors with per-step decay weights.
    Step weights [1.0, 0.9, 0.8, 0.7, 0.6, 0.5] -- later steps weighted less.

    When error > 0 (y_true > y_pred, i.e. under-prediction), weight is 2.0.
    When error <= 0 (y_true <= y_pred, i.e. over-prediction), weight is 1.0.
    Defined at module level (not lambda/nested) for joblib serialization.
    """
    error = y_true - y_pred
    squared_error = tf.square(error)
    under_weight = tf.where(error > 0, 2.0, 1.0)

    # Per-step decay weights for multi-step output (D-03)
    # Use tf.shape for dynamic compatibility with both scalar and vector targets
    n_steps = tf.shape(y_true)[-1]
    step_weights = tf.constant([1.0, 0.9, 0.8, 0.7, 0.6, 0.5], dtype=tf.float32)
    step_weights = step_weights[:n_steps]
    step_weights = tf.reshape(step_weights, (1, -1))  # broadcast over batch

    return tf.reduce_mean(under_weight * step_weights * squared_error)


class LSTMForecastModel:
    """LSTM-based forecasting model for time series prediction."""

    def __init__(self, sequence_length: int = 144):
        """
        Initialize LSTM model.

        Args:
            sequence_length: Number of timesteps to use for prediction.
                Default: 144 = 24 hours at 10-min intervals.
                This allows the model to see a full daily cycle.
        """
        self.model = None
        self.sequence_length = sequence_length
        self.last_sequence = None
        self.scaler = None
        self.is_trained = False
        self.metadata = {}
        self.training_data = None  # Store for pattern learning
        self.trained_at = None  # Track when model was trained
        self.training_timestamps = None  # Store for pattern alignment

    def train(self, data: pd.DataFrame, target_column: str = 'value', epochs: int = 50,
              imputed: Optional[np.ndarray] = None) -> Dict:
        """
        Train LSTM model on the provided data.

        Phase 16: Dense(6) multi-step output with 5-feature input
        (metric + hour_sin + hour_cos + dow_sin + dow_cos).

        Args:
            data: DataFrame with datetime index and target column
            target_column: Name of column to predict
            epochs: Number of training epochs

        Returns:
            Dictionary with training results
        """
        try:
            import tensorflow as tf
            from tensorflow.keras.models import Sequential
            from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional
            from sklearn.preprocessing import RobustScaler
        except ImportError as e:
            logger.error(f"Missing dependency: {e}")
            logger.error("Install: pip install tensorflow scikit-learn")
            raise ImportError("TensorFlow and scikit-learn required for LSTM training")

        logger.info(f"Training LSTM model on {len(data)} data points")

        # Prepare data
        if not isinstance(data.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have datetime index")

        values = data[target_column].values.reshape(-1, 1)

        # Phase 16: minimum data check updated for multi-step targets
        min_required = self.sequence_length + STEPS_AHEAD
        if len(values) < min_required:
            raise ValueError(f"Insufficient data: {len(values)} points (need {min_required})")

        # Scale the data -- Phase 16 (D-08): RobustScaler for outlier-resistant normalization
        self.scaler = RobustScaler()
        scaled_data = self.scaler.fit_transform(values)

        # Phase 16 (D-05): Generate time features from DatetimeIndex
        time_features = generate_time_features(data.index)

        # Phase 16 (D-04): Create multi-step sequences with time features
        X, y = self._create_sequences(scaled_data.flatten(), time_features)

        if len(X) == 0:
            raise ValueError("Failed to create training sequences")

        # X already has shape (N, seq_len, 5) from _create_sequences -- no reshape needed

        # Split data: 80% train, 20% validation
        train_size = int(0.8 * len(X))
        X_train, X_val = X[:train_size], X[train_size:]
        y_train, y_val = y[:train_size], y[train_size:]

        # Gap-filled (imputed) slots may serve as inputs, never as validation labels or as labels in the
        # reported training metrics (data/gapfill.py rule). A sequence i has targets at rows
        # i+seq_len .. i+seq_len+STEPS_AHEAD-1.
        genuine_target = np.ones(len(X), dtype=bool)
        n_val_dropped = 0
        if imputed is not None and len(imputed) == len(values) and imputed.any():
            imp = np.asarray(imputed, dtype=bool)
            for i in range(len(X)):
                if imp[i + self.sequence_length:i + self.sequence_length + STEPS_AHEAD].any():
                    genuine_target[i] = False
            val_keep = genuine_target[train_size:]
            n_val_dropped = int((~val_keep).sum())
            X_val, y_val = X_val[val_keep], y_val[val_keep]
        if len(X_val) == 0:
            raise ValueError("no validation sequences with genuine target labels")

        logger.info(f"Training set: {len(X_train)}, Validation set: {len(X_val)} (imputed-target sequences excluded: {n_val_dropped})")

        # Build BiLSTM model -- Phase 16 (D-01): Dense(STEPS_AHEAD) output
        model = Sequential([
            Bidirectional(LSTM(128, activation='relu', return_sequences=True),
                          input_shape=(self.sequence_length, 5)),  # 5 input features
            Dropout(0.2),
            Bidirectional(LSTM(64, activation='relu', return_sequences=True)),
            Dropout(0.2),
            Bidirectional(LSTM(32, activation='relu')),
            Dropout(0.2),
            Dense(16, activation='relu'),
            Dense(STEPS_AHEAD)  # Phase 16 (D-01): 6-step direct output
        ])

        # Compile with Adam optimizer and asymmetric loss (ACC-01)
        # Asymmetric MSE penalizes under-prediction 2:1 vs over-prediction
        optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
        model.compile(optimizer=optimizer, loss=asymmetric_mse, metrics=['mae'])

        # Train model
        history = model.fit(
            X_train, y_train,
            batch_size=32,
            epochs=epochs,
            validation_data=(X_val, y_val),
            verbose=0,
            callbacks=[
                tf.keras.callbacks.EarlyStopping(
                    monitor='val_loss',
                    patience=15,
                    restore_best_weights=True,
                    verbose=1
                )
            ]
        )

        self.model = model
        self.last_sequence = scaled_data[-self.sequence_length:].flatten()
        self.training_data = scaled_data.flatten()  # Store for pattern analysis
        self.raw_training_values = values.flatten()  # Store unscaled values for pattern prediction
        self.is_trained = True
        self.trained_at = datetime.utcnow()
        self.training_timestamps = data.index  # Phase 16: store for pattern alignment

        # Calculate training metrics -- Phase 16: handle (N, 6) predictions; imputed-target sequences excluded
        keep_train = genuine_target[:train_size]
        X_metric, y_metric = (X_train[keep_train], y_train[keep_train]) if keep_train.any() else (X_train, y_train)
        train_pred = model.predict(X_metric, verbose=0)  # shape (N, 6)
        # Inverse transform each column separately (scaler was fit on (n,1))
        train_pred_rescaled = self.scaler.inverse_transform(train_pred.reshape(-1, 1)).reshape(train_pred.shape)
        y_train_rescaled = self.scaler.inverse_transform(y_metric.reshape(-1, 1)).reshape(y_metric.shape)

        # Average RMSE/MAE across all steps
        train_rmse = np.sqrt(np.mean((y_train_rescaled - train_pred_rescaled) ** 2))
        train_mae = np.mean(np.abs(y_train_rescaled - train_pred_rescaled))

        # Store metadata
        self.metadata = {
            'sequence_length': self.sequence_length,
            'target_column': target_column,
            'training_samples': len(X_train),
            'validation_samples': len(X_val),
            'validation_sequences_dropped_imputed_target': n_val_dropped,
            'training_metric_sequences': int(len(X_metric)),
            'epochs_trained': len(history.history['loss']),
            'final_train_loss': float(history.history['loss'][-1]),
            'final_val_loss': float(history.history['val_loss'][-1]) if history.history.get('val_loss') else None
        }

        logger.info(f"Training completed. RMSE: {train_rmse:.4f}, MAE: {train_mae:.4f}")

        return {
            'success': True,
            'training_metrics': {
                'rmse': float(train_rmse),
                'mae': float(train_mae)
            },
            'metadata': self.metadata
        }

    def predict(self, steps_ahead: int = STEPS_AHEAD, confidence_level: float = 0.95,
                origin: Optional[datetime] = None,
                input_timestamps: Optional[list] = None,
                seasonal_history: Optional[pd.Series] = None) -> Dict:
        """
        Make predictions using Dense(6) direct multi-step output with hybrid blending.

        Phase 16: Single forward pass replaces autoregressive loop.
        1. LSTM direct multi-step prediction (single model.predict call)
        2. Historical pattern lookup (same time-of-day on previous days)
        3. Blend both, weighted toward the pattern for longer horizons

        Args:
            steps_ahead: Number of future steps (Dense(6) always outputs 6, sliced if fewer requested)
            confidence_level: Confidence level
            origin: Forecast origin -- the timestamp of the LAST input observation. Targets are
                origin + 10min * (step + 1). Defaults to `input_timestamps[-1]`, else wall-clock UTC.
                (C-45: wall clock is only a last resort; it makes replay non-deterministic.)
            input_timestamps: Actual timestamps of `last_sequence`, used for the calendar features.
                Falls back to `self.input_timestamps`, then to a grid ending at `origin`.
            seasonal_history: Timestamp-indexed observations covering MORE than one day, used for
                the previous-day pattern lookup. Falls back to `self.seasonal_history`.
                (C-44: the 144-point inference window cannot supply a previous-day value; passing it
                here silently turned the blend into a no-op.)

        Returns:
            Dictionary with predictions and confidence
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before making predictions")

        # --- Forecast origin and input calendar features (C-45) ---
        if input_timestamps is None:
            input_timestamps = getattr(self, 'input_timestamps', None)
        if origin is None:
            if input_timestamps is not None and len(input_timestamps):
                origin = pd.Timestamp(input_timestamps[-1]).to_pydatetime()
            else:
                origin = datetime.utcnow()
                logger.warning("predict(): no origin and no input timestamps; "
                               "falling back to wall-clock UTC, which is not replayable")
        origin = pd.Timestamp(origin).tz_localize(None).to_pydatetime() \
            if pd.Timestamp(origin).tzinfo is not None else pd.Timestamp(origin).to_pydatetime()

        if input_timestamps is not None and len(input_timestamps) == self.sequence_length:
            seq_timestamps = [pd.Timestamp(t).to_pydatetime() for t in input_timestamps]
        else:
            seq_timestamps = [origin - timedelta(minutes=10 * (self.sequence_length - 1 - i))
                              for i in range(self.sequence_length)]
        seq_time_features = generate_time_features(seq_timestamps)

        # Build input: (1, 144, 5) -- [scaled_value, hour_sin, hour_cos, dow_sin, dow_cos]
        seq_values = self.last_sequence.reshape(-1, 1)  # (144, 1) -- already scaled
        X_input = np.hstack([seq_values, seq_time_features])  # (144, 5)
        X_input = X_input.reshape(1, self.sequence_length, 5)

        # Single forward pass -- outputs (1, 6) scaled values
        pred_scaled = self.model.predict(X_input, verbose=0)  # shape (1, 6)

        # Inverse transform: reshape to (6, 1) for scaler compatibility
        lstm_rescaled = self.scaler.inverse_transform(
            pred_scaled.reshape(-1, 1)
        ).flatten()  # shape (6,)

        # Handle steps_ahead != 6: slice or pad
        if steps_ahead < STEPS_AHEAD:
            lstm_rescaled = lstm_rescaled[:steps_ahead]
        elif steps_ahead > STEPS_AHEAD:
            # Pad with last predicted value
            pad_count = steps_ahead - STEPS_AHEAD
            lstm_rescaled = np.concatenate([lstm_rescaled, np.full(pad_count, lstm_rescaled[-1])])

        # --- Directional percentile (PRED-03) ---
        if self.last_sequence is not None and len(self.last_sequence) >= 12:
            prev_mean = np.mean(self.last_sequence[-12:-6])
            last_mean = np.mean(self.last_sequence[-6:])
            direction_pct = 70 if last_mean < prev_mean else 75  # Phase 14 (D-04): was 90
        else:
            direction_pct = 75  # Phase 14 (D-04): was 90

        # --- MAPE-adaptive percentile (PRED-02) ---
        mape_for_pct = getattr(self, 'mape_for_floor', 0.0)
        mape_pct = max(50, min(75, 75 - max(0, mape_for_pct - 10) * (25.0 / 20.0)))

        # --- Effective percentile ---
        if direction_pct >= 75:
            effective_pct = max(50, min(75, max(direction_pct, mape_pct)))
        else:
            effective_pct = max(50, min(75, min(direction_pct, mape_pct)))

        # --- Strategy 2: previous-day pattern, looked up BY TIMESTAMP (C-44, C-45) ---
        if seasonal_history is None:
            seasonal_history = getattr(self, 'seasonal_history', None)
        pattern_preds, pattern_source = self._pattern_forecast(
            origin=origin, steps_ahead=steps_ahead, seasonal_history=seasonal_history,
            effective_pct=effective_pct)
        pattern_available = pattern_preds is not None
        if not pattern_available:
            # No seasonal history: serve the network alone and SAY SO. Substituting the network
            # for the pattern (the pre-fix behaviour) made the blend a no-op and pinned agreement
            # at 1.0, inflating confidence.
            pattern_preds = lstm_rescaled.copy()

        # --- Blend strategies ---
        final_predictions = []
        blended_pre_floor = []
        pattern_weights = []
        for step in range(steps_ahead):
            pattern_weight = min(0.95, 0.7 + (step / max(steps_ahead, 1)) * 0.25)
            if not pattern_available:
                pattern_weight = 0.0  # network only; do not pretend a second opinion exists
            pattern_weights.append(float(pattern_weight))
            lstm_weight = 1.0 - pattern_weight

            blended = lstm_weight * lstm_rescaled[step] + pattern_weight * pattern_preds[step]
            blended_pre_floor.append(float(blended))
            # Phase 16 (D-13): Floor set to 0.0 (was 0.05 in Phase 14)
            floor_pct = 0.0  # Phase 16 (D-13, D-15): floor removed, code kept
            blended = blended * (1.0 + floor_pct)
            final_predictions.append(blended)

        final_predictions = np.array(final_predictions)
        # Phase 16 (D-15): Floor gauge reports 0.0
        floor_pct_value = 0.0  # Phase 16 (D-13): was 0.05

        # --- Confidence calculation ---
        # Agreement is only meaningful when two independent components exist (C-44).
        if pattern_available and len(final_predictions) > 1:
            agreement = 1.0 - np.mean(np.abs(lstm_rescaled - pattern_preds) /
                                      (np.maximum(lstm_rescaled, pattern_preds) + 1e-8))
            agreement = float(max(0.0, min(1.0, agreement)))
        else:
            agreement = None

        horizon_penalty = max(0.4, 1.0 - (steps_ahead / 288))
        # Without a second component there is no agreement evidence: use the neutral 0.5 that the
        # single-step path has always used, never the 1.0 that self-comparison produced.
        agreement_term = agreement if agreement is not None else 0.5
        confidence = 0.5 * agreement_term + 0.5 * horizon_penalty
        confidence = max(0.3, min(0.9, confidence))

        logger.info(f"Prediction blend: LSTM range [{lstm_rescaled.min():.0f}-{lstm_rescaled.max():.0f}], "
                     f"Pattern range [{pattern_preds.min():.0f}-{pattern_preds.max():.0f}] "
                     f"(source={pattern_source}), "
                     f"Final range [{final_predictions.min():.0f}-{final_predictions.max():.0f}], "
                     f"pct={effective_pct:.0f}, origin={origin.isoformat()}, "
                     f"confidence={confidence:.3f}")

        return {
            'predictions': final_predictions,
            'confidence': float(confidence),
            'model_type': 'lstm',
            'origin': origin.isoformat(),
            'target_timestamps': [(origin + timedelta(minutes=10 * (s + 1))).isoformat()
                                  for s in range(steps_ahead)],
            'components': {
                'lstm': lstm_rescaled.tolist() if hasattr(lstm_rescaled, 'tolist') else list(lstm_rescaled),
                'pattern': pattern_preds.tolist() if hasattr(pattern_preds, 'tolist') else list(pattern_preds),
                'pattern_source': pattern_source,
                'pattern_available': bool(pattern_available),
                'pattern_weights': pattern_weights,
                'agreement': agreement,
                'blended': blended_pre_floor,
                'final': [float(v) for v in final_predictions],
            },
            'floor_pct': float(floor_pct_value)
        }

    def _pattern_forecast(self, origin: datetime, steps_ahead: int,
                          seasonal_history, effective_pct: float):
        """Previous-day-same-time lookup, indexed by timestamp.

        Returns (values, source). `values` is None when no genuine previous-day observation is
        available for any step -- the caller must then serve the network alone rather than
        blending the network with itself (C-44).

        Each step's target time is origin + 10min*(step+1); we look for that clock time on each of
        the previous up to seven days, weighting the most recent day most heavily.
        """
        if seasonal_history is None or len(seasonal_history) == 0:
            return None, "unavailable"
        try:
            s = pd.Series(seasonal_history)
            idx = pd.DatetimeIndex(s.index)
            if idx.tz is not None:
                idx = idx.tz_convert(None)
            s = pd.Series(s.values, index=idx).sort_index()
        except Exception:
            logger.warning("pattern lookup: seasonal history is not timestamp-indexed")
            return None, "unindexed"

        span = (s.index[-1] - s.index[0]).total_seconds()
        if span < 24 * 3600:
            # Less than one full day cannot contain yesterday's value for any target.
            return None, "network_fallback"

        tolerance = pd.Timedelta(minutes=5)
        pattern, any_hit = [], False
        for step in range(steps_ahead):
            target = pd.Timestamp(origin) + pd.Timedelta(minutes=10 * (step + 1))
            day_values, day_weights = [], []
            for d in range(1, 8):
                want = target - pd.Timedelta(days=d)
                if want < s.index[0] - tolerance:
                    break
                pos = s.index.get_indexer([want], method="nearest")[0]
                if pos < 0:
                    continue
                if abs(s.index[pos] - want) <= tolerance:
                    day_values.append(float(s.iloc[pos]))
                    day_weights.append(0.3 ** (d - 1))
            if day_values:
                any_hit = True
                pattern.append(float(weighted_percentile(day_values, day_weights, effective_pct)))
            else:
                pattern.append(np.nan)

        if not any_hit:
            return None, "no_matching_history"

        # Fill any step with no history from the nearest step that had one.
        arr = np.array(pattern, dtype=float)
        if np.isnan(arr).any():
            good = np.flatnonzero(~np.isnan(arr))
            for i in np.flatnonzero(np.isnan(arr)):
                arr[i] = arr[good[np.argmin(np.abs(good - i))]]
        return arr, "seasonal_history"

    def evaluate(self, test_data: pd.DataFrame, target_column: str = 'value',
                 imputed: Optional[np.ndarray] = None) -> Dict:
        """
        Evaluate model performance on test data.

        Phase 16: Updated for Dense(6) multi-step output.

        Args:
            test_data: DataFrame with datetime index
            target_column: Name of target column

        Returns:
            Dictionary with evaluation metrics
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before evaluation")

        # Prepare test data
        values = test_data[target_column].values.reshape(-1, 1)
        scaled_data = self.scaler.transform(values)

        # Phase 16: Generate time features for evaluation data
        time_features = generate_time_features(test_data.index)

        # Create sequences with time features; drop sequences whose target labels include an imputed slot
        X_test, y_test = self._create_sequences(scaled_data.flatten(), time_features)
        n_eval_dropped = 0
        if imputed is not None and len(imputed) == len(values) and len(X_test) and np.asarray(imputed, dtype=bool).any():
            imp = np.asarray(imputed, dtype=bool)
            keep = np.array([not imp[i + self.sequence_length:i + self.sequence_length + STEPS_AHEAD].any() for i in range(len(X_test))])
            n_eval_dropped = int((~keep).sum())
            X_test, y_test = X_test[keep], y_test[keep]

        if len(X_test) == 0:
            logger.warning("Evaluation unavailable: test partition shorter than sequence_length + STEPS_AHEAD")
            return {
                'rmse': None,
                'mae': None,
                'mape': None,
                'r2': None,
                'confidence': 0.0,
                'evaluation': 'unavailable: test partition too short'
            }

        # X_test already has shape (N, seq_len, 5) from _create_sequences

        # Make predictions -- shape (N, 6)
        test_pred_scaled = self.model.predict(X_test, verbose=0)

        # Inverse transform: reshape all values to (n, 1) then back
        test_pred = self.scaler.inverse_transform(test_pred_scaled.reshape(-1, 1)).reshape(test_pred_scaled.shape)
        y_test_rescaled = self.scaler.inverse_transform(y_test.reshape(-1, 1)).reshape(y_test.shape)

        # Calculate metrics (averaged across all steps)
        rmse = np.sqrt(np.mean((y_test_rescaled - test_pred) ** 2))
        mae = np.mean(np.abs(y_test_rescaled - test_pred))

        # MAPE
        mape = np.mean(np.abs((y_test_rescaled - test_pred) / (y_test_rescaled + 1e-8))) * 100

        # R2 score
        ss_res = np.sum((y_test_rescaled - test_pred) ** 2)
        ss_tot = np.sum((y_test_rescaled - np.mean(y_test_rescaled)) ** 2)
        r2 = 1 - (ss_res / (ss_tot + 1e-8))

        # Confidence
        confidence = max(0.3, min(0.9, 1.0 - (rmse / (np.mean(y_test_rescaled) + 1e-8))))

        return {
            'rmse': float(rmse),
            'mae': float(mae),
            'mape': float(mape),
            'r2': float(r2),
            'confidence': float(confidence)
        }

    def _create_sequences(self, scaled_values: np.ndarray,
                          time_features: Optional[np.ndarray] = None,
                          steps_ahead: Optional[int] = None) -> Tuple[np.ndarray, np.ndarray]:
        """
        Create sequences for LSTM training with multi-step targets.

        Phase 16 (D-04, D-05): Produces (N, seq_len, 5) X and (N, steps_ahead) y.

        Args:
            scaled_values: 1D array of scaled metric values
            time_features: 2D array of shape (n_timesteps, 4) -- time features.
                           If None, X will have shape (N, seq_len, 1) for backward compat.
            steps_ahead: Number of future steps per target. Defaults to STEPS_AHEAD (6).

        Returns:
            Tuple of (X, y) arrays
        """
        if steps_ahead is None:
            steps_ahead = STEPS_AHEAD

        X, y = [], []
        n = len(scaled_values)

        for i in range(n - self.sequence_length - steps_ahead + 1):
            # Input: sequence of values with optional time features
            seq_vals = scaled_values[i:i + self.sequence_length].reshape(-1, 1)

            if time_features is not None:
                seq_time = time_features[i:i + self.sequence_length]
                x_sample = np.hstack([seq_vals, seq_time])  # (seq_len, 5)
            else:
                x_sample = seq_vals  # (seq_len, 1) -- backward compat

            X.append(x_sample)

            # Target: next `steps_ahead` scaled values
            y_sample = scaled_values[i + self.sequence_length:i + self.sequence_length + steps_ahead]
            y.append(y_sample)

        return np.array(X), np.array(y)

    @staticmethod
    def _is_old_model_format(model_obj) -> bool:
        """Check if loaded model is old Dense(1) format.

        Phase 16 (D-10, D-12): Detects models needing retraining for Dense(6).

        Returns True if model needs retraining.
        """
        if not hasattr(model_obj, 'model') or model_obj.model is None:
            return True  # No Keras model -- needs training
        try:
            output_shape = model_obj.model.output_shape
            # Old format: (None, 1) -- single step output
            # New format: (None, 6) -- multi-step output
            return output_shape[-1] == 1
        except Exception:
            return True  # Can't determine -- retrain to be safe
