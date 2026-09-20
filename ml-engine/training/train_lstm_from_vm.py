"""
LSTM training script using VictoriaMetrics data.
Train LSTM models on request rate for Kubernetes workloads.
"""

import os
import sys
import pandas as pd
import numpy as np
import json
from datetime import datetime
import logging
from pathlib import Path
import joblib

# Add the project root to the path
sys.path.append(str(Path(__file__).parent.parent))

from models.lstm_model import LSTMForecastModel
from data.victoriametrics_collector import VictoriaMetricsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



# The model needs sequence_length input steps plus STEPS_AHEAD target steps per training sequence,
# and both the outer 80/20 (train/test) split here and the inner 80/20 (train/validation) split in
# LSTMForecastModel.train must leave at least one sequence. Validity is decided by counting the
# sequences that would actually be built, not by a fractional threshold (see sequence_budget()).
EXPECTED_CADENCE_S = 600
CADENCE_TOLERANCE_S = 30      # a grid sample may deviate this much from its 10-minute slot
DEFAULT_SEQUENCE_LENGTH = 144  # 24 h of ten-minute steps (LSTMForecastModel default)
STEPS_AHEAD = 6                # one hour of ten-minute steps (LSTMForecastModel.STEPS_AHEAD)


def sequence_budget(n_points: int, sequence_length: int = DEFAULT_SEQUENCE_LENGTH,
                    steps_ahead: int = STEPS_AHEAD) -> dict:
    """How many sequences a contiguous run of n_points ten-minute observations yields.

    Mirrors the two splits exactly: train_rows = int(0.8 * n) (train_lstm_for_metric), then the model
    builds train_rows - L - S + 1 sequences and splits them int(0.8 * m) / rest (LSTMForecastModel.train).
    Evaluation needs the remaining test rows to hold at least one sequence as well; it is optional.
    """
    b = _budget(n_points, sequence_length, steps_ahead)
    b["min_points_for_training"] = min_points_for_training(sequence_length, steps_ahead)
    return b


def _budget(n_points, sequence_length, steps_ahead):
    window = sequence_length + steps_ahead
    train_rows = int(0.8 * n_points)
    test_rows = n_points - train_rows
    seqs = max(0, train_rows - window + 1)
    inner_train = int(0.8 * seqs)
    inner_val = seqs - inner_train
    return {"points": n_points, "train_rows": train_rows, "test_rows": test_rows, "window": window,
            "sequences": seqs, "train_sequences": inner_train, "validation_sequences": inner_val,
            "evaluation_available": test_rows >= window}


def min_points_for_training(sequence_length: int = DEFAULT_SEQUENCE_LENGTH, steps_ahead: int = STEPS_AHEAD) -> int:
    """Smallest n such that the budget has >= 1 training and >= 1 validation sequence."""
    n = sequence_length + steps_ahead
    while True:
        b = _budget(n, sequence_length, steps_ahead)
        if b["train_sequences"] >= 1 and b["validation_sequences"] >= 1:
            return n
        n += 1


def preflight_history(df, sequence_length: int = DEFAULT_SEQUENCE_LENGTH, steps_ahead: int = STEPS_AHEAD):
    """Validate fetched history against an explicit ten-minute grid.

    Returns (ok, reason, prepared_df, info). prepared_df is the longest contiguous run of grid slots
    (sorted, one row per slot, duplicates and non-finite values removed) — the data the model trains on.
    """
    import numpy as np
    import pandas as pd
    info = {"raw_points": 0 if df is None else int(len(df)), "pandas": pd.__version__}
    if df is None or df.empty:
        return False, "insufficient history: have 0 of %d ten-minute points" % min_points_for_training(sequence_length, steps_ahead), None, info
    d = df[["timestamp", "value"]].copy()
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    n_nonfinite = int((~np.isfinite(d["value"])).sum())
    d = d[np.isfinite(d["value"])]
    d["timestamp"] = pd.to_datetime(d["timestamp"], utc=True)
    d = d.sort_values("timestamp")
    n_dup = int(d["timestamp"].duplicated().sum())
    d = d.drop_duplicates("timestamp", keep="last")
    info.update({"nonfinite_dropped": n_nonfinite, "duplicates_dropped": n_dup})
    if d.empty:
        return False, "insufficient history: no finite observations", None, info
    # Snap every sample to its ten-minute slot; a sample farther than the tolerance from any slot is
    # a cadence violation, and two samples in one slot after snapping are duplicates (keep the last).
    # Seconds since the epoch, computed with Timestamp arithmetic (portable across pandas versions;
    # .astype("int64") on tz-aware columns is not).
    epoch = ((d["timestamp"] - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).astype("int64")
    slot = ((epoch + EXPECTED_CADENCE_S // 2) // EXPECTED_CADENCE_S) * EXPECTED_CADENCE_S
    off = (epoch - slot).abs()
    n_offgrid = int((off > CADENCE_TOLERANCE_S).sum())
    d = d[off <= CADENCE_TOLERANCE_S].copy()
    d["slot"] = slot[off <= CADENCE_TOLERANCE_S]
    d = d.drop_duplicates("slot", keep="last")
    info["offgrid_dropped"] = n_offgrid
    if d.empty:
        return False, "unexpected cadence: no samples on the ten-minute grid", None, info
    slots = d["slot"].to_numpy()
    expected = int((slots[-1] - slots[0]) // EXPECTED_CADENCE_S) + 1
    missing = expected - len(slots)
    # Longest contiguous run of consecutive slots (missing slots break the run).
    breaks = np.flatnonzero(np.diff(slots) != EXPECTED_CADENCE_S)
    starts = np.concatenate(([0], breaks + 1)); ends = np.concatenate((breaks + 1, [len(slots)]))
    lengths = ends - starts
    k = int(np.argmax(lengths)); run = d.iloc[starts[k]:ends[k]]
    info.update({"grid_first": pd.Timestamp(slots[0], unit="s", tz="UTC").isoformat(),
                 "grid_last": pd.Timestamp(slots[-1], unit="s", tz="UTC").isoformat(),
                 "expected_slots": expected, "present_slots": int(len(slots)), "missing_slots": int(missing),
                 "contiguous_runs": int(len(lengths)), "longest_run_points": int(lengths[k]),
                 "run_first": run["timestamp"].iloc[0].isoformat(), "run_last": run["timestamp"].iloc[-1].isoformat()})
    budget = sequence_budget(int(lengths[k]), sequence_length, steps_ahead)
    info["sequence_budget"] = budget
    need = budget["min_points_for_training"]
    if budget["train_sequences"] < 1 or budget["validation_sequences"] < 1:
        why = "insufficient history: have %d of %d ten-minute points" % (int(lengths[k]), need)
        if missing:
            why += " in the longest contiguous run (%d slot(s) missing across %d run(s))" % (missing, len(lengths))
        return False, why, None, info
    prepared = run[["timestamp", "value"]].reset_index(drop=True)
    prepared["timestamp"] = prepared["timestamp"].dt.tz_localize(None)  # model uses naive UTC index
    return True, "ok: %d contiguous points (%d train / %d validation sequences; evaluation %s)" % (
        int(lengths[k]), budget["train_sequences"], budget["validation_sequences"],
        "available" if budget["evaluation_available"] else "unavailable"), prepared, info


def train_lstm_for_metric(
    df: pd.DataFrame,
    app_name: str,
    metric_type: str,
    model_dir: Path,
    epochs: int = 50,
    min_data_points: int = None,
    atomic: bool = False,
    sequence_length: int = DEFAULT_SEQUENCE_LENGTH
) -> dict:
    """
    Train LSTM model for a specific metric.

    Args:
        df: DataFrame with columns [timestamp, value]
        app_name: Application name
        metric_type: 'cpu', 'memory', or 'requests'
        model_dir: Directory to save trained model
        epochs: Number of training epochs
        min_data_points: Minimum data points (auto-adjusted based on metric type)
        atomic: If True, save to .tmp file and return tmp path (caller handles rename)

    Returns:
        Training results dictionary
    """
    logger.info(f"Training LSTM for {app_name} - {metric_type}")
    logger.info(f"Data points: {len(df)}")
    logger.info(f"Value range: {df['value'].min():.2f} - {df['value'].max():.2f}")

    # Auto-adjust minimum based on metric type
    if min_data_points is None:
        if metric_type == "requests":
            min_data_points = 60  # Lower threshold for sparse Istio metrics
        else:
            min_data_points = 120  # Standard threshold

    if len(df) < min_data_points:
        raise ValueError(f"Insufficient data: {len(df)} points (minimum {min_data_points} required)")

    # Prepare data
    df = df.copy()
    df.set_index('timestamp', inplace=True)
    df = df.sort_index()

    # Split data: 80% train, 20% test
    train_size = int(0.8 * len(df))
    train_data = df[:train_size]
    test_data = df[train_size:]

    logger.info(f"Train size: {len(train_data)}, Test size: {len(test_data)}")

    # Provenance boundaries (UTC): training data start/end (= training cutoff), outer split boundary
    data_start = df.index[0]; train_end = train_data.index[-1]
    test_start = test_data.index[0] if len(test_data) else None
    data_end = df.index[-1]

    # Initialize and train model (sequence_length is the model default unless a smoke test overrides it)
    model = LSTMForecastModel(sequence_length=sequence_length)

    try:
        training_result = model.train(train_data, target_column='value', epochs=epochs)
        logger.info(f"Training completed successfully")

        # Evaluate on test data
        eval_result = model.evaluate(test_data, target_column='value')
        if eval_result.get('rmse') is None:
            logger.info(f"Evaluation - {eval_result.get('evaluation', 'unavailable')}")
        else:
            logger.info(f"Evaluation - RMSE: {eval_result['rmse']:.4f}, MAE: {eval_result['mae']:.4f}")

        # Make sample prediction -- Phase 16: Dense(6) outputs exactly 6 steps
        prediction = model.predict(steps_ahead=6, confidence_level=0.95)  # 1 hour ahead (6 * 10min)
        logger.info(f"Sample prediction confidence: {prediction['confidence']:.3f}")

        # Save model
        model_path = model_dir / f"lstm_{app_name}_{metric_type}.pkl"
        if atomic:
            tmp_path = model_dir / f"lstm_{app_name}_{metric_type}.pkl.tmp"
            joblib.dump(model, tmp_path)
            logger.info(f"Model saved to {tmp_path} (atomic mode, awaiting rename)")
        else:
            joblib.dump(model, model_path)
            logger.info(f"Model saved to {model_path}")

        result = {
            "success": True,
            "app_name": app_name,
            "metric_type": metric_type,
            "data_points": len(df),
            "train_points": len(train_data),
            "test_points": len(test_data),
            "training_metrics": training_result.get('training_metrics', {}),
            "evaluation_metrics": eval_result,
            "sample_prediction_confidence": prediction['confidence'],
            "model_path": str(model_path),
            "trained_at": datetime.utcnow().isoformat() + "Z",
            "sequence_length": sequence_length,
            "steps_ahead": STEPS_AHEAD,
            "epochs_requested": epochs,
            "data_start": data_start.isoformat() + "Z",
            "training_cutoff": data_end.isoformat() + "Z",
            "train_end": train_end.isoformat() + "Z",
            "test_start": (test_start.isoformat() + "Z") if test_start is not None else None,
            "model_metadata": training_result.get('metadata', {})
        }

        if atomic:
            result["tmp_model_path"] = str(tmp_path)

        return result

    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "app_name": app_name,
            "metric_type": metric_type
        }


def train_requests_only(
    vm_url: str,
    namespace: str,
    workload_name: str,
    app_name: str,
    baseline_rpm: float = 60000,
    hours: int = 168,
    model_dir: Path = None,
    epochs: int = 50,
    sequence_length: int = DEFAULT_SEQUENCE_LENGTH
) -> dict:
    """
    Train LSTM model for requests metric only.

    Used by CronJob training script. Fetches Istio request rate
    from VictoriaMetrics and trains a single requests model.

    Args:
        vm_url: VictoriaMetrics URL
        namespace: Kubernetes namespace
        workload_name: Workload name for Istio metrics
        app_name: Application name for model naming
        baseline_rpm: Baseline requests per minute for normalization
        hours: Hours of historical data to fetch
        model_dir: Directory to save models
        epochs: Training epochs

    Returns:
        Training result dictionary with tmp_model_path for atomic rename
    """
    logger.info(f"=== Training requests-only LSTM model for {app_name} ===")
    logger.info(f"VictoriaMetrics: {vm_url}")
    logger.info(f"Target: {namespace}/{workload_name}")
    logger.info(f"Fetching {hours} hours of data")

    # Initialize collector
    collector = VictoriaMetricsCollector(vm_url)

    # Fetch only request rate metrics
    df = collector.get_istio_request_rate(
        destination_workload=workload_name,
        namespace=namespace,
        hours=hours
    )

    if df is None or df.empty:
        logger.error("No request rate metrics retrieved from VictoriaMetrics")
        return {"success": False, "error": "No request rate metrics available"}

    logger.info(f"Fetched {len(df)} data points for requests metric")

    # Explicit ten-minute-grid preflight: decides by the sequences that will actually be built.
    ok, reason, prepared, info = preflight_history(df, sequence_length=sequence_length)
    logger.info("Preflight: %s", json.dumps(info, default=str))
    if not ok:
        logger.warning("Training skipped - %s", reason)
        return {"success": False, "skipped": True, "error": reason, "preflight": info}
    logger.info("Preflight %s", reason)
    df = prepared

    # Prepare model directory
    if model_dir is None:
        model_dir = Path(__file__).parent / "models"
    model_dir.mkdir(parents=True, exist_ok=True)

    # Train requests model with atomic write
    result = train_lstm_for_metric(
        df=df,
        app_name=app_name,
        metric_type="requests",
        model_dir=model_dir,
        epochs=epochs,
        atomic=True,
        sequence_length=sequence_length
    )
    if isinstance(result, dict):
        result["preflight"] = info

    return result


# Legacy -- use train_requests_only() for CronJob
def train_from_victoriametrics(
    vm_url: str,
    namespace: str,
    pod_pattern: str,
    container: str,
    app_name: str,
    cpu_request_millicores: float = 50,
    memory_request_mb: float = 64,
    workload_name: str = None,
    baseline_rpm: float = 10000,
    use_request_metrics: bool = False,
    hours: int = 168,  # 7 days
    model_dir: Path = None,
    epochs: int = 50
) -> dict:
    """
    Fetch data from VictoriaMetrics and train LSTM models.

    Legacy function kept for manual use and /train endpoint compatibility.
    For CronJob training, use train_requests_only() instead.

    Args:
        vm_url: VictoriaMetrics URL
        namespace: Kubernetes namespace
        pod_pattern: Pod name pattern
        container: Container name
        app_name: Application name for model naming
        cpu_request_millicores: CPU request in millicores
        memory_request_mb: Memory request in MB
        workload_name: Workload name for Istio metrics (optional)
        baseline_rpm: Baseline requests per minute for normalization
        use_request_metrics: Whether to train on Istio request rate
        hours: Hours of historical data to fetch
        model_dir: Directory to save models
        epochs: Training epochs

    Returns:
        Dictionary with training results
    """
    logger.info(f"=== Training LSTM models for {app_name} ===")
    logger.info(f"VictoriaMetrics: {vm_url}")
    logger.info(f"Target: {namespace}/{pod_pattern}/{container}")
    logger.info(f"Fetching {hours} hours of data")
    if use_request_metrics:
        logger.info(f"Including Istio request rate (baseline: {baseline_rpm} rpm)")

    # Initialize collector
    collector = VictoriaMetricsCollector(vm_url)

    # Fetch metrics (CPU, memory, and optionally requests)
    if use_request_metrics:
        metrics = collector.get_application_metrics_with_requests(
            namespace=namespace,
            pod_pattern=pod_pattern,
            container=container,
            workload_name=workload_name,
            cpu_request_millicores=cpu_request_millicores,
            memory_request_mb=memory_request_mb,
            baseline_rpm=baseline_rpm,
            hours=hours,
            include_requests=True
        )
    else:
        metrics = collector.get_application_metrics(
            namespace=namespace,
            pod_pattern=pod_pattern,
            container=container,
            cpu_request_millicores=cpu_request_millicores,
            memory_request_mb=memory_request_mb,
            hours=hours
        )

    if not metrics:
        logger.error("No metrics retrieved from VictoriaMetrics")
        return {"success": False, "error": "No metrics available"}
    for key in list(metrics):
        ok, reason, prepared, _info = preflight_history(metrics[key])
        if not ok:
            logger.warning("Skipping %s model - %s", key, reason)
            metrics.pop(key)
        else:
            metrics[key] = prepared
    if not metrics:
        return {"success": False, "skipped": True, "error": "no metric passed the history preflight"}

    # Prepare model directory
    if model_dir is None:
        model_dir = Path(__file__).parent / "models"
    model_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "app_name": app_name,
        "namespace": namespace,
        "pod_pattern": pod_pattern,
        "container": container,
        "models": {}
    }

    # Train CPU model
    if "cpu" in metrics:
        logger.info("\n--- Training CPU model ---")
        cpu_result = train_lstm_for_metric(
            df=metrics["cpu"],
            app_name=app_name,
            metric_type="cpu",
            model_dir=model_dir,
            epochs=epochs
        )
        results["models"]["cpu"] = cpu_result

    # Train Memory model
    if "memory" in metrics:
        logger.info("\n--- Training Memory model ---")
        memory_result = train_lstm_for_metric(
            df=metrics["memory"],
            app_name=app_name,
            metric_type="memory",
            model_dir=model_dir,
            epochs=epochs
        )
        results["models"]["memory"] = memory_result

    # Train Request Rate model (if available)
    if "requests" in metrics:
        logger.info("\n--- Training Request Rate model ---")
        request_result = train_lstm_for_metric(
            df=metrics["requests"],
            app_name=app_name,
            metric_type="requests",
            model_dir=model_dir,
            epochs=epochs
        )
        results["models"]["requests"] = request_result

    return results


def main():
    """Main training function."""

    # Import configuration
    sys.path.append(str(Path(__file__).parent.parent))
    from config import VICTORIA_METRICS_CONFIG, APPLICATIONS, TRAINING_CONFIG

    # Build configuration
    CONFIG = {
        "victoria_metrics_url": VICTORIA_METRICS_CONFIG["url"],
        "applications": APPLICATIONS,
        "hours": TRAINING_CONFIG["training_hours"],
        "epochs": TRAINING_CONFIG["epochs"],
        "model_dir": Path(TRAINING_CONFIG["model_dir"])
    }

    logger.info("=== LSTM Training Pipeline ===")
    logger.info(f"Configuration: {json.dumps(CONFIG, indent=2, default=str)}")

    all_results = []

    for app_config in CONFIG["applications"]:
        try:
            result = train_requests_only(
                vm_url=CONFIG["victoria_metrics_url"],
                namespace=app_config["namespace"],
                workload_name=app_config.get("workload_name", app_config["name"]),
                app_name=app_config["name"],
                hours=CONFIG["hours"],
                model_dir=CONFIG["model_dir"],
                epochs=CONFIG["epochs"]
            )
            all_results.append(result)

        except Exception as e:
            logger.error(f"Failed to train models for {app_config['name']}: {e}", exc_info=True)
            all_results.append({
                "app_name": app_config["name"],
                "success": False,
                "error": str(e)
            })

    # Save summary
    summary_path = CONFIG["model_dir"] / "training_summary.json"
    with open(summary_path, 'w') as f:
        json.dump({
            "training_date": datetime.utcnow().isoformat(),
            "config": CONFIG,
            "results": all_results
        }, f, indent=2, default=str)

    logger.info(f"\n=== Training Complete ===")
    logger.info(f"Summary saved to: {summary_path}")

    # Print summary
    for result in all_results:
        app_name = result.get("app_name", "unknown")
        if result.get("success"):
            logger.info(f"  {app_name}: Successfully trained")
            if result.get("evaluation_metrics"):
                if result['evaluation_metrics'].get('rmse') is not None:
                    logger.info(f"    RMSE={result['evaluation_metrics']['rmse']:.4f}")
        else:
            logger.error(f"  {app_name}: Failed - {result.get('error')}")


if __name__ == "__main__":
    main()
