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


def train_lstm_for_metric(
    df: pd.DataFrame,
    app_name: str,
    metric_type: str,
    model_dir: Path,
    epochs: int = 50,
    min_data_points: int = None,
    atomic: bool = False
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

    # Initialize and train model
    model = LSTMForecastModel()

    try:
        training_result = model.train(train_data, target_column='value', epochs=epochs)
        logger.info(f"Training completed successfully")

        # Evaluate on test data
        eval_result = model.evaluate(test_data, target_column='value')
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
            "trained_at": datetime.utcnow().isoformat()
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
    epochs: int = 50
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
        atomic=True
    )

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
                logger.info(f"    RMSE={result['evaluation_metrics']['rmse']:.4f}")
        else:
            logger.error(f"  {app_name}: Failed - {result.get('error')}")


if __name__ == "__main__":
    main()
