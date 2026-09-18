#!/usr/bin/env python3
"""
Train LSTM model for nginx-test deployment from VictoriaMetrics.
Trains only the requests model with atomic writes and sanity checks.
"""

import os
import sys
import json
import time
from pathlib import Path
import logging

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from training.train_lstm_from_vm import train_requests_only
from config import VICTORIA_METRICS_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main training function for nginx-test (requests model only)."""
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("Training LSTM Requests Model for Nginx Test Deployment")
    logger.info("=" * 80)

    # Configuration
    vm_url = VICTORIA_METRICS_CONFIG["url"]
    logger.info(f"VictoriaMetrics URL: {vm_url}")

    app_config = {
        "name": "nginx-test",
        "namespace": "default",
        "workload_name": "nginx-test",
        "baseline_rpm": 60000,
    }

    logger.info("Application Configuration:")
    for key, value in app_config.items():
        logger.info(f"  {key}: {value}")
    logger.info("")

    # Training parameters
    hours = 168  # 7 days of data
    epochs = 50

    logger.info(f"Training Parameters:")
    logger.info(f"  Historical data: {hours} hours ({hours // 24} days)")
    logger.info(f"  Training epochs: {epochs}")
    logger.info("")

    # Model save directory - use MODEL_DIR env var if set (for K8s), fallback to local path
    model_dir_env = os.environ.get("MODEL_DIR")
    if model_dir_env:
        model_dir = Path(model_dir_env)
        logger.info(f"  Using MODEL_DIR from environment: {model_dir}")
    else:
        model_dir = Path(__file__).parent.parent / "models" / "trained"
        logger.info(f"  Using local model directory: {model_dir}")
    model_dir.mkdir(parents=True, exist_ok=True)
    logger.info("")

    # Delete leftover CPU/Memory models (idempotent)
    for pattern in ["lstm_*_cpu.pkl", "lstm_*_memory.pkl"]:
        for f in model_dir.glob(pattern):
            logger.info(f"Removing unused model: {f}")
            f.unlink()

    # Train requests model with atomic write
    try:
        result = train_requests_only(
            vm_url=vm_url,
            namespace=app_config["namespace"],
            workload_name=app_config["workload_name"],
            app_name=app_config["name"],
            baseline_rpm=app_config["baseline_rpm"],
            hours=hours,
            model_dir=model_dir,
            epochs=epochs
        )

        if not result.get("success"):
            logger.error(f"Training failed: {result.get('error')}")
            sys.exit(1)

        # Sanity check: load the .tmp model and validate predictions
        tmp_path = result.get("tmp_model_path")
        model_path = result.get("model_path")

        if tmp_path:
            import joblib
            import numpy as np
            tmp_path = Path(tmp_path)
            logger.info(f"Running sanity check on {tmp_path}")

            try:
                test_model = joblib.load(tmp_path)
                test_pred = test_model.predict(steps_ahead=6)
                predictions = test_pred.get("predictions", [])

                # Validate predictions
                if len(predictions) == 0:
                    raise ValueError("No predictions returned")
                if any(np.isnan(v) for v in predictions):
                    raise ValueError(f"NaN values in predictions: {predictions}")
                if all(v == 0 for v in predictions):
                    raise ValueError(f"All-zero predictions: {predictions}")
                for v in predictions:
                    if v < 0 or v > 1_000_000:
                        raise ValueError(f"Prediction {v} outside sanity range [0, 1000000]")

                logger.info(f"Sanity check passed: predictions={[round(v, 2) for v in predictions]}")

            except Exception as e:
                logger.error(f"Sanity check FAILED: {e}")
                tmp_path.unlink(missing_ok=True)
                sys.exit(1)

            # Atomic rename: .tmp -> final path
            final_path = Path(model_path)
            os.rename(str(tmp_path), str(final_path))
            logger.info(f"Atomic rename: {tmp_path} -> {final_path}")
        else:
            logger.warning("No tmp_model_path in result, model was saved directly")

        # Print results
        logger.info("")
        logger.info("=" * 80)
        logger.info("Training Results")
        logger.info("=" * 80)

        req_info = result
        logger.info(f"Requests Model Trained Successfully")
        logger.info(f"  Model saved: {result.get('model_path')}")
        logger.info(f"  Training data points: {result.get('data_points')}")
        if result.get('evaluation_metrics'):
            logger.info(f"  RMSE: {result['evaluation_metrics']['rmse']:.4f}")
            logger.info(f"  MAE: {result['evaluation_metrics']['mae']:.4f}")

        # JSON summary line
        training_time = round(time.time() - start_time, 1)
        summary = {
            "status": "success",
            "model_path": str(result.get("model_path", "")),
            "data_points": result.get("data_points", 0),
            "rmse": result.get("evaluation_metrics", {}).get("rmse", 0),
            "mae": result.get("evaluation_metrics", {}).get("mae", 0),
            "training_time_seconds": training_time
        }
        print(json.dumps(summary))

        sys.exit(0)

    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"Training failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
