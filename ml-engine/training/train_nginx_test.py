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

    # Workload identity comes from the environment so the same image trains any target
    # (defaults keep the original example). TRAINING_HOURS and TRAINING_EPOCHS are optional overrides.
    app_config = {
        "name": os.environ.get("TRAINING_WORKLOAD", "nginx-test"),
        "namespace": os.environ.get("TRAINING_NAMESPACE", "default"),
        "workload_name": os.environ.get("TRAINING_WORKLOAD", "nginx-test"),
        "baseline_rpm": int(os.environ.get("TRAINING_BASELINE_RPM", "60000")),
    }

    logger.info("Application Configuration:")
    for key, value in app_config.items():
        logger.info(f"  {key}: {value}")
    logger.info("")

    # Training parameters
    hours = int(os.environ.get("TRAINING_HOURS", "168"))  # 7 days of data by default
    epochs = int(os.environ.get("TRAINING_EPOCHS", "50"))
    # SMOKE-TEST ONLY: a shorter input window lets the whole train -> save -> load -> forecast pipeline
    # be exercised on a few hours of data. The benchmark model always uses the default (144 = 24 h);
    # the API serves whatever window the artifact carries, so never point the real API at a smoke model.
    sequence_length = int(os.environ.get("TRAINING_SEQUENCE_LENGTH", "144"))
    if sequence_length != 144:
        logger.warning("TRAINING_SEQUENCE_LENGTH=%d: smoke-test setting, not the benchmark model", sequence_length)

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
            epochs=epochs,
            sequence_length=sequence_length
        )

        if result.get("skipped"):
            logger.info(f"Training skipped (not an error): {result.get('error')}")
            sys.exit(0)
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
            final_path = Path(model_path)

        # Provenance sidecar (read by the API and reported to the operator): what the artifact was
        # trained on, where the splits fall, and the artifact's own hash. Written after the rename so the
        # hash is of the published file; the API treats a model without a sidecar as "provenance unknown".
        import hashlib
        h = hashlib.sha256()
        with open(final_path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        meta = {
            "artifact": final_path.name,
            "artifact_sha256": h.hexdigest(),
            "artifact_bytes": final_path.stat().st_size,
            "trained_at": result.get("trained_at"),
            "training_cutoff": result.get("training_cutoff"),
            "data_start": result.get("data_start"),
            "train_end": result.get("train_end"),
            "test_start": result.get("test_start"),
            "sequence_length": result.get("sequence_length"),
            "steps_ahead": result.get("steps_ahead"),
            "epochs_requested": result.get("epochs_requested"),
            "data_points": result.get("data_points"),
            "train_points": result.get("train_points"),
            "test_points": result.get("test_points"),
            "model_metadata": result.get("model_metadata"),
            "evaluation_metrics": result.get("evaluation_metrics"),
            "preflight": result.get("preflight"),
            "workload": app_config["workload_name"],
            "namespace": app_config["namespace"],
            "training_hours_requested": hours,
            "image": os.environ.get("IMAGE_REF", ""),
            "git_commit": os.environ.get("GIT_COMMIT", ""),
            "smoke_test": sequence_length != 144,
        }
        meta_tmp = final_path.with_suffix(".meta.json.tmp")
        with open(meta_tmp, "w") as fh:
            json.dump(meta, fh, indent=2, default=str)
        os.rename(str(meta_tmp), str(final_path.with_suffix(".meta.json")))
        logger.info(f"Provenance written: {final_path.with_suffix('.meta.json')} sha256={meta['artifact_sha256']}")

        # Print results
        logger.info("")
        logger.info("=" * 80)
        logger.info("Training Results")
        logger.info("=" * 80)

        req_info = result
        logger.info(f"Requests Model Trained Successfully")
        logger.info(f"  Model saved: {result.get('model_path')}")
        logger.info(f"  Training data points: {result.get('data_points')}")
        ev = result.get('evaluation_metrics') or {}
        if ev.get('rmse') is None:
            logger.info(f"  Evaluation: {ev.get('evaluation', 'unavailable')}")
        else:
            logger.info(f"  RMSE: {ev['rmse']:.4f}")
            logger.info(f"  MAE: {ev['mae']:.4f}")

        # JSON summary line
        training_time = round(time.time() - start_time, 1)
        summary = {
            "status": "success",
            "model_path": str(result.get("model_path", "")),
            "data_points": result.get("data_points", 0),
            "rmse": ev.get("rmse"),
            "mae": ev.get("mae"),
            "evaluation": ev.get("evaluation", "ok" if ev.get("rmse") is not None else "unavailable"),
            "training_cutoff": result.get("training_cutoff"),
            "artifact_sha256": meta["artifact_sha256"] if tmp_path else None,
            "sequence_length": sequence_length,
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
