"""
Configuration for training LSTM models from VictoriaMetrics.
Each app gets its own trained model.
"""

import os

# VictoriaMetrics configuration
VICTORIA_METRICS_CONFIG = {
    # VictoriaMetrics query endpoint (in-cluster default; override with VICTORIA_METRICS_URL)
    "url": os.getenv("VICTORIA_METRICS_URL", "http://vmselect-vmst.monitoring.svc.cluster.local:8481/select/0/prometheus"),
}

# Applications to train models for
# Only requests model is trained -- CPU/Memory models removed in v4.3 Phase 7
APPLICATIONS = [
    {
        "name": "nginx-test",
        "namespace": "default",
        "pod_pattern": "nginx-test.*",
        "workload_name": "nginx-test",      # For Istio metrics
        "container": "nginx",
        "baseline_rpm": 60000,              # Baseline: 60k requests/min (100%)
        "use_request_metrics": True,        # Train on Istio request rate
        "description": "Nginx test deployment with Istio"
    },
    # Add more apps here as needed
]

# Training configuration
TRAINING_CONFIG = {
    # How many days of historical data to use
    "training_hours": int(os.getenv("TRAINING_HOURS", "168")),  # 7 days default
    
    # LSTM training epochs
    "epochs": int(os.getenv("TRAINING_EPOCHS", "50")),
    
    # Where to save trained models
    "model_dir": os.getenv("MODEL_DIR", "./models"),
    
    # Minimum data points required for training
    "min_data_points": 120,  # 10 hours at 5-minute intervals
}

# Prediction configuration
PREDICTION_CONFIG = {
    # How far ahead to predict (in minutes)
    "horizon_minutes": 60,  # 1 hour
    
    # Confidence threshold for scaling decisions
    "min_confidence": 0.7,
    
    # How often to retrain models (in hours)
    "retrain_interval_hours": 6,  # Match CronJob schedule (every 6 hours)
}
