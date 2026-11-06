"""Model metadata with undefined training losses must remain inspectable over HTTP."""
from datetime import datetime
from types import SimpleNamespace

import numpy as np
from fastapi.testclient import TestClient


def test_models_endpoint_serializes_missing_statistics_without_mutation(monkeypatch):
    from api import main
    from tests.b4_helpers import install_model, make_signal
    signal = make_signal(namespace="demo", name="nginx-test")
    metrics = {'final_train_loss': float('nan'), 'final_val_loss': float('inf'),
               'finite_loss': 0.25, 'per_step': [np.float64('-inf'), 1.0]}
    model = SimpleNamespace(scaler=SimpleNamespace(center_=[600.0], scale_=[250.0]))
    rec = install_model(main.predictor, model, signal, training_cutoff='2026-09-25T00:00:00Z',
                        observed_at=datetime(2026, 9, 25), count=np.int64(3), metrics=metrics)
    response = TestClient(main.app, raise_server_exceptions=False).get('/models')
    assert response.status_code == 200, response.text
    entry = response.json()['models'][rec.key]
    assert entry['namespace'] == 'demo' and entry['name'] == 'nginx-test'
    assert entry['metric_query_sha256'] == signal.sha256
    assert entry['provenance']['observed_at'] == '2026-09-25T00:00:00'
    assert entry['provenance']['count'] == 3
    assert entry['scaler_range'] == {'center': 600.0, 'scale': 250.0}
    assert entry['provenance']['metrics'] == {'final_train_loss': None, 'final_val_loss': None,
                                             'finite_loss': 0.25, 'per_step': [None, 1.0]}
    assert entry['provenance']['artifact_sha256'] == 'a' * 64
    # The sandbox's validation results (train_on_data) are not part of the served models' description.
    assert 'validation_mape' not in entry and 'validation_status' not in entry
    # Serialization does not mutate the record's provenance.
    assert np.isnan(rec.meta['metrics']['final_train_loss'])
    assert np.isposinf(rec.meta['metrics']['final_val_loss'])
