"""Model metadata with undefined training losses must remain inspectable over HTTP."""
from datetime import datetime
from types import SimpleNamespace

import numpy as np
from fastapi.testclient import TestClient


def test_models_endpoint_serializes_missing_statistics_without_mutation(monkeypatch):
    from api import main
    key = 'nginx-test_requests'
    meta = {'artifact_sha256': 'a' * 64, 'training_cutoff': '2026-09-25T00:00:00Z',
            'observed_at': datetime(2026, 9, 25), 'count': np.int64(3),
            'metrics': {'final_train_loss': float('nan'), 'final_val_loss': float('inf'),
                        'finite_loss': 0.25, 'per_step': [np.float64('-inf'), 1.0]}}
    validation = {'status': 'insufficient', 'mape': float('nan'), 'old_mape': float('inf'),
                  'scored': 0}
    model = SimpleNamespace(scaler=SimpleNamespace(center_=[600.0], scale_=[250.0]))
    monkeypatch.setattr(main.predictor, 'trained_models', {key: model})
    monkeypatch.setattr(main.predictor, 'model_meta', {key: meta})
    monkeypatch.setattr(main.predictor, 'model_train_times', {key: datetime.utcnow()})
    monkeypatch.setattr(main.predictor, 'validation_metadata', {key: validation})
    response = TestClient(main.app, raise_server_exceptions=False).get('/models')
    assert response.status_code == 200, response.text
    entry = response.json()['models'][key]
    assert entry['provenance']['observed_at'] == '2026-09-25T00:00:00'
    assert entry['provenance']['count'] == 3
    assert entry['validation_mape'] is None
    assert entry['validation_old_mape'] is None
    assert entry['validation_scored'] == 0
    assert entry['validation_status'] == 'insufficient'
    assert entry['scaler_range'] == {'center': 600.0, 'scale': 250.0}
    assert entry['provenance']['metrics'] == {'final_train_loss': None, 'final_val_loss': None,
                                             'finite_loss': 0.25, 'per_step': [None, 1.0]}
    assert entry['provenance']['artifact_sha256'] == 'a' * 64
    assert np.isnan(meta['metrics']['final_train_loss'])
    assert np.isposinf(meta['metrics']['final_val_loss'])
    assert np.isnan(validation['mape'])
