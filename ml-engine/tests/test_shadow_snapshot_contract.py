"""Real model-wrapper regressions for the observer's recorded-input contract."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
import pytest
from test_blend_components import _fitted_model
from test_shadow_observer import issuance, make_history, END, GRID, SEQ
from observer.shadow import ShadowObserver, _parse

class Echo:
    output_shape = (None, 6)
    def __init__(self):
        self.inputs = []
    def predict(self, x, verbose=0):
        self.inputs.append(x.copy())
        return np.full((1, 6), x[0, -1, 0])

def fixture():
    history = pd.Series(600., index=pd.date_range(end=END, periods=8*144, freq='10min'))
    model = _fitted_model(history)
    model.model = Echo()
    model.last_sequence = model.scaler.transform(np.full((SEQ, 1), 100.)).ravel()
    model.input_timestamps = [END-timedelta(days=20)+GRID*i for i in range(SEQ)]
    return model, list(zip(history.index.to_pydatetime(), history.values))

def state(_):
    return {'mape_for_floor': 0., 'source': 'fixture chronology'}

def test_real_wrapper_uses_scaled_recorded_values_and_calendar():
    model, pts = fixture()
    before = model.last_sequence.copy()
    rec = ShadowObserver(make_history(pts), lambda _: model).observe(issuance())
    assert rec.predictors['raw_network'][0]['value'] == 600.
    from models.lstm_model import generate_time_features
    timestamps = [END-GRID*(SEQ-1-i) for i in range(SEQ)]
    np.testing.assert_allclose(model.model.inputs[0][0, :, 1:], generate_time_features(timestamps))
    np.testing.assert_array_equal(model.last_sequence, before)
    assert not hasattr(model, 'last_pattern_per_step')

def test_seasonal_history_uses_timestamp_index_and_explicit_feedback():
    model, pts = fixture()
    rec = ShadowObserver(make_history(pts), lambda _: model, replay_state_lookup=state).observe(issuance())
    assert rec.predictors['seasonal_only'][0]['value'] == 600.
    assert rec.reconstruction['feedback_source'] == 'fixture chronology'
    assert rec.observer_version == '2'

def test_missing_feedback_does_not_invent_a_seasonal_baseline():
    model, pts = fixture()
    model.mape_for_floor = 55.  # not known to be the state at this issuance
    rec = ShadowObserver(make_history(pts), lambda _: model).observe(issuance())
    assert rec.predictors['raw_network'][0]['value'] == 600.
    assert all(o['status']=='unavailable' and 'feedback' in o['detail'] for o in rec.predictors['seasonal_only'])
    assert model.mape_for_floor == 55.

@pytest.mark.parametrize('bad', ['missing','duplicate','offgrid','nonfinite'])
def test_inexact_window_never_reaches_network(bad):
    model, pts = fixture()
    window = [(t,v) for t,v in pts if t>=END-GRID*(SEQ-1)]
    if bad=='missing': window.pop(40)
    elif bad=='duplicate': window[40]=window[39]
    elif bad=='offgrid': window[40]=(window[40][0]+timedelta(seconds=1),600.)
    else: window[40]=(window[40][0],float('nan'))
    rec = ShadowObserver(lambda a,b: window, lambda _:model).observe(issuance())
    assert not rec.window_complete
    assert not model.model.inputs
    assert rec.predictors['raw_network'][0]['status']!='ok'
    assert rec.predictors['served_hybrid'][0]['value']==700.

def test_recorded_served_forecast_survives_missing_model_and_history():
    def boom(a,b): raise OSError('offline')
    rec = ShadowObserver(boom, lambda _:None).observe(issuance())
    assert all(o['value']==700. and o['status']=='ok' for o in rec.predictors['served_hybrid'])

@pytest.mark.parametrize('field',['target','step','length'])
def test_misaligned_issuance_or_artifact_fails_before_inference(field):
    model, pts = fixture(); item=issuance()
    if field=='target': item['forecasts'][0]['target_at']=item['forecasts'][1]['target_at']
    elif field=='step': item['forecasts'][0]['step']=2
    else: model.sequence_length=12
    rec=ShadowObserver(make_history(pts),lambda _:model).observe(item)
    assert not model.model.inputs
    assert rec.predictors['raw_network'][0]['status']!='ok'

def test_naive_timestamps_have_explicit_utc_meaning():
    assert _parse('2026-09-22T11:50:00') == END
    assert _parse('2026-09-22T13:50:00+02:00') == END

@pytest.mark.parametrize('state_value', [float('nan'), -1, None, 2.])
def test_feedback_without_valid_value_and_provenance_is_unavailable(state_value):
    model, pts=fixture()
    supplied={'mape_for_floor':state_value,'source':'' if state_value==2. else 'fixture'}
    rec=ShadowObserver(make_history(pts),lambda _:model,
                      replay_state_lookup=lambda _:supplied).observe(issuance())
    assert rec.predictors['raw_network'][0]['value']==600.
    assert rec.predictors['seasonal_only'][0]['status']=='unavailable'
    assert any('feedback state unavailable' in n for n in rec.notes)


def test_feedback_changes_across_origins_without_mutating_loaded_wrapper():
    model, pts=fixture()
    pts=[(t,100.+100.*(END.date()-t.date()).days) for t,v in pts]
    pts[-12:]=[(t,100.-i*5.) for i,(t,v) in enumerate(pts[-12:])]
    pts.append((END+GRID,40.))
    model.mape_for_floor=99.;old_sequence=model.last_sequence.copy()
    old_timestamps=model.input_timestamps.copy()
    supplied=iter([0.,30.])
    observer=ShadowObserver(make_history(pts),lambda _:model,
        replay_state_lookup=lambda _: {'mape_for_floor':next(supplied),'source':'chronology'})
    first=observer.observe(issuance());second=observer.observe(issuance(end=END+GRID))
    assert first.predictors['seasonal_only'][0]['status']=='ok'
    assert second.predictors['seasonal_only'][0]['status']=='ok'
    assert first.predictors['seasonal_only'][0]['value'] != second.predictors['seasonal_only'][0]['value']
    assert model.mape_for_floor==99.
    np.testing.assert_array_equal(model.last_sequence,old_sequence)
    assert model.input_timestamps==old_timestamps
    assert not hasattr(model,'last_pattern_per_step')


def test_wrong_model_output_alignment_does_not_relabel_components():
    model,pts=fixture()
    real_predict=model.predict
    def wrong(**kwargs):
        out=real_predict(**kwargs)
        out['target_timestamps'][0]=out['target_timestamps'][1]
        return out
    model.predict=wrong
    rec=ShadowObserver(make_history(pts),lambda _:model).observe(issuance())
    assert rec.predictors['raw_network'][0]['status']=='failed'
    assert 'origin/targets' in rec.predictors['raw_network'][0]['detail']
    assert rec.predictors['served_hybrid'][0]['value']==700.


def test_previous_day_lookup_failure_preserves_recorded_served_values():
    model,pts=fixture();lookup=make_history(pts)
    def intermittent(a,b):
        if b-a==timedelta(minutes=10): raise OSError('history not available')
        return lookup(a,b)
    rec=ShadowObserver(intermittent,lambda _:model).observe(issuance())
    assert rec.predictors['previous_day'][0]['status']=='failed'
    assert rec.predictors['served_hybrid'][0]['value']==700.


def test_malformed_recorded_target_is_not_a_valid_served_comparison():
    model,pts=fixture();item=issuance()
    item['forecasts'][0]['target_at']=item['forecasts'][1]['target_at']
    rec=ShadowObserver(make_history(pts),lambda _:model).observe(item)
    assert rec.predictors['served_hybrid'][0]['status']=='failed'
    assert rec.predictors['served_hybrid'][0]['value']==700.  # keep the malformed record's value

@pytest.mark.parametrize('bad', ['missing-end','duplicate-end'])
def test_persistence_requires_an_unambiguous_observation_at_recorded_origin(bad):
    model,pts=fixture()
    if bad=='missing-end':pts=pts[:-1]
    else:pts.append((END,123.))
    rec=ShadowObserver(make_history(pts),lambda _:model).observe(issuance())
    assert rec.predictors['persistence'][0]['status']=='unavailable'


def test_raw_component_is_independent_of_feedback_and_existing_wrapper_state_survives():
    model,pts=fixture();model.mape_for_floor=99.;model.last_pattern_per_step=['untouched']
    outputs=[]
    for value in [0.,40.]:
        rec=ShadowObserver(make_history(pts),lambda _:model,
                          replay_state_lookup=lambda _,v=value:{'mape_for_floor':v,'source':'fixture'}).observe(issuance())
        outputs.append(rec.predictors['raw_network'])
    assert outputs[0]==outputs[1]
    assert all(o['value']==600. for o in outputs[0])
    assert model.mape_for_floor==99. and model.last_pattern_per_step==['untouched']
