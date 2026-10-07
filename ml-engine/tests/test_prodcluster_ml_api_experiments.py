"""The prodcluster ml-api experiment configuration (Task 03 U-31): E1 keeps the absolute q90 margin, E2 serves the
same ensemble forecasts with the relative q90 margin (the model lab's D-1062 candidate), configured, not coded.
(Plain-text checks: the test environment has no YAML parser.)
"""
import json
import os
import re

from api.ensemble_experiment import EnsembleExperiment

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
EXPERIMENTS = os.path.join(ROOT, "deploy", "prodcluster", "ml-engine", "ml-api-experiments.yaml")
DEMO = os.path.join(ROOT, "deploy", "prodcluster", "demo", "kustomization.yaml")


def ensemble_config():
    m = re.search(r"- name: ENSEMBLE_EXPERIMENT\n\s+value: '(\[.*\])'\n", open(EXPERIMENTS).read())
    assert m, "ENSEMBLE_EXPERIMENT must be a single-quoted JSON list"
    return m.group(1)


def test_e1_absolute_q90_and_e2_relative_q90_share_the_ensemble_source():
    exps = {e.application: e for e in EnsembleExperiment.parse_all(ensemble_config())}
    assert set(exps) == {"nginx-ensemble", "nginx-ensemble-q95"}
    e1, e2 = exps["nginx-ensemble"], exps["nginx-ensemble-q95"]
    assert (e1.id, e1.margin_mode, e1.margin_quantile, e1.partial_rule) == ("seasonal-ensemble-q90-v1", "absolute", 0.90, "refuse")
    assert (e2.id, e2.margin_mode, e2.margin_quantile, e2.partial_rule) == ("seasonal-ensemble-rq90-v1", "relative", 0.90, "refuse")
    assert e2.forecast_mode.startswith("seasonal-ensemble-rq90")
    for e in (e1, e2):
        assert (e.source_application, e.source_namespace, e.forecaster) == ("nginx-test", "demo", "seasonal-ensemble")


def test_e2_autoscaler_is_labelled_with_the_experiment_it_is_served():
    text = open(DEMO).read()
    block = [b for b in re.split(r"\n  - target: ", text)
             if b.startswith("{ kind: PredictiveAutoscaler, name: nginx-ensemble-q95-autoscaler }")]
    assert len(block) == 1
    assert re.search(r"path: /metadata/labels/experiment\n\s+value: seasonal-ensemble-rq90-v1\n", block[0])


def test_the_r1_forecaster_stays_disabled():
    assert all(e.forecaster == "seasonal-ensemble" for e in EnsembleExperiment.parse_all(ensemble_config()))
    assert "relative-profile-ar" not in json.dumps(json.loads(ensemble_config()))
