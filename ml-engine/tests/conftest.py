"""Test-wide safety: the suite never reaches a real cluster.

The forecasting service can run kubectl (the cold-start training Job). Whatever kubeconfig the developer has active
(for example a production context), tests run against an empty one, and the cold-start opt-in is cleared.
"""

import os

os.environ["KUBECONFIG"] = "/dev/null"
os.environ.pop("COLD_START_CRONJOB", None)


import sys

import pytest


@pytest.fixture(autouse=True)
def _empty_model_registry():
    """Each test starts and ends with an empty served-model registry in the module-level predictor."""
    yield
    main = sys.modules.get("api.main")
    if main is not None:
        for m in main.predictor.registry.snapshot():
            main.predictor.registry.remove(m["key"])
