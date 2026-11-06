"""C2: the statistical forecaster's modules import without TensorFlow, Keras, the legacy service (api.main), pandas or
scikit-learn (the light statistical image carries NumPy and SciPy only).

Each module is imported in a fresh interpreter with those imports blocked (an import of any of them raises), and the
interpreter then confirms none of them was loaded. A fresh process is needed: this test session may already hold
TensorFlow from the legacy tests.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ML_ENGINE = Path(__file__).resolve().parent.parent
BLOCKED = ("tensorflow", "keras", "tf_keras", "api.main", "pandas", "sklearn")
PACKAGES = ("forecasting", "evaluation")

PROBE = r"""
import importlib, importlib.abc, json, sys
BLOCKED = tuple(json.loads(sys.argv[1]))

class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if any(name == b or name.startswith(b + ".") for b in BLOCKED):
            raise ImportError(f"blocked import of {name}")
        return None

sys.meta_path.insert(0, Block())
importlib.import_module(sys.argv[2])
loaded = sorted(m for m in sys.modules if any(m == b or m.startswith(b + ".") for b in BLOCKED))
print(json.dumps(loaded))
"""


def modules():
    out = []
    for pkg in PACKAGES:
        d = ML_ENGINE / pkg
        if d.is_dir():
            out += [f"{pkg}.{p.stem}" if p.stem != "__init__" else pkg for p in sorted(d.glob("*.py"))]
    return out


def test_the_package_list_is_not_empty():
    assert {"forecasting.contracts", "forecasting.history", "forecasting.statistical", "forecasting.artifacts",
            "forecasting.worker"} <= set(modules())


@pytest.mark.parametrize("module", modules())
def test_module_imports_without_tensorflow_or_the_legacy_service(module):
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    env.update(KUBECONFIG="/dev/null", PYTHONPATH=str(ML_ENGINE))
    p = subprocess.run([sys.executable, "-c", PROBE, json.dumps(BLOCKED), module], cwd=ML_ENGINE, env=env,
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, p.stderr[-2000:]
    assert json.loads(p.stdout.strip().splitlines()[-1]) == []


def test_the_blocker_really_blocks():
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    env.update(KUBECONFIG="/dev/null", PYTHONPATH=str(ML_ENGINE))
    p = subprocess.run([sys.executable, "-c", PROBE, json.dumps(BLOCKED), "api.main"], cwd=ML_ENGINE, env=env,
                       capture_output=True, text=True, timeout=120)
    assert p.returncode != 0 and "blocked import" in p.stderr
