"""Shared helpers of the demonstration-stage tests: the kind demonstration params, and the node simulation that runs
the REAL load/c28-demo.js (demo_sim.js) without k6."""

import copy
import json
import os
import shutil
import subprocess

import pytest

from c28 import demo, schedule

HERE = os.path.dirname(os.path.abspath(__file__))
PARAMS_DIR = os.path.join(HERE, "..", "..", "params")
SCRIPT = os.path.join(HERE, "..", "..", "load", "c28-demo.js")
SIM = os.path.join(HERE, "demo_sim.js")
MIN = 60_000
DAY = 86_400_000


def params():
    with open(os.path.join(PARAMS_DIR, "kind-demo.json")) as fh:
        return json.load(fh)


def prod_like():
    """The kind params with the shape of the real run: its own run id on another run's reservation, twelve shadow
    replicas, a per-replica capacity like the calibrated one."""
    p = params()
    p["run_id"] = "c28-demo-1"
    p["nodes"] = {"workload": "worker-w", "support": "worker-s"}
    p["reservation"]["taint_value"] = "c28-prod-1"
    d = p["demonstration"]
    d["run_id"] = "c28-demo-1"
    d["reservation_ref"] = {"run_id": "c28-prod-1", "taint_value": "c28-prod-1",
                            "nodes": {"workload": {"name": "worker-w", "uid": "aaaaaaaa-0000-4000-8000-000000000001"},
                                      "support": {"name": "worker-s", "uid": "aaaaaaaa-0000-4000-8000-000000000002"}}}
    d["shadow_replicas"] = 12
    d["capacity_per_replica_rps"] = 17.5
    p["residency"] = {r: {"allocatable": {"cpu": "8", "memory": "16Gi"},
                          "system_requests": {"cpu": "300m", "memory": "512Mi"}} for r in ("workload", "support")}
    p["residency"]["support_extra"] = []
    return p


def config(p=None, **kw):
    return demo.generator_config(p or params(), **kw)


def intended(cfg, m):
    """The table's count of ABSOLUTE minute m from the origin (its day's row); None outside the finite schedule."""
    if not 0 <= m < cfg["schedule_minutes"]:
        return None
    return cfg["per_minute_days"][m // 1440][m % 1440]


def edited(cfg, **fields):
    """A copy of a generator configuration with fields replaced and its digest recomputed (a test-only table)."""
    out = dict(cfg, **fields)
    out["digest"] = schedule.digest(out)
    return out


def scaled_day(cfg, day, factor):
    """The configuration with ONE day's row scaled: adjacent days that nobody can mistake for each other."""
    table = [list(row) for row in cfg["per_minute_days"]]
    table[day] = [round(n * factor) for n in table[day]]
    return edited(cfg, per_minute_days=table)


def node():
    exe = shutil.which("node")
    if exe is None:
        if os.environ.get("CI"):
            pytest.fail("node is required in CI for the c28-demo.js tests")
        pytest.skip("node not installed: the c28-demo.js tests cannot run here")
    return exe


def simulate(tmp_path, cfg, scenario):
    """Run demo_sim.js: the real script's generator on a virtual clock. Returns its JSON output."""
    cpath, spath = tmp_path / "config.json", tmp_path / "scenario.json"
    cpath.write_text(json.dumps(cfg))
    spath.write_text(json.dumps(scenario))
    out = subprocess.run([node(), SIM, SCRIPT, str(cpath), str(spath)], capture_output=True, text=True, timeout=300,
                         env={"PATH": os.environ.get("PATH", ""), "KUBECONFIG": "/dev/null"})
    doc = json.loads(out.stdout.strip().splitlines()[-1]) if out.stdout.strip() else {"error": out.stderr}
    assert out.returncode == 0 and "error" not in doc, doc.get("error") or out.stderr
    return doc


def scrape(start_ms, end_ms, **kw):
    return dict({"interval_ms": 15_000, "offset_ms": 3_000, "from_ms": start_ms, "to_ms": end_ms}, **kw)


def changed(p, path, value):
    """A deep copy of p with the dotted path set (a list index as [i])."""
    q = copy.deepcopy(p)
    cur = q
    parts = path.replace("]", "").replace("[", ".").split(".")
    for k in parts[:-1]:
        cur = cur[int(k)] if isinstance(cur, list) else cur[k]
    last = parts[-1]
    if value is KeyError:
        del cur[last]
    elif isinstance(cur, list):
        cur[int(last)] = value
    else:
        cur[last] = value
    return q
