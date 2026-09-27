"""challenge-v1 offered-load schedule: bounds, statistics, and JS/Python equivalence (U-22)."""
import json
import math
import re
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKLOAD = ROOT / "deploy" / "eks-benchmark" / "workload" / "challenge-v1"
sys.path.insert(0, str(WORKLOAD))
import challenge_profile as cp  # noqa: E402

REPEATING_V2 = [250, 200, 150, 150, 200, 300, 750, 1250, 2000, 3000, 3750, 4250,
                4500, 5000, 5500, 6000, 5000, 4250, 3000, 2400, 1000, 600, 400, 300]
DAYS = 20
N = DAYS * 144


def js_schedule_block():
    text = (WORKLOAD / "k6-load-script-challenge-v1.yaml").read_text()
    m = re.search(r"// BEGIN SCHEDULE\n(.*?)// END SCHEDULE", text, re.S)
    assert m, "schedule markers missing from the k6 script"
    return "\n".join(line[4:] if line.startswith("    ") else line for line in m.group(1).splitlines())


def run_node(expr):
    node = shutil.which("node")
    if node is None:
        pytest.skip("node not installed")
    code = js_schedule_block() + f"\nconsole.log(JSON.stringify({expr}));\n"
    out = subprocess.run([node, "-e", code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_before_the_regime_the_schedule_is_exactly_repeating_v2():
    assert cp.PROFILE["base_pattern_utc_rpm"] == REPEATING_V2
    for h in range(48):
        t = cp.START - 48 * 3600 + h * 3600 + 300
        assert cp.rate_at(t) == REPEATING_V2[h % 24]


def test_multipliers_bounded_and_peak_inside_the_twelve_pod_envelope():
    m = np.array([x[0] for x in cp.multipliers(N)])
    lo, hi = cp.PROFILE["multiplier_bounds"]
    assert m.min() >= lo and m.max() <= hi
    peak = max(cp.rate_at(cp.START + k * cp.SLOT) for k in range(N))
    assert peak <= 12 * 600  # the arms' ceiling is 12 pods at 600 rpm each


def test_noise_is_correlated_and_drift_is_a_reversing_triangle():
    comps = cp.multipliers(N)
    noise = np.array([c[1] for c in comps])
    lag1 = np.corrcoef(noise[:-1], noise[1:])[0, 1]
    assert 0.75 < lag1 < 0.92            # AR(1) with phi 0.85, before clipping
    assert abs(noise.std() - cp.PROFILE["noise"]["sigma"]) < 0.015
    drift = np.array([c[2] for c in comps[: 72 * 6 + 1]])
    assert drift[0] == 0 and abs(drift.max() - 0.06) < 1e-9 and abs(drift.min() + 0.06) < 1e-9
    assert np.argmax(drift) == 18 * 6 and np.argmin(drift) == 54 * 6


def test_level_shifts_respect_the_dwell_and_the_bound():
    comps = cp.multipliers(N)
    levels = [c[3] for c in comps]
    changes = [k for k in range(1, N) if levels[k] != levels[k - 1]]
    assert len(changes) >= DAYS              # about two per day
    assert all(b - a >= 12 for a, b in zip(changes, changes[1:]))
    assert max(abs(v) for v in levels) <= 0.12


def test_hourly_planned_requests_follow_the_slots():
    h0 = cp.START + 15 * 3600
    expected = sum(cp.rate_at(h0 + i * 600) * 10 for i in range(6))
    assert cp.planned_requests(h0) == pytest.approx(expected)
    ratio = cp.planned_requests(h0) / (6000 * 60)
    assert 0.75 <= ratio <= 1.15


def test_js_profile_matches_the_sealed_profile_file():
    js = run_node("PROFILE")
    p = cp.PROFILE
    assert js["regimeStart"] == cp.START and js["seed"] == p["seed"] and js["slot"] == p["slot_seconds"]
    assert js["base"] == p["base_pattern_utc_rpm"]
    assert (js["phi"], js["sigma"], js["noiseClip"]) == (p["noise"]["phi"], p["noise"]["sigma"], p["noise"]["clip"])
    assert (js["driftAmplitude"], js["driftPeriodHours"]) == (p["drift"]["amplitude"], p["drift"]["period_hours"])
    assert js["shiftProbability"] == pytest.approx(p["shifts"]["probability_per_slot"], rel=1e-15)
    assert (js["shiftMinDwell"], js["shiftMaxAbs"]) == (p["shifts"]["min_dwell_slots"], p["shifts"]["max_abs_level"])
    assert [js["lo"], js["hi"]] == p["multiplier_bounds"]


def test_js_random_stream_is_bit_identical():
    ks = list(range(0, 5000, 7)) + [2 ** 20, 2 ** 31 - 1]
    js = run_node("[" + ",".join(f"[u01(20260928,{k},0),u01(20260928,{k},2),u01(20260928,{k},3)]" for k in ks) + "]")
    py = [[cp.u01(20260928, k, 0), cp.u01(20260928, k, 2), cp.u01(20260928, k, 3)] for k in ks]
    assert js == py


def test_js_schedule_matches_python_rates_exactly():
    js_m = run_node(f"multipliers({N})")
    py_m = [c[0] for c in cp.multipliers(N)]
    assert len(js_m) == len(py_m)
    # Math.log/cos may differ in the last ulp between runtimes; the served integer rates may not.
    assert max(abs(a - b) for a, b in zip(js_m, py_m)) < 1e-9
    times = [cp.START + k * 600 + 17 for k in range(0, N, 5)] + [cp.START - 3600 * h for h in range(1, 30)]
    js_r = run_node("(function(){const r=makeRateAt(%d);return [%s].map(r);})()"
                    % (cp.START + N * 600, ",".join(str(t) for t in times)))
    assert js_r == [cp.rate_at(t) for t in times]
