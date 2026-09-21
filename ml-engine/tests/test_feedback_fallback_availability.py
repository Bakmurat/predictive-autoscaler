"""The feedback fallback must test availability, not a zero sentinel (Codex C-88 / D-110).

C-85 made the accuracy getters return None when nothing has been scored, so "unmeasured" and
"measured zero" stopped being the same value. The fallback that feeds the percentile rule was
never updated and still reads the old sentinel:

    mape = get_component_mape(..., "blended")
    if mape == 0.0:                       # <- "not enough entries yet"
        mape = get_mape(...)

so it has the two cases exactly backwards:
  * component None (nothing scored) -> `None == 0.0` is False -> the overall error is IGNORED
    and the floor input falls to 0.0, the neutral value. Demonstrated: (None, 25) -> 0.
  * component measured 0.0 (a perfect run) -> treated as missing -> REPLACED by the overall
    error. Demonstrated: (0, 25) -> 25.

A measured zero is evidence and must survive; an unmeasured component is what the fallback
exists for.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

APP, NS, MT = "nginx-test", "demo", "requests"


def _resolve(monkeypatch, component, overall):
    """Drive the API's own floor-input resolution with the two getters stubbed."""
    from api import main as api_main

    monkeypatch.setattr(api_main.accuracy_tracker, "get_component_mape",
                        lambda *a, **k: component)
    monkeypatch.setattr(api_main.accuracy_tracker, "get_mape", lambda *a, **k: overall)
    return api_main.resolve_floor_mape(APP, NS, MT)


def test_unmeasured_component_falls_back_to_the_overall_error(monkeypatch):
    """(None, 25) -> 25. Pre-fix this returned 0: `None == 0.0` is False, so the fallback
    never fired and the floor input silently became neutral."""
    assert _resolve(monkeypatch, None, 25.0) == 25.0


def test_a_measured_zero_component_is_kept_not_replaced(monkeypatch):
    """(0, 25) -> 0. Pre-fix this returned 25: a perfect component was mistaken for missing
    and overwritten by the overall error."""
    assert _resolve(monkeypatch, 0.0, 25.0) == 0.0


def test_both_unmeasured_is_the_neutral_input(monkeypatch):
    """Nothing scored anywhere: the percentile rule's neutral input, not a fabricated error."""
    assert _resolve(monkeypatch, None, None) == 0.0


def test_measured_component_wins_over_overall(monkeypatch):
    assert _resolve(monkeypatch, 12.5, 25.0) == 12.5


def test_a_getter_that_raises_is_neutral_not_fatal(monkeypatch):
    from api import main as api_main

    def boom(*a, **k):
        raise RuntimeError("tracker exploded")

    monkeypatch.setattr(api_main.accuracy_tracker, "get_component_mape", boom)
    monkeypatch.setattr(api_main.accuracy_tracker, "get_mape", lambda *a, **k: 25.0)
    assert api_main.resolve_floor_mape(APP, NS, MT) == 0.0


def test_a_measured_error_reaches_the_next_prediction(monkeypatch):
    """End to end over two requests: score a forecast, then prove the measured error is what
    the next prediction's floor input receives -- not the neutral 0.0."""
    from api import main as api_main

    tracker = api_main.accuracy_tracker
    tracker.pending.clear()
    tracker.history.clear()
    tracker.component_history.clear()

    target = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    # Request 1 queued a blended forecast of 1250 for `target`. The actual must clear
    # MIN_TRAFFIC_RPM (1000) or the entry is recorded but deliberately not scored.
    tracker.store_forecast(APP, NS, MT, target, 1250.0, component="blended")
    # The observation for that target arrives: actual 1000 -> 25% error on that entry.
    matured = tracker.take_matured(APP, NS, MT, target + timedelta(seconds=30), component="blended")
    assert matured == [1250.0], "the forecast must mature once its target has passed"
    tracker.record_component(APP, NS, MT, "blended", 1250.0, 1000.0)

    measured = tracker.get_component_mape(APP, NS, MT, "blended")
    assert measured is not None and measured > 0, f"expected a measured error, got {measured!r}"

    # Request 2: the floor input is that measured value, with no stubbing at all.
    assert api_main.resolve_floor_mape(APP, NS, MT) == pytest.approx(measured)
