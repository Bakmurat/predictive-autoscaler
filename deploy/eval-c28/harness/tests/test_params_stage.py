"""Stage-scoped params validation: the reservation stage validates exactly the fields reserve.py consumes, strictly;
experiment readiness is only ever the full validation (Codex: reservation applied is not ready for the experiment)."""

import copy
import json
import os

import pytest

from c28 import render

PARAMS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "params")


def load(name):
    with open(os.path.join(PARAMS_DIR, name)) as fh:
        return json.load(fh)


def reservation_ready():
    p = load("prodcluster.template.json")
    p["run_id"] = "c28-r1"
    p["nodes"] = {"workload": "worker-w", "support": "worker-s"}
    p["reservation"]["taint_value"] = "c28-r1"
    p["release"] = "pa-eval"
    return p


def test_template_needs_its_reservation_fields_filled():
    problems = render.stage_problems(load("prodcluster.template.json"), "reservation")
    text = "; ".join(problems)
    for f in ("run_id", "nodes.workload", "nodes.support", "reservation.taint_value", "release"):
        assert f"{f} is still a placeholder" in text
    with pytest.raises(render.ParamsError, match="reservation stage"):
        render.validate_stage(load("prodcluster.template.json"), "reservation")


def test_reservation_stage_passes_while_experiment_fields_are_placeholders():
    p = reservation_ready()
    render.validate_stage(p, "reservation")
    problems = render.full_validation_problems(p)
    assert any("c0_rps" in x for x in problems)
    with pytest.raises(render.ParamsError):
        render.validate(p)


def test_full_validation_problems_is_empty_exactly_when_validate_passes():
    p = load("kind-smoke.json")
    render.validate(p)
    assert render.full_validation_problems(p) == []
    assert render.stage_problems(p, "reservation") == []


def test_full_validation_refuses_a_placeholder_in_an_otherwise_unchecked_field():
    p = load("kind-smoke.json")
    p["fixture"]["work_iterations"] = "<tuned once>"
    with pytest.raises(render.ParamsError, match="fixture.work_iterations"):
        render.validate(p)
    assert any("fixture.work_iterations" in x for x in render.full_validation_problems(p))


@pytest.mark.parametrize("value", ["<tuned once>", "  <x>", "__WORK_ITERATIONS__", " __X__ "])
def test_both_placeholder_styles_are_recognised(value):
    assert render._placeholder(value)
    p = load("kind-smoke.json")
    p["fixture"]["work_iterations"] = value
    assert render.placeholder_fields(p) == ["fixture.work_iterations"]
    with pytest.raises(render.ParamsError, match="fixture.work_iterations"):
        render.validate(p)


@pytest.mark.parametrize("value", ["", "__", "____", "___", "__x", "x__", "a__b__c", "/", [], {}, 0, None])
def test_non_placeholders_are_not_mistaken_for_one(value):
    assert not render._placeholder(value)


def test_double_underscore_placeholders_fail_the_reservation_stage():
    p = reservation_ready()
    p["nodes"]["workload"] = "__W_NODE__"
    p["reservation"]["taint_key"] = "__TAINT_KEY__"
    text = "; ".join(render.stage_problems(p, "reservation"))
    assert "nodes.workload is still a placeholder" in text and "reservation.taint_key is still a placeholder" in text


def test_an_optional_empty_value_still_passes():
    p = load("kind-smoke.json")
    p["residency"] = {role: {"allocatable": {"cpu": "4000m", "memory": "7.75Gi"},
                             "system_requests": {"cpu": "300m", "memory": "512Mi"}}
                      for role in ("workload", "support")}
    p["residency"]["support_extra"] = []                       # optional and legitimately empty
    render.validate(p)
    assert render.placeholder_fields(p) == [] and render.full_validation_problems(p) == []
    assert render.residency_params(p)["support_extra"] == []


def test_a_required_empty_value_is_refused():
    p = load("kind-smoke.json")
    p["routes"]["metrics_query_path"] = ""
    with pytest.raises(render.ParamsError, match="routes.metrics_query_path is required"):
        render.validate(p)
    assert any("routes.metrics_query_path is required" in x for x in render.full_validation_problems(p))


def test_full_validation_includes_the_reservation_stage():
    p = load("kind-smoke.json")
    p["reservation"]["taint_key"] = "not a key"
    with pytest.raises(render.ParamsError, match="qualified name"):
        render.validate(p)
    p = load("kind-smoke.json")
    p["reservation"]["taint_value"] = "someone-else"
    with pytest.raises(render.ParamsError, match="must be the run_id"):
        render.validate(p)


def test_full_validation_problems_never_raises_on_malformed_params():
    for bad in ([], {"schema": "c28-params/v1"}, dict(load("kind-smoke.json"), nodes="x")):
        assert render.full_validation_problems(bad)


def test_unknown_stage_is_a_programming_error():
    with pytest.raises(ValueError, match="unknown params stage"):
        render.stage_problems(reservation_ready(), "calibration")


@pytest.mark.parametrize("key", ["c28.devkuban.com/reserved", "reserved", "a.b/c_d.e-f"])
def test_qualified_names_accepted(key):
    p = copy.deepcopy(reservation_ready())
    p["reservation"]["taint_key"] = key
    assert render.stage_problems(p, "reservation") == []


@pytest.mark.parametrize("key", ["", "/x", "a/b/c", "Bad_Prefix.com/x", "x/-y", "x" * 64, 5])
def test_qualified_names_refused(key):
    p = copy.deepcopy(reservation_ready())
    p["reservation"]["taint_key"] = key
    assert any("taint_key" in x for x in render.stage_problems(p, "reservation"))
