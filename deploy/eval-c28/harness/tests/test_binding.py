"""Reduction and scoring are bound to the locked run-lock, the intact bundle, its queries, scope and plan (Codex review
of #28, finding 3: editing a hashed response must stop the reduction)."""

import copy
import json
import os

import pytest

from c28 import collect, runlock, schedule
from c28.__main__ import BindingError, check_bindings, reduce_bound
from test_collect_reduce import B, T, scheduled, synthetic_fetch
from test_runlock import valid_doc


@pytest.fixture
def locked(tmp_path):
    manifest = tmp_path / "EVAL-C28-MANIFEST.md"
    manifest.write_text("frozen manifest\n")
    plan = {"schema": "c28-k6-plan/v1", "kind": "scored", "seed": 201, "capacity_rps": 35.0, "target": T,
            "accelerate": 1, "scheduled_per_minute": scheduled()}
    plan["digest"] = schedule.digest(plan)
    doc = valid_doc(str(manifest))
    doc["workload"]["plans"][f"scored/201/{T}"] = plan["digest"]
    doc["schedule"] = {"start_utc": "2026-11-19T00:00:00Z", "end_utc": "2026-11-26T00:00:00Z",
                       "blocks": {k: ["2026-11-20T00:00:00Z"] if k == "PA-N/s20/201" else
                                  [f"2026-11-{19 + int(k.rsplit('/', 1)[1]) - 200:02d}T00:00:00Z"]
                                  for k in doc["schedule"]["blocks"]}}
    for role in ("executor", "reviewer"):                    # locked before the first scored run (section 12)
        doc["acknowledgments"][role]["at"] = "2026-11-18T12:00:00Z"
    path = tmp_path / "runlock.json"
    path.write_text(json.dumps(doc))
    digest = runlock.lock(str(path))
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan))
    bundle = str(tmp_path / "bundle")
    collect.collect(bundle, doc["collection"]["query_api"], [T], "demo", B, B + 86400, digest,
                    files={"plan": [str(plan_file)]}, queries=doc["queries"], fetch=synthetic_fetch)
    return doc, digest, bundle, plan_file


def test_a_bound_reduction_carries_its_binding(locked):
    doc, digest, bundle, _ = locked
    out = reduce_bound(doc, digest, bundle, T, B, 201)
    assert out["day"]["slo_violation_minutes"] == 16
    b = out["binding"]
    assert b["runlock_sha256"] == digest and b["target"] == T and b["capacity_rps"] == "35"
    assert len(b["bundle_sums_sha256"]) == 64 and b["block_start"] == "2026-11-20T00:00:00Z"


def test_an_edited_response_stops_the_reduction(locked):
    doc, digest, bundle, _ = locked
    chunk = next(os.path.join(root, n) for root, _, names in os.walk(os.path.join(bundle, "queries", "k6_requests_per_minute"))
                 for n in names)
    with open(chunk) as fh:
        data = json.load(fh)
    data["data"]["result"][1]["values"] = [[t, "0"] for t, _ in data["data"]["result"][1]["values"]]
    with open(chunk, "w") as fh:
        json.dump(data, fh)
    with pytest.raises(BindingError, match="hashes do not verify"):
        reduce_bound(doc, digest, bundle, T, B, 201)


@pytest.mark.parametrize("change, message", [
    (lambda doc: doc["queries"][0].update(expr=doc["queries"][0]["expr"].replace("60s", "5m")), "not the locked definition"),
    (lambda doc: doc["collection"].update(namespace="other"), "namespace or query API"),
    (lambda doc: doc["workload"]["plans"].update({f"scored/201/{T}": "4" * 64}), "locked confirmation plan"),
    (lambda doc: doc["capacity"].update(c0_rps=80, c_rps="70"), "locked confirmation plan"),
    (lambda doc: doc["schedule"].update(end_utc="2026-11-20T12:00:00Z"), "outside"),
    (lambda doc: doc["schedule"]["blocks"].update({"PA-N/s20/201": ["2026-11-21T00:00:00Z"]}), "not a permitted date"),
])
def test_reduction_needs_the_locked_inputs(locked, change, message):
    doc, digest, bundle, _ = locked
    d = copy.deepcopy(doc)
    change(d)
    with pytest.raises(BindingError, match=message):
        reduce_bound(d, digest, bundle, T, B, 201)


def test_another_runlock_or_target_is_refused(locked):
    doc, digest, bundle, _ = locked
    with pytest.raises(BindingError, match="another run-lock"):
        reduce_bound(doc, "0" * 64, bundle, T, B, 201)
    with pytest.raises(BindingError, match="permitted date|not a target"):
        reduce_bound(doc, digest, bundle, "c28-elsewhere-s20", B, 201)


def test_the_seed_must_match_its_locked_plan(locked):
    doc, digest, bundle, _ = locked
    with pytest.raises(BindingError, match="permitted date"):
        reduce_bound(doc, digest, bundle, T, B, 202)      # seed 202's slot is another date
    d = copy.deepcopy(doc)
    d["schedule"]["blocks"]["PA-N/s20/202"].append("2026-11-20T00:00:00Z")
    with pytest.raises(BindingError, match="plan of seed 202"):
        reduce_bound(d, digest, bundle, T, B, 202)        # a permitted date, but seed 202's locked plan is not here


def scored_block(out, seed=201, bundle=""):
    return {"arm": "PA-N", "profile": "s20", "seed": seed, "bundle": bundle, "binding": copy.deepcopy(out["binding"]),
            "windows": copy.deepcopy(out["windows"]), "day": copy.deepcopy(out["day"]),
            "validity": {"recorder_coverage": out["recorder_coverage"], "dispatch_fraction_min": out["dispatch_fraction_min"],
                         "reconciled": out["reconciled"], "max_transition_gap_s": 5, "timestamp_skew_s": 0.1}}


def test_scoring_rederives_every_block_from_its_bundle(locked):
    doc, digest, bundle, _ = locked
    out = reduce_bound(doc, digest, bundle, T, B, 201)
    good = scored_block(out, bundle=bundle)
    check_bindings({"blocks": [good]}, doc, digest)
    # The same seed-201 evidence presented as seeds 201-205 (Codex repro), with or without digests: refused.
    for seed in (202, 203):
        reused = scored_block(out, seed=seed, bundle=bundle)
        reused["binding"]["seed"] = seed
        with pytest.raises(BindingError):
            check_bindings({"blocks": [good, reused]}, doc, digest)
    stripped = scored_block(out, bundle=bundle)
    stripped["binding"].pop("bundle_sums_sha256")
    stripped["binding"].pop("plan_digest")
    with pytest.raises(BindingError, match="binding differs"):
        check_bindings({"blocks": [stripped]}, doc, digest)
    edited = scored_block(out, bundle=bundle)
    edited["windows"]["w01-step"]["slo_violation_minutes"] = 5
    with pytest.raises(BindingError, match="metrics differ"):
        check_bindings({"blocks": [edited]}, doc, digest)
    measured = scored_block(out, bundle=bundle)
    measured["validity"]["reconciled"] = True
    with pytest.raises(BindingError, match="metrics differ"):
        check_bindings({"blocks": [measured]}, doc, digest)
    for bad in ({"bundle": ""}, {"binding": {}}, {"profile": "s90"}):
        with pytest.raises(BindingError):
            check_bindings({"blocks": [dict(good, **bad)]}, doc, digest)


def test_the_study_scores_under_the_runlocks_applicability(locked):
    from c28.__main__ import check_applicability
    doc, _digest, _bundle, _ = locked
    blocks = [{"arm": "PA-N", "profile": "s20", "seed": 201, "families": ["seasonalNaive"]}]
    check_applicability({"applicability": copy.deepcopy(doc["applicability"]), "blocks": blocks}, doc)
    other = copy.deepcopy(doc["applicability"])
    other["manifest_sha256"] = "9" * 64                       # another manifest version
    with pytest.raises(BindingError, match="applicability"):
        check_applicability({"applicability": other, "blocks": blocks}, doc)
    # An observed family mismatch is a runtime violation for the scorer (contract FAIL), not a malformed binding.
    check_applicability({"applicability": copy.deepcopy(doc["applicability"]),
                         "blocks": [dict(blocks[0], families=["seasonalNaive", "statistical"])]}, doc)
