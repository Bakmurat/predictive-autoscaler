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
    doc["workload"]["plans"] = {f"scored/201/{T}": plan["digest"]}
    doc["schedule"] = {"start_utc": "2026-11-19T00:00:00Z", "end_utc": "2026-11-22T00:00:00Z"}
    path = tmp_path / "runlock.json"
    path.write_text(json.dumps(doc))
    digest = runlock.lock(str(path))
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan))
    bundle = str(tmp_path / "bundle")
    collect.collect(bundle, doc["collection"]["query_api"], [T], "demo", B, B + 86400, digest,
                    files={"plan": [str(plan_file)]}, queries=doc["queries"], fetch=synthetic_fetch)
    return doc, digest, bundle


def test_a_bound_reduction_carries_its_binding(locked):
    doc, digest, bundle = locked
    out = reduce_bound(doc, digest, bundle, T, B)
    assert out["day"]["slo_violation_minutes"] == 16
    b = out["binding"]
    assert b["runlock_sha256"] == digest and b["target"] == T and b["capacity_rps"] == "35"
    assert len(b["bundle_sums_sha256"]) == 64 and b["block_start"] == "2026-11-20T00:00:00Z"


def test_an_edited_response_stops_the_reduction(locked):
    doc, digest, bundle = locked
    chunk = next(os.path.join(root, n) for root, _, names in os.walk(os.path.join(bundle, "queries", "k6_requests_per_minute"))
                 for n in names)
    with open(chunk) as fh:
        data = json.load(fh)
    data["data"]["result"][1]["values"] = [[t, "0"] for t, _ in data["data"]["result"][1]["values"]]
    with open(chunk, "w") as fh:
        json.dump(data, fh)
    with pytest.raises(BindingError, match="hashes do not verify"):
        reduce_bound(doc, digest, bundle, T, B)


@pytest.mark.parametrize("change, message", [
    (lambda doc: doc["queries"][0].update(expr=doc["queries"][0]["expr"].replace("60s", "5m")), "not the locked definition"),
    (lambda doc: doc["collection"].update(namespace="other"), "namespace or query API"),
    (lambda doc: doc["workload"].update(plans={"x": "4" * 64}), "no locked confirmation plan"),
    (lambda doc: doc["capacity"].update(c0_rps=80, c_rps="70"), "no locked confirmation plan"),
    (lambda doc: doc["schedule"].update(end_utc="2026-11-20T12:00:00Z"), "outside"),
])
def test_reduction_needs_the_locked_inputs(locked, change, message):
    doc, digest, bundle = locked
    d = copy.deepcopy(doc)
    change(d)
    with pytest.raises(BindingError, match=message):
        reduce_bound(d, digest, bundle, T, B)


def test_another_runlock_or_target_is_refused(locked):
    doc, digest, bundle = locked
    with pytest.raises(BindingError, match="another run-lock"):
        reduce_bound(doc, "0" * 64, bundle, T, B)
    with pytest.raises(BindingError, match="not a target"):
        reduce_bound(doc, digest, bundle, "c28-elsewhere-s20", B)


def test_scoring_requires_bound_blocks(locked):
    doc, digest, bundle = locked
    out = reduce_bound(doc, digest, bundle, T, B)
    good = {"blocks": [{"arm": "PA-N", "profile": "s20", "seed": 201, "binding": out["binding"]}]}
    check_bindings(good, doc, digest)
    for bad in ({"binding": {}}, {"binding": dict(out["binding"], runlock_sha256="0" * 64)},
                {"binding": dict(out["binding"], capacity_rps="36")}, {"profile": "s90"}):
        blk = dict(good["blocks"][0], **bad)
        with pytest.raises(BindingError):
            check_bindings({"blocks": [blk]}, doc, digest)
