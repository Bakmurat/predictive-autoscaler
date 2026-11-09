import json
import os

import pytest

import execlib as X
import freeze
from part2_fixtures import RUN, api, make_record, sign, ssh_keys, world  # noqa: F401


def reviewed(record, fn, helper, name="plan"):
    p = fn(record)
    saved = X.save_plan(record, helper, p, name)
    return X.load_plan(saved["plan"]), saved["plan_digest"]


def assemble(world):
    record = world["record"]
    plan, digest = reviewed(record, freeze.plan, "freeze")
    assert plan["problems"] == [], plan["problems"]
    freeze.assemble(record, plan, digest)
    return plan


def write_receipt(world, who, digest=None, at="2026-11-22T00:00:00Z", key=None, tamper=False, name=None):
    record = world["record"]
    body = freeze.receipt_body(record, who if who in ("alice", "bob") else "alice", now=lambda: at)
    body["reviewer"] = who
    if digest:
        body["runlock_sha256"] = digest
    path = world["tmp"] / "receipts" / f"{name or who}.json"
    path.write_text(json.dumps(body, sort_keys=True))
    sign(key or world["keys"][who], freeze.RECEIPT_NS, path)
    if tamper:
        body["statement"] = "edited after signing"
        path.write_text(json.dumps(body, sort_keys=True))
    return body


def write_gate(world, gate, who, outcome="pass", digest=None):
    d, _ = freeze._verified_lock(world["record"])
    body = {"schema": freeze.GATE_SCHEMA, "run_id": RUN, "runlock_sha256": digest or d, "gate": gate,
            "outcome": outcome, "reviewer": who, "evidence": {"report": "f" * 64}}
    path = world["tmp"] / "gates" / f"{gate}-{who}.json"
    path.write_text(json.dumps(body, sort_keys=True))
    sign(world["keys"][who], freeze.GATE_NS, path)


def test_plan_assembles_a_valid_run_lock_read_only(world):
    record = world["record"]
    before = len(world["api"].mutations())
    p = freeze.plan(record)
    assert p["problems"] == []
    lock = p["runlock"]
    assert lock["kubeconfig"]["cluster_uid"] == record.cluster_uid
    assert lock["capacity"] == {"c0_rps": 8, "c_rps": "7"}
    assert len(lock["activation_scope"]["target_uids"]) == 10
    assert lock["fault_scope"]["quota_uid"] == world["api"].obj("ResourceQuota", "c28-fixture-quota", "demo")[
        "metadata"]["uid"]
    assert [n["role"] for n in lock["infrastructure"]["nodes"]] == ["workload", "support"]
    assert len(lock["workload"]["plans"]) == 50
    assert len(world["api"].mutations()) == before
    assert not os.path.exists(record.resolve("evidence/runlock.json"))
    assert X.digest(freeze.plan(record)) == X.digest(p)


def test_assemble_locks_once(world):
    record = world["record"]
    p = assemble(world)
    c28 = X.import_harness()
    digest = c28.runlock.verify(record.resolve("evidence/runlock.json"), str(world["tmp"]))
    assert digest == p["runlock_canonical_sha256"]
    assert len(os.listdir(record.resolve("evidence/plans"))) == 50
    plan2, digest2 = reviewed(record, freeze.plan, "freeze")
    assert any("written and locked once" in x for x in plan2["problems"])
    with pytest.raises(X.StagingError):
        freeze.assemble(record, plan2, digest2)


def test_inputs_that_contradict_the_params_are_problems(world):
    inputs = json.load(open(world["tmp"] / "inputs.json"))
    inputs["capacity"]["c0_rps"] = 9
    (world["tmp"] / "inputs.json").write_text(json.dumps(inputs))
    probs = freeze.plan(world["record"])["problems"]
    assert any("differs from the params" in x for x in probs)


def test_prerequisite_hash_mismatch_is_a_problem(world, tmp_path):
    (tmp_path / "archive-receipt.json").write_text("{}")
    extra = dict(world["extra"])
    extra["freeze"] = dict(extra["freeze"], prerequisites={"archive": {"path": "archive-receipt.json",
                                                                      "sha256": "0" * 64}})
    record = X.load_record(world["make_record"](extra), ("freeze",))
    probs = freeze.plan(record)["problems"]
    assert any("prerequisite archive" in x for x in probs)


def test_unapplied_reservation_is_a_problem(world):
    import reserve
    record = world["record"]
    p = reserve.rollback_plan(record, world["kube"])
    saved = X.save_plan(record, p["helper"], p, "rollback-plan")
    reserve.rollback(record, world["kube"], X.load_plan(saved["plan"]), saved["plan_digest"])
    probs = freeze.plan(world["record"])["problems"]
    assert any("reservation is not applied" in x for x in probs)


def needs_ssh(world):
    if not world["have_ssh"]:
        pytest.skip("ssh-keygen is not available")


def test_two_distinct_signed_receipts_are_required(world):
    needs_ssh(world)
    assemble(world)
    record = world["record"]
    assert freeze.verify(record)["result"] == "FAIL"
    write_receipt(world, "alice")
    write_receipt(world, "alice", name="alice-again")          # the same reviewer twice counts once
    write_receipt(world, "mallory", key=world["keys"]["mallory"])   # not allowlisted
    out = freeze.verify(record)
    assert out["result"] == "FAIL" and "1 valid reviewer receipt" in out["reviewers"]
    write_receipt(world, "bob")
    out = freeze.verify(record)
    assert out["result"] == "PASS" and out["reviewers"] == "signed by ['alice', 'bob']"


def test_invalid_receipts_do_not_count(world):
    needs_ssh(world)
    assemble(world)
    record = world["record"]
    write_receipt(world, "alice", tamper=True)                       # edited after signing
    write_receipt(world, "bob", digest="0" * 64)                     # another run-lock
    ok, detail = freeze.reviewer_status(record, freeze._verified_lock(record)[0])
    assert not ok and "does not verify" in detail and "another run-lock digest" in detail
    write_receipt(world, "alice", at="2026-12-02T00:00:00Z", name="late")   # after the schedule start
    ok, detail = freeze.reviewer_status(record, freeze._verified_lock(record)[0])
    assert not ok and "not before the schedule start" in detail
    (world["tmp"] / "receipts" / "forged.json").write_text(json.dumps({"reviewer": "bob"}))   # unsigned
    assert "unsigned" in freeze.reviewer_status(record, freeze._verified_lock(record)[0])[1]


def test_gates_are_reported_never_approved(world):
    needs_ssh(world)
    assemble(world)
    record = world["record"]
    write_receipt(world, "alice"); write_receipt(world, "bob")
    before = sorted(os.listdir(world["tmp"] / "gates"))
    out = freeze.verify(record)
    assert {g: v["outcome"] for g, v in out["gates"].items()} == {g: "pending" for g in freeze.QUAL_GATES}
    assert sorted(os.listdir(world["tmp"] / "gates")) == before
    q, qd = reviewed(record, freeze.plan_qualification, "freeze-qualification", "qualification-plan")
    assert q["document"] is None and any("G3 is pending" in p for p in q["problems"])
    with pytest.raises(X.StagingError, match="no qualification"):
        freeze.qualification(record, q, qd)


def test_one_dissent_keeps_a_gate_from_passing(world):
    needs_ssh(world)
    assemble(world)
    write_gate(world, "G3", "alice"); write_gate(world, "G3", "bob", outcome="fail")
    g = freeze.gate_status(world["record"], freeze._verified_lock(world["record"])[0])["G3"]
    assert g["outcome"].startswith("not pass")


def test_qualification_from_signed_passes_is_valid(world):
    needs_ssh(world)
    assemble(world)
    record = world["record"]
    write_receipt(world, "alice"); write_receipt(world, "bob")
    for g in freeze.QUAL_GATES:
        write_gate(world, g, "alice"); write_gate(world, g, "bob")
    q, qd = reviewed(record, freeze.plan_qualification, "freeze-qualification", "qualification-plan")
    assert q["problems"] == [], q["problems"]
    assert "successor" in q["successor_required"]          # 60-day cap vs a two-month schedule
    doc = q["document"]
    assert doc["reviewers"] == ["alice", "bob"] and len(doc["scope"]["targets"]) == 6
    assert doc["image_digest"] == "sha256:" + "c" * 64 and doc["source_tree"] == "d" * 40
    freeze.qualification(record, q, qd)
    written = json.load(open(record.resolve("evidence/lab-qualification.json")))
    assert written == doc


def test_qualification_expiring_before_the_schedule_needs_a_successor(world):
    needs_ssh(world)
    extra = dict(world["extra"])
    extra["freeze"] = dict(extra["freeze"], qualification={"not_before": "2026-11-25T00:00:00Z",
                                                           "expires": "2027-01-10T00:00:00Z", "out": "q.json"})
    record = X.load_record(world["make_record"](extra), ("freeze",))
    world["record"] = record
    assemble(world)
    q = freeze.plan_qualification(record)
    assert "schedule and review a successor" in q["successor_required"]


def test_qualification_derives_from_the_signed_lock_and_rejects_drift(world):
    """Codex #32 item 1: changing the image digest or a target UID after signing must not qualify."""
    needs_ssh(world)
    assemble(world)
    record = world["record"]
    write_receipt(world, "alice"); write_receipt(world, "bob")
    for g in freeze.QUAL_GATES:
        write_gate(world, g, "alice"); write_gate(world, g, "bob")
    q = freeze.plan_qualification(record)
    assert q["problems"] == [] and q["document"]["scope"]["targets"][0]["uid"] == \
        json.load(open(record.resolve("evidence/runlock.json")))["activation_scope"]["targets"]["c28-pa-e-s20"]
    images_path = record.resolve("evidence/images.json")
    images = json.load(open(images_path))
    images["images"]["statistical"]["ref"] = "harbor.example/autoscaler/statistical-evaluation@sha256:" + "9" * 64
    open(images_path, "w").write(json.dumps(images))
    probs = freeze.plan_qualification(record)["problems"]
    assert any("images.json differs from the signed run-lock" in p for p in probs)
    images["images"]["statistical"]["ref"] = "harbor.example/autoscaler/statistical-evaluation@sha256:" + "c" * 64
    open(images_path, "w").write(json.dumps(images))
    targets_path = os.path.join(record.evidence_dir, "targets.json")
    targets = json.load(open(targets_path))
    targets["deployments"]["c28-pa-n-s20"] = "99999999-0000-4000-8000-000000000000"
    open(targets_path, "w").write(json.dumps(targets))
    probs = freeze.plan_qualification(record)["problems"]
    assert any("targets.json differs from the signed run-lock" in p for p in probs)


def test_assembly_requires_the_deadline_and_lateness_limit(world):
    inputs = json.load(open(world["tmp"] / "inputs.json"))
    del inputs["schedule"]["qualification_deadline_utc"]
    inputs["schedule"]["max_release_lateness_s"] = 0
    (world["tmp"] / "inputs.json").write_text(json.dumps(inputs))
    probs = "\n".join(freeze.plan(world["record"])["problems"])
    assert "qualification_deadline_utc" in probs and "max_release_lateness_s" in probs


def test_cli_live_plan(world, capsys):
    """Codex #32 item 6: the --live path lists Deployments by label (it used to raise TypeError)."""
    record = world["record"]
    assert freeze.main(["--record", record.path, "plan", "--live"], api=world["api"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["problems"] == []
    world["api"].obj("ResourceQuota", "c28-fixture-quota", "demo")["metadata"]["uid"] = \
        "99999999-0000-4000-8000-000000000001"
    assert freeze.main(["--record", record.path, "plan", "--live"], api=world["api"]) == 0
    assert any("quota's live UID differs" in p for p in json.loads(capsys.readouterr().out)["problems"])


def test_freeze_reads_a_rebuild_images_json_by_digest(tmp_path, make_record):
    """build_images.py build.attempt: freeze takes refs (digests) and the source commit, never a tag or attempt."""
    from part2_fixtures import COMMIT
    h = "ab" * 32
    doc = {"schema": "c28-images/v1", "run_id": RUN, "source_commit": COMMIT, "verified": True, "attempt": "r2",
           "images": {k: {"ref": f"harbor.example/autoscaler/{n}@sha256:{h}",
                          "tag": f"harbor.example/autoscaler/{n}:c28-{RUN}-r2"}
                      for k, n in (("operator", "operator"), ("statistical", "statistical-evaluation"),
                                   ("fixture", "c28-fixture"))}}
    doc["images"]["generator"] = {"ref": "grafana/k6@sha256:" + h}
    (tmp_path / "evidence").mkdir()
    (tmp_path / "evidence" / "images.json").write_text(json.dumps(doc))
    record = X.load_record(make_record())
    refs, commit = freeze._images(record, {"images": "evidence/images.json"})
    assert commit == COMMIT and refs == {k: v["ref"] for k, v in sorted(doc["images"].items())}
