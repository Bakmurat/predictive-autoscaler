import hashlib
import json
import os
import subprocess

import pytest

import build_images as B
import execlib as X

from part2_fixtures import COMMIT, make_record  # noqa: F401
GEN_RAW = b'{"schemaVersion":2,"generator":true}'
GEN = "grafana/k6@sha256:" + hashlib.sha256(GEN_RAW).hexdigest()


class FakeTools:
    """A fake git/docker/tar/bash: a registry of manifests, local images, and a call log."""

    def __init__(self):
        self.registry = {GEN: GEN_RAW}
        self.local = set()
        self.calls = []
        self.head = COMMIT
        self.dirty = ""
        self.pushed_branch = "origin/main"
        self.registry_error = ""
        self.sbom_rc = 0

    def __call__(self, argv, capture_output=True, text=True, timeout=None, env=None, input=None):
        self.calls.append(list(argv))
        assert env["KUBECONFIG"] == "/dev/null"
        ok = lambda out="": subprocess.CompletedProcess(argv, 0, out, "")  # noqa: E731
        fail = lambda err: subprocess.CompletedProcess(argv, 1, "", err)  # noqa: E731
        if argv[0] == "git":
            sub = argv[3] if len(argv) > 3 else argv[1]
            if sub == "rev-parse":
                return ok(self.head + "\n")
            if sub == "status":
                return ok(self.dirty)
            if sub == "branch":
                return ok(self.pushed_branch)
            if sub == "archive":
                open(argv[argv.index("-o") + 1], "wb").write(b"tar")
                return ok()
            return ok("git version 2.43\n")
        if argv[0] == "tar" or argv[0] == "bash":
            if argv[0] == "bash":
                self.local.add(argv[-1])
            return ok()
        if len(argv) > 1 and argv[1].endswith("sbom-check-statistical.py"):
            return subprocess.CompletedProcess(argv, self.sbom_rc, "", "")
        assert argv[0] == "docker", argv
        if argv[1:4] == ["buildx", "imagetools", "inspect"]:
            ref = argv[-1]
            if self.registry_error:
                return fail(self.registry_error)
            if ref in self.registry:
                return ok(self.registry[ref].decode())
            return fail(f"ERROR: {ref}: not found")
        if argv[1:3] == ["image", "inspect"]:
            ref = argv[-1]
            if "--format" in argv:
                return ok(json.dumps([ref]))
            return ok("[]") if ref in self.local else fail("Error: No such image")
        if argv[1] == "build":
            self.local.add(argv[argv.index("-t") + 1])
            return ok()
        if argv[1] == "push":
            tag = argv[2]
            raw = json.dumps({"image": tag}).encode()
            digest = "sha256:" + hashlib.sha256(raw).hexdigest()
            repo = tag.rsplit(":", 1)[0]
            self.registry[tag] = raw
            self.registry[f"{repo}@{digest}"] = raw
            return ok(f"{tag.rsplit(':', 1)[1]}: digest: {digest} size: 528\n")
        if argv[1] == "pull":
            return ok()
        if argv[1] == "version":
            return ok("29.9.0\n")
        raise AssertionError(argv)


@pytest.fixture
def env(tmp_path, make_record):
    dc = tmp_path / "docker"
    dc.mkdir()
    (dc / "config.json").write_text(json.dumps({"credHelpers": {"harbor.example": "pass"}}))
    (tmp_path / "checkout").mkdir()
    rec_path = make_record({"build": {"checkout": "checkout", "harbor": "harbor.example",
                                      "project": "autoscaler", "build_dir": "build", "generator_image": GEN,
                                      "docker_config": str(dc)}})
    record = X.load_record(rec_path, ("build",))
    fake = FakeTools()
    return record, fake, B.Tools(fake, str(dc)), None, dc


def reviewed(record, t):
    p = B.plan(record, t)
    saved = X.save_plan(record, "build", p)
    return X.load_plan(saved["plan"]), saved["plan_digest"]


def test_plan_is_clean_and_read_only(env):
    record, fake, t, _, _ = env
    p = B.plan(record, t)
    assert p["problems"] == []
    assert p["tags"]["operator"] == "harbor.example/autoscaler/operator:c28-c28-r1"
    assert not any(c[0] == "docker" and c[1] in ("build", "push", "rmi", "tag") for c in fake.calls)


def test_plan_refuses_dirty_or_wrong_checkout(env):
    record, fake, t, _, _ = env
    fake.dirty = " M k8s-operator/main.go\n"
    fake.head = "e" * 40
    fake.pushed_branch = ""
    probs = "\n".join(B.plan(record, t)["problems"])
    assert "not clean" in probs and "not the reviewed commit" in probs and "no pushed branch" in probs


def test_existing_local_or_remote_tag_is_refused(env):
    record, fake, t, _, _ = env
    tg = B.tags(record, record.section("build"))
    fake.local.add(tg["fixture"])
    fake.registry[tg["operator"]] = b"{}"
    probs = "\n".join(B.plan(record, t)["problems"])
    assert "c28-fixture:c28-c28-r1 exists locally" in probs and "operator:c28-c28-r1 exists in the registry" in probs


def test_unprovable_absence_is_refused(env):
    record, fake, t, _, _ = env
    fake.registry_error = "ERROR: unauthorized: authentication required"
    probs = "\n".join(B.plan(record, t)["problems"])
    assert "cannot prove whether" in probs


def test_plaintext_registry_credential_is_refused(env):
    record, fake, t, _, dc = env
    (dc / "config.json").write_text(json.dumps({"auths": {"harbor.example": {"auth": "eDp5"}}}))
    probs = B.plan(record, t)["problems"]
    assert any("plaintext credential" in p for p in probs)
    assert any("no credential helper" in p for p in probs)
    assert not any("eDp5" in p for p in probs)


def test_apply_builds_pushes_verifies_and_records(env):
    record, fake, t, rec, _ = env
    out = B.apply(record, t, *reviewed(record, t))
    doc = json.load(open(os.path.join(record.evidence_dir, "images.json")))
    assert doc["verified"] is True and doc["source_commit"] == COMMIT
    for k in ("operator", "fixture", "statistical"):
        ref = doc["images"][k]["ref"]
        assert ref.split("@")[1] == "sha256:" + hashlib.sha256(fake.registry[ref]).hexdigest()
    assert doc["images"]["generator"]["ref"] == GEN
    assert out["event"] == "images-recorded"
    bash = [c for c in fake.calls if c[0] == "bash"]
    assert bash and bash[0][-2:] == [COMMIT, "harbor.example/autoscaler/statistical-evaluation:c28-c28-r1"]
    assert "--push" not in bash[0]
    assert not any(c[0] == "docker" and c[1] in ("rmi", "tag", "login") for c in fake.calls)
    assert B.verify(record, t)["result"] == "PASS"


def test_apply_needs_the_digest_and_never_overwrites(env):
    record, fake, t, rec, _ = env
    plan, digest = reviewed(record, t)
    with pytest.raises(X.StagingError, match="needs --plan-digest"):
        B.apply(record, t, plan, "")
    B.apply(record, t, plan, digest)
    plan2, digest2 = reviewed(record, t)
    assert any("exists" in x for x in plan2["problems"])
    with pytest.raises(X.StagingError):
        B.apply(record, t, plan2, digest2)


def test_failed_sbom_check_stops_without_fallback(env):
    record, fake, t, rec, _ = env
    fake.sbom_rc = 1
    with pytest.raises(X.StagingError, match="SBOM"):
        B.apply(record, t, *reviewed(record, t))
    assert not os.path.exists(os.path.join(record.evidence_dir, "images.json"))


def test_a_registry_that_lies_about_the_digest_is_refused(env):
    record, fake, t, rec, _ = env
    plan, digest = reviewed(record, t)
    real_push = fake.__call__

    def lying(argv, **kw):
        res = real_push(argv, **kw)
        if argv[:2] == ["docker", "push"]:
            tag = argv[2]
            fake.registry[tag] = b"tampered"
        return res
    t.run = lying
    with pytest.raises(X.StagingError, match="does not resolve to the pushed digest"):
        B.apply(record, t, plan, digest)


def test_credentials_in_arguments_are_refused(env):
    _, _, t, _, _ = env
    with pytest.raises(X.StagingError, match="credential argument"):
        t(["docker", "login", "--password", "x", "harbor.example"])


def test_verify_reports_a_moved_tag(env):
    record, fake, t, rec, _ = env
    B.apply(record, t, *reviewed(record, t))
    tg = B.tags(record, record.section("build"))
    fake.registry[tg["fixture"]] = b"moved"
    out = B.verify(record, t)
    assert out["result"] == "FAIL" and "moved or vanished" in "\n".join(out["problems"])


# --- build.attempt: a later build of the same run --------------------------------------------------------------------

_ABSENT = object()


def attempt_env(tmp_path, make_record, attempt="r2", evidence="evidence-r2", build="build-r2", run_id=None):
    """A second record of the same run: the same checkout and docker config, its own attempt and directories."""
    dc = tmp_path / "docker"
    if not dc.exists():
        dc.mkdir()
        (dc / "config.json").write_text(json.dumps({"credHelpers": {"harbor.example": "pass"}}))
    (tmp_path / "checkout").mkdir(exist_ok=True)
    sec = {"checkout": "checkout", "harbor": "harbor.example", "project": "autoscaler", "build_dir": build,
           "generator_image": GEN, "docker_config": str(dc)}
    if attempt is not _ABSENT:
        sec["attempt"] = attempt                 # None stays in the record as an explicit null
    extra = {"build": sec, "evidence_dir": evidence}
    if run_id:
        extra["run_id"] = run_id
    src = make_record(extra)
    path = tmp_path / f"record-{len(list(tmp_path.glob('record-*.json')))}.json"
    os.replace(src, path)                        # make_record writes one fixed name; keep each record apart
    return X.load_record(str(path), ("build",))


def test_without_attempt_tags_plan_and_images_json_are_unchanged(env):
    record, fake, t, _, _ = env
    assert B.tag_suffix(record, record.section("build")) == "c28-c28-r1"
    assert B.tags(record, record.section("build")) == {
        "operator": "harbor.example/autoscaler/operator:c28-c28-r1",
        "statistical": "harbor.example/autoscaler/statistical-evaluation:c28-c28-r1",
        "fixture": "harbor.example/autoscaler/c28-fixture:c28-c28-r1"}
    p = B.plan(record, t)
    # exactly the plan document of the first S1 build (plan-a8dc4d9a3178), key for key: no attempt field
    assert set(p) == {"actions", "build_dir", "cluster_uid", "commit", "generator", "helper", "never", "problems",
                      "record_sha256", "remote_state", "run_id", "schema", "source_commit", "tags", "tools"}
    out = B.apply(record, t, *reviewed(record, t))
    doc = json.load(open(os.path.join(record.evidence_dir, "images.json")))
    assert set(doc) == {"at", "images", "plan_sha256", "run_id", "sbom", "schema", "source_commit", "tools", "verified"}
    assert "attempt" not in out["data"]


@pytest.mark.parametrize("bad", ["", "2", "r", "R2", "r1234", "r-2", "r2 ", " r2", "r2;x", "rr2", "r2\n", None, 2, ["r2"]])
def test_an_invalid_attempt_is_refused(tmp_path, make_record, bad):
    record = attempt_env(tmp_path, make_record, attempt=bad)
    fake = FakeTools()
    t = B.Tools(fake, str(tmp_path / "docker"))
    with pytest.raises(X.StagingError, match=r"build.attempt, when set, must be a rebuild slug"):
        B.plan(record, t)
    with pytest.raises(X.StagingError, match=r"build.attempt"):
        B.tags(record, B._section(record))
    assert not any(c[0] == "docker" for c in fake.calls)


@pytest.mark.parametrize("good", ["r0", "r2", "r10", "r999"])
def test_valid_attempts_give_suffixed_tags(tmp_path, make_record, good):
    record = attempt_env(tmp_path, make_record, attempt=good)
    tg = B.tags(record, B._section(record))
    assert tg["operator"] == f"harbor.example/autoscaler/operator:c28-c28-r1-{good}"
    assert tg["statistical"] == f"harbor.example/autoscaler/statistical-evaluation:c28-c28-r1-{good}"
    assert tg["fixture"] == f"harbor.example/autoscaler/c28-fixture:c28-c28-r1-{good}"


def test_a_second_attempt_builds_new_tags_and_never_touches_the_first(env, tmp_path, make_record):
    first, fake, t, _, _ = env
    B.apply(first, t, *reviewed(first, t))
    first_tags = B.tags(first, first.section("build"))
    before = {ref: fake.registry[ref] for ref in first_tags.values()}
    first_images = open(os.path.join(first.evidence_dir, "images.json")).read()

    second = attempt_env(tmp_path, make_record, attempt="r2")
    p = B.plan(second, t)
    assert p["problems"] == [], p["problems"]
    assert p["attempt"] == "r2" and p["run_id"] == first.run_id
    assert p["tags"] == {k: v + "-r2" for k, v in first_tags.items()}
    assert p["remote_state"] == {"fixture": "absent", "operator": "absent", "statistical": "absent"}
    assert {a["push"] for a in p["actions"] if "push" in a} == set(p["tags"].values())

    calls = len(fake.calls)
    out = B.apply(second, t, *reviewed(second, t))
    doc = json.load(open(os.path.join(second.evidence_dir, "images.json")))
    assert doc["attempt"] == "r2" and doc["run_id"] == first.run_id and doc["verified"] is True
    assert {k: doc["images"][k]["tag"] for k in ("operator", "statistical", "fixture")} == p["tags"]
    assert out["data"]["attempt"] == "r2"
    # the first build's tags, manifests and evidence are untouched; nothing was removed or retagged
    assert {ref: fake.registry[ref] for ref in first_tags.values()} == before
    assert open(os.path.join(first.evidence_dir, "images.json")).read() == first_images
    new = fake.calls[calls:]
    assert not any(c[0] == "docker" and c[1] in ("rmi", "tag", "login") for c in new)
    assert {c[2] for c in new if c[:2] == ["docker", "push"]} == set(p["tags"].values())
    assert B.verify(second, t)["result"] == "PASS"
    assert B.verify(first, t)["result"] == "PASS"
    # the same attempt again: its tags exist now, so a third build needs a new attempt
    again = attempt_env(tmp_path, make_record, attempt="r2", evidence="evidence-r2b", build="build-r2b")
    probs = "\n".join(B.plan(again, t)["problems"])
    assert "c28-c28-r1-r2 exists locally" in probs and "c28-c28-r1-r2 exists in the registry" in probs


def test_an_attempt_needs_a_fresh_evidence_dir(env, tmp_path, make_record):
    first, fake, t, _, _ = env
    B.apply(first, t, *reviewed(first, t))
    # the first build's evidence_dir (its images.json and receipt chain)
    reuse = attempt_env(tmp_path, make_record, attempt="r2", evidence="evidence")
    probs = "\n".join(B.plan(reuse, t)["problems"])
    assert "images.json already exists (run 'c28-r1', attempt none)" in probs
    assert "already holds a build receipt chain" in probs
    with pytest.raises(X.StagingError, match="images.json already exists"):
        B.apply(reuse, t, *reviewed(reuse, t))
    # a receipt chain alone (an attempt whose apply stopped before images.json) is refused too
    lone = tmp_path / "evidence-lone" / "receipts"
    lone.mkdir(parents=True)
    (lone / "build.jsonl").write_text(open(os.path.join(first.evidence_dir, "receipts", "build.jsonl")).readline())
    probs = "\n".join(B.plan(attempt_env(tmp_path, make_record, attempt="r3", evidence="evidence-lone",
                                         build="build-r3"), t)["problems"])
    assert "already holds a build receipt chain" in probs and "images.json" not in probs
    # another attempt's images.json is named by its attempt
    r2 = attempt_env(tmp_path, make_record, attempt="r2")
    B.apply(r2, t, *reviewed(r2, t))
    clash = attempt_env(tmp_path, make_record, attempt="r4", evidence="evidence-r2", build="build-r4")
    assert any("attempt r2" in x for x in B.plan(clash, t)["problems"])
    # a fresh evidence_dir with an empty receipts directory is fine
    (tmp_path / "evidence-r5" / "receipts").mkdir(parents=True)
    assert B.plan(attempt_env(tmp_path, make_record, attempt="r5", evidence="evidence-r5", build="build-r5"),
                  t)["problems"] == []


def test_an_attempt_needs_a_fresh_build_dir(env, tmp_path, make_record):
    first, fake, t, _, _ = env
    B.apply(first, t, *reviewed(first, t))
    reuse = attempt_env(tmp_path, make_record, attempt="r2", build="build")
    assert any("build is not empty" in x for x in B.plan(reuse, t)["problems"])


def test_verify_refuses_another_attempts_or_runs_images_json(env, tmp_path, make_record):
    first, fake, t, _, _ = env
    B.apply(first, t, *reviewed(first, t))
    # a record that claims attempt r2 over the first build's evidence
    claim = attempt_env(tmp_path, make_record, attempt="r2", evidence="evidence", build="build-x")
    out = B.verify(claim, t)
    joined = "\n".join(out["problems"])
    assert out["result"] == "FAIL" and "attempt none, not this record's r2" in joined
    assert "images.json's tag is not this record's" in joined
    # the same evidence under another run id
    other = attempt_env(tmp_path, make_record, attempt=_ABSENT, evidence="evidence", build="build-y",
                        run_id="c28-r9")
    out = B.verify(other, t)
    assert out["result"] == "FAIL" and "not this record's run 'c28-r9'" in "\n".join(out["problems"])


# --- the first real S1 build (c28-prod-1, 2026-11-08, source 6742e2d) still verifies --------------------------------

S1 = os.path.join(os.path.dirname(__file__), "data", "build-s1-c28-prod-1")


def test_the_recorded_s1_build_still_verifies(tmp_path, make_record):
    """images.json is a byte copy of the S1 evidence (sha256 e7ded8d2..., as recorded by its images-recorded receipt),
    the manifests are the registry's raw bytes for its four digests; the record is the S1 record's build section."""
    raw = open(os.path.join(S1, "images.json"), "rb").read()
    assert hashlib.sha256(raw).hexdigest() == "e7ded8d2c8baa2086f53cf82059250b956ca4305d4452fe286723bdd0a1fc82c"
    (tmp_path / "evidence").mkdir()
    (tmp_path / "evidence" / "images.json").write_bytes(raw)
    dc = tmp_path / "docker"
    dc.mkdir()
    (dc / "config.json").write_text(json.dumps({"credHelpers": {"harbor.devkuban.com": "pass"}}))
    doc = json.loads(raw)
    gen = doc["images"]["generator"]["ref"]
    rec = make_record({"run_id": "c28-prod-1", "build": {
        "build_dir": "build-src", "checkout": "checkout", "docker_config": str(dc), "generator_image": gen,
        "harbor": "harbor.devkuban.com", "project": "autoscaler"}})
    record = X.load_record(rec, ("build",))
    assert "attempt" not in record.section("build")
    assert B.tags(record, record.section("build")) == {k: doc["images"][k]["tag"]
                                                       for k in ("operator", "statistical", "fixture")}
    fake = FakeTools()
    fake.registry = {}
    for k, v in doc["images"].items():
        digest = v["ref"].split("@sha256:")[1]
        body = open(os.path.join(S1, "manifests", digest + ".json"), "rb").read()
        assert hashlib.sha256(body).hexdigest() == digest
        fake.registry[v["ref"]] = body
        if v.get("tag"):
            fake.registry[v["tag"]] = body
    out = B.verify(record, B.Tools(fake, str(dc)))
    assert out == {"helper": "build", "action": "verify", "problems": [], "result": "PASS",
                   "images": {k: v["ref"] for k, v in doc["images"].items()}}
    # an attempt record over the same evidence is not this build
    rec2 = make_record({"run_id": "c28-prod-1", "build": dict(record.section("build"), attempt="r2")})
    assert B.verify(X.load_record(rec2, ("build",)), B.Tools(fake, str(dc)))["result"] == "FAIL"
