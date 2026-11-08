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
