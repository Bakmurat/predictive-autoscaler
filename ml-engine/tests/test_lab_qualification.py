"""C2.8a: a lab qualification is honoured only by the evaluation build it names, only from a complete reviewed file, and
only for its cluster, namespaces and target UIDs while valid; anything else fails closed (it never widens)."""

import json
import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting import qualification as Q  # noqa: E402

TREE = "a" * 40
DIGEST = "sha256:" + "d" * 64
EVAL = Q.BuildInfo(TREE, True)
NOW = int(datetime(2026, 11, 10, tzinfo=timezone.utc).timestamp())


def doc(**over):
    d = {"schema": "lab-qualification/v1", "source_tree": TREE, "image_digest": DIGEST, "run_lock_sha256": "b" * 64,
         "gates": {g: "pass" for g in ("G0", "G1", "G2", "G3", "G4")}, "reviewers": ["executor", "reviewer"],
         "not_before": "2026-11-08T00:00:00Z", "expires": "2026-12-08T00:00:00Z",
         "scope": {"cluster_uid": "cluster-uid", "namespaces": ["demo"],
                   "targets": [{"namespace": "demo", "name": "web", "uid": "dep-uid"}]}}
    d.update(over)
    return json.dumps(d).encode()


def test_a_complete_reviewed_file_is_honoured_for_exactly_its_scope():
    q = Q.parse(doc(), EVAL, NOW, DIGEST)
    assert q.covers("cluster-uid", "demo", "demo", "web", "dep-uid", NOW)
    assert not q.covers("other-cluster", "demo", "demo", "web", "dep-uid", NOW)
    assert not q.covers(None, "demo", "demo", "web", "dep-uid", NOW)              # cluster unknown: fail closed
    assert not q.covers("cluster-uid", "shop", "demo", "web", "dep-uid", NOW)     # autoscaler elsewhere
    assert not q.covers("cluster-uid", "demo", "demo", "web", "recreated-uid", NOW)   # a recreated Deployment
    assert not q.covers("cluster-uid", "demo", "demo", "api", "dep-uid", NOW)
    assert not q.covers("cluster-uid", "demo", "demo", "web", "dep-uid", q.expires)  # expired
    assert not q.covers("cluster-uid", "demo", "demo", "web", "dep-uid", q.not_before - 1)
    assert len(q.sha256) == 64 and q.summary()["targets"] == 1


@pytest.mark.parametrize("build,why", [(Q.BuildInfo(TREE, False), "release build"),
                                       (Q.BuildInfo("unknown", True), "no source tree"),
                                       (Q.BuildInfo("c" * 40, True), "another source tree")])
def test_only_the_named_evaluation_build_honours_it(build, why):
    with pytest.raises(Q.QualificationRefused, match=why):
        Q.parse(doc(), build, NOW, DIGEST)


@pytest.mark.parametrize("running,why", [(None, "digest is unknown"), ("latest", "digest is unknown"),
                                         ("sha256:" + "e" * 64, "another image digest")])
def test_only_the_named_image_honours_it(running, why):
    with pytest.raises(Q.QualificationRefused, match=why):
        Q.parse(doc(), EVAL, NOW, running)


@pytest.mark.parametrize("over,why", [
    ({"schema": "lab-qualification/v2"}, "exactly the keys"),
    ({"benchmark": True}, "exactly the keys"),                                       # no extra switches
    ({"run_lock_sha256": "nope"}, "run_lock"),
    ({"gates": {"G0": "pass", "G1": "pass", "G2": "pass", "G3": "pass"}}, "every gate"),
    ({"gates": {"G0": "pass", "G1": "pass", "G2": "pass", "G3": "pass", "G4": "waived"}}, "every gate"),
    ({"reviewers": ["executor"]}, "two distinct reviewers"),
    ({"reviewers": ["same", "same"]}, "two distinct reviewers"),
    ({"expires": "2026-11-01T00:00:00Z", "not_before": "2026-10-01T00:00:00Z"}, "expired"),
    ({"expires": "2027-02-01T00:00:00Z"}, "at most 60 days"),
    ({"expires": "2026-11-07T00:00:00Z"}, "positive"),
    ({"expires": "2026-12-08T00:00:00+00:00"}, "ending in Z"),
    ({"scope": {"cluster_uid": "", "namespaces": ["demo"], "targets": []}}, "cluster_uid"),
    ({"scope": {"cluster_uid": "u", "namespaces": [], "targets": []}}, "namespaces"),
    ({"scope": {"cluster_uid": "u", "namespaces": ["demo"], "targets": []}}, "targets"),
    ({"scope": {"cluster_uid": "u", "namespaces": ["demo"],
                "targets": [{"namespace": "shop", "name": "web", "uid": "x"}]}}, "in scope"),
    ({"scope": {"cluster_uid": "u", "namespaces": ["demo"], "targets": [{"namespace": "demo", "name": "web"}]}},
     "uid"),
    ({"scope": {"cluster_uid": "u", "namespaces": ["demo"], "targets": [], "everything": True}}, "exactly"),
])
def test_anything_incomplete_or_wider_is_refused(over, why):
    with pytest.raises(Q.QualificationRefused, match=why):
        Q.parse(doc(**over), EVAL, NOW, DIGEST)


def test_load_and_build_info(tmp_path):
    assert Q.load(None, EVAL, NOW) is None
    with pytest.raises(Q.QualificationRefused, match="unreadable"):
        Q.load(str(tmp_path / "missing.json"), EVAL, NOW, DIGEST)
    f = tmp_path / "q.json"
    f.write_bytes(doc())
    assert Q.load(str(f), EVAL, NOW, DIGEST).cluster_uid == "cluster-uid"
    assert Q.read_build_info(str(tmp_path / "none.json")) == Q.BuildInfo("unknown", False)
    b = tmp_path / "build-info.json"
    b.write_text(json.dumps({"source_tree": TREE, "evaluation_build": True}))
    assert Q.read_build_info(str(b)) == EVAL
    b.write_text(json.dumps({"source_tree": TREE, "evaluation_build": "true"}))     # not a boolean: not evaluation
    assert Q.read_build_info(str(b)) == Q.BuildInfo("unknown", False)


def test_the_cluster_uid_is_the_kube_system_namespace_uid_or_nothing():
    class R:
        def __init__(self, answer):
            self.answer, self.paths = answer, []

        def get(self, path):
            self.paths.append(path)
            if isinstance(self.answer, Exception):
                raise self.answer
            return self.answer

    r = R({"metadata": {"uid": "u-1"}})
    assert Q.cluster_uid(r) == "u-1" and r.paths == ["/api/v1/namespaces/kube-system"]
    assert Q.cluster_uid(R(None)) is None and Q.cluster_uid(R({"metadata": {}})) is None
    assert Q.cluster_uid(R(RuntimeError("forbidden"))) is None


def test_the_app_refuses_to_start_with_a_qualification_it_cannot_honour(tmp_path, monkeypatch):
    from api import statistical as app
    f = tmp_path / "q.json"
    f.write_bytes(doc())
    env = {"PROMETHEUS_URL": "http://prom", "FORECAST_STATE_DIR": str(tmp_path / "state"), "FORECAST_FIT_MODE": "off",
           "FORECAST_LAB_QUALIFICATION": str(f), "FORECAST_IMAGE_DIGEST": DIGEST}
    with pytest.raises(app.Refused, match="release build"):            # this checkout is no evaluation build
        app.default_service(env)
    monkeypatch.setattr(Q, "read_build_info", lambda path=Q.BUILD_INFO_PATH: EVAL)
    monkeypatch.setattr(app.time, "time", lambda: NOW)
    svc = app.default_service(env)
    try:
        h = svc.health()
        assert h["lab_qualification"]["cluster_uid"] == "cluster-uid" and h["build"]["evaluation_build"] is True
        assert h["release_gates_passed"] is False                        # the release gate itself never moves
    finally:
        svc.close()
    with pytest.raises(app.Refused, match="another image digest"):
        app.default_service({**env, "FORECAST_IMAGE_DIGEST": "sha256:" + "e" * 64})
    no_lab = {k: v for k, v in env.items() if k != "FORECAST_LAB_QUALIFICATION"}
    svc = app.default_service(no_lab)
    try:
        assert svc.health()["lab_qualification"] is None
    finally:
        svc.close()
