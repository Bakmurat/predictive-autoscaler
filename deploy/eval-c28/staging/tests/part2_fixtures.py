"""Fixtures of the part-2 staging tests (reserve, prepare, freeze, session, build_images), on part 1's conftest (which
sets KUBECONFIG=/dev/null and the import path) and its in-memory API. Test modules import what they use from here."""

import copy
import json
import os
import shutil
import subprocess

import pytest

import common_records as C
import execlib as X
import fake2

STAGING = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.abspath(os.path.join(STAGING, "..", "..", ".."))
W_UID = "aaaaaaaa-0000-4000-8000-000000000001"
S_UID = "aaaaaaaa-0000-4000-8000-000000000002"
RUN = "c28-r1"
COMMIT = "d" * 40
START = "2026-12-01T00:00:00Z"
END = "2027-02-01T00:00:00Z"


def base_params():
    with open(os.path.join(STAGING, "..", "params", "kind-smoke.json")) as fh:
        p = json.load(fh)
    p["run_id"] = RUN
    p["nodes"] = {"workload": "worker-w", "support": "worker-s"}
    p["reservation"]["taint_value"] = RUN
    p["image"] = "harbor.example/autoscaler/c28-fixture@sha256:" + "a" * 64
    return p


def mutations(api):
    return api.mutations()


@pytest.fixture
def api():
    return fake2.FakeApi2()


@pytest.fixture
def make_record(tmp_path):
    def _make(extra=None, params=None):
        (tmp_path / "params.json").write_text(json.dumps(params or base_params()))
        doc = {"schema": C.RECORD_SCHEMA, "run_id": RUN, "cluster_uid": fake2.CLUSTER,
               "kubeconfig": {"path": "/home/ops/.kube/eval-prod.yaml", "context": "prodcluster"},
               "evidence_dir": "evidence", "source": {"commit": COMMIT}, "params": "params.json"}
        doc.update(copy.deepcopy(extra or {}))
        path = tmp_path / "preparation.json"
        path.write_text(json.dumps(doc))
        return str(path)
    return _make


def reserve_section(**over):
    s = {"nodes": {"workload": {"name": "worker-w", "uid": W_UID}, "support": {"name": "worker-s", "uid": S_UID}},
         "taint_key": "c28.devkuban.com/reserved", "role_label": "c28.devkuban.com/role",
         "allowed": {"daemonsets": [], "mirror_pod_prefixes": [], "broad_tolerations": []}, "movable": []}
    s.update(over)
    return s


def prepare_section():
    return {"storage": {"storage_class": "retained-ssd", "access_mode": "ReadWriteOnce",
                        "ledger_claim": {"name": "pa-eval-ledger", "size": "1Gi"},
                        "state_claim": {"name": "pa-eval-forecast-state", "size": "2Gi"}},
            "release": {"operator_image": "harbor.example/autoscaler/operator@sha256:" + "b" * 64,
                        "statistical_image": "harbor.example/autoscaler/statistical-evaluation@sha256:" + "c" * 64,
                        "registry_pull_ref": "harbor-pull", "network_policy": {"enabled": True}}}


def freeze_inputs():
    Fz = X.import_harness().frozen
    blocks = {f"{a}/{p}/{s}": ["2026-12-10T00:00:00Z", "2027-01-20T00:00:00Z"]
              for a in Fz.ARMS for p in Fz.PROFILES for s in Fz.CONFIRMATION_SEEDS}
    return {"manifest": {"path": "EVAL-C28-MANIFEST.md"}, "capacity": {"c0_rps": 8},
            "cpu_tuning": {"seeds": [101, 102, 103, 104, 105], "decision": "receipt hash abc"},
            "startup": {"s20": {"creation_to_ready_p90_s": 24.0, "samples": 40, "frozen_lead_seconds": 180},
                        "s90": {"creation_to_ready_p90_s": 100.0, "samples": 40, "frozen_lead_seconds": 300}},
            "queries": [{"name": n, "expr": f'sum(x{{target="{{target}}",q="{n}"}})', "step": 15}
                        for n in Fz.REQUIRED_QUERIES],
            "collection": {"query_api": "http://vmselect.monitoring.svc:8481/select/0/prometheus", "namespace": "demo"},
            "schedule": {"start_utc": START, "end_utc": END, "blocks": blocks},
            "admission": {"headroom": 0.25}, "data_licenses": [], "retention": {"days": 400},
            "acknowledgments": {"executor": {"name": "exec", "at": "2026-11-20T00:00:00Z"},
                                "reviewer": {"name": "alice", "at": "2026-11-21T00:00:00Z"}}}


def sign(key, ns, path):
    subprocess.run(["ssh-keygen", "-q", "-Y", "sign", "-f", str(key), "-n", ns, str(path)], check=True,
                   capture_output=True)


def plan_and_apply(mod, record, kube, *args, **kw):
    """Plan, save it (the reviewed file), then apply with its digest."""
    p = mod.plan(record, kube, *args)
    assert p.get("problems", []) == [], p.get("problems")
    saved = X.save_plan(record, p["helper"], p)
    return mod.apply(record, kube, *args, X.load_plan(saved["plan"]), saved["plan_digest"], **kw)


@pytest.fixture(scope="session")
def ssh_keys(tmp_path_factory):
    """Reviewer keys, generated once: alice and bob allowlisted, mallory not."""
    d = tmp_path_factory.mktemp("keys")
    if shutil.which("ssh-keygen") is None:
        return {}, ""
    keys, signers = {}, []
    for who in ("alice", "bob", "mallory"):
        k = d / f"{who}_key"
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", who, "-f", str(k)], check=True)
        if who != "mallory":
            pub = (d / f"{who}_key.pub").read_text().strip()
            signers.append(f'{who} namespaces="c28-reviewer-receipt,c28-gate-receipt" {pub}')
        keys[who] = k
    return keys, "\n".join(signers) + "\n"


@pytest.fixture
def world(tmp_path, api, make_record, ssh_keys):
    """The fake cluster after reserve apply and every prepare phase, plus a freeze section and its inputs."""
    import prepare
    import reserve
    api.put(fake2.node("worker-w", uid=W_UID))
    api.put(fake2.node("worker-s", uid=S_UID))
    api.put({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": "retained-ssd"},
             "reclaimPolicy": "Retain"})
    (tmp_path / "EVAL-C28-MANIFEST.md").write_text("# manifest (test copy)\n")
    (tmp_path / "inputs.json").write_text(json.dumps(freeze_inputs()))
    keys, signers = ssh_keys
    (tmp_path / "allowed_signers").write_text(signers)
    (tmp_path / "receipts").mkdir()
    (tmp_path / "gates").mkdir()
    extra = {"reserve": reserve_section(), "prepare": prepare_section(),
             "freeze": {"inputs": "inputs.json", "source_dir": REPO, "images": "evidence/images.json",
                        "manifest_root": ".", "runlock": "evidence/runlock.json",
                        "allowed_signers": "allowed_signers", "reviewers": ["alice", "bob"],
                        "receipts_dir": "receipts", "gates_dir": "gates",
                        "qualification": {"not_before": "2026-11-25T00:00:00Z", "expires": "2027-01-24T00:00:00Z",
                                          "out": "evidence/lab-qualification.json"}}}
    record = X.load_record(make_record(extra))
    kube = record.kube(api)
    plan_and_apply(reserve, record, kube)
    for ph in prepare.PHASES:
        plan_and_apply(prepare, record, kube, ph)
    images = {"schema": "c28-images/v1", "run_id": RUN, "source_commit": COMMIT, "verified": True,
              "images": {"operator": {"ref": "harbor.example/autoscaler/operator@sha256:" + "b" * 64},
                         "statistical": {"ref": "harbor.example/autoscaler/statistical-evaluation@sha256:" + "c" * 64},
                         "fixture": {"ref": "harbor.example/autoscaler/c28-fixture@sha256:" + "a" * 64},
                         "generator": {"ref": "grafana/k6@sha256:" + "e" * 64}}}
    (tmp_path / "evidence" / "images.json").write_text(json.dumps(images))
    return {"record": record, "kube": kube, "api": api, "tmp": tmp_path, "keys": keys, "have_ssh": bool(keys),
            "make_record": make_record, "extra": extra}
