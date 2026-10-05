"""The prodcluster demo overlay spreads generator traffic over every Ready pod of an arm.

2026-10-05: with keep-alive, each k6 VU kept the connection it opened while its arm had one pod (no sidecar on the
generators, per-connection balancing), so pods added by the autoscalers received no requests. Every generator must
therefore run with K6_NO_VU_CONNECTION_REUSE=true, and the challenge-v1 script itself must stay unchanged.
(Plain-text checks: the test environment has no YAML parser.)
"""
import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
OVERLAY = os.path.join(ROOT, "deploy", "prodcluster", "demo", "kustomization.yaml")
GENERATORS = ("deploy/eks-benchmark/demo/k6-nginx-test-deployment.yaml",
              "deploy/eks-benchmark/demo/k6-nginx-reactive-deployment.yaml",
              "deploy/eks-benchmark/demo/k6-myapptwo-deployment.yaml",
              "deploy/eks-benchmark/experiments/seasonal-pattern-v1/k6-nginx-seasonal-deployment.yaml",
              "deploy/eks-benchmark/experiments/seasonal-ensemble-v1/k6-nginx-ensemble-deployment.yaml",
              "deploy/eks-benchmark/experiments/seasonal-ensemble-q95-v1/k6-nginx-ensemble-q95-deployment.yaml")


def k6_patch():
    text = open(OVERLAY).read()
    blocks = re.split(r"\n  - target: ", text)
    k6 = [b for b in blocks if b.startswith('{ kind: Deployment, labelSelector: "app=k6" }')]
    assert len(k6) == 1
    return k6[0]


def test_every_generator_opens_a_connection_per_request():
    p = k6_patch()
    assert re.search(r"- op: add\n\s+path: /spec/template/spec/containers/0/env/-\n"
                     r"\s+value: \{ name: K6_NO_VU_CONNECTION_REUSE, value: \"true\" \}", p), p


def test_generators_still_mount_challenge_v1_and_issue_one_request_per_iteration():
    assert re.search(r"path: /spec/template/spec/volumes/0/configMap/name\n\s+value: k6-load-script-challenge-v1\n",
                     k6_patch())
    script = open(os.path.join(ROOT, "deploy", "eks-benchmark", "workload", "challenge-v1",
                               "k6-load-script-challenge-v1.yaml")).read()
    body = script.split("export default function () {", 1)[1].split("}", 1)[0]
    assert body.count("http.") == 1, "connection-per-request assumes one request per iteration"
    assert "ConnectionReuse" not in script


def test_base_generators_recreate_without_sidecar_and_with_an_env_list():
    for rel in GENERATORS:
        text = open(os.path.join(ROOT, rel)).read()
        assert re.search(r"strategy:\n\s+type: Recreate", text), rel
        assert re.search(r'sidecar\.istio\.io/inject: "false"', text), rel
        assert len(re.findall(r"^\s+containers:\n", text, re.M)) == 1, rel
        assert re.search(r"containers:\n\s+- name: k6\n(?:.*\n)*?\s+env:\n\s+- name: TARGET_URL", text), rel
