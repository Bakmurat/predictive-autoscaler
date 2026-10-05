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


ARMS_SELECTOR = '{ kind: Deployment, labelSelector: "app in (nginx-test,nginx-reactive,myapptwo,nginx-seasonal,nginx-ensemble,nginx-ensemble-q95)" }'


def final_snapshot_patch():
    text = open(OVERLAY).read()
    blocks = [b for b in re.split(r"\n  - target: ", text) if b.startswith(ARMS_SELECTOR) and "containers/0/lifecycle" in b]
    assert len(blocks) == 1
    return blocks[0]


def test_arms_run_the_final_snapshot_hook_from_a_generated_config_map():
    text = open(OVERLAY).read()
    assert re.search(r"configMapGenerator:\n  - name: final-snapshot-script\n    namespace: demo\n    files: \[ final-snapshot.sh \]", text)
    p = final_snapshot_patch()
    assert 'command: ["/bin/sh", "/opt/final-snapshot/final-snapshot.sh"]' in p
    assert "configMap: { name: final-snapshot-script, defaultMode: 365 }" in p
    for env in ("POD_NAME", "POD_UID", "POD_NAMESPACE"):
        assert f"name: {env}, valueFrom" in p
    assert '{ name: SNAPSHOT_URL, value: "VM_IMPORT_URL" }' in p


def test_envoy_outlives_the_hook_and_the_pod_outlives_envoy():
    p = final_snapshot_patch()
    drain = int(re.search(r"terminationDrainDuration: (\d+)s", p).group(1))
    grace = int(re.search(r"terminationGracePeriodSeconds\n\s+value: (\d+)", p).group(1))
    script = open(os.path.join(ROOT, "deploy", "prodcluster", "demo", "final-snapshot.sh")).read()
    settle = int(re.search(r"SETTLE=\$\{FINAL_SNAPSHOT_SETTLE:-(\d+)\}", script).group(1))
    wait = int(re.search(r"WAIT=\$\{FINAL_SNAPSHOT_WAIT:-(\d+)\}", script).group(1))
    # every request at its timeout: listener_stopped read and drain (2 + 2), polling until the deadline plus one last
    # poll (wait + 2), three read pairs 1 s apart (3 * (2 + 1 + 2)), uptime and epoch (2 + 2), two uploads (2 * 5)
    worst = settle + 2 + 2 + wait + 2 + 3 * (2 + 1 + 2) + 2 + 2 + 2 * 5
    assert worst < drain < grace, (worst, drain, grace)


def test_the_hook_reads_gauges_istio_does_not_export_by_default():
    p = final_snapshot_patch()
    assert r'"listener\\.0\\.0\\.0\\.0_15006\\.downstream_(pre_)?cx_active"' in p
    assert r'"http\\.inbound_.*\\.downstream_rq_active"' in p


LOAD_EVIDENCE = os.path.join(ROOT, "deploy", "prodcluster", "ml-engine", "load-evidence.yaml")
PLACEHOLDERS = ("HARBOR_REGISTRY", "ML_API_DIGEST", "OPERATOR_DIGEST", "GIT_COMMIT_VALUE", "VM_QUERY_URL", "VM_WRITE_URL",
                "VM_IMPORT_URL", "ECR_REGISTRY", "IMAGE_TAG")


def test_files_embedded_in_config_maps_contain_no_render_placeholder():
    # deploy.sh substitutes these tokens across the whole rendered YAML, including ConfigMap file contents
    for rel in ("deploy/prodcluster/collect_load_evidence.py", "deploy/eks-benchmark/workload/challenge-v1/challenge_profile.py",
                "deploy/eks-benchmark/workload/challenge-v1/profile.json", "deploy/prodcluster/demo/final-snapshot.sh",
                "deploy/prodcluster/load-gate/approvals.json", "deploy/prodcluster/load-gate/terminations.json"):
        text = open(os.path.join(ROOT, rel)).read()
        assert not [p for p in PLACEHOLDERS if re.search(r"\b" + p + r"\b", text)], rel


def test_load_evidence_cronjob_is_read_only_and_never_reports_success_for_a_failed_gate():
    text = open(LOAD_EVIDENCE).read()
    assert 'schedule: "14 * * * *"' in text and "timeZone: Etc/UTC" in text and "concurrencyPolicy: Forbid" in text
    assert "sidecar.istio.io/inject: \"false\"" in text and "automountServiceAccountToken: false" in text
    assert "readOnlyRootFilesystem: true" in text and "runAsNonRoot: true" in text
    assert '"--json-dir", "/evidence", "--exit-zero"' in text and '"--prom", "VM_QUERY_URL"' in text
    archive = open(os.path.join(ROOT, "deploy", "eks-benchmark", "instrumentation", "archive.yaml")).read()
    digest = re.search(r"python@(sha256:[0-9a-f]{64})", text).group(1)
    assert digest in archive                     # the image already pulled and running for the evidence archive
    k = open(os.path.join(ROOT, "deploy", "prodcluster", "ml-engine", "kustomization.yaml")).read()
    assert "  - load-evidence.yaml" in k and "../collect_load_evidence.py" in k and "../load-gate/settings.env" in k
    settings = open(os.path.join(ROOT, "deploy", "prodcluster", "load-gate", "settings.env")).read()
    assert settings.strip() == "INVENTORY_START=2026-10-05T08:14:00Z"
