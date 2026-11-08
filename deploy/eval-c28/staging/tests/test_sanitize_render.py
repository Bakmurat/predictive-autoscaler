"""sanitize-render.py: Secrets discarded, credential fields removed, unknown documents fail closed, nothing raw kept."""

import importlib.util
import io
import json
import os
import shutil
import subprocess

import pytest
import yaml

import common_records as C
from fakes import LAST_APPLIED_MARKER, SECRET_DATA, SECRET_VALUE, URL_PASSWORD, record

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
spec = importlib.util.spec_from_file_location("sanitize_render", os.path.join(HERE, "sanitize-render.py"))
SR = importlib.util.module_from_spec(spec)
spec.loader.exec_module(SR)

RENDER = f"""
apiVersion: v1
kind: Secret
metadata: {{name: pa-forecaster-tls}}
data: {{tls.key: {SECRET_DATA}}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pa-operator
  annotations:
    {C.LAST_APPLIED}: '{{"env": "{LAST_APPLIED_MARKER}"}}'
spec:
  template:
    spec:
      containers:
        - name: operator
          image: harbor.example/operator@sha256:{"c" * 64}
          env:
            - {{name: VM_PASSWORD, value: {SECRET_VALUE}}}
            - {{name: METRICS_URL, value: "http://u:{URL_PASSWORD}@vm:8428"}}
            - {{name: FORECASTER_TLS_CERT, value: /etc/predictive-autoscaler/tls/tls.crt}}
          args: ["--token={SECRET_VALUE}", "--leader-elect=true"]
---
apiVersion: v1
kind: ConfigMap
metadata: {{name: pa-dashboards}}
data: {{dash.json: "{SECRET_VALUE}"}}
"""


def run(text, tmp_path):
    rec = tmp_path / "rec.json"
    rec.write_text(json.dumps(record(tmp_path)))
    out, err = io.StringIO(), io.StringIO()
    rc = SR.main(["--record", str(rec)], stdin=io.StringIO(text), stdout=out, stderr=err)
    return rc, out.getvalue(), err.getvalue()


def test_secrets_and_credential_fields_never_pass(tmp_path):
    rc, out, err = run(RENDER, tmp_path)
    assert rc == 0, err
    for m in (SECRET_DATA, SECRET_VALUE, LAST_APPLIED_MARKER, URL_PASSWORD, C.LAST_APPLIED):
        assert m not in out and m not in err
    docs = [d for d in yaml.safe_load_all(out) if d]
    assert [d["kind"] for d in docs] == ["Deployment", "ConfigMap"] and "1 Secret documents discarded" in out
    env = {e["name"]: e.get("value") for e in docs[0]["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["VM_PASSWORD"] == C.REDACTED and env["METRICS_URL"] == C.REDACTED        # user-info: whole value
    assert env["FORECASTER_TLS_CERT"] == "/etc/predictive-autoscaler/tls/tls.crt"        # a path is kept for review
    assert docs[0]["spec"]["template"]["spec"]["containers"][0]["args"] == [f"--token={C.REDACTED}",
                                                                           "--leader-elect=true"]
    assert set(docs[1]["data_sha256"]) == {"dash.json"} and "data" not in docs[1]


@pytest.mark.parametrize("doc, why", [
    ("apiVersion: v1\nkind: MysteryThing\nmetadata: {name: m}\n", "unknown document MysteryThing/m"),
    ("apiVersion: v1\nkind: List\nitems: []\n", "List"),
    ("- just\n- a list\n", "without a kind"),
    ("apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata: {name: np}\n"
     f"spec: {{note: 'https://user:{URL_PASSWORD}@x'}}\n", "credential material"),
    ("kind: Deployment\nmetadata: {name: x\n", "not valid YAML"),
])
def test_unknown_or_unreviewable_documents_fail_closed(tmp_path, doc, why):
    rc, out, err = run(RENDER + "---\n" + doc, tmp_path)
    assert rc == 1 and out == "" and why in err
    assert URL_PASSWORD not in err and SECRET_VALUE not in err


def test_the_real_chart_rendering_passes_without_its_tls_secret(tmp_path):
    if not shutil.which("helm"):
        pytest.skip("helm not installed")
    values = tmp_path / "v.yaml"
    values.write_text("prometheus: {url: 'http://vm:8481/select/0/prometheus'}\nforecaster: {engine: statistical}\n"
                      "training: {targets: []}\nnetworkPolicy: {prometheus: {namespaceSelector: {matchLabels: "
                      "{kubernetes.io/metadata.name: monitoring}}}}\n")
    p = subprocess.run(["helm", "template", "pa-eval", os.path.join(ROOT, "charts", "predictive-autoscaler"), "-n",
                        "pa-eval-system", "-f", str(values)], capture_output=True, text=True,
                       env={**os.environ, "KUBECONFIG": "/dev/null"})
    assert p.returncode == 0, p.stderr
    assert "kind: Secret" in p.stdout                                    # the self-signed TLS Secret is rendered
    rc, out, err = run(p.stdout, tmp_path)
    assert rc == 0, err
    kinds = {d["kind"] for d in yaml.safe_load_all(out) if d}
    assert "Secret" not in kinds and {"Deployment", "Service", "ClusterRole"} <= kinds
    assert "PRIVATE KEY" not in out and "BEGIN CERTIFICATE" not in out      # file paths remain, contents never
