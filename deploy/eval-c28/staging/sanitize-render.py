#!/usr/bin/env python3
"""Sanitize a Helm rendering for review (STAGING-C28-PROD.md S3; "Helpers to implement": sanitize-render.py).

  helm template ... | sanitize-render.py --record preparation.json > release-rendered-sanitized.yaml

Reads the multi-document YAML on stdin, never logs or persists it, and writes only the sanitized documents to stdout:
  * Secret documents are discarded (only their count reaches stderr);
  * every document loses managedFields, the last-applied configuration and secret-named annotations; literal
    credential env values and credential flags are redacted; ConfigMap contents become per-key sha256;
  * only kinds on the review list pass (the chart's own kinds); an unknown kind, a PEM private key, a bearer token or
    a URL with user-info anywhere in a kept document fails closed (exit 1; stderr names only the kind and name, never
    the content), and nothing is written.
--record is required by the helper contract (it binds the output to the run in a trailing comment); this tool never
contacts the cluster.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common_records as C  # noqa: E402

REVIEW_KINDS = {
    "Namespace", "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "Deployment",
    "StatefulSet", "DaemonSet", "Service", "ConfigMap", "PersistentVolumeClaim", "NetworkPolicy",
    "CiliumNetworkPolicy", "ServiceMonitor", "PodMonitor", "VMServiceScrape", "VMPodScrape", "PrometheusRule",
    "VMRule", "CustomResourceDefinition", "Certificate", "Issuer", "PodDisruptionBudget", "CronJob", "Job",
    "PriorityClass", "ResourceQuota", "HorizontalPodAutoscaler", "PredictiveAutoscaler", "ScaledObject",
}


def sanitize_stream(text: str):
    """(sanitized documents, number of discarded Secrets). Raises StagingError without echoing content."""
    try:
        docs = [d for d in yaml.safe_load_all(text) if d]
    except yaml.YAMLError as e:
        raise C.StagingError(f"the rendering is not valid YAML (line {getattr(e, 'problem_mark', None)})") from None
    out, secrets = [], 0
    for d in docs:
        if not isinstance(d, dict) or "kind" not in d:
            raise C.StagingError("a document without a kind needs secret review")
        kind, name = d.get("kind"), (d.get("metadata") or {}).get("name")
        if kind == "Secret":
            secrets += 1
            continue
        if kind == "List":
            raise C.StagingError("a List document needs secret review; render plain documents")
        if kind not in REVIEW_KINDS:
            raise C.StagingError(f"unknown document {kind}/{name} needs secret review")
        s = C.sanitize_object(d)
        try:
            C.assert_clean(json.dumps(s))
        except C.StagingError:
            raise C.StagingError(f"{kind}/{name} carries credential material in a field that is kept") from None
        out.append(s)
    return out, secrets


def main(argv=None, stdin=None, stdout=None, stderr=None) -> int:
    stdin, stdout, stderr = stdin or sys.stdin, stdout or sys.stdout, stderr or sys.stderr
    ap = argparse.ArgumentParser(prog="sanitize-render.py", description=__doc__.splitlines()[0])
    C.add_common_args(ap)
    a = ap.parse_args(argv)
    try:
        rec = C.load_record(a.record)
        docs, secrets = sanitize_stream(stdin.read())
        body = yaml.safe_dump_all(docs, sort_keys=True)
        C.assert_clean(body)
    except C.StagingError as e:
        print(f"sanitize-render: refused: {e}", file=stderr)
        return 1
    stdout.write(body)
    stdout.write(f"# sanitized for run {rec['run_id']}: {len(docs)} documents kept, {secrets} Secret documents "
                 f"discarded, sha256 {C.digest(docs)}\n")
    print(f"sanitize-render: {len(docs)} kept, {secrets} Secrets discarded", file=stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
