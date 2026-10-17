#!/usr/bin/env python3
"""release_checks.py — the release workflow's gate checks (.github/workflows/release.yml), kept here so that every
decision is unit-tested (release_checks_test.py; Codex partner review of PR #14). Standard library only.

  release_checks.py semver <version>
  release_checks.py absent <registry> <repository> <tag>    # registry API (GHCR_USER/GHCR_TOKEN); exit 0 only on an
                                                             # authenticated HTTP 404 MANIFEST_UNKNOWN/NAME_UNKNOWN
  release_checks.py ci <repo> <sha> <check-runs.json> <workflow-runs.json>
  release_checks.py chart-images <version> <registry/owner> <rendered.jsonl>
  release_checks.py digest <value>

Each command exits 0 when the check passes and prints the problems otherwise (exit 1). A failed check never falls back
to a default: anything the check cannot confirm is a problem.
"""
import json
import re
import base64
import sys
import urllib.error
import urllib.parse
import urllib.request

DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
SEMVER = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")

# The CI checks a release commit must have passed: check-run name -> the workflow file that must have produced it.
REQUIRED_CHECKS = {
    "operator (go vet, go test -race)": ".github/workflows/ci.yml",
    "operator write path (envtest, Kubernetes 1.35 API server)": ".github/workflows/ci.yml",
    "forecaster + evaluation tooling (pytest, scorer, archive)": ".github/workflows/ci.yml",
    "chart (helm v3.22.0 lint + template, kubeconform 1.33–1.35)": ".github/workflows/ci.yml",
    "chart (helm v4.3.0 lint + template, kubeconform 1.33–1.35)": ".github/workflows/ci.yml",
    "newcomer install (kind, Kubernetes 1.35)": ".github/workflows/quickstart.yml",
    "install smoke test (kind, Kubernetes 1.35)": ".github/workflows/smoke.yml",
}
ACTIONS_APP = "github-actions"


def check_semver(v):
    return [] if isinstance(v, str) and SEMVER.fullmatch(v) else [f"not a strict semantic version: {v!r}"]


def check_digest(d):
    return [] if isinstance(d, str) and DIGEST.fullmatch(d) else [f"not a sha256 digest: {d!r}"]


def classify_http(status, body):
    """'present' or 'absent' for the HTTP response of an authenticated manifest GET. Only 200 is present and only a 404
    whose registry error code is MANIFEST_UNKNOWN or NAME_UNKNOWN is absent; every other status (401, 403, 429, 5xx, ...)
    or an unrecognised 404 raises, so a registry problem is never mistaken for an absent version."""
    if status == 200:
        return "present"
    codes = set()
    try:
        codes = {e.get("code") for e in (json.loads(body or "{}").get("errors") or []) if isinstance(e, dict)}
    except (ValueError, AttributeError):
        pass
    codes.discard(None)
    if status == 404 and codes and codes <= {"MANIFEST_UNKNOWN", "NAME_UNKNOWN"}:   # only absence codes; mixed fails closed
        return "absent"
    raise RuntimeError(f"registry answered HTTP {status} {sorted(c for c in codes if c)}: not a confirmed absence")


MANIFEST_TYPES = ", ".join(["application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json",
                            "application/vnd.docker.distribution.manifest.list.v2+json",
                            "application/vnd.docker.distribution.manifest.v2+json"])


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect: credentials (Basic or Bearer) must never be re-sent to another location."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError(f"refusing HTTP {code} redirect to {newurl}")


_OPENER = urllib.request.build_opener(_NoRedirect)


def _get(url, headers):
    """(status, body) of a GET without following redirects; HTTP errors return their status."""
    try:
        with _OPENER.open(urllib.request.Request(url, headers=headers, method="GET"), timeout=60) as r:
            return r.status, r.read(65536).decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read(65536).decode(errors="replace")


def lookup(registry, repository, tag, user, token):
    """(status, body) of GET /v2/<repository>/manifests/<tag>, with a pull token from the registry's token endpoint
    (Basic auth with the workflow token). A failed token exchange raises."""
    basic = base64.b64encode(f"{user}:{token}".encode()).decode()
    q = urllib.parse.urlencode({"scope": f"repository:{repository}:pull", "service": registry})
    status, body = _get(f"https://{registry}/token?{q}", {"Authorization": f"Basic {basic}"})
    if status != 200:
        raise RuntimeError(f"registry token exchange answered HTTP {status}")
    bearer = json.loads(body).get("token")
    if not bearer:
        raise RuntimeError("no registry token")
    return _get(f"https://{registry}/v2/{repository}/manifests/{tag}",
                {"Authorization": f"Bearer {bearer}", "Accept": MANIFEST_TYPES})


def check_ci(check_runs, workflow_runs, sha, repo, required=REQUIRED_CHECKS, event="push", branch="main"):
    """For each required workflow file, the NEWEST eligible run (GitHub Actions, this repository, this exact commit, a
    push to main; no forks) is selected first; its latest attempt must have succeeded, and each required check of that
    workflow must exist in that run's check suite with its newest check run successful. Attempt numbers are only
    compared within one run. A same-name check from another producer is a problem, never a substitute."""
    problems = []
    for name in required:
        foreign = [c for c in check_runs if c.get("name") == name and (c.get("app") or {}).get("slug") != ACTIONS_APP]
        if foreign:
            problems.append(f"{name}: a check with this name from another producer ({(foreign[0].get('app') or {}).get('slug')!r})")
    for path in sorted(set(required.values())):
        eligible = [r for r in workflow_runs if r.get("path") == path and r.get("head_sha") == sha
                    and (r.get("head_repository") or {}).get("full_name") == repo
                    and r.get("event") == event and r.get("head_branch") == branch]
        if not eligible:
            problems.append(f"{path}: no {event} run on {branch} for {sha[:12]} in {repo}")
            continue
        run = max(eligible, key=lambda r: (r.get("created_at") or "", r.get("run_number") or 0, r.get("id") or 0))
        if run.get("status") != "completed" or run.get("conclusion") != "success":
            problems.append(f"{path}: newest run {run.get('id')} (attempt {run.get('run_attempt')}) "
                            f"{run.get('status')}/{run.get('conclusion')}")
        for name in [n for n, p in required.items() if p == path]:
            mine = [c for c in check_runs if c.get("name") == name and (c.get("app") or {}).get("slug") == ACTIONS_APP
                    and c.get("head_sha") == sha and (c.get("check_suite") or {}).get("id") == run.get("check_suite_id")]
            if not mine:
                problems.append(f"{name}: not in the newest {path} run {run.get('id')}")
                continue
            c = max(mine, key=lambda c: (c.get("started_at") or "", c.get("id") or 0))
            if c.get("status") != "completed" or c.get("conclusion") != "success":
                problems.append(f"{name}: newest check run {c.get('status')}/{c.get('conclusion')}")
    return problems


def check_chart_images(docs, version, prefix):
    """The packaged chart's defaults, rendered with only the required non-image values and one training target, select
    exactly this release's images, component by component: operator, forecaster, and every trainer CronJob."""
    want = {"operator": f"{prefix}/predictive-autoscaler-operator:{version}",
            "forecaster": f"{prefix}/predictive-autoscaler-ml-api:{version}",
            "trainer": f"{prefix}/predictive-autoscaler-ml-api:{version}"}
    found = {"operator": [], "forecaster": [], "trainer": []}
    for d in docs:
        if not isinstance(d, dict):
            continue
        kind, name = d.get("kind"), (d.get("metadata") or {}).get("name", "")
        if kind == "Deployment" and name.endswith("-operator"):
            comp, spec = "operator", ((d.get("spec") or {}).get("template") or {}).get("spec") or {}
        elif kind == "Deployment" and name.endswith("-forecaster"):
            comp, spec = "forecaster", ((d.get("spec") or {}).get("template") or {}).get("spec") or {}
        elif kind == "CronJob" and "-train-" in name:
            comp = "trainer"
            spec = ((((d.get("spec") or {}).get("jobTemplate") or {}).get("spec") or {}).get("template") or {}).get("spec") or {}
        else:
            continue
        images = [c.get("image") for c in (spec.get("initContainers") or []) + (spec.get("containers") or [])]
        found[comp].append((name, images))
    problems = []
    for comp, items in found.items():
        if not items or (comp != "trainer" and len(items) != 1):
            problems.append(f"{comp}: expected {'at least one' if comp == 'trainer' else 'exactly one'} workload, found {len(items)}")
        for name, images in items:
            bad = [i for i in images if i != want[comp]]
            if not images or bad:
                problems.append(f"{comp} {name}: images {images}, expected only {want[comp]}")
    return problems


def main(argv):
    cmd, args = argv[0], argv[1:]
    if cmd == "semver":
        problems = check_semver(args[0])
    elif cmd == "digest":
        problems = check_digest(args[0])
    elif cmd == "absent":
        import os
        registry, repository, tag = args
        try:
            status, body = lookup(registry, repository, tag, os.environ["GHCR_USER"], os.environ["GHCR_TOKEN"])
            state = classify_http(status, body)
        except (RuntimeError, OSError, KeyError, ValueError) as e:
            print(f"{registry}/{repository}:{tag}: {e}")
            return 1
        problems = [] if state == "absent" else [f"{registry}/{repository}:{tag} already exists: versions are immutable"]
    elif cmd == "ci":
        repo, sha, cr, wr = args
        problems = check_ci(json.load(open(cr)).get("check_runs", []), json.load(open(wr)).get("workflow_runs", []), sha, repo)
    elif cmd == "chart-images":
        version, prefix, path = args
        docs = [json.loads(line) for line in open(path) if line.strip() not in ("", "null")]
        problems = check_chart_images(docs, version, prefix)
    else:
        print(__doc__)
        return 2
    for p in problems:
        print(p)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
