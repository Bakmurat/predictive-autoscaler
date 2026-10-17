#!/usr/bin/env python3
"""release_checks.py — the release workflow's gate checks (.github/workflows/release.yml), kept here so that every
decision is unit-tested (release_checks_test.py; Codex partner review of PR #14). Standard library only.

  release_checks.py semver <version>
  release_checks.py absent <oci-reference>                  # runs `crane manifest`; exit 0 only on a confirmed 404
  release_checks.py ci <repo> <sha> <check-runs.json> <workflow-runs.json>
  release_checks.py chart-images <version> <registry/owner> <rendered.jsonl>
  release_checks.py digest <value>

Each command exits 0 when the check passes and prints the problems otherwise (exit 1). A failed check never falls back
to a default: anything the check cannot confirm is a problem.
"""
import json
import re
import subprocess
import sys

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


def classify_lookup(returncode, stderr):
    """'present' or 'absent' for a `crane manifest` result; anything unconfirmed (authentication, permission, timeout,
    server error, unknown output) raises, so a registry problem is never mistaken for an absent version."""
    if returncode == 0:
        return "present"
    if re.search(r"UNAUTHORIZED|DENIED|FORBIDDEN|TOOMANYREQUESTS|timeout|i/o timeout|connection refused|5\d\d ", stderr, re.I):
        raise RuntimeError(f"registry lookup failed, not a confirmed absence: {stderr.strip()[:300]}")
    if re.search(r"\b(MANIFEST_UNKNOWN|NAME_UNKNOWN)\b", stderr):
        return "absent"
    raise RuntimeError(f"unrecognised registry response, not a confirmed absence: {stderr.strip()[:300]}")


def check_ci(check_runs, workflow_runs, sha, repo, required=REQUIRED_CHECKS):
    """Every required check is bound to a GitHub Actions run of the expected workflow file, for exactly this commit and
    repository (no fork), and the newest attempt of that check succeeded. A same-name check from another producer
    (another app, another workflow, another repository) is a problem, never a substitute."""
    problems = []
    runs = {r.get("check_suite_id"): r for r in workflow_runs}
    for name, path in required.items():
        same = [c for c in check_runs if c.get("name") == name]
        foreign = [c for c in same if (c.get("app") or {}).get("slug") != ACTIONS_APP]
        if foreign:
            problems.append(f"{name}: a check with this name from another producer ({(foreign[0].get('app') or {}).get('slug')!r})")
        bound = []
        for c in same:
            if (c.get("app") or {}).get("slug") != ACTIONS_APP or c.get("head_sha") != sha:
                continue
            r = runs.get((c.get("check_suite") or {}).get("id"))
            if not r:
                problems.append(f"{name}: no workflow run for its check suite")
                continue
            if r.get("path") != path or r.get("head_sha") != sha or (r.get("head_repository") or {}).get("full_name") != repo:
                problems.append(f"{name}: produced by {r.get('path')} at {str(r.get('head_sha'))[:12]} in "
                                f"{(r.get('head_repository') or {}).get('full_name')}, expected {path} at {sha[:12]} in {repo}")
                continue
            bound.append((r.get("run_attempt") or 0, c.get("started_at") or "", c, r))
        if not bound:
            problems.append(f"{name}: no check from {path} on {sha[:12]}")
            continue
        _, _, c, r = max(bound, key=lambda t: (t[0], t[1]))
        if c.get("status") != "completed" or c.get("conclusion") != "success" \
                or r.get("status") != "completed" or r.get("conclusion") != "success":
            problems.append(f"{name}: newest attempt {c.get('status')}/{c.get('conclusion')} (run {r.get('conclusion')})")
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
        p = subprocess.run(["crane", "manifest", args[0]], capture_output=True, text=True, timeout=120)
        try:
            state = classify_lookup(p.returncode, p.stderr)
        except RuntimeError as e:
            print(f"{args[0]}: {e}")
            return 1
        problems = [] if state == "absent" else [f"{args[0]} already exists: versions are immutable"]
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
