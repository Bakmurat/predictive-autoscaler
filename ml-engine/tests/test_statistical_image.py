"""C2.6: the light image's bill of materials (offline): pinned, TensorFlow/CUDA-free requirements, a Dockerfile that copies
exactly the modules the statistical service imports, the image-level detectors, and the resource-envelope harness."""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ML = ROOT / "ml-engine"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sbom = load("sbom_check_statistical", ROOT / "hack" / "sbom-check-statistical.py")


def clean_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    env["KUBECONFIG"] = "/dev/null"
    return env


def test_the_static_bill_of_materials_passes():
    p = subprocess.run([sys.executable, str(ROOT / "hack" / "sbom-check-statistical.py")], capture_output=True,
                       text=True, timeout=60, env=clean_env())
    assert p.returncode == 0 and "PASS" in p.stdout, p.stdout + p.stderr


@pytest.mark.parametrize("text,problem", [
    ("tensorflow==2.15.0\n", "banned"), ("tf-keras==2.15.0\n", "banned"), ("nvidia-cudnn-cu12==9.1.0\n", "banned"),
    ("pandas==2.2.0\n", "banned"), ("numpy>=1.26\n", "exact"), ("numpy\n", "exact"),
    ("numpy==1.26.4\nnumpy==1.26.4\n", "twice"), ("# nothing\n", "no packages"),
])
def test_requirements_problems_are_found(text, problem):
    assert any(problem in p for p in sbom.check_requirements(text))


GOOD_DOCKERFILE = (ML / "Dockerfile.statistical").read_text()


@pytest.mark.parametrize("mutate,problem", [
    (lambda s: s.replace("COPY forecasting/ ./forecasting/", "COPY . ."), "outside the allowlist"),
    (lambda s: s.replace("api/statistical.py", "api/statistical.py api/main.py"), "legacy"),
    (lambda s: s.replace("USER 10001:10001", "USER 0"), "non-root"),
    (lambda s: s.replace("USER 10001:10001", "USER app"), "non-root"),
    (lambda s: s.replace("--no-deps ", ""), "pip install"),
    (lambda s: s.replace("requirements-statistical.txt .", "requirements.txt .").replace(
        "-r requirements-statistical.txt", "-r requirements.txt"), "pip install"),
    (lambda s: s.replace('"statistical"]', '"legacy"]'), "CMD"),
    (lambda s: s.replace("COPY --from=verify /build-info.json /app/build-info.json",
                         "COPY --from=verify /ctx /app/ctx"), "another stage"),
    (lambda s: s.replace("COPY . /ctx", "COPY . /ctx\nCOPY models/ /models/"), "earlier stage"),
])
def test_dockerfile_problems_are_found(mutate, problem):
    assert sbom.check_dockerfile(GOOD_DOCKERFILE) == []
    assert any(problem in p for p in sbom.check_dockerfile(mutate(GOOD_DOCKERFILE)))


def test_image_level_detectors():
    assert sbom.check_packages(["numpy", "scipy", "fastapi"]) == []
    assert len(sbom.check_packages(["tensorflow", "keras", "nvidia-cublas-cu12", "torch", "numpy"])) == 4
    assert sbom.check_libraries(["/usr/lib/libc.so.6", "/usr/local/lib/python3.12/site-packages/numpy/x.so"]) == []
    assert len(sbom.check_libraries(["/usr/lib/libcudart.so.12", "/x/_pywrap_tensorflow_internal.so"])) == 2


def test_the_dockerfile_copies_exactly_what_the_service_imports():
    probe = ("import sys, json\n"
             "import api.serve, api.statistical, forecasting.service, forecasting.worker, forecasting.artifacts\n"
             "root = sys.argv[1]\n"
             "print(json.dumps(sorted({m.__file__[len(root) + 1:] for m in list(sys.modules.values())\n"
             "      if getattr(m, '__file__', None) and m.__file__.startswith(root + '/')})))\n")
    env = clean_env()
    env["PYTHONPATH"] = str(ML)
    p = subprocess.run([sys.executable, "-c", probe, str(ML)], capture_output=True, text=True, timeout=120, cwd=ML,
                       env=env)
    assert p.returncode == 0, p.stderr
    used = set(json.loads(p.stdout.strip().splitlines()[-1]))
    assert "api/main.py" not in used and used
    copied = {s for s in sbom.ALLOWED_COPIES if s.endswith(".py")}
    for path in used:
        assert path in copied or path.startswith("forecasting/"), f"{path} is imported but not in the image"
    assert copied <= used, f"copied but unused: {copied - used}"


def test_the_requirements_cover_the_runtime_closure():
    pins = {line.split("==")[0].lower() for line in (ML / "requirements-statistical.txt").read_text().splitlines()
            if "==" in line and not line.startswith("#")}
    assert {"fastapi", "uvicorn", "starlette", "pydantic", "pydantic-core", "numpy", "scipy",
            "prometheus-client"} <= pins


def test_the_envelope_harness_runs_offline_and_checks_its_budget():
    p = subprocess.run([sys.executable, str(ROOT / "hack" / "statistical-envelope.py"), "--pas", "2", "--interval",
                        "300", "--hours", "1", "--check"], capture_output=True, text=True, timeout=300, env=clean_env())
    assert p.returncode == 0, p.stdout + p.stderr
    out = json.loads(p.stdout)
    assert out["requests"] == 24 and out["failures"] == 0 and out["breaches"] == []
    assert out["journal_bytes"] > 0 and out["peak_rss_mib"] > 0 and out["p95_seconds"] < 2


envelope = load("statistical_envelope", ROOT / "hack" / "statistical-envelope.py")


def test_the_envelope_gate_fails_on_hidden_fit_failures_and_missing_measurements():
    good = {"peak_rss_mib": 80, "p95_seconds": 0.1, "cgroup_memory_peak_mib": 200, "failures": 0, "fit_mode": "worker",
            "fit_failures": {}, "statistical_issuances": 5}
    assert envelope.breaches(good, cgroup=True) == []
    assert envelope.breaches({**good, "fit_failures": {"numeric": 3}}) == ["fit_failures"]
    assert envelope.breaches({**good, "statistical_issuances": 0}) == ["no_statistical_issuances"]
    assert envelope.breaches({**good, "cgroup_memory_peak_mib": None}, cgroup=True) == ["cgroup_unavailable"]
    assert envelope.breaches({**good, "cgroup_memory_peak_mib": 600}, cgroup=True) == ["cgroup_memory_peak_mib"]


def test_failing_fits_cannot_pass_behind_the_naive_fallback():
    # Codex: with every fit forced to fail, the run reported zero failures and no breach.
    out = envelope.run(pas=1, interval=300, hours=1, history_days=8, fit_mode="worker",
                       fits={"e1-product/v1": "tests.c2_fit_helpers:history_fit"})
    assert out["failures"] == 0 and out["fit_failures"].get("history", 0) > 0 and out["statistical_issuances"] == 0
    assert {"fit_failures", "no_statistical_issuances"} <= set(envelope.breaches(out))


def test_real_worker_fits_reach_the_issuances():
    # A fresh process: the budget check measures the harness itself, not this test session.
    p = subprocess.run([sys.executable, str(ROOT / "hack" / "statistical-envelope.py"), "--pas", "1", "--interval",
                        "300", "--hours", "2", "--history-days", "8", "--fit-mode", "worker", "--check"],
                       capture_output=True, text=True, timeout=300, env=clean_env())
    assert p.returncode == 0, p.stdout + p.stderr
    out = json.loads(p.stdout)
    assert out["fit_failures"] == {} and out["statistical_issuances"] > 0 and out["breaches"] == []


def test_inherited_blas_threads_are_overridden_for_fit_processes():
    # Codex repro: OPENBLAS/OMP/MKL_NUM_THREADS=8 inherited by the fit processes gave fit timeouts and no fitted
    # forecasts (the harness's main module loads NumPy in the child before _child() could reset them).
    env = {**clean_env(), "OPENBLAS_NUM_THREADS": "8", "OMP_NUM_THREADS": "8", "MKL_NUM_THREADS": "8"}
    p = subprocess.run([sys.executable, str(ROOT / "hack" / "statistical-envelope.py"), "--pas", "1", "--interval",
                        "300", "--hours", "2", "--history-days", "8", "--fit-mode", "worker", "--check"],
                       capture_output=True, text=True, timeout=300, env=env)
    assert p.returncode == 0, p.stdout + p.stderr
    out = json.loads(p.stdout)
    assert out["fit_failures"] == {} and out["statistical_issuances"] > 0


def test_the_spawn_environment_is_forced_and_then_restored(monkeypatch):
    from forecasting import worker as W
    monkeypatch.setenv("OPENBLAS_NUM_THREADS", "8")
    monkeypatch.setenv("OMP_NUM_THREADS", "8")
    monkeypatch.delenv("MKL_NUM_THREADS", raising=False)
    with W._one_thread_environment():
        assert [os.environ[n] for n in W._THREAD_ENV] == ["1", "1", "1"]
    assert os.environ["OPENBLAS_NUM_THREADS"] == "8" and os.environ["OMP_NUM_THREADS"] == "8"
    assert "MKL_NUM_THREADS" not in os.environ


def git_free_env():
    """clean_env() without any GIT_* variable: a commit hook exports GIT_DIR / GIT_INDEX_FILE, and a scratch repository
    command must never reach the checkout under test."""
    return {k: v for k, v in clean_env().items() if not k.startswith("GIT_")}


def _scratch_repo(tmp_path):
    """A git repository whose ml-engine/ holds exactly the files the image copies, the Dockerfile and the recorder."""
    repo = tmp_path / "repo"
    (repo / "ml-engine").mkdir(parents=True)
    names = ["Dockerfile.statistical", "build_record.py", "requirements-statistical.txt", ".dockerignore"] + \
        [s for s in sbom.ALLOWED_COPIES if s.endswith(".py")]
    for n in names:
        dst = repo / "ml-engine" / n
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes((ML / n).read_bytes())
    for f in sorted((ML / "forecasting").glob("*.py")):
        dst = repo / "ml-engine" / "forecasting" / f.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(f.read_bytes())
    git = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, text=True,  # noqa
                                    env=git_free_env())
    git("init", "-q")
    assert os.path.realpath(git("rev-parse", "--show-toplevel").stdout.strip()) == os.path.realpath(repo)
    git("add", "-A")
    git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "x")
    return repo, git("rev-parse", "HEAD").stdout.strip()


def _record(ctx, tree, flag, out):
    return subprocess.run([sys.executable, str(ML / "build_record.py"), str(ctx), tree, flag, str(out)],
                          capture_output=True, text=True, env=clean_env())


def test_an_evaluation_context_is_the_commit_and_nothing_else(tmp_path):
    # C2.8a (Codex r1 on #27): the evaluation image is built from `git archive <commit>` and verified file by file.
    repo, commit = _scratch_repo(tmp_path)
    env = {**git_free_env(), "EVAL_BUILD_REPO": str(repo)}
    script = str(ROOT / "hack" / "build-statistical-evaluation.sh")
    p = subprocess.run([script, "--context-only", str(tmp_path / "ctx"), commit], capture_output=True, text=True,
                       env=env)
    assert p.returncode == 0, p.stderr
    rec = json.loads((tmp_path / "ctx" / "build-info.json").read_text())
    assert rec["source_tree"] == commit and rec["evaluation_build"] is True and rec["verified_files"] > 10
    from forecasting import qualification as Q
    assert Q.read_build_info(str(tmp_path / "ctx" / "build-info.json")) == Q.BuildInfo(commit, True)
    ctx = tmp_path / "ctx" / "ml-engine"
    assert _record(ctx, commit, "true", tmp_path / "ok.json").returncode == 0
    # Dirty: a changed copied file, an extra module, a missing module, a changed Dockerfile; or another commit.
    for name, change in (("forecasting/trust.py", lambda f: f.write_text(f.read_text() + "# edited\n")),
                         ("forecasting/extra.py", lambda f: f.write_text("x = 1\n")),
                         ("forecasting/store.py", lambda f: f.unlink()),
                         ("Dockerfile.statistical", lambda f: f.write_text(f.read_text() + "# edited\n")),
                         # Codex r2 on #27: a builder-stage input (a loosened pin) is checked like the modules.
                         ("requirements-statistical.txt",
                          lambda f: f.write_text(f.read_text().replace("numpy==", "numpy>=")))):
        bad = tmp_path / ("bad-" + name.replace("/", "-"))
        subprocess.run(["cp", "-a", str(ctx), str(bad)], check=True)
        change(bad / name)
        r = _record(bad, commit, "true", tmp_path / "bad.json")
        assert r.returncode == 1 and "refused" in r.stderr, name
    assert _record(ctx, "b" * 40, "true", tmp_path / "other.json").returncode == 1      # manifest of another commit
    nomani = tmp_path / "no-manifest"
    subprocess.run(["cp", "-a", str(ctx), str(nomani)], check=True)
    (nomani / "source-manifest.txt").unlink()
    r = _record(nomani, commit, "true", tmp_path / "nm.json")                           # a working-tree context
    assert r.returncode == 1 and "source-manifest.txt" in r.stderr
    p = subprocess.run([script, "b" * 40, "x:y"], capture_output=True, text=True, env=env)
    assert p.returncode == 1 and "not a commit" in p.stderr
    p = subprocess.run([script, commit, "x:y"], capture_output=True, text=True, env=env)
    assert p.returncode == 1 and "no pushed branch" in p.stderr                          # never an unpublished commit


@pytest.mark.parametrize("tree,flag,ok", [("unknown", "false", True), ("a" * 40, "false", True),
                                         ("unknown", "true", False), ("a" * 40, "yes", False)])
def test_a_release_record_is_never_an_evaluation_build(tmp_path, tree, flag, ok):
    p = _record(ML, tree, flag, tmp_path / "r.json")
    assert (p.returncode == 0) == ok, p.stderr
    if ok:
        assert json.loads((tmp_path / "r.json").read_text()) == {"source_tree": tree, "evaluation_build": False}
    text = (ML / "Dockerfile.statistical").read_text()
    assert 'RUN python /ctx/build_record.py /ctx "$SOURCE_TREE" "$EVALUATION_BUILD" /build-info.json' in text
    assert "COPY --from=verify /build-info.json /app/build-info.json" in text
    assert "chown root:root /app/build-info.json && chmod 0444 /app/build-info.json" in text
