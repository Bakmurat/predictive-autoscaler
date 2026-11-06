#!/usr/bin/env python3
"""Prove the statistical forecaster's image carries no TensorFlow, Keras, CUDA (or pandas / scikit-learn).

  --static (default; offline, run by the test suite): ml-engine/requirements-statistical.txt pins every package of the
      runtime closure (name==version, nothing banned), and ml-engine/Dockerfile.statistical installs only that file
      (--no-deps, wheels only), copies only the listed modules (never api/main.py, never the whole tree), runs as a
      numeric non-root user and starts `api.serve statistical`.
  --image REF (needs docker; a release step): inside the built image, the installed distributions (pip list), every
      shared library on the filesystem, and an import of api.statistical with TensorFlow blocked.
Exit status 0 only when every check passes; each problem is printed.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "ml-engine" / "requirements-statistical.txt"
DOCKERFILE = ROOT / "ml-engine" / "Dockerfile.statistical"
BANNED_PACKAGES = re.compile(r"^(tensorflow.*|tf[-_].*|keras.*|nvidia[-_].*|.*cuda.*|jax.*|jaxlib|torch.*|"
                             r"pandas|scikit[-_]learn|sklearn)$", re.I)
BANNED_LIBRARIES = re.compile(r"(libcuda|libcudart|libcudnn|libcublas|libnvinfer|libnccl|libtensorflow|"
                              r"_pywrap_tensorflow)", re.I)
PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([0-9][A-Za-z0-9.+!-]*)$")
ALLOWED_COPIES = {"requirements-statistical.txt", "api/admission.py", "api/auth.py", "api/identity.py", "api/serve.py",
                  "api/statistical.py", "data/bounded_http.py", "data/history.py", "forecasting/"}


def check_requirements(text: str) -> list:
    problems, names = [], set()
    for n, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = PIN.match(line)
        if not m:
            problems.append(f"requirements line {n}: {line!r} is not an exact name==version pin")
            continue
        name = m.group(1).lower().replace("_", "-")
        if BANNED_PACKAGES.match(name):
            problems.append(f"requirements line {n}: {name} is banned in the statistical image")
        if name in names:
            problems.append(f"requirements line {n}: {name} is listed twice")
        names.add(name)
    if not names:
        problems.append("requirements: no packages")
    return problems


def check_dockerfile(text: str) -> list:
    problems = []
    lines = [l.strip() for l in text.splitlines() if l.strip() and not l.strip().startswith("#")]
    installs = [l for l in lines if "pip install" in l]
    if len(installs) != 1 or "-r requirements-statistical.txt" not in installs[0] or "--no-deps" not in installs[0] \
            or "--only-binary=:all:" not in installs[0]:
        problems.append(f"Dockerfile: exactly one pip install of requirements-statistical.txt with --no-deps and "
                        f"--only-binary=:all: is required, found {installs}")
    for l in lines:
        if l.startswith("COPY") and "--from=builder" not in l:
            sources = l.split()[1:-1]
            for src in sources:
                if src not in ALLOWED_COPIES:
                    problems.append(f"Dockerfile: COPY of {src!r} is outside the allowlist")
        if "api/main.py" in l or "requirements.txt" in l.replace("requirements-statistical.txt", ""):
            problems.append(f"Dockerfile: {l!r} brings the legacy service or its dependencies")
    users = [l for l in lines if l.startswith("USER ")]
    if not users or not re.match(r"^USER [1-9][0-9]*(:[1-9][0-9]*)?$", users[-1]):
        problems.append("Dockerfile: the final USER must be a numeric non-root user")
    if not any(l == 'CMD ["python", "-m", "api.serve", "statistical"]' for l in lines):
        problems.append("Dockerfile: CMD must start api.serve statistical")
    return problems


def check_packages(names) -> list:
    return [f"image: installed package {n} is banned" for n in sorted(names) if BANNED_PACKAGES.match(n)]


def check_libraries(paths) -> list:
    return [f"image: shared library {p} is banned" for p in sorted(paths) if BANNED_LIBRARIES.search(p)]


PROBE = ("import sys, importlib.abc\n"
         "class B(importlib.abc.MetaPathFinder):\n"
         "    def find_spec(self, n, p=None, t=None):\n"
         "        if n.split('.')[0] in ('tensorflow', 'keras', 'pandas', 'sklearn') or n == 'api.main':\n"
         "            raise ImportError('blocked ' + n)\n"
         "sys.meta_path.insert(0, B())\n"
         "import api.statistical, forecasting.service, forecasting.worker\n"
         "print('import-ok')\n")


def check_image(ref: str) -> list:
    def run(*args):
        p = subprocess.run(["docker", "run", "--rm", "--network", "none", "--entrypoint", *args], capture_output=True,
                           text=True, timeout=300)
        return p.returncode, p.stdout, p.stderr
    problems = []
    rc, out, err = run("python", ref, "-m", "pip", "list", "--format=json")
    if rc != 0:
        return [f"image: pip list failed: {err.strip()[-300:]}"]
    problems += check_packages(d["name"].lower().replace("_", "-") for d in json.loads(out))
    rc, out, err = run("sh", ref, "-c", "find / -xdev -name '*.so*' 2>/dev/null")
    problems += check_libraries(out.split())
    rc, out, err = run("python", ref, "-c", PROBE)
    if rc != 0 or "import-ok" not in out:
        problems.append(f"image: api.statistical does not import with TensorFlow blocked: {err.strip()[-300:]}")
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--image", help="check a built image (needs docker)")
    args = ap.parse_args(argv)
    problems = check_requirements(REQUIREMENTS.read_text()) + check_dockerfile(DOCKERFILE.read_text())
    if args.image:
        problems += check_image(args.image)
    for p in problems:
        print(f"FAIL {p}")
    print("sbom-check-statistical: " + ("PASS" if not problems else f"{len(problems)} problem(s)"))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
