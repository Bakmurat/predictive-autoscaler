"""Test-wide safety: the suite does not use the developer's cluster credentials.

The forecasting service can run kubectl (the cold-start training Job). Whatever kubeconfig or in-cluster environment the
developer has (for example a production context), tests run against an empty kubeconfig with the in-cluster variables
removed and the cold-start opt-in cleared.

As a second line, any kubectl started through Python's subprocess, whatever its path (the pinned tools/bin/kubectl
included), including through `executable=`, is refused unless it is the offline `kubectl kustomize` or
`kubectl version --client`. A shell command line (`sh -c`, `bash -lc`, shell=True) or any other program whose arguments
mention kubectl (`env`, `timeout`, `python -c` ...) is refused unless it is a single plain offline command: shell
syntax is not parsed, only refused. The refusal fails the test that made the call even when
the calling code swallows the error (the cold-start path does), and fails the session when the call happened outside a
test (at collection or import). Exact stub binaries a test creates may be allowed with `allow_stub(path)`.

Limits: shell scripts under test and their child processes start kubectl outside Python, so this patch does not see
them; for those, only the empty kubeconfig and the cleared in-cluster variables apply, and an explicit `--kubeconfig`,
`--server` or `--token` in such a script would bypass both. Those scripts are tested with stub kubectl binaries; the
guarantee that nothing can reach a cluster needs a runner without cluster credentials (CI has none). The guard is
meant to catch accidental calls, not deliberately obfuscated ones.
"""

import os
import shlex
import subprocess
import sys

import pytest

os.environ["KUBECONFIG"] = "/dev/null"
for _var in ("COLD_START_CRONJOB", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT"):
    os.environ.pop(_var, None)

CLUSTER_CALLS = []      # refused kubectl invocations (sanitized), in order; each test consumes the ones it made
_STUBS = set()          # exact, test-created stub binaries that may run
_SHELLS = {"sh", "bash", "zsh", "dash"}
_SHELL_SYNTAX = set(";&|$`()<>{}\n\\*?[]~!#")
_SECRET_FLAGS = {"--token", "--password", "--username", "--client-key", "--client-certificate", "--kubeconfig",
                 "--server", "-s", "--as", "--as-group"}
_real_popen_init = subprocess.Popen.__init__


def allow_stub(path):
    """Allow one exact stub binary (a file the test itself created) to run."""
    _STUBS.add(os.path.realpath(str(path)))


def _is_kubectl(word) -> bool:
    return os.path.basename(str(word)) == "kubectl"


_OUTPUT = {"--output=json", "--output=yaml", "-ojson", "-oyaml"}


def _offline(args) -> bool:
    """The two offline forms, in a narrow grammar: `kubectl kustomize <args>` (local rendering), and `kubectl version`
    with exactly one `--client` or `--client=true` and at most an output format (plain `version` asks the server)."""
    if not args:
        return False
    if args[0] == "kustomize":
        return True
    if args[0] != "version":
        return False
    rest, client = list(args[1:]), 0
    while rest:
        a = rest.pop(0)
        if a in ("--client", "--client=true"):
            client += 1
        elif a in _OUTPUT:
            pass
        elif a in ("-o", "--output") and rest and rest[0] in ("json", "yaml"):
            rest.pop(0)
        else:
            return False        # anything else, including --client=false or another flag
    return client == 1


def _sanitize(argv) -> str:
    out, redact_next = [], False
    for a in map(str, argv):
        if redact_next:
            out.append("<redacted>")
            redact_next = False
        elif a.startswith("-") and "=" in a:
            out.append(a.split("=", 1)[0] + "=<redacted>")
        else:
            out.append(a)
            redact_next = a in _SECRET_FLAGS
    return " ".join(out)


def _split(command):
    try:
        return shlex.split(command)
    except ValueError:
        return command.split()


def _shell_command(argv, program):
    """The command string of `sh -c CMD` (also -ec, -lc, ... option clusters), else None."""
    if os.path.basename(program) not in _SHELLS:
        return None
    for i, a in enumerate(argv[1:], 1):
        if a.startswith("-") and not a.startswith("--") and "c" in a[1:]:
            return argv[i + 1] if i + 1 < len(argv) else ""
    return None


def _plain_offline(words, text) -> bool:
    """One plain command (no shell syntax in text) that is a registered stub or an offline kubectl form."""
    if any(c in text for c in _SHELL_SYNTAX) or not words or not _is_kubectl(words[0]):
        return False
    return os.path.realpath(words[0]) in _STUBS or _offline(words[1:])


def cluster_call(args, executable=None, shell=False):
    """The sanitized kubectl command a Popen(args, executable=, shell=) would run against a cluster, else None.

    Conservative outside the direct form: a shell command line, or any other program whose arguments mention kubectl
    (env, timeout, an interpreter's -c code ...), is refused unless what follows is one plain offline kubectl command.
    shlex is not a shell parser, so shell syntax is refused, never interpreted."""
    if isinstance(args, (str, bytes)):
        argv = _split(os.fsdecode(args))
    elif isinstance(args, os.PathLike):
        argv = [os.fsdecode(args)]
    else:
        argv = [os.fsdecode(a) if isinstance(a, (bytes, os.PathLike)) else str(a) for a in args]
    program = os.fsdecode(executable) if executable is not None else (argv[0] if argv else "")
    if not shell and _is_kubectl(program):
        if os.path.realpath(program) in _STUBS or _offline(argv[1:]):
            return None
        return _sanitize(["kubectl", *argv[1:]])
    if shell:
        text = os.fsdecode(args) if isinstance(args, (str, bytes)) else (argv[0] if argv else "")
    else:
        text = _shell_command(argv, program)
    if text is not None:                                     # a command line interpreted by a shell
        if "kubectl" not in text:
            return None
        words = _split(text)
        return None if _plain_offline(words, text) else "sh -c " + _sanitize(words)
    if not any("kubectl" in a for a in argv[1:]):
        return None
    at = next((i for i, a in enumerate(argv) if _is_kubectl(a)), None)
    if at is not None and _plain_offline(argv[at:], " ".join(argv[at:])):
        return None
    return _sanitize(argv)


def _guarded_popen_init(self, args, *a, **kw):
    refused = cluster_call(args, kw.get("executable"), kw.get("shell", False))
    if refused:
        CLUSTER_CALLS.append(refused)
        raise RuntimeError(f"tests must not call kubectl against a cluster: {refused}")
    _real_popen_init(self, args, *a, **kw)


subprocess.Popen.__init__ = _guarded_popen_init


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item):
    """A call made by the test body fails the test itself."""
    before = len(CLUSTER_CALLS)
    try:
        return (yield)
    finally:
        calls = CLUSTER_CALLS[before:]
        del CLUSTER_CALLS[before:]
        if calls:
            pytest.fail(f"the test tried to reach a cluster with kubectl: {calls}", pytrace=False)


@pytest.fixture(autouse=True)
def _no_cluster_calls():
    """A call made by a fixture around the test errors the test."""
    before = len(CLUSTER_CALLS)
    yield
    calls = CLUSTER_CALLS[before:]
    del CLUSTER_CALLS[before:]
    if calls:
        pytest.fail(f"the test tried to reach a cluster with kubectl: {calls}")


def pytest_sessionfinish(session, exitstatus):
    """Calls no test consumed (made at collection, import or in a session fixture) fail the run."""
    if CLUSTER_CALLS:
        sys.stderr.write(f"\nkubectl called against a cluster outside any test: {CLUSTER_CALLS}\n")
        if session.exitstatus == 0:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.fixture(autouse=True)
def _empty_model_registry():
    """Each test starts and ends with an empty served-model registry in the module-level predictor."""
    yield
    main = sys.modules.get("api.main")
    if main is not None:
        for m in main.predictor.registry.snapshot():
            main.predictor.registry.remove(m["key"])
