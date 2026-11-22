#!/usr/bin/env python3
"""release.py -- the reviewed Helm helper of the demonstration release: ONE Recommend-only upgrade and its rollback.

    release.py --record R status
    release.py --record R plan-upgrade                              read-only; writes the plan, prints its digest
    release.py --record R apply-upgrade  --plan F --plan-digest D
    release.py --record R plan-rollback                             read-only
    release.py --record R apply-rollback --plan F --plan-digest D
    release.py --record R plan-dispose                              read-only: evidence for a reviewed disposition
    release.py --record R dispose        --plan F --plan-digest D
    release.py --record R recover-plan | recover --plan F --plan-digest D

Version 1 changes exactly one thing: the forecaster Deployment's image, and one added environment entry
(WATCH_NAMESPACES=<the demonstration namespace>). The plan renders the installed chart tree with the installed values
and the new chart tree with the new values, offline, and refuses any other difference (an added or removed object, a
Role, a NetworkPolicy, a ConfigMap, another field). It is not a grant-capable upgrade.

What the plan binds: the record and its commit, the release's history (revision, status, chart, the CLASS of each
description and its sha256, never its text), the storage Secret of the current revision by UID (metadata only, through
c28-secretmeta), both chart trees (tree id, archive sha256) and both values files, the sanitized renderings' hashes,
the difference, the live forecaster against the installed rendering (every field, minus enumerated server defaults),
the TLS Secret's metadata, the release's pods, the safety conditions, the operation identifier and the exact argument
array.

Helm runs once, through procgroup.py (its own process group, recorded durably before it can execute; a deadline, TERM,
KILL; the group proven empty; output only counted and hashed). The call is journalled with the shared intent protocol:
the lock is marked unsafe, `intent-<n>` is published, then the command is released. Afterwards the outcome is read, not
inferred from the exit status:

    applied     revision from+1 is the newest, `deployed`, and its storage Secret carries this operation's identifier
                (`--labels c28.devkuban.com/release-op=<op>`) with the UID first seen   -> step-<n>, lock released
    failed      that revision, ours, `failed`                    -> lock kept; a reviewed rollback
    unresolved  ours and `pending-upgrade`; or Helm executed and there is no revision evidence (whatever its exit)
    foreign     a revision that is not ours, anything newer, a gap, a storage Secret whose UID changed
    unknown     the group could not be proven empty, or a read failed

Only `applied` resolves the intent. Nothing is retried; the same operation identifier is never submitted twice. A
command that was never released (the identity could not be recorded) certainly did not execute: step-<n> not-applied.

Rollback (`helm rollback <release> <from revision>`, a NEW revision that restores the unfixed forecaster; the write
gate stays closed after it) has two forms. After an applied upgrade it is an ordinary reviewed operation. For a failed
or pending upgrade it runs under the recovery guard, with the stale lock retained and only that upgrade's own intent
open. Helm copies the destination's labels and writes a generic description, so a rollback is ours only when our own
Helm process ended cleanly AND the expected revision number appeared as "Rollback to <n>", `deployed`; an interrupted
rollback stays unresolved.

The application read-back (pods, certificate, PredictiveAutoscalers, ledger, permissions) is a separate reviewed step:
`applied` means "Helm recorded our revision as deployed", nothing about the application."""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tarfile
from typing import Callable, Dict, List, Optional, Tuple

import yaml

import common_records as C
import execlib as X
import imageroll
import prepare
import procgroup

HELPER = "release"
UPGRADE_OP, ROLLBACK_OP = "helm-upgrade", "helm-rollback"
OP_LABEL = "c28.devkuban.com/release-op"
CHART = "charts/predictive-autoscaler"
COMPONENT_LABEL, INSTANCE_LABEL = "app.kubernetes.io/component", "app.kubernetes.io/instance"
FORECASTER, WATCH_ENV = "forecaster", "WATCH_NAMESPACES"
MARGIN_S, READ_DEADLINE_S, HISTORY_MAX, HISTORY_BYTES, RENDER_BYTES, META_BYTES = 90, 120, 20, 1 << 20, 8 << 20, 1 << 16
# Flags that must never reach a mutating Helm call of this helper.
FORBIDDEN = ("--atomic", "--rollback-on-failure", "--cleanup-on-fail", "--force", "--take-ownership", "--install",
             "-i", "--reuse-values", "--reset-then-reuse-values", "--reset-values", "--debug", "--dry-run", "-o",
             "--output", "--recreate-pods", "--no-hooks", "--description", "--set", "--set-string", "--set-file",
             "--set-json", "--post-renderer")
DESCRIPTIONS = ("Install complete", "Upgrade complete", "Preparing upgrade")
ROLLBACK_DESCRIPTION = re.compile(r"Rollback to (\d+)$")
# The options a mutating call carries, in order: an exact list, not only "nothing forbidden".
UPGRADE_OPTIONS = ("--namespace", "--kubeconfig", "--kube-context", "-f", "--wait", "--timeout", "--labels")
ROLLBACK_OPTIONS = ("--namespace", "--kubeconfig", "--kube-context", "--wait", "--timeout")
ENV_KEEP = ("PATH", "HOME", "LANG")       # the whole environment of every command this helper starts
TLS_TYPE = "kubernetes.io/tls"
# The ONE reviewed use of `lookup`: the forecaster's TLS Secret is kept across upgrades by looking up the existing
# Secret of the release's own namespace. The exception is this template CONTENT (and the helpers it includes), by
# sha256: a changed template, or another lookup under the same file name, is not covered and needs a new review.
REVIEWED_LOOKUP = {"templates/forecaster-tls.yaml": {
    "sha256": ("e106607acb395f1965acec2ead357658b66e0ca024b0b357bc128fc950af0418",),
    "with": {"templates/_helpers.tpl": ("cc5fe690f32d54b39d139f2e3e033af4412c441ba246277fb5d9c7090d724901",)}}}
# A finding about the live object, in fixed words: never the text of imageroll.spec_findings, which quotes values.
FINDING = {"absent-live": "rendered, absent live", "live-only": "live only",
           "entries": "the number of entries differs", "values": "the values differ"}
ROLES = ("operator", "forecaster", "trainer")
RUNNER: Callable = procgroup.run          # tests replace these three; the reconcilers have no other way in
CHARTS: Optional[Callable] = None
PERMISSIONS: Optional[Callable] = None


class ReadError(X.StagingError):
    """A read-only command did not give a usable answer. Carries counts and hashes, never output."""


class ToolError(ReadError):
    """The executable is not the one the record names: it was NOT run."""


# --- the context ------------------------------------------------------------------------------------------------------

class Ctx:
    def __init__(self, record: X.Record, kube: X.Kube, runner: Optional[Callable] = None,
                 charts: Optional[Callable[[str], Dict]] = None, permission_check: Optional[Callable[[], Dict]] = None,
                 checkout: Optional[Callable[[], List[str]]] = None):
        self.record, self.kube = record, kube
        self.s = prepare._section(record)
        if prepare.stage_of(self.s) != "demonstration":
            raise X.StagingError("the release helper belongs to the demonstration stage")
        self.params = prepare.load_params(record, "demonstration")
        self.cfg = record.section("release_upgrade")
        self.c28 = X.import_harness()
        self.ns, self.control_ns, self.release = (self.params["namespace"], self.params["control_namespace"],
                                                  self.params["release"])
        self.runner = runner or RUNNER
        self.charts = charts or CHARTS or (lambda commit: chart_artifact(record, commit))
        self.permission_check = permission_check or PERMISSIONS or (lambda: _permissions(self))
        self.checkout = checkout or (lambda: checkout_problems(repo_root(), record.commit))
        self.k = record.doc["kubeconfig"]

    @property
    def helm(self) -> str:
        return self.cfg["helm"]["binary"]

    def conn(self) -> List[str]:
        return ["--namespace", self.control_ns, "--kubeconfig", self.k["path"], "--kube-context", self.k["context"]]

    def run(self, argv: List[str], **kw):
        """EVERY command of this helper starts here: the executable is verified first, the environment is the
        whitelist, and the runner is procgroup's (its own group, on record before it can act, bounded, proven
        empty)."""
        require_tool(self, argv[0])
        return self.runner(argv, env=environment(), cwd="/", **kw)


def environment() -> Dict[str, str]:
    return {k: os.environ[k] for k in ENV_KEEP if k in os.environ}


def require_tool(c: Ctx, path_: str) -> None:
    """Nothing is executed unless it is the record's helm or metadata reader, byte for byte."""
    known = {c.cfg["helm"]["binary"]: c.cfg["helm"]["sha256"],
             c.cfg["metadata_reader"]["binary"]: c.cfg["metadata_reader"]["sha256"]}
    if path_ not in known or not os.path.isabs(path_) or not os.path.isfile(path_) or \
            C.sha256_file(path_) != known[path_]:
        raise ToolError(f"{os.path.basename(str(path_))} is not the executable the record names (path or sha256): "
                        "it was not run")


def clean(r) -> bool:
    """A procgroup result that can be believed: the command ran once, ended by itself with status 0, all of its
    output was read and nothing of its group is left."""
    return bool(r.started and not r.runner_error and r.exit_kind == "exited" and r.exit_status == 0
                and r.group_empty is True and r.output_complete and not r.survivors and not r.deadline_exceeded)


class _Capture:
    """The bounded stdout of a READ, in memory only."""

    def __init__(self, limit: int):
        self.limit, self.kept, self.overflow = limit, bytearray(), False

    def add(self, chunk: bytes) -> None:
        room = self.limit - len(self.kept)
        self.kept += chunk[:max(room, 0)]
        self.overflow = self.overflow or len(chunk) > room


def _permissions(c: Ctx) -> Dict[str, Dict]:
    """The permissions helper's proof, in process, for each of the release's three identities."""
    import permissions
    targets = c.c28.demo.target_names(c.params)

    def kubectl(args):
        p = subprocess.run(["kubectl", "--kubeconfig", c.k["path"], "--context", c.k["context"],
                            "--request-timeout=30s", *args], capture_output=True, text=True, timeout=120)
        return p.returncode, p.stdout or ""
    out = {}
    for role in ROLES:
        sa = f"{c.release}-predictive-autoscaler-{role}"
        out[role] = permissions.verdict(permissions.collect(kubectl, c.control_ns, sa, c.ns, targets, role),
                                        c.control_ns, sa, targets, c.record.cluster_uid)
    return out


def repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _timeout_s(text: str) -> int:
    m = re.fullmatch(r"(\d+)([sm])", str(text))
    if not m or not 0 < int(m.group(1)) * (60 if m.group(2) == "m" else 1) <= 900:
        raise X.StagingError("release_upgrade.helm.timeout must be like 5m or 300s, at most 15 minutes")
    return int(m.group(1)) * (60 if m.group(2) == "m" else 1)


# --- the chart trees --------------------------------------------------------------------------------------------------

def _git(repo: str, *args: str) -> bytes:
    # no GIT_* of the caller: inside a git hook GIT_DIR and GIT_INDEX_FILE would redirect -C to the hook's repository
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    p = subprocess.run(["git", "-C", repo, *args], capture_output=True, timeout=120, env=env)
    if p.returncode != 0:
        raise X.StagingError(f"git {args[0]} failed in {repo} (exit {p.returncode})")
    return p.stdout


def checkout_problems(repo: str, commit: str) -> List[str]:
    """The single-commit model: the executing checkout IS the record's commit, clean."""
    out = []
    if _git(repo, "rev-parse", "HEAD").decode().strip() != commit:
        out.append("the executing checkout is not at the record's source.commit")
    if _git(repo, "status", "--porcelain", "--untracked-files=all").strip():
        out.append("the executing checkout is not clean")
    return out


def _blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()


def chart_artifact(record: X.Record, commit: str, repo: Optional[str] = None) -> Dict:
    """The chart tree of a commit, extracted once under <evidence>/release/chart-<tree id>/ and verified file by file
    against git's own listing (regular files only). The path is a function of the tree, so re-planning reuses it."""
    repo = repo or repo_root()
    commit = _git(repo, "rev-parse", "--verify", f"{commit}^{{commit}}").decode().strip()
    tree = _git(repo, "rev-parse", "--verify", f"{commit}:{CHART}").decode().strip()
    listing = {}
    for entry in _git(repo, "ls-tree", "-r", "-z", commit, CHART).split(b"\x00"):
        if not entry:
            continue
        meta, path_ = entry.split(b"\t", 1)
        mode, kind, sha = meta.decode().split()
        if kind != "blob" or mode not in ("100644", "100755"):
            raise X.StagingError(f"the chart tree holds something that is not a regular file ({mode} {kind})")
        listing[path_.decode()] = sha
    if not listing:
        raise X.StagingError(f"commit {commit[:12]} has no {CHART}")
    archive = _git(repo, "archive", "--format=tar", commit, CHART)
    dest = os.path.join(record.evidence_dir, HELPER, f"chart-{tree}")
    if not os.path.isdir(dest):
        tmp = f"{dest}.tmp-{os.getpid()}"
        os.makedirs(tmp)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            for m in tar.getmembers():
                if m.isdir():
                    continue
                if not m.isfile() or m.name not in listing:
                    raise X.StagingError("the chart archive holds an entry git did not list")
                target = os.path.join(tmp, m.name)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with open(target, "wb") as fh:
                    fh.write(tar.extractfile(m).read())
        os.rename(tmp, dest)
    found = {}
    for root, _dirs, files in os.walk(dest):
        for f in files:
            p = os.path.join(root, f)
            if os.path.islink(p):
                raise X.StagingError("the extracted chart holds a link")
            with open(p, "rb") as fh:
                found[os.path.relpath(p, dest)] = _blob_sha(fh.read())
    if found != listing:
        raise X.StagingError(f"{dest} does not equal the chart tree {tree[:12]} (changed, missing or extra files)")
    return {"commit": commit, "tree": tree, "archive_sha256": hashlib.sha256(archive).hexdigest(),
            "dir": os.path.join(dest, CHART), "files": len(listing)}


# --- reads: Helm and the storage Secret's metadata --------------------------------------------------------------------

def _read(c: Ctx, argv: List[str], capture: int, what: str) -> bytes:
    cap = _Capture(capture)
    r = c.run(argv, deadline_s=READ_DEADLINE_S, record=lambda _identity: None, stdout_sink=cap.add)
    if not clean(r) or cap.overflow:
        raise ReadError(f"{what} gave no usable answer (started {r.started}, exit {r.exit_kind} {r.exit_status}, "
                        f"stdout {r.stdout_bytes} bytes, stderr {r.stderr_bytes} bytes, group empty {r.group_empty}, "
                        f"runner error {r.runner_error}, over the bound {cap.overflow})")
    return bytes(cap.kept)


def describe(text) -> Dict:
    """A history description as a class and a hash: its text may be an error message and is never kept."""
    text = "" if text is None else str(text)
    m = ROLLBACK_DESCRIPTION.fullmatch(text)
    cls = text if text in DESCRIPTIONS else f"Rollback to {m.group(1)}" if m else "other"
    return {"description_class": cls, "description_sha256": hashlib.sha256(text.encode()).hexdigest()}


def history(c: Ctx) -> List[Dict]:
    raw = _read(c, [c.helm, "history", c.release, *c.conn(), "--max", str(HISTORY_MAX), "-o", "json"], HISTORY_BYTES,
                "helm history")
    try:
        rows = json.loads(raw)
        out = [{"revision": int(r["revision"]), "status": str(r["status"]), "chart": str(r.get("chart", "")),
                "app_version": str(r.get("app_version", "")), **describe(r.get("description"))} for r in rows]
    except (ValueError, KeyError, TypeError):
        raise ReadError(f"helm history's answer is not the expected JSON ({len(raw)} bytes)") from None
    out.sort(key=lambda r: r["revision"])
    if not out or len({r["revision"] for r in out}) != len(out):
        raise ReadError("helm history lists no revision, or one twice")
    return out


def helm_version(c: Ctx) -> str:
    return _read(c, [c.helm, "version", "--template", "{{.Version}}"], 256, "helm version").decode().strip()


def render(c: Ctx, chart_dir: str, values: str) -> Dict:
    """helm template, offline. Returns {"raw": the documents (IN MEMORY ONLY: they are compared, never stored or
    printed), "sanitized": what may be hashed and shown, "secrets": the Secret documents as key, type and key names}."""
    argv = [c.helm, "template", c.release, chart_dir, "--namespace", c.control_ns, "-f", values, "--kube-version",
            str(c.cfg["helm"]["kube_version"])]
    for api in c.cfg["helm"].get("api_versions") or []:     # the capabilities the chart may ask the cluster for
        argv += ["--api-versions", str(api)]
    raw = _read(c, argv, RENDER_BYTES, "helm template")
    try:
        text = raw.decode("utf-8")
        docs = [d for d in yaml.safe_load_all(text) if d]
    except UnicodeDecodeError:
        raise ReadError("helm template's output is not UTF-8") from None
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)            # numbers only: the mark's text quotes the source
        where = f"line {mark.line + 1}, column {mark.column + 1}" if mark is not None else "position unknown"
        raise ReadError(f"helm template's output is not valid YAML ({where})") from None
    if any(not isinstance(d, dict) or not d.get("kind") or not isinstance(d.get("metadata"), dict) for d in docs):
        raise ReadError("helm template's output holds a document without a kind or metadata")
    sanitized, _discarded = _sanitizer().sanitize_stream(text)
    secrets = sorted(({"key": _key(d), "type": d.get("type"), "data_keys": _secret_shape(d)["data_keys"]}
                      for d in docs if d["kind"] == "Secret"), key=lambda s: s["key"])
    return {"raw": docs, "sanitized": sanitized, "secrets": secrets}


def _sanitizer():
    spec = importlib.util.spec_from_file_location(
        "sanitize_render", os.path.join(os.path.dirname(os.path.abspath(__file__)), "sanitize-render.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def secret_meta(c: Ctx, name: str, label_keys: Tuple[str, ...] = ()) -> Dict:
    """One Secret's metadata through c28-secretmeta (PartialObjectMetadata only; never its contents)."""
    argv = [c.cfg["metadata_reader"]["binary"], "--kubeconfig", c.k["path"], "--context", c.k["context"], "--namespace",
            c.control_ns, "--name", name, "--timeout", "15s"]
    for k in label_keys:
        argv += ["--label-key", k]
    raw = _read(c, argv, META_BYTES, "the Secret metadata reader")
    try:
        doc = json.loads(raw)
        ok = doc["schema"] == "c28-secret-metadata/v1" and doc["namespace"] == c.control_ns and doc["name"] == name \
            and isinstance(doc["found"], bool) and (not doc["found"] or (doc["uid"] and doc["resourceVersion"]))
    except (ValueError, KeyError, TypeError):
        ok = False
    if not ok:
        raise ReadError("the Secret metadata reader's answer is not the expected document")
    return {"found": doc["found"], "name": name, "uid": doc.get("uid", ""),
            "resourceVersion": doc.get("resourceVersion", ""), "labels": dict(doc.get("labels") or {})}


def storage_name(c: Ctx, revision: int) -> str:
    return f"sh.helm.release.v1.{c.release}.v{revision}"


def storage(c: Ctx, revision: int) -> Dict:
    """The storage Secret of a revision: found, UID, whether it is Helm's object for exactly this release and
    revision, and the operation identifier it carries."""
    m = secret_meta(c, storage_name(c, revision), ("name", "owner", "version", "status", OP_LABEL))
    lab = m["labels"]
    return {"found": m["found"], "name": m["name"], "uid": m["uid"], "operation": lab.get(OP_LABEL, ""),
            "helm_object": m["found"] and (lab.get("name"), lab.get("owner"), lab.get("version")) ==
            (c.release, "helm", str(revision))}


# --- the difference ---------------------------------------------------------------------------------------------------

def _key(d: Dict) -> str:
    m = d.get("metadata") or {}
    return f"{d.get('kind')}/{m.get('namespace') or ''}/{m.get('name')}"


def _index(docs: List[Dict]) -> Dict[str, Dict]:
    out = {}
    for d in docs:
        if _key(d) in out:
            raise X.StagingError(f"the rendering holds {_key(d)} twice")
        out[_key(d)] = d
    return out


def _sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _forecaster(docs: List[Dict]) -> Dict:
    hits = [d for d in docs if d.get("kind") == "Deployment" and
            ((d.get("metadata") or {}).get("labels") or {}).get(COMPONENT_LABEL) == FORECASTER]
    if len(hits) != 1:
        raise X.StagingError(f"the rendering holds {len(hits)} forecaster Deployments, not one")
    return hits[0]


def _container(dep: Dict) -> Dict:
    hits = [x for x in dep["spec"]["template"]["spec"]["containers"] if x.get("name") == FORECASTER]
    if len(hits) != 1:
        raise X.StagingError("the forecaster Deployment does not have exactly one container named forecaster")
    return hits[0]


def _secret_shape(d: Dict) -> Dict:
    """A Secret without its values: everything but the contents of data and stringData, whose key names stay."""
    out = {k: v for k, v in d.items() if k not in ("data", "stringData")}
    out["data_keys"] = sorted(set(d.get("data") or {}) | set(d.get("stringData") or {}))
    return out


def _same(key: str, a: Dict, b: Dict, tls_key: str) -> bool:
    """Whether two renderings of one object are equal, compared in memory on the UNSANITIZED documents (a redacted
    value equals every other redacted value, so the sanitized ones cannot show equality). The one exception is the
    forecaster's generated TLS Secret: offline there is no lookup, each rendering makes a new certificate, and only
    its shape is compared."""
    if a.get("kind") != "Secret" or b.get("kind") != "Secret":
        return a == b
    if _secret_shape(a) != _secret_shape(b):
        return False
    if key == tls_key and a.get("type") == TLS_TYPE:
        return True
    return (a.get("data"), a.get("stringData")) == (b.get("data"), b.get("stringData"))


def difference(c: Ctx, frm: Dict, to: Dict) -> Tuple[Dict, List[str]]:
    """What the upgrade changes, and why it is refused if it is anything but: the forecaster container's image
    (installed digest -> the record's statistical image) and exactly one added environment entry. Every document of
    both renderings takes part, Secrets included; the result names objects, never values."""
    a, b = _index(frm["raw"]), _index(to["raw"])
    fc = _forecaster(frm["raw"])
    key, tls_key = _key(fc), f"Secret/{fc['metadata'].get('namespace') or ''}/{fc['metadata']['name']}-tls"
    diff = {"added": sorted(set(b) - set(a)), "removed": sorted(set(a) - set(b)),
            "changed": sorted(k for k in set(a) & set(b) if not _same(k, a[k], b[k], tls_key)), "forecaster": key,
            "tls_exception": tls_key if a.get(tls_key, {}).get("type") == TLS_TYPE and tls_key in b else None}
    problems = [f"the new rendering adds {k}" for k in diff["added"]] + \
               [f"the new rendering removes {k}" for k in diff["removed"]] + \
               [f"{k} differs between the renderings (only the forecaster's image and one environment entry may)"
                for k in diff["changed"] if k != key]
    if key not in b:
        return diff, problems
    old, new = _container(a[key]), _container(b[key])
    want_to = c.s["release"]["statistical_image"]
    entry = {"name": WATCH_ENV, "value": c.ns}
    old_env, new_env = list(old.get("env") or []), list(new.get("env") or [])
    env_ok = entry not in old_env and new_env.count(entry) == 1 and [e for e in new_env if e != entry] == old_env
    expected = copy.deepcopy(a[key])                  # the installed object with exactly the two allowed changes
    ctr = _container(expected)
    ctr["image"] = want_to
    if env_ok:
        ctr["env"] = new_env
    else:
        problems.append(f"the new rendering does not add exactly the one environment entry {WATCH_ENV}={c.ns}")
    if new.get("image") != want_to:
        problems.append("the new rendering's forecaster image is not the record's statistical image")
    if old.get("image") == want_to or "@sha256:" not in str(old.get("image")) or "@sha256:" not in str(want_to):
        problems.append("the forecaster image does not change from one digest to another")
    if expected != b[key]:
        problems.append("the forecaster Deployment differs in more than its image and that environment entry")
    diff.update(from_image=old.get("image"), to_image=new.get("image"),
                added_env_names=sorted(str(e.get("name")) for e in new_env if e not in old_env))
    return diff, problems


def _safe_path(path_: str) -> str:
    """A field path as it may be shown: the characters of field, label and annotation names, nothing else."""
    return re.sub(r"[^A-Za-z0-9_./\[\]-]", "?", path_)[:200]


def _shown(path_: str, kind: str, parent: str) -> str:
    """What a finding about the live object says: a path made of keys the RENDERING has (reviewed chart content),
    and fixed words. A key that only the live object has is content of the cluster and is not shown."""
    where = f"{_safe_path(parent)}.<a key the rendering does not have>" if kind == "live-only" else _safe_path(path_)
    return f"{where}: {FINDING[kind]}"


def live_problems(c: Ctx, rendered: Dict) -> Tuple[Dict, List[str]]:
    """The live forecaster against the UNSANITIZED rendering: every field, minus the enumerated server defaults,
    compared in memory; a finding names the path, never a value."""
    name = rendered["metadata"]["name"]
    live = c.kube.get("Deployment", name, c.control_ns)
    if live is None:
        return {"name": name}, [f"Deployment {name} does not exist"]
    findings = imageroll.spec_findings(rendered["spec"], live["spec"])
    return ({"name": name, "uid": live["metadata"]["uid"], "generation": live["metadata"].get("generation")},
            [f"the live forecaster differs from the rendering: {_shown(p, kind, parent)}"
             for p, kind, _text_that_may_quote_values, parent in findings])


def live_dependence(c: Ctx, chart_dir: str) -> Tuple[Dict, List[str]]:
    """Where a chart tree reads the live cluster while rendering, which an offline rendering cannot show: `lookup`
    and `.Capabilities`, counted per template file (an inventory). A template with a lookup is accepted only when its
    CONTENT and the helpers it includes are the reviewed ones (REVIEWED_LOOKUP, by sha256); capabilities must be bound
    by the record."""
    found: Dict[str, Dict] = {"lookup": {}, "capabilities": {}, "lookup_sha256": {}}
    sha: Dict[str, str] = {}
    for root, dirs, files in os.walk(chart_dir):
        dirs.sort()
        for f in sorted(files):
            rel = os.path.relpath(os.path.join(root, f), chart_dir)
            if rel.split(os.sep)[0] not in ("templates", "charts"):
                continue
            with open(os.path.join(root, f), "rb") as fh:
                data = fh.read()
            sha[rel] = hashlib.sha256(data).hexdigest()
            text = data.decode("utf-8", errors="replace")
            for name, rx in (("lookup", r"\blookup\b"), ("capabilities", r"\.Capabilities\b")):
                if re.findall(rx, text):
                    found[name][rel] = len(re.findall(rx, text))
    problems = []
    for rel in sorted(found["lookup"]):
        found["lookup_sha256"][rel] = sha[rel]
        rule = REVIEWED_LOOKUP.get(rel)
        if rule is None or sha[rel] not in rule["sha256"] or \
                any(sha.get(dep) not in ok for dep, ok in rule["with"].items()):
            problems.append(f"the chart reads the live cluster with lookup in {rel}, and that template (or a helper "
                            "it includes) is not the reviewed content: its lookups are not covered")
    if found["capabilities"] and "api_versions" not in c.cfg["helm"]:
        problems.append(f"the chart asks for the cluster's capabilities in {sorted(found['capabilities'])}: the "
                        "record must bind helm.api_versions")
    return found, problems


# --- conditions -------------------------------------------------------------------------------------------------------

def config_problems(c: Ctx) -> List[str]:
    cfg, out = c.cfg, []
    try:
        frm, to, helm, meta = cfg["from"], cfg["to"], cfg["helm"], cfg["metadata_reader"]
        int(frm["revision"]), frm["commit"], helm["kube_version"], _timeout_s(helm["timeout"])
    except (KeyError, TypeError, ValueError):
        raise X.StagingError("release_upgrade needs from {revision, commit, values, values_sha256}, to {values, "
                             "values_sha256}, helm {binary, sha256, version, timeout, kube_version} and metadata_reader "
                             "{binary, sha256}") from None
    for what, path_, sha in (("the installed values", frm["values"], frm["values_sha256"]),
                             ("the new values", to["values"], to["values_sha256"]),
                             ("the helm binary", helm["binary"], helm["sha256"]),
                             ("the Secret metadata reader", meta["binary"], meta["sha256"])):
        p = c.record.resolve(path_)
        if not os.path.isabs(str(path_)) or not os.path.isfile(p):
            out.append(f"{what}: {path_} is not an existing absolute file")
        elif C.sha256_file(p) != sha:
            out.append(f"{what}: {path_} does not have the recorded sha256")
    if frm["values"] == to["values"]:
        out.append("the new values need a new immutable file, not the installed one")
    return out + list(c.checkout())


def safety_problems(c: Ctx, admit: Optional[Dict[str, List[str]]] = None) -> List[str]:
    """What must hold whenever Helm is about to change the release. admit: the one open intent a rollback of an
    unresolved upgrade is allowed to find."""
    import bounded
    out = list(bounded.fence_problems(bounded.Ctx(c.record, c.kube)))
    left = C.unresolved_intents(c.record.doc)
    if left != (admit or {}):
        out.append(f"intents with unknown outcomes {left}: recover first")
    out += prepare.verify(c.record, c.kube)["problems"]
    out += prepare.demonstration_gate(c.record, c.kube, c.params, c.s, prepare.split(c.params, "demonstration"))[1]
    perms = c.permission_check()
    for role in ROLES:                                     # each of the release's identities, none assumed
        p = perms.get(role) or {}
        if p.get("verdict") != "pass" or p.get("write_grants"):
            out.append(f"the {role}'s replica-write permission is not proven absent (verdict {p.get('verdict')})")
    return out


def _gap(hist: List[Dict]) -> bool:
    revs = [h["revision"] for h in hist]
    return revs != list(range(revs[0], revs[0] + len(revs)))


def history_problems(hist: List[Dict]) -> List[str]:
    out = ["the release's history has a gap"] if _gap(hist) else []
    if [h["status"] for h in hist].count("deployed") != 1 or hist[-1]["status"] != "deployed":
        out.append("the newest revision is not the one deployed revision")
    return out


def release_pods(c: Ctx) -> List[Dict]:
    return sorted(({"name": p["metadata"]["name"], "uid": p["metadata"]["uid"],
                    "restarts": sum(int(s.get("restartCount") or 0)
                                    for s in (p.get("status") or {}).get("containerStatuses") or [])}
                   for p in c.kube.list("Pod", c.control_ns, labels={INSTANCE_LABEL: c.release})),
                  key=lambda p: p["name"])


def operation_id(c: Ctx, from_revision: int, frm: Dict, to: Dict) -> str:
    parts = ["c28-release-op/v1", c.record.sha256, c.release, str(from_revision), frm["tree"],
             c.cfg["from"]["values_sha256"], to["tree"], c.cfg["to"]["values_sha256"]]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:20]


def _submitted(c: Ctx, op: str, kind: str = UPGRADE_OP) -> List[str]:
    """The steps that submitted this operation to Helm. A command that was provably never released (step not-applied,
    or reconciled not-done) did not submit anything."""
    return [n for n, s in sorted(X._steps(X.receipts(c.record, HELPER)).items(), key=lambda kv: int(kv[0]))
            if s["intent"] and s["intent"]["op"] == kind and s["intent"]["context"].get("release_op") == op
            and (s["outcome"] or {}).get("result") != "not-applied"
            and (s["reconciled"] or {}).get("observed") != "not-done"]


def _forbidden(argv: List[str]) -> List[str]:
    return [f"the argument array carries {a}" for a in argv if a.split("=", 1)[0] in FORBIDDEN]


def argv_problems(c: Ctx, argv: List[str], verb: str, options: Tuple[str, ...]) -> List[str]:
    """The mutating call's shape is an exact allowlist: helm, the verb, the release, one positional, and exactly
    these options in this order (each but --wait with one value)."""
    out = _forbidden(argv)
    if argv[:3] != [c.helm, verb, c.release] or len(argv) != 4 + 2 * len(options) - 1:
        out.append(f"the argument array is not helm {verb} <release> <one argument> and the reviewed options")
    got = tuple(a for a in argv[4:] if a.startswith("-"))
    if got != options:
        out.append(f"the argument array's options are {list(got)}, not exactly {list(options)}")
    return out


# --- plan-upgrade -----------------------------------------------------------------------------------------------------

def _common(c: Ctx) -> Tuple[Dict, List[str], Dict, Dict, Dict]:
    """What both plans bind: configuration, helm, the charts, the renderings and their difference."""
    problems = config_problems(c)
    cfg = c.cfg
    for tool in (cfg["helm"]["binary"], cfg["metadata_reader"]["binary"]):
        require_tool(c, tool)                              # before ANY invocation: a wrong executable is never run
    version = helm_version(c)
    if version != cfg["helm"]["version"]:
        problems.append(f"helm is {version}, the record names {cfg['helm']['version']}")
    frm, to = c.charts(cfg["from"]["commit"]), c.charts(c.record.commit)
    dep_from, p1 = live_dependence(c, frm["dir"])
    dep_to, p2 = live_dependence(c, to["dir"])
    frm_r = render(c, frm["dir"], c.record.resolve(cfg["from"]["values"]))
    to_r = render(c, to["dir"], c.record.resolve(cfg["to"]["values"]))
    diff, dp = difference(c, frm_r, to_r)
    problems += p1 + p2
    bound = {"release": c.release, "namespace": c.control_ns, "cluster_uid": c.record.cluster_uid,
             "source_commit": c.record.commit,
             "helm": {"binary": cfg["helm"]["binary"], "sha256": cfg["helm"]["sha256"], "version": version},
             "charts": {"from": frm, "to": to},
             "values": {"from": {"path": cfg["from"]["values"], "sha256": cfg["from"]["values_sha256"]},
                        "to": {"path": cfg["to"]["values"], "sha256": cfg["to"]["values_sha256"]}},
             "renders": {which: {"sha256": _sha(r["sanitized"]), "objects": len(r["sanitized"]),
                                 "secrets": r["secrets"]} for which, r in (("from", frm_r), ("to", to_r))},
             "live_dependence": {"from": dep_from, "to": dep_to},
             "difference": diff}
    return bound, problems + dp, {"from": frm, "to": to}, frm_r, to_r


def plan_upgrade(c: Ctx) -> Dict:
    bound, problems, charts, frm_r, _to_r = _common(c)
    cfg, frm_rev = c.cfg, int(c.cfg["from"]["revision"])
    hist = history(c)
    problems += history_problems(hist)
    if hist[-1]["revision"] != frm_rev:
        problems.append(f"the newest revision is {hist[-1]['revision']}, the record's installed revision is {frm_rev}")
    st = storage(c, frm_rev)
    if not st["helm_object"]:
        problems.append(f"the storage Secret of revision {frm_rev} was not found as Helm's object for this release")
    fc = _forecaster(frm_r["raw"])
    live, lp = live_problems(c, fc)
    tls = secret_meta(c, f"{fc['metadata']['name']}-tls")
    if not tls["found"]:
        problems.append("the forecaster's TLS Secret was not found")
    op = operation_id(c, frm_rev, charts["from"], charts["to"])
    again = _submitted(c, op)
    if again:
        problems.append(f"this operation ({op}) was already submitted (step {again}): it is never submitted twice")
    timeout = cfg["helm"]["timeout"]
    argv = [c.helm, "upgrade", c.release, charts["to"]["dir"], *c.conn(), "-f", c.record.resolve(cfg["to"]["values"]),
            "--wait", "--timeout", timeout, "--labels", f"{OP_LABEL}={op}"]
    problems += lp + safety_problems(c) + argv_problems(c, argv, "upgrade", UPGRADE_OPTIONS)
    action = {"op": UPGRADE_OP, "argv": argv, "deadline_s": _timeout_s(timeout) + MARGIN_S,
              "from_revision": frm_rev, "expected_revision": frm_rev + 1, "rollback_to": frm_rev}
    return X.new_plan(HELPER, c.record, [action], kind="upgrade", operation=op, history=hist,
                      storage={"revision": frm_rev, "name": st["name"], "uid": st["uid"]}, forecaster=live,
                      tls={k: tls[k] for k in ("name", "uid", "resourceVersion")}, pods=release_pods(c),
                      notes=["applied means Helm recorded this revision as deployed; the application read-back is a "
                             "separate reviewed step",
                             "both renderings are made offline with the record's kube_version and api_versions; "
                             "live_dependence lists every template that reads the live cluster, and the only "
                             "reviewed one is the forecaster's TLS Secret (kept with lookup)",
                             "offline the TLS Secret is generated anew in each rendering: only its shape is "
                             "compared; its live UID and resourceVersion above are what the read-back compares"],
                      problems=sorted(set(problems)), **bound)


# --- the journalled Helm call -----------------------------------------------------------------------------------------

def _first_seen(c: Ctx, revision: int, uid: str) -> Optional[str]:
    """The storage UID recorded at the first observation of a revision; None when this is the first."""
    e = X.last(X.receipts(c.record, HELPER), "helm-storage", revision=revision)
    if e is None:
        X.receipt(c.record, HELPER, "helm-storage", {"revision": revision, "uid": uid})
        return None
    return e["data"]["uid"]


def observe_upgrade(c: Ctx, from_revision: int, from_uid: str, op: str) -> Dict:
    """The outcome of a released upgrade, read from the release's history and the storage Secrets' metadata."""
    try:
        hist = history(c)
        newest, want = hist[-1], from_revision + 1
        base = {"history": hist[-3:]}
        if newest["revision"] == from_revision:
            return {**base, "classification": "unresolved",
                    "why": "Helm executed and there is no revision evidence (an accepted request may still apply)"}
        if newest["revision"] != want or _gap(hist):
            return {**base, "classification": "foreign", "why": "the history is not the installed revision plus one"}
        old, st = storage(c, from_revision), storage(c, want)
        if not old["helm_object"]:                         # missing evidence is not continuity
            return {**base, "classification": "unknown",
                    "why": "the installed revision's storage Secret is missing or is not Helm's object"}
        if old["uid"] != from_uid:
            return {**base, "classification": "foreign", "why": "the installed revision's storage Secret was replaced"}
        if not st["helm_object"] or st["operation"] != op:
            return {**base, "classification": "foreign",
                    "why": "the new revision's storage Secret does not carry this operation's identifier"}
        first = _first_seen(c, want, st["uid"])
        if first not in (None, st["uid"]):
            return {**base, "classification": "foreign", "why": "the new revision's storage Secret was replaced"}
        post = {"revision": want, "uid": st["uid"], "storage": st["name"]}
        if newest["status"] == "deployed":
            return {**base, "classification": "applied", "post": post}
        if newest["status"] == "failed":
            return {**base, "classification": "failed", "post": post,
                    "why": "our revision failed; objects of it may be applied"}
        if newest["status"] == "pending-upgrade":
            return {**base, "classification": "unresolved", "post": post, "why": "our revision is pending-upgrade"}
        return {**base, "classification": "unknown", "post": post, "why": f"our revision is {newest['status']}"}
    except ReadError as e:
        return {"classification": "unknown", "why": str(e)}


def observe_rollback(c: Ctx, before: int, destination: int, clean: bool) -> Dict:
    try:
        hist = history(c)
        newest, want = hist[-1], before + 1
        base = {"history": hist[-3:]}
        if newest["revision"] == before:
            return {**base, "classification": "unresolved", "why": "Helm executed and there is no new revision"}
        if newest["revision"] != want or _gap(hist):
            return {**base, "classification": "foreign", "why": "the history is not the previous revision plus one"}
        if newest["description_class"] != f"Rollback to {destination}":
            return {**base, "classification": "foreign", "why": "the new revision is not a rollback to the destination"}
        st = storage(c, want)
        if not st["helm_object"]:
            return {**base, "classification": "unknown", "why": "the new revision's storage Secret was not found"}
        first = _first_seen(c, want, st["uid"])
        if first not in (None, st["uid"]):
            return {**base, "classification": "foreign", "why": "the new revision's storage Secret was replaced"}
        post = {"revision": want, "uid": st["uid"], "storage": st["name"]}
        if newest["status"] == "failed":
            return {**base, "classification": "failed", "post": post, "why": "the rollback revision failed"}
        if newest["status"] != "deployed":
            return {**base, "classification": "unresolved", "post": post, "why": f"it is {newest['status']}"}
        if not clean:
            return {**base, "classification": "unresolved", "post": post,
                    "why": "a rollback revision exists, but our Helm process did not end cleanly: its provenance is "
                           "ambiguous (Helm does not mark a rollback as ours)"}
        return {**base, "classification": "applied", "post": post}
    except ReadError as e:
        return {"classification": "unknown", "why": str(e)}


class _Retained:
    """Stands in for the lock when the stale lock is retained by the recovery guard."""
    safe = False


def _submit(c: Ctx, lock, plan: Dict, plan_digest: str, pre: Dict, judge: Callable) -> Dict:
    """unsafe -> intent-<n> -> the command's identity durable -> released -> the outcome read. Only `applied` writes
    step-<n> applied; a command that was never released writes step-<n> not-applied; everything else leaves the
    intent open and the lock held."""
    action = plan["actions"][0]
    n = str(max([int(k) for k in X._steps(X.receipts(c.record, HELPER))] or [0]) + 1)
    intent = {"op": action["op"], "kind": "HelmRelease", "namespace": c.control_ns, "name": c.release, "uid": "",
              "pre": pre, "expect": [], "body_sha256": _sha(action["argv"]),
              "mark": f"{c.record.run_id}/{HELPER}/{n}",
              "context": {"operation": action["op"], "release_op": plan["operation"], "plan_sha256": plan_digest}}
    require_tool(c, action["argv"][0])                    # before anything is journalled
    lock.safe = False
    X.receipt(c.record, HELPER, f"intent-{n}", intent)

    def record_identity(ident: Dict) -> None:
        X.receipt(c.record, HELPER, "helm-launch", {"step": n, "identity": ident})
    try:
        result = c.run(action["argv"], deadline_s=action["deadline_s"], record=record_identity)
        never = None if result.started else (result.runner_error or "the gate was not opened")
    except ToolError:
        result, never = None, "the executable changed after the plan"
    if never is not None:                                  # procgroup: started is False only when nothing executed
        X.receipt(c.record, HELPER, f"step-{n}", {**intent, "result": "not-applied",
                                                   "post": {"why": "the command was never released", "runner": never}})
        lock.safe = True
        raise X.StagingError(f"the Helm command was never released ({never}): nothing was executed")
    summary = result.as_dict()
    outcome = judge(result) if result.group_empty is True else {
        "classification": "unknown", "why": "the command's process group could not be proven empty"}
    if outcome["classification"] == "applied":
        r = X.receipt(c.record, HELPER, f"step-{n}", {**intent, "result": "applied", "post": outcome["post"],
                                                       "helm": summary, "history": outcome.get("history")})
        lock.safe = True
        return r
    X.receipt(c.record, HELPER, "helm-outcome", {"step": n, "op": action["op"], "helm": summary, **outcome})
    raise X.StagingError(f"the Helm {action['op'][5:]} is {outcome['classification']} ({outcome.get('why', '')}): "
                         f"the lock stays; nothing is retried (status, then plan-rollback or a reviewed disposition)")


def apply_upgrade(c: Ctx, reviewed: Dict, reviewed_digest: str) -> Dict:
    with X.mutating(c.record, c.kube, HELPER, {"operation": UPGRADE_OP}) as lock:
        live = plan_upgrade(c)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
        if live.get("kind") != "upgrade" or live["problems"]:
            raise X.StagingError(f"the upgrade plan has problems: {live['problems']}")
        a = live["actions"][0]
        return _submit(c, lock, live, reviewed_digest, {"revision": a["from_revision"],
                                                        "storage_uid": live["storage"]["uid"]},
                       lambda _res: observe_upgrade(c, a["from_revision"], live["storage"]["uid"], live["operation"]))


# --- rollback ---------------------------------------------------------------------------------------------------------

def _open(c: Ctx) -> List[Dict]:
    return X.open_intents(X.receipts(c.record, HELPER))


def _lock_sha(c: Ctx) -> str:
    p = X.lock_file(c.record)
    return C.sha256_file(p) if os.path.exists(p) else ""


def _launch(c: Ctx, step: str) -> Optional[Dict]:
    e = X.last(X.receipts(c.record, HELPER), "helm-launch", step=step)
    return e["data"]["identity"] if e else None


def plan_rollback(c: Ctx) -> Dict:
    for tool in (c.cfg["helm"]["binary"], c.cfg["metadata_reader"]["binary"]):
        require_tool(c, tool)
    fin = _finalize_plan(c)
    if fin is not None:
        return fin
    bound, problems, charts, _frm_r, to_r = _common(c)
    to_docs = to_r["raw"]
    cfg, dest = c.cfg, int(c.cfg["from"]["revision"])
    hist = history(c)
    cur = hist[-1]
    op = operation_id(c, dest, charts["from"], charts["to"])
    if cur["revision"] != dest + 1 or _gap(hist):
        problems.append(f"the newest revision is {cur['revision']}, not the upgrade's revision {dest + 1}")
    st = storage(c, dest + 1)
    if not st["helm_object"] or st["operation"] != op:
        problems.append("the newest revision's storage Secret does not carry this record's operation identifier")
    open_, admit, form, lock_sha = _open(c), None, "applied", ""
    if open_:
        form, lock_sha = "unresolved", _lock_sha(c)
        mine = [it for it in open_ if it["data"]["op"] == UPGRADE_OP and it["data"]["context"].get("release_op") == op]
        if len(open_) != 1 or not mine:
            problems.append(f"the release journal's open intents {[it['step'] for it in open_]} are not exactly this "
                            "operation's upgrade")
        else:
            admit = {HELPER: [mine[0]["step"]]}
            ident = _launch(c, mine[0]["step"])
            state = procgroup.group_state(ident) if ident else "empty"       # no launch receipt: never released
            if state != "empty":
                problems.append(f"the upgrade's Helm process group is not proven gone: it is {state}")
        if cur["status"] not in ("failed", "pending-upgrade"):
            problems.append(f"the unresolved upgrade's revision is {cur['status']}: only failed or pending-upgrade is "
                            "rolled back under the recovery guard")
        if not lock_sha:
            problems.append("an intent is open but no lock is held")
        live, _lp = live_problems(c, _forecaster(to_docs))      # partially applied: recorded, not required to match
    else:
        if cur["status"] != "deployed":
            problems.append(f"the newest revision is {cur['status']}, not deployed")
        if not _submitted(c, op):
            problems.append("this record's journal holds no upgrade of this operation")
        live, lp = live_problems(c, _forecaster(to_docs))
        problems += lp
    timeout = cfg["helm"]["timeout"]
    argv = [c.helm, "rollback", c.release, str(dest), *c.conn(), "--wait", "--timeout", timeout]
    problems += safety_problems(c, admit) + argv_problems(c, argv, "rollback", ROLLBACK_OPTIONS)
    action = {"op": ROLLBACK_OP, "argv": argv, "deadline_s": _timeout_s(timeout) + MARGIN_S,
              "from_revision": cur["revision"], "expected_revision": cur["revision"] + 1, "destination": dest}
    return X.new_plan(HELPER, c.record, [action], kind="rollback", form=form, operation=op, history=hist,
                      storage={"revision": cur["revision"], "name": st["name"], "uid": st["uid"]}, forecaster=live,
                      lock_sha256=lock_sha, pods=release_pods(c),
                      notes=["a rollback is a NEW revision that restores the unfixed forecaster",
                             "the Active / write-grant gate stays closed after a rollback",
                             "a rollback is ours only when our own Helm process ended cleanly and the expected "
                             "revision appeared as a rollback to the destination; an interrupted one stays unresolved"],
                      problems=sorted(set(problems)), **bound)


def apply_rollback(c: Ctx, reviewed: Dict, reviewed_digest: str) -> Dict:
    def go(lock) -> Tuple[Dict, Dict]:
        live = plan_rollback(c)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
        if live.get("kind") != "rollback" or live["problems"]:
            raise X.StagingError(f"the rollback plan has problems: {live['problems']}")
        a = live["actions"][0]
        return live, _submit(c, lock, live, reviewed_digest,
                             {"revision": a["from_revision"], "destination": a["destination"]},
                             lambda res: observe_rollback(c, a["from_revision"], a["destination"], clean(res)))
    if reviewed.get("form") == "applied":
        with X.mutating(c.record, c.kube, HELPER, {"operation": ROLLBACK_OP}) as lock:
            return go(lock)[1]
    # The upgrade is unresolved: its executor's stale lock stays in place for the whole rollback (no helper can start),
    # the recovery guard proves that executor gone, and the plan admitted only that upgrade's own open intent.
    with _guard(c, reviewed) as guard:
        if reviewed.get("form") == "finalize":             # the rollback is already applied and receipted: no Helm
            live = plan_rollback(c)
            X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
            if live.get("form") != "finalize" or live["problems"]:
                raise X.StagingError(f"the finalize plan has problems: {live['problems']}")
            a = live["actions"][0]
            return _settle(c, guard, [a["upgrade_step"]], "rolled-back", f"step-{a['rollback_step']}",
                           live["operation"], reviewed_digest, reviewed)
        upgrade = [it["step"] for it in _open(c)]
        live, done = go(_Retained())
        _settle(c, guard, upgrade, "rolled-back", done["event"], live["operation"], reviewed_digest, reviewed)
        return done


@contextlib.contextmanager
def _guard(c: Ctx, reviewed: Dict):
    """The recovery guard on the lock the reviewed plan bound (its executor proven gone, one recoverer), with the
    cluster and the image fence checked inside."""
    with C.recovery_guard(c.record.doc, X.LOCK_SCOPE, reviewed.get("lock_sha256", "")) as guard:
        C.require_cluster(c.kube.api, c.record.cluster_uid)
        fence = X.image_fence(c.record)
        if fence:
            raise X.StagingError(f"image transition {fence} is open: refused; the lock stays")
        yield guard


def _settle(c: Ctx, guard, steps: List[str], observed: str, by: str, op: str, digest: str, reviewed: Dict,
            **extra) -> Dict:
    """Under the entered guard: reconciled-<n> for each named open intent, then the stale lock is released if
    nothing else holds it. Safe to repeat after an interruption (an intent already reconciled is skipped)."""
    last = None
    still = {it["step"] for it in _open(c)}
    for n in steps:
        if n in still:
            last = X.receipt(c.record, HELPER, f"reconciled-{n}", {
                "observed": observed, "by": by, "plan_sha256": digest, "context": {"release_op": op}, **extra})
    import transition_records
    held = transition_records.release_problem(c.record, guard)
    if held:
        raise X.StagingError(f"settled, but the Active transition still needs handling ({held}): the lock stays")
    guard.release_stale()
    X.receipt(c.record, "recover", "lock-recovered", {"plan_sha256": digest,
                                                      "lock_sha256": reviewed.get("lock_sha256", ""),
                                                      "by": f"release {reviewed.get('kind')}"})
    return last or {"event": "nothing-to-reconcile", "data": {}}


def _applied_rollback(c: Ctx, op: str, after: str) -> Optional[Tuple[str, Dict]]:
    """(step, its data) of a rollback of this operation that Helm applied and that is durably receipted, journalled
    after the given step."""
    for n, st in sorted(X._steps(X.receipts(c.record, HELPER)).items(), key=lambda kv: -int(kv[0])):
        it = st["intent"]
        if it and it["op"] == ROLLBACK_OP and it["context"].get("release_op") == op and int(n) > int(after) \
                and (st["outcome"] or {}).get("result") == "applied":
            return n, st["outcome"]
    return None


def _finalize_plan(c: Ctx) -> Optional[Dict]:
    """The state an interruption leaves between a rollback's durable `applied` receipt and the reconciliation of the
    upgrade it rolled back: the upgrade's intent is the one open intent and the lock is still held. Finishing it
    needs no Helm: the plan binds the evidence that the applied rollback is still the release's state."""
    open_ = _open(c)
    if len(open_) != 1 or open_[0]["data"]["op"] != UPGRADE_OP:
        return None
    n, op = open_[0]["step"], open_[0]["data"]["context"].get("release_op")
    done = _applied_rollback(c, op, n)
    if done is None:
        return None
    m, step = done
    problems = []
    hist = history(c)
    cur, post = hist[-1], step["post"]
    if cur["revision"] != post["revision"] or cur["status"] != "deployed" or _gap(hist) or \
            cur["description_class"] != f"Rollback to {step['pre']['destination']}":
        problems.append(f"the newest revision is not the applied rollback's revision {post['revision']}, deployed")
    st = storage(c, post["revision"])
    if not st["helm_object"] or st["uid"] != post["uid"]:
        problems.append("the rollback revision's storage Secret is not the one its receipt recorded")
    lock_sha = _lock_sha(c)
    if not lock_sha:
        problems.append("an intent is open but no lock is held")
    left = C.unresolved_intents(c.record.doc)
    if left != {HELPER: [n]}:
        problems.append(f"intents with unknown outcomes {left}: only the rolled-back upgrade's may be open")
    action = {"op": "finalize-rollback", "upgrade_step": n, "rollback_step": m, "revision": post["revision"]}
    return X.new_plan(HELPER, c.record, [action], kind="rollback", form="finalize", operation=op, history=hist,
                      storage={"revision": post["revision"], "name": st["name"], "uid": st["uid"]},
                      lock_sha256=lock_sha, release=c.release, namespace=c.control_ns,
                      notes=["the rollback was applied and receipted; this only records that for the upgrade's "
                             "intent and releases the dead executor's lock: Helm is not run"],
                      problems=sorted(set(problems)))


# --- the reviewed disposition of what the helper cannot decide ----------------------------------------------------------

def plan_dispose(c: Ctx) -> Dict:
    """For open Helm intents that neither recovery nor a rollback may decide, when the revision each of them was to
    write EXISTS (an interrupted rollback whose revision is there): the evidence that the release is in ONE of the two
    reviewed states, for a person to review. Required, all of it:
      - only this helper's intents are open, and the Helm process group of each is proven gone;
      - every intent whose command was released has its revision in the history. An intent WITHOUT revision evidence
        is not disposable: a request the API server accepted can still commit after the client is gone, and neither
        two equal observations nor a waiting time shows that it will not. That case stays unresolved in version 1;
      - the newest revision is the one `deployed` revision, without a gap;
      - installed: the live forecaster equals the installed rendering in every field, and the newest revision is
        the record's installed revision (its bound storage UID) or a "Rollback to <it>";
        upgraded:  the live forecaster equals the new rendering in every field, and the newest revision is installed+1
        with this operation's identifier on its storage Secret;
      - the plan is recomputed at `dispose` and must be identical: the state did not move during the review.
    Anything else is refused: bring the release into one of the two states by hand, then plan again."""
    bound, problems, charts, frm_r, to_r = _common(c)
    frm_rev = int(c.cfg["from"]["revision"])
    op = operation_id(c, frm_rev, charts["from"], charts["to"])
    open_, lock_sha = _open(c), _lock_sha(c)
    if not open_:
        problems.append("no Helm intent of this helper is open: there is nothing to dispose of")
    left = C.unresolved_intents(c.record.doc)
    if left != ({HELPER: [it["step"] for it in open_]} if open_ else {}):
        problems.append(f"intents with unknown outcomes {left}: only this helper's Helm intents may be open")
    groups = {}
    hist = history(c)
    revisions = {h["revision"] for h in hist}
    for it in open_:
        ident = _launch(c, it["step"])
        groups[it["step"]] = procgroup.group_state(ident) if ident else "never-released"
        if groups[it["step"]] not in ("empty", "never-released"):
            problems.append(f"step {it['step']}: its Helm process group is not proven gone ({groups[it['step']]})")
        made = int(it["data"]["pre"].get("revision", -1)) + 1
        if ident and made not in revisions:                # released, and the revision it was to write is not there
            problems.append(f"step {it['step']}: Helm executed and revision {made} does not exist. A request the API "
                            "server accepted can still commit, and nothing read here can show that it will not: this "
                            "case is not disposable and stays unresolved")
        if it["data"]["context"].get("release_op") != op:
            problems.append(f"step {it['step']} belongs to another operation than this record's")
    if open_ and not lock_sha:
        problems.append("an intent is open but no lock is held")
    cur = hist[-1]
    problems += history_problems(hist)
    st = storage(c, cur["revision"])
    if not st["helm_object"]:
        problems.append("the newest revision's storage Secret was not found as Helm's object")
    bound_uid = next((it["data"]["pre"].get("storage_uid") for it in open_ if it["data"]["op"] == UPGRADE_OP), None)
    live_from, d_from = live_problems(c, _forecaster(frm_r["raw"]))
    _live_to, d_to = live_problems(c, _forecaster(to_r["raw"]))
    installed_rev = (cur["revision"] == frm_rev and (bound_uid is None or st["uid"] == bound_uid)) or \
        cur["description_class"] == f"Rollback to {frm_rev}"
    upgraded_rev = cur["revision"] == frm_rev + 1 and st["operation"] == op
    state = "installed" if not d_from and installed_rev else "upgraded" if not d_to and upgraded_rev else None
    if state is None:
        problems.append(f"the release is in neither reviewed state (the live forecaster differs from the installed "
                        f"rendering in {len(d_from)} fields and from the new one in {len(d_to)}; newest revision "
                        f"{cur['revision']}, {cur['description_class']}): bring it into one by hand, then plan again")
    perms = c.permission_check()
    for role in ROLES:
        p = perms.get(role) or {}
        if p.get("verdict") != "pass" or p.get("write_grants"):
            problems.append(f"the {role}'s replica-write permission is not proven absent (verdict {p.get('verdict')})")
    actions = [{"op": "dispose", "step": it["step"], "intent_op": it["data"]["op"], "state": state} for it in open_]
    return X.new_plan(HELPER, c.record, actions, kind="dispose", operation=op, state=state, history=hist,
                      storage={"revision": cur["revision"], "name": st["name"], "uid": st["uid"]},
                      forecaster=live_from, groups=groups, lock_sha256=lock_sha, pods=release_pods(c),
                      evidence={"differs_from_installed": len(d_from), "differs_from_new": len(d_to)},
                      notes=["a disposition is a reviewed statement that the release is in the named state; it runs "
                             "no Helm and changes nothing in the cluster",
                             "after it, a further upgrade needs a record whose installed revision is the newest one"],
                      problems=sorted(set(problems)), **bound)


def dispose(c: Ctx, reviewed: Dict, reviewed_digest: str) -> Dict:
    with _guard(c, reviewed) as guard:
        live = plan_dispose(c)
        X.require_live(reviewed, reviewed_digest, HELPER, c.record, live)
        if live.get("kind") != "dispose" or live["problems"] or not live["actions"]:
            raise X.StagingError(f"the disposition plan has problems: {live['problems']}")
        X.receipt(c.record, HELPER, "disposition", {"plan_sha256": reviewed_digest, "state": live["state"],
                                                    "steps": [a["step"] for a in live["actions"]],
                                                    "revision": live["storage"]["revision"]})
        return _settle(c, guard, [a["step"] for a in live["actions"]], "disposed", "disposition", live["operation"],
                       reviewed_digest, reviewed, state=live["state"])


# --- recovery ---------------------------------------------------------------------------------------------------------

def _owner_gone(record: X.Record) -> Tuple[bool, str]:
    p = X.lock_file(record)
    if not os.path.exists(p):
        return True, "no lock is held"
    with open(p) as fh:
        return C.owner_terminated(json.load(fh))


def _reconcile(kube: X.Kube, d: Dict, record: X.Record) -> Dict:
    """An open Helm intent, for the generic recovery: decided only when it is PROVEN. First the lock's executor must
    be gone (then no launch receipt can still appear); an intent without a launch receipt never executed; a released
    upgrade is done only when its own revision is deployed; everything else, and every released rollback, stays
    unresolved."""
    gone, why = _owner_gone(record)
    if not gone:
        return {"observed": "unresolved", "why": f"the executor is not proven gone: {why}"}
    step = d["mark"].rsplit("/", 1)[1]
    ident = X.last(X.receipts(record, HELPER), "helm-launch", step=step)
    if ident is None:
        return {"observed": "not-done", "state": "not-applied", "why": "the command was never released"}
    state = procgroup.group_state(ident["data"]["identity"])
    if state != "empty":                                   # "alive", or "unknown" (another host or boot, no /proc)
        return {"observed": "unresolved", "why": f"the Helm process group is not proven gone: it is {state}"}
    if d["op"] != UPGRADE_OP:
        return {"observed": "unresolved", "why": "an interrupted rollback has ambiguous provenance: reviewed "
                                                 "disposition by hand"}
    c = Ctx(record, kube)
    o = observe_upgrade(c, int(d["pre"]["revision"]), d["pre"]["storage_uid"], d["context"]["release_op"])
    if o["classification"] == "applied":
        return {"observed": "done", "uid": o["post"]["uid"], "state": f"revision {o['post']['revision']} deployed"}
    return {"observed": "unresolved", "why": f"the upgrade is {o['classification']}: {o.get('why', '')} "
                                             "(plan-rollback, or a reviewed disposition)"}


X.RECONCILERS[UPGRADE_OP] = _reconcile
X.RECONCILERS[ROLLBACK_OP] = _reconcile


def status(c: Ctx) -> Dict:
    """A report, not a gate."""
    out = {"release": c.release, "namespace": c.control_ns, "lock_held": os.path.exists(X.lock_file(c.record)),
           "open_intents": [{"step": it["step"], "op": it["data"]["op"]} for it in _open(c)]}
    last = X.last(X.receipts(c.record, HELPER), "helm-outcome")
    if last:
        out["last_outcome"] = {k: last["data"].get(k) for k in ("step", "op", "classification", "why")}
    try:
        out["history"] = history(c)
    except ReadError as e:
        out["history_error"] = str(e)
    return out


def main(argv: Optional[List[str]] = None, api=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--record", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("status", "plan-upgrade", "plan-rollback", "plan-dispose", "recover-plan"):
        sub.add_parser(name)
    for name in ("apply-upgrade", "apply-rollback", "dispose", "recover"):
        p = sub.add_parser(name)
        p.add_argument("--plan", required=True)
        p.add_argument("--plan-digest", required=True)
    a = ap.parse_args(argv)
    record = X.load_record(a.record, () if a.cmd.startswith("recover") else ("prepare",))
    kube = record.kube(api)
    if a.cmd == "recover-plan":
        X.emit(X.cli_plan(record, "recover", X.recover_plan(record, kube), "recover-plan"))
        return 0
    if a.cmd == "recover":
        X.emit(X.recover(record, kube, X.load_plan(a.plan), a.plan_digest))
        return 0
    c = Ctx(record, kube)
    if a.cmd == "status":
        X.emit(status(c))
    elif a.cmd == "plan-upgrade":
        X.emit(X.cli_plan(record, HELPER, plan_upgrade(c), "plan-upgrade"))
    elif a.cmd == "apply-upgrade":
        X.emit(apply_upgrade(c, X.load_plan(a.plan), a.plan_digest))
    elif a.cmd == "plan-rollback":
        X.emit(X.cli_plan(record, HELPER, plan_rollback(c), "plan-rollback"))
    elif a.cmd == "plan-dispose":
        X.emit(X.cli_plan(record, HELPER, plan_dispose(c), "plan-dispose"))
    elif a.cmd == "dispose":
        X.emit(dispose(c, X.load_plan(a.plan), a.plan_digest))
    else:
        X.emit(apply_rollback(c, X.load_plan(a.plan), a.plan_digest))
    return 0


if __name__ == "__main__":
    sys.exit(main())
