#!/usr/bin/env python3
"""Freeze capture before T0 (protocol P6 step 5, P9 "Records"; Codex r50/r51 effective configuration, r53/r54 review).

Read-only against the cluster: kubectl get/top/logs/version and `kubectl diff --server-side` (a server-side dry run).
No Secret is ever read. The destination <out> is reserved first (an empty directory created exclusively); everything is
collected into a private staging directory <out>.partial-<pid>; every write is redacted, then checked by an independent
audit of the exact bytes (a credential-like literal that survives aborts the write); at the end SHA256SUMS and
COMPLETE.json are written and the staging directory is renamed onto the empty reservation (rename never replaces a
non-empty directory). Rendered manifests are not stored: they are reproducible from the recorded commit, the deploy.env
values and the kustomize version; their sha256 and the redacted server-side diffs are stored.
Gates (all PASS = a usable freeze; the capture is written either way):
  tooling             kubectl client within one minor version of the server
  clean_checkout      no uncommitted change, HEAD in origin/main; HEAD and the frozen files unchanged at the end
  live_equals_render  the server-side diffs of both rendered parts are empty, at the start and again at the end
  stable_during       Deployment/CronJob/ConfigMap/PA/ScaledObject/HPA/Service/PVC configuration (replica counts
                      excluded) unchanged between the start and the end of the collection
  mask                ConfigMap ml-engine/validity-mask equals validity-mask.json byte for byte
  p8_code             the ConfigMap named for the current P8 code (p8-job.sh naming) is immutable and holds exactly
                      that code; absent is allowed (p8-job.sh creates it)
  runtime             every Deployment's rollout observed and complete; every pod belongs to the current ReplicaSet;
                      every template container present in the pod with the same image, command, args, env, envFrom,
                      workingDir and mounts (only the API server's kube-api-access mount and the istio sidecar may be
                      added), the template's volumes unchanged; complete container status with the template digest;
                      ml-api and the operator fully Ready, ConfigMap-sourced env unchanged since their containers
                      started; their templates carry the deploy.env digests and GIT_COMMIT
  declared_processes  the two newest scheduled slots by capture time are four-leg verified (D-1097: two --training-check
                      results; never substituted by older slots — a newest training still running fails as pending);
                      the newest slot's latest finished ml-training Job Complete with the deploy.env digest, the current
                      CronJob container configuration and GIT_COMMIT; the archive pair and ml-api's latest load name
                      that slot and artifact; the latest evidence-archive Job reported no errors; the latest
                      load-evidence Job completed
  envelope_config     effective ml-api ENSEMBLE_HISTORY_HOURS (env, envFrom and ConfigMap refs resolved with Kubernetes
                      precedence, from the template and from every running ml-api pod) equals
                      forecasts.frozen_envelope_config(); no TRAINING_HOURS in the trainer; no unresolved or opaque
                      source (Secret, field reference, missing ConfigMap, $(VAR)) in either
  arms                the six arms and six generators; exactly one PredictiveAutoscaler per operator arm, bounded 1..12;
                      the KEDA arm's ScaledObject 1..12 and active; the hybrid's KEDA fallback paused
  nodes               the frozen workers (infra-identities.json; six since 2026-10-09, U-32) present, Ready, without
                      Disk/Memory/PID pressure
  telemetry           kubectl top rows and physically valid (0 ≤ avail ≤ size, size > 0), unambiguous, fresh (both
                      series, no future timestamps) root-disk samples for every frozen worker; the required benchmark
                      series (VictoriaMetrics with deny_partial_response=1). Collection only: no disk threshold
  qualification       the immutable load-gate attempts of the given consecutive hours (fetch_load_attempts.sh): every final
                      row PASS and qualifies=True for all six arms, conflicting final rows fail closed
  generator_continuity  (D-1097) each arm's qualification rows name one generator pod, running now, its k6 container
                      never restarted and started before the first qualification hour (stricter than "no restart since
                      qualification": a qualified generator that restarts must be replaced and re-qualified)
  fresh_issuances     (D-1097, r66) issuance records (--issuances: an extract_decisions.sh v3 read just before the capture):
                      every forecasting arm issued within 20 min, the hybrid's and S1's newest with the newest verified
                      artifact and cutoff; no "Prediction unavailable" in the operator's last 20 min (stored)
  matches_previous    (with --compare-with) the configuration identity equals the earlier capture's
  no_secret_values    nothing credential-like found (found values are redacted before writing, never stored)
Other evidence kept outside the code repo (the protocol, training verification records) is bound by --evidence.

  source env.sh; set -a; . deploy.env; set +a
  KUBE_CONTEXT=prodcluster VM_URL=http://127.0.0.1:18481/select/0/prometheus freeze_capture.py --out <new dir> \\
    --evidence <protocol> --qualification-attempts <fetch output> --qualification-hours 2026-10-07T21:00:00Z,… \\
    [--compare-with <earlier capture>]
"""
import argparse, calendar, collections, copy, hashlib, importlib.util, json, math, os, re, shutil, subprocess, sys, tempfile, time
import urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
NAMESPACES = ("ml-engine", "demo")
ARMS = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
BOUNDS = (1, 12)
STRICT = ("ml-api", "predictive-operator")
INJECTED_CONTAINERS = ("istio-proxy",)               # added by istio injection; recorded, not compared
INJECTED_MOUNT = re.compile(r"^kube-api-access-[a-z0-9]{5}$")
CONTROLLED = ("image", "command", "args", "env", "envFrom", "workingDir")
SLOT = 6 * 3600                                      # ml-training schedule 0 */6 * * *
KINDS = ("deployments.apps", "replicasets.apps", "cronjobs.batch", "jobs.batch", "configmaps", "persistentvolumeclaims",
         "services", "horizontalpodautoscalers.autoscaling", "scaledobjects.keda.sh",
         "predictiveautoscalers.autoscaler.example.com")
STATIC_KINDS = ("deployments.apps", "cronjobs.batch", "configmaps", "persistentvolumeclaims", "services",
                "horizontalpodautoscalers.autoscaling", "scaledobjects.keda.sh",
                "predictiveautoscalers.autoscaler.example.com")
P8_FILES = ("infra_events.py", "infra-identities.json", "validity-mask.json", "p8_launcher.py")   # p8-job.sh FILES
FROZEN_FILES = tuple("deploy/prodcluster/" + f for f in P8_FILES) + (
    "deploy/prodcluster/p8-job.sh", "deploy/prodcluster/deploy.sh", "deploy/prodcluster/freeze_capture.py",
    "deploy/prodcluster/fetch_load_attempts.sh",
    "deploy/prodcluster/collect_load_evidence.py", "deploy/prodcluster/load-gate/approvals.json",
    "deploy/prodcluster/load-gate/terminations.json", "deploy/prodcluster/load-gate/settings.env",
    "deploy/prodcluster/scoring/capacity.py", "deploy/prodcluster/scoring/forecasts.py",
    "deploy/prodcluster/scoring/shortage_events.py", "deploy/prodcluster/scoring/extract_decisions.sh",
    "deploy/prodcluster/scoring/extract_ensemble.sh", "deploy/prodcluster/ml-engine/ml-api-experiments.yaml",
    "deploy/prodcluster/ml-engine/training-env.yaml",
    "deploy/eks-benchmark/workload/challenge-v1/challenge_profile.py",
    "deploy/eks-benchmark/workload/challenge-v1/profile.json",
    "deploy/eks-benchmark/workload/challenge-v1/k6-load-script-challenge-v1.yaml")
TREES = ("deploy/prodcluster", "deploy/eks-benchmark", "k8s-manifests", "ml-engine", "k8s-operator")
REQUIRED_SERIES = ("k6_bench_req_failed_total", "bench_final_snapshot_receipt")
DEPLOY_ENV = ("HARBOR_REGISTRY", "ML_API_DIGEST", "OPERATOR_DIGEST", "GIT_COMMIT_VALUE", "VM_QUERY_URL", "VM_WRITE_URL",
              "VM_IMPORT_URL")
TIMEOUT = 180            # seconds per subprocess
FRESH = 180              # seconds: the newest root-disk samples of every worker must be at most this old
SKEW = 60                # seconds: tolerated sample timestamps ahead of the local clock
LEGS = ("job", "api_reload", "archive_pair", "first_issuance")    # training-check.sh v3.5 legs
VERSION = "freeze_capture.py v6"
SUPPORTED_BASELINES = ("freeze_capture.py v6",)
MANDATORY_GATES = ("tooling", "clean_checkout", "live_equals_render", "stable_during", "mask", "p8_code", "runtime",
                   "declared_processes", "envelope_config", "qualification", "generator_continuity",
                   "fresh_issuances", "arms", "nodes", "telemetry", "no_secret_values")
FORECASTING_ARMS = ("nginx-test", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")

# ---- redaction (rewrites) and audit (independent check of the bytes written)
CRED = r"(?:passw|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential)"
CREDENTIAL = re.compile(CRED, re.I)
ASSIGN = re.compile(r"([\w.-]*" + CRED + r"[\w.-]*)(\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s\"',;}&]+)", re.I)
URL_CRED = re.compile(r"([a-z][a-z0-9+.-]*://[^/\s:@]+:)([^/\s@]+)(@)", re.I)
PEM = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(-----END [A-Z ]*PRIVATE KEY-----|$)", re.S)
REDACTED = "<redacted>"
OPAQUE = re.compile(r"^(<redacted>|<secret [^<>]*>|<field [^<>]*>|<resource [^<>]*>)$")
REFERENCE_KEYS = {"secretName", "secretRef", "secretKeyRef", "imagePullSecrets", "automountServiceAccountToken",
                  "serviceAccountToken", "expirationSeconds", "nodePublishSecretRef"}
REFERENCE_SUFFIXES = ("Name", "Ref", "Refs", "Path", "File")      # camelCase reference/path fields and flags


def sha_bytes(b):
    return hashlib.sha256(b).hexdigest()


def sha(path):
    return sha_bytes(open(path, "rb").read())


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def epoch(s):
    return calendar.timegm(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S"))


def _reference_key(key):
    k = key.strip().strip("\"'").lstrip("-").split(".")[-1]
    return k in REFERENCE_KEYS or k.endswith(REFERENCE_SUFFIXES)


def redact_string(s, found, where):
    """Redact credential-like literals inside one string (flags such as -x.password=…, quoted assignments, URL
    userinfo, PEM private keys). Only reference/path-named keys are exempt, never by the look of the value."""
    def assign(m):
        value = m.group(3).strip("\"'")
        if _reference_key(m.group(1)) or OPAQUE.match(value):
            return m.group(0)
        found.append(f"{where}: {m.group(1)}")
        q = m.group(3)[0] if m.group(3)[:1] in "\"'" else ""
        return m.group(1) + m.group(2) + q + REDACTED + q

    def url(m):
        if m.group(2) == REDACTED:
            return m.group(0)
        found.append(f"{where}: URL credentials")
        return m.group(1) + REDACTED + m.group(3)

    if PEM.search(s):
        found.append(f"{where}: private key")
        s = PEM.sub(REDACTED, s)
    return URL_CRED.sub(url, ASSIGN.sub(assign, s))


def redact(node, found, path=""):
    """Redact a JSON-like structure in place: credential-named env values and fields (whatever their value), and
    literals inside every string."""
    if isinstance(node, dict):
        if isinstance(node.get("name"), str) and isinstance(node.get("value"), str) and node["value"] \
                and CREDENTIAL.search(node["name"]) and not OPAQUE.match(node["value"]):
            found.append(f"{path}[name={node['name']}]")
            node["value"] = REDACTED
        for k in list(node):
            v = node[k]
            if k in REFERENCE_KEYS:
                continue
            if isinstance(v, str):
                if v and CREDENTIAL.search(k) and not _reference_key(k) and not OPAQUE.match(v):
                    found.append(f"{path}.{k}")
                    node[k] = REDACTED
                else:
                    node[k] = redact_string(v, found, f"{path}.{k}")
            else:
                redact(v, found, f"{path}.{k}")
    elif isinstance(node, list):
        for i, v in enumerate(node):
            if isinstance(v, str):
                node[i] = redact_string(v, found, f"{path}[{i}]")
            else:
                redact(v, found, f"{path}[{i}]")
    return found


def redact_text(text, found, where):
    """Line-wise redaction of YAML-like text (server-side diffs), including `name: X` / `value: Y` env pairs."""
    out, pending = [], False
    for n, line in enumerate(text.splitlines(keepends=True)):
        body = line[1:] if line[:1] in "+- " else line
        m = re.match(r"\s*(?:-\s+)?name:\s*['\"]?([^'\"\s]+)", body)
        if m:
            pending = bool(CREDENTIAL.search(m.group(1)))
        elif pending and re.match(r"\s*value:", body):
            if not re.match(r"\s*value:\s*(['\"]?<redacted>['\"]?|['\"]{2})?\s*$", body):
                found.append(f"{where}:{n + 1}: env value")
                line = re.sub(r"(value:\s*).*", r"\g<1>" + REDACTED, line)
            pending = False
        elif re.match(r"\s*-\s", body):
            pending = False
        out.append(redact_string(line, found, f"{where}:{n + 1}"))
    return "".join(out)


# The audit is deliberately written apart from the redaction: a credential word followed by an assignment and any
# value other than the redaction marker or an opaque reference fails, in any quoting, unless the key is a reference.
_AUDIT_ASSIGN = re.compile(r"([A-Za-z0-9_.\-]*" + CRED + r"[A-Za-z0-9_.\-]*)\\?[\"']?\s*[=:]\s*\\?[\"']?([^\"'\s,;}&\\]*)",
                           re.I)                       # also inside JSON-escaped strings (\"…\")
_AUDIT_URL = re.compile(r"://[^/\s:@\"']+:([^/\s@\"']+)@")


def audit_text(text):
    """Credential-like literals left in serialized output (must be none)."""
    left = []
    for m in _AUDIT_ASSIGN.finditer(text):
        if m.group(2).startswith(("{", "[")) or re.fullmatch(r"true|false|null|-?\d+(\.\d+)?", m.group(2)):
            continue                                    # a JSON structure (audited by audit_json) or a JSON literal
        if m.group(2) and not _reference_key(m.group(1)) and not OPAQUE.match(m.group(2)) \
                and not m.group(2).startswith(("<redacted>", "<secret ", "<field ", "<resource ")):
            left.append(m.group(1))
    left += ["URL credentials" for m in _AUDIT_URL.finditer(text) if m.group(1) != REDACTED]
    left += ["private key"] if "PRIVATE KEY-----" in text else []
    return left


def audit_json(obj, path=""):
    """Structural audit of parsed output: credential-named keys and env pairs with literal values, and strings."""
    left = []
    if isinstance(obj, dict):
        if isinstance(obj.get("name"), str) and CREDENTIAL.search(obj["name"]) and isinstance(obj.get("value"), str) \
                and obj["value"] and not OPAQUE.match(obj["value"]):
            left.append(f"{path}[name={obj['name']}]")
        for k, v in obj.items():
            if isinstance(v, str) and v and CREDENTIAL.search(k) and k not in REFERENCE_KEYS \
                    and not _reference_key(k) and not OPAQUE.match(v):
                left.append(f"{path}.{k}")
            elif k not in REFERENCE_KEYS:
                left += audit_json(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            left += audit_json(v, f"{path}[{i}]")
    elif isinstance(obj, str):
        left += [f"{path}: {x}" for x in audit_text(obj)]
    return left


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Staging:
    """Reservation of <out> plus a private staging directory; every write is redacted and audited first."""

    def __init__(self, out):
        self.out, self.dir, self.found = out, f"{out}.partial-{os.getpid()}", []
        os.mkdir(out, 0o700)                                # exclusive: fails if <out> exists
        os.makedirs(self.dir, mode=0o700)

    def _write(self, rel, data):
        p = os.path.join(self.dir, rel)
        os.makedirs(os.path.dirname(p), mode=0o700, exist_ok=True)
        with open(os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())

    def json(self, rel, obj):
        obj = copy.deepcopy(obj)
        redact(obj, self.found, rel)
        data = json.dumps(obj, indent=1, sort_keys=True) + "\n"
        left = audit_json(json.loads(data)) + audit_text(data)
        if left:
            raise SystemExit(f"refusing to write {rel}: credential-like literal survived redaction ({len(left)})")
        self._write(rel, data)

    def text(self, rel, text):
        data = redact_text(text, self.found, rel)
        if audit_text(data):                                # keep the size, withhold the content
            self.found.append(f"{rel}: content withheld")
            data = f"(withheld: {len(text.splitlines())} lines with credential-like content after redaction)\n"
        self._write(rel, data)

    def abort(self):
        """Release the reservation if nothing was published (the partial directory is kept for inspection)."""
        try:
            os.rmdir(self.out)
        except OSError:
            pass

    def publish(self, receipt):
        sums = []
        for root, _, files in os.walk(self.dir):
            for f in files:
                p = os.path.join(root, f)
                sums.append(f"{sha(p)}  {os.path.relpath(p, self.dir)}\n")
        self._write("SHA256SUMS", "".join(sorted(sums, key=lambda l: l.split("  ", 1)[1])))
        receipt["sha256sums_sha256"] = sha(os.path.join(self.dir, "SHA256SUMS"))
        self._write("COMPLETE.json", json.dumps(receipt, indent=1, sort_keys=True) + "\n")
        fsync_dir(self.dir)
        os.rename(self.dir, self.out)                       # onto the empty reservation; a non-empty one refuses
        fsync_dir(os.path.dirname(os.path.abspath(self.out)))


# ---------------------------------------------------------------------------------------------- cluster access
def run(cmd, ok=(0,), timeout=TIMEOUT, env=None):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    except subprocess.TimeoutExpired:
        raise SystemExit(f"{' '.join(cmd[:5])}… timed out after {timeout} s")
    if p.returncode not in ok:
        raise SystemExit(f"{' '.join(cmd[:5])}… failed ({p.returncode}): {p.stderr.strip()[:400]}")
    return p


class Kube:
    def __init__(self, binary, ctx):
        self.bin, self.ctx = binary, ctx

    def __call__(self, *args, ok=(0,)):
        return run([self.bin, "--request-timeout=60s", "--context", self.ctx, *args], ok=ok)

    def get(self, *args):
        return json.loads(self("get", *args, "-o", "json").stdout)


def vm(url, path, **params):
    params["deny_partial_response"] = "1"
    with urllib.request.urlopen(f"{url}{path}?{urllib.parse.urlencode(params, doseq=True)}", timeout=30) as r:
        d = json.load(r)
    if d.get("status") != "success" or d.get("isPartial"):
        raise SystemExit(f"VictoriaMetrics {path}: status {d.get('status')}, partial {d.get('isPartial')}")
    return d["data"]


def strip(obj):
    """Drop server bookkeeping that is not configuration."""
    md = obj.get("metadata", {})
    for k in ("managedFields", "resourceVersion"):
        md.pop(k, None)
    (md.get("annotations") or {}).pop("kubectl.kubernetes.io/last-applied-configuration", None)
    return obj


def cm_provenance(o):
    """When a ConfigMap's current data was last written: its latest managedFields write, whether or not it is immutable
    now (a mutable ConfigMap may be made immutable later, so immutability does not date the data); unknown without
    write history (managedFields can be reset — absence proves nothing)."""
    times = [m.get("time") for m in o["metadata"].get("managedFields") or [] if m.get("time")]
    if times:
        return {"updated": max(times), "provenance": "latest managedFields write"}
    return {"updated": None, "provenance": "unknown: no write history"}


def collect(kube):
    """Live objects of the benchmark namespaces. ConfigMap data is replaced by per-key sha256/size; the raw data, the
    immutability flag and the time of the last write (managedFields) are kept apart."""
    live, cm_raw = {}, {}
    for ns in NAMESPACES:
        for kind in KINDS:
            extra = ("--show-managed-fields",) if kind == "configmaps" else ()
            items = kube.get("-n", ns, kind, *extra)["items"]
            if kind == "configmaps":
                for o in items:
                    cm_raw[(ns, o["metadata"]["name"])] = dict(cm_provenance(o), data=dict(o.get("data") or {}),
                                                               immutable=o.get("immutable"))
                    for field in ("data", "binaryData"):
                        if o.get(field):
                            o[field] = {k: {"sha256": sha_bytes(v.encode()), "bytes": len(v.encode())}
                                        for k, v in o[field].items()}
            live[(ns, kind)] = [strip(o) for o in items]
    return live, cm_raw


def config_fingerprint(live):
    """Hash of the static configuration per object (status, replica counts and bookkeeping excluded)."""
    fp = {}
    for (ns, kind), items in live.items():
        if kind not in STATIC_KINDS:
            continue
        for o in items:
            md = o["metadata"]
            if kind == "configmaps":
                body = {"data": o.get("data"), "binaryData": o.get("binaryData"), "immutable": o.get("immutable")}
            else:
                spec = copy.deepcopy(o.get("spec", {}))
                if kind == "deployments.apps":
                    spec.pop("replicas", None)
                body = {"spec": spec, "labels": md.get("labels"),
                        "keda": {k: v for k, v in (md.get("annotations") or {}).items()
                                 if k.startswith("autoscaling.keda.sh/")}}
            fp[f"{ns}/{kind}/{md['name']}"] = sha_bytes(json.dumps(body, sort_keys=True).encode())
    return fp


def fingerprint_changes(a, b):
    return [f"{k}: {'added' if k not in a else 'removed' if k not in b else 'changed'}"
            for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)]


# ------------------------------------------------------------------------------------------------------- gates
def quantity(q):
    """Kubernetes quantity -> float (cores for cpu, bytes for memory)."""
    if q is None:
        return 0.0
    units = {"": 1.0, "m": 1e-3, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "Ki": 2 ** 10, "Mi": 2 ** 20,
             "Gi": 2 ** 30, "Ti": 2 ** 40}
    m = re.fullmatch(r"([0-9.]+(?:[eE][+-]?[0-9]+)?)([A-Za-z]*)", str(q))
    if not m or m.group(2) not in units:
        raise ValueError(f"unparsable quantity {q!r}")
    return float(m.group(1)) * units[m.group(2)]


def pod_requests(spec, res):
    """A pod's effective request as the scheduler counts it: restartable (sidecar) init containers add to the regular
    containers, ordinary init containers run alone next to the sidecars started before them, plus pod overhead."""
    req = lambda c: quantity(((c.get("resources") or {}).get("requests") or {}).get(res))
    sidecars = peak = 0.0
    for c in spec.get("initContainers", []):
        if c.get("restartPolicy") == "Always":
            sidecars += req(c)
            peak = max(peak, sidecars)
        else:
            peak = max(peak, sidecars + req(c))
    return max(sum(req(c) for c in spec["containers"]) + sidecars, peak) + \
        quantity((spec.get("overhead") or {}).get(res))


def node_requests(pods):
    out = {}
    for p in pods:
        node = p["spec"].get("nodeName")
        if not node or p.get("status", {}).get("phase") in ("Succeeded", "Failed"):
            continue
        acc = out.setdefault(node, {"cpu": 0.0, "memory": 0.0, "pods": 0})
        acc["cpu"] += pod_requests(p["spec"], "cpu")
        acc["memory"] += pod_requests(p["spec"], "memory")
        acc["pods"] += 1
    return out


def effective_env(container, configmaps):
    """(env, unresolved, sources): Kubernetes precedence — envFrom sources in order, then env entries; ConfigMap
    references resolved from `configmaps` {name: data}; Secret, field and resource references stay opaque (listed as
    unresolved: their values are not known here) and Secrets are never read. sources: variable -> ConfigMap name."""
    env, unresolved, sources = {}, [], {}
    for src in container.get("envFrom", []):
        prefix = src.get("prefix", "")
        if "configMapRef" in src:
            ref = src["configMapRef"]
            data = configmaps.get(ref["name"])
            if data is None:
                if not ref.get("optional"):
                    unresolved.append(f"envFrom ConfigMap {ref['name']} missing")
                continue
            for k, v in data.items():
                env[prefix + k], sources[prefix + k] = v, ref["name"]
        elif "secretRef" in src:
            unresolved.append(f"envFrom Secret {src['secretRef'].get('name')} (keys unknown; Secrets are not read)")
    for e in container.get("env", []):
        name, vf = e["name"], e.get("valueFrom")
        sources.pop(name, None)
        if vf is None:
            env[name] = e.get("value", "")
            if "$(" in env[name]:
                unresolved.append(f"{name} references another variable")
        elif "configMapKeyRef" in vf:
            ref = vf["configMapKeyRef"]
            data = configmaps.get(ref["name"]) or {}
            if ref["key"] in data:
                env[name], sources[name] = data[ref["key"]], ref["name"]
            elif not ref.get("optional"):
                unresolved.append(f"{name} from missing ConfigMap key {ref['name']}/{ref['key']}")
        elif "secretKeyRef" in vf:
            env[name] = f"<secret {vf['secretKeyRef'].get('name')}/{vf['secretKeyRef'].get('key')}>"
            unresolved.append(f"{name} from Secret (opaque)")
        elif "fieldRef" in vf:
            env[name] = f"<field {vf['fieldRef'].get('fieldPath')}>"
            unresolved.append(f"{name} from a field reference (opaque)")
        else:
            env[name] = f"<resource {json.dumps(vf.get('resourceFieldRef'), sort_keys=True)}>"
            unresolved.append(f"{name} from a resource reference (opaque)")
    return env, unresolved, sources


def envelope_problems(who, api_env, trainer_env, cfg):
    problems = []
    if api_env is not None:
        got = api_env.get("ENSEMBLE_HISTORY_HOURS")
        if got != str(cfg["ENSEMBLE_HISTORY_HOURS"]):
            problems.append(f"{who} ml-api ENSEMBLE_HISTORY_HOURS {got!r} != {cfg['ENSEMBLE_HISTORY_HOURS']}")
    if trainer_env is not None and "TRAINING_HOURS" in trainer_env:
        problems.append(f"{who} trainer sets TRAINING_HOURS")
    return problems


def gate_ml_engine(deploys, cronjobs, configmaps, env, cfg, api_pods=()):
    """(template problems, envelope problems, effective env): digests and GIT_COMMIT of the ml-engine templates, and the
    contamination-envelope settings from the resolved environment of the templates and of every running ml-api pod
    (api_pods: (pod name, container) pairs)."""
    by = {d["metadata"]["name"]: d for d in deploys}
    api = by["ml-api"]["spec"]["template"]["spec"]["containers"][0]
    op = by["predictive-operator"]["spec"]["template"]["spec"]["containers"][0]
    tr = next(c for c in cronjobs if c["metadata"]["name"] == "ml-training")
    trc = tr["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
    eff = {name: effective_env(c, configmaps) for name, c in (("ml-api", api), ("operator", op), ("trainer", trc))}
    problems, envelope = [], []
    for name, c, digest in (("ml-api", api, env["ML_API_DIGEST"]), ("operator", op, env["OPERATOR_DIGEST"]),
                            ("trainer", trc, env["ML_API_DIGEST"])):
        if not c["image"].endswith("@" + digest):
            problems.append(f"{name} image {c['image']} is not @{digest}")
    for name in ("ml-api", "trainer"):
        if eff[name][0].get("GIT_COMMIT") != env["GIT_COMMIT_VALUE"]:
            problems.append(f"{name} GIT_COMMIT {eff[name][0].get('GIT_COMMIT')} != {env['GIT_COMMIT_VALUE']}")
        envelope += [f"{name}: {u}" for u in eff[name][1]]
    envelope += envelope_problems("template", eff["ml-api"][0], eff["trainer"][0], cfg)
    for pod, c in api_pods:
        penv, punres, _ = effective_env(c, configmaps)
        envelope += [f"pod {pod}: {u}" for u in punres] + envelope_problems(f"pod {pod}", penv, None, cfg)
    effective = {k: {"env": v[0], "unresolved": v[1], "configmap_sources": v[2]} for k, v in eff.items()}
    effective["ml-training_schedule"] = tr["spec"]["schedule"]
    return problems, envelope, effective


def owned_by(obj, uid):
    return any(o.get("uid") == uid for o in obj["metadata"].get("ownerReferences", []))


def container_drift(template_spec, pod_spec):
    """Differences between a pod and the template it was admitted from, allowing only the declared injections."""
    problems = []
    tmpl = {c["name"]: c for c in template_spec["containers"]}
    pod = {c["name"]: c for c in pod_spec["containers"]}
    problems += [f"template container {n} absent" for n in tmpl if n not in pod]
    problems += [f"container {n} not in the template" for n in pod if n not in tmpl and n not in INJECTED_CONTAINERS]
    for n, t in tmpl.items():
        p = pod.get(n)
        if p is None:
            continue
        problems += [f"{n}.{f} differs" for f in CONTROLLED if t.get(f) != p.get(f)]
        mounts = [m for m in p.get("volumeMounts", []) if not INJECTED_MOUNT.match(m.get("name", ""))]
        if mounts != t.get("volumeMounts", []):
            problems.append(f"{n}.volumeMounts differ")
    pvol = {v["name"]: v for v in pod_spec.get("volumes", [])}
    problems += [f"volume {v['name']} differs" for v in template_spec.get("volumes", []) if pvol.get(v["name"]) != v]
    return problems


def gate_runtime(deploys, replicasets, pods, cm_raw=None):
    """Every Deployment's rollout observed and complete and every pod on its current ReplicaSet with the template's
    container configuration, complete status and digests; STRICT Deployments fully Ready, and their ConfigMap-sourced
    environment not written after their containers started (cm_raw: {(ns, name): {"data", "updated"}})."""
    problems, rows = [], []
    cm_raw = cm_raw or {}
    for d in deploys:
        md, st = d["metadata"], d.get("status", {})
        name, want, ns = md["name"], d["spec"].get("replicas", 1), md.get("namespace")
        if st.get("observedGeneration") != md.get("generation"):
            problems.append(f"{name}: generation {md.get('generation')} not observed ({st.get('observedGeneration')})")
        if (st.get("updatedReplicas") or 0) != (st.get("replicas") or 0):
            problems.append(f"{name}: rollout incomplete ({st.get('updatedReplicas')} of {st.get('replicas')} updated)")
        rev = (md.get("annotations") or {}).get("deployment.kubernetes.io/revision")
        mine = [rs for rs in replicasets if owned_by(rs, md["uid"])]
        cur = [rs for rs in mine
               if (rs["metadata"].get("annotations") or {}).get("deployment.kubernetes.io/revision") == rev]
        if len(cur) != 1:
            problems.append(f"{name}: {len(cur)} ReplicaSets at revision {rev}")
            continue
        cur_uid = cur[0]["metadata"]["uid"]
        tspec = d["spec"]["template"]["spec"]
        tmpl = {c["name"]: c["image"] for c in tspec["containers"]}
        strict, ready = name in STRICT, 0
        cms = {n: v["data"] for (cns, n), v in cm_raw.items() if cns == ns}
        for rs in mine:
            for p in (p for p in pods if owned_by(p, rs["metadata"]["uid"])):
                pn, phase = p["metadata"]["name"], p.get("status", {}).get("phase")
                row = {"deployment": name, "pod": pn, "uid": p["metadata"]["uid"], "replicaset": rs["metadata"]["name"],
                       "node": p["spec"].get("nodeName"), "phase": phase,
                       "terminating": bool(p["metadata"].get("deletionTimestamp")), "containers": []}
                rows.append(row)
                if rs["metadata"]["uid"] != cur_uid:
                    problems.append(f"{name}: pod {pn} belongs to the old ReplicaSet {rs['metadata']['name']}")
                    continue
                problems += [f"{name}: pod {pn}: {x}" for x in container_drift(tspec, p["spec"])]
                if row["terminating"]:
                    continue                                  # scale-down in progress
                if phase == "Pending":
                    if strict:
                        problems.append(f"{name}: pod {pn} Pending")
                    continue
                if phase != "Running":
                    problems.append(f"{name}: pod {pn} {phase}")
                    continue
                spec_c = {c["name"]: c for c in p["spec"]["containers"]}
                stat = {cs["name"]: cs for cs in p.get("status", {}).get("containerStatuses", [])}
                if set(stat) != set(spec_c):
                    problems.append(f"{name}: pod {pn} reports status for {sorted(stat)} of {sorted(spec_c)}")
                all_ready = True
                for cname, c in spec_c.items():
                    cs, image = stat.get(cname, {}), c["image"]
                    started = ((cs.get("state") or {}).get("running") or {}).get("startedAt")
                    row["containers"].append({"name": cname, "image": image, "imageID": cs.get("imageID"),
                                              "ready": cs.get("ready"), "restarts": cs.get("restartCount"),
                                              "started": started, "injected": cname not in tmpl})
                    if not cs.get("imageID"):
                        problems.append(f"{name}: pod {pn}/{cname} has no imageID")
                    elif "@sha256:" in image and not cs["imageID"].endswith(image.split("@", 1)[1]):
                        problems.append(f"{name}: pod {pn}/{cname} runs {cs['imageID']}, not {image}")
                    all_ready = all_ready and cs.get("ready") is True
                    if strict and cname in tmpl:
                        for var, cm in effective_env(c, cms)[2].items():
                            upd = (cm_raw.get((ns, cm)) or {}).get("updated")
                            if not started or not upd or epoch(upd) > epoch(started):
                                problems.append(f"{name}: pod {pn}/{cname} {var} from ConfigMap {cm} written {upd} "
                                                f"after the container started {started}")
                if all_ready:
                    ready += 1
                elif strict:
                    problems.append(f"{name}: pod {pn} not Ready")
        if strict and not (ready == want == st.get("readyReplicas") == st.get("availableReplicas")):
            problems.append(f"{name}: {ready} Ready pods; spec {want}, ready {st.get('readyReplicas')}, "
                            f"available {st.get('availableReplicas')}")
        if not strict and (st.get("availableReplicas") or 0) < 1:
            problems.append(f"{name}: no available replica")
    return problems, rows


def job_finished(j):
    for c in j.get("status", {}).get("conditions", []):
        if c.get("type") in ("Complete", "Failed") and c.get("status") == "True":
            return c["type"]
    return None


def scheduled(job):
    """A CronJob Job's scheduled time (epoch s) from its name suffix (minutes since the epoch)."""
    m = re.search(r"-(\d{6,})$", job["metadata"]["name"])
    return int(m.group(1)) * 60 if m else None


def latest_finished(jobs, cronjob):
    mine = [j for j in jobs if any(o.get("kind") == "CronJob" and o.get("name") == cronjob
                                   for o in j["metadata"].get("ownerReferences", []))]
    done = [j for j in mine if job_finished(j)]
    return (max(done, key=lambda j: j["metadata"]["creationTimestamp"]) if done else None,
            [j for j in mine if not job_finished(j)])


def last_reload(api_log):
    """The last artifact sha prefix of ml-api's `Reloaded model nginx-test_requests … artifact sha256=<hex>` or
    startup `Loaded model for nginx-test_requests (… sha256=<hex> …)` lines."""
    # a reload in a running pod, or the load at a pod's start (after an ml-api restart), whichever is later in the log
    hits = re.findall(r"(?:Reloaded model nginx-test_requests .*?artifact sha256=|"
                      r"Loaded model for nginx-test_requests \(.*?sha256=)([0-9a-f]{12,64})", api_log or "")
    return hits[-1] if hits else None


def check_training_result(tc, expected, job_name):
    """The four-leg training check (training-check.sh v3.5) for the expected slot: every leg ok, the full artifact."""
    if not isinstance(tc, dict):
        return ["no training-check result for the expected slot"], None
    problems, art = [], tc.get("artifact_sha256") or ""
    slot = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expected))
    if not str(tc.get("checker", "")).startswith("training-check.sh v3.5"):
        problems.append(f"training check by {tc.get('checker')!r}, not training-check.sh v3.5")
    if tc.get("slot") != slot or tc.get("job") != job_name:
        problems.append(f"training check is for {tc.get('slot')} / {tc.get('job')}, expected {slot} / {job_name}")
    legs = tc.get("legs") or {}
    if tc.get("ok") is not True or tc.get("status") != "verified" or sorted(legs) != sorted(LEGS) \
            or any((legs.get(l) or {}).get("status") != "ok" for l in LEGS):
        problems.append(f"training check not verified: {tc.get('status')} "
                        f"{ {l: (legs.get(l) or {}).get('status') for l in LEGS} }")
    if not re.fullmatch(r"[0-9a-f]{64}", art):
        problems.append("training check has no full artifact sha256")
    elif ((tc.get("first_issuance") or {}).get("artifact_sha256")) != art:
        problems.append("the first issuance in the training check does not carry its artifact")
    return problems, art if re.fullmatch(r"[0-9a-f]{64}", art) else None


def gate_processes(jobs, pods, env, archive_log, api_log, trainer_template, now, training_checks=()):
    """The declared processes are working: the two newest scheduled slots by capture time (D-1097, Codex r66: never
    substituted by older ones) four-leg verified; the newest bound by Job, full artifact, the archive pair and ml-api's
    latest load of the hybrid's model; the latest archive and load-gate Jobs completed. A training still running for the
    newest slot is reported as pending and fails the gate — capture after its verification."""
    problems, summary = [], {}
    tr, active = latest_finished(jobs, "ml-training")
    newest = int(now) // SLOT * SLOT
    running = [j for j in active if scheduled(j) == newest]
    expected = newest
    if running or now - newest < 300:
        problems.append(f"the training for the newest slot {time.strftime('%H:%MZ', time.gmtime(newest))} is still "
                        f"pending; capture after its four-leg verification")
    summary["ml-training"] = {"job": tr and tr["metadata"]["name"], "result": tr and job_finished(tr),
                              "slot": tr and scheduled(tr), "expected_slot": expected,
                              "pending": [j["metadata"]["name"] for j in running],
                              "completion": tr and tr.get("status", {}).get("completionTime")}
    if tr is None or job_finished(tr) != "Complete":
        problems.append(f"latest ml-training Job {summary['ml-training']['job']} did not complete")
    else:
        if scheduled(tr) != expected:
            problems.append(f"latest finished ml-training Job {tr['metadata']['name']} is for slot {scheduled(tr)}, "
                            f"expected {expected}")
        c = tr["spec"]["template"]["spec"]["containers"][0]
        if not c["image"].endswith("@" + env["ML_API_DIGEST"]):
            problems.append(f"ml-training Job {tr['metadata']['name']} image {c['image']}")
        drift = [f for f in CONTROLLED if c.get(f) != trainer_template.get(f)]
        if drift:
            problems.append(f"ml-training Job {tr['metadata']['name']} differs from the CronJob template in {drift}")
        if effective_env(c, {})[0].get("GIT_COMMIT") != env["GIT_COMMIT_VALUE"]:
            problems.append(f"ml-training Job {tr['metadata']['name']} GIT_COMMIT differs")
        tpods = [p for p in pods
                 if owned_by(p, tr["metadata"]["uid"]) and p.get("status", {}).get("phase") == "Succeeded"]
        ids = [cs.get("imageID", "") for p in tpods for cs in p["status"].get("containerStatuses", [])]
        summary["ml-training"]["imageIDs"] = ids
        if not ids or not all(i.endswith(env["ML_API_DIGEST"]) for i in ids):
            problems.append(f"ml-training Job {tr['metadata']['name']}: succeeded pod imageIDs {ids}")
    ar, _ = latest_finished(jobs, "evidence-archive")
    last = None
    for line in (archive_log or "").splitlines():
        try:
            last = json.loads(line)
        except ValueError:
            continue
    pair = (last or {}).get("pair") or {} if isinstance(last, dict) else {}
    m = re.fullmatch(r"(\d{8}T\d{6}Z)-([0-9a-f]{12,64})", pair.get("key") or "")
    reload_sha = last_reload(api_log)
    summary["evidence-archive"] = {"job": ar and ar["metadata"]["name"], "result": ar and job_finished(ar),
                                   "report": last}
    summary["artifact"] = {"pair_key": pair.get("key"), "ml_api_last_reload_sha256_prefix": reload_sha}
    if ar is None or job_finished(ar) != "Complete" or not isinstance(last, dict) or last.get("errors") != [] \
            or pair.get("result") not in ("archived", "already_archived"):
        problems.append(f"latest evidence-archive Job {summary['evidence-archive']['job']}: "
                        f"{summary['evidence-archive']['result']}, report {json.dumps(last)[:200]}")
    elif not m or calendar.timegm(time.strptime(m.group(1), "%Y%m%dT%H%M%SZ")) != expected:
        problems.append(f"archive pair {pair.get('key')} is not for the expected slot {expected}")
    elif not reload_sha or not (m.group(2).startswith(reload_sha) or reload_sha.startswith(m.group(2))):
        problems.append(f"ml-api's last reload {reload_sha} is not the archived artifact {m.group(2)}")
    by_slot = {}
    for t in training_checks:
        by_slot.setdefault(t.get("slot") if isinstance(t, dict) else None, []).append(t)
    iso_slot = lambda x: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(x))
    if len(training_checks) != 2 or any(len(v) != 1 for v in by_slot.values()):
        problems.append(f"expected exactly two training-check results (slots {iso_slot(expected - SLOT)} and "
                        f"{iso_slot(expected)}), got slots {sorted(map(str, by_slot))}")
    prev_problems, _ = check_training_result((by_slot.get(iso_slot(expected - SLOT)) or [None])[0], expected - SLOT,
                                             f"ml-training-{(expected - SLOT) // 60}")
    problems += [f"previous slot: {x}" for x in prev_problems]                      # D-1097: two preceding slots
    tc_problems, art = check_training_result((by_slot.get(iso_slot(expected)) or [None])[0], expected,
                                             tr and tr["metadata"]["name"])
    problems += tc_problems
    summary["artifact"]["training_check_sha256"] = art
    if art and not (m and art.startswith(m.group(2)) and reload_sha and art.startswith(reload_sha)):
        problems.append(f"the verified artifact {art[:12]} is not the archived pair's / ml-api's last reload")
    le, _ = latest_finished(jobs, "load-evidence")
    summary["load-evidence"] = {"job": le and le["metadata"]["name"], "result": le and job_finished(le)}
    if le is None or job_finished(le) != "Complete":
        problems.append(f"latest load-evidence Job {summary['load-evidence']['job']} did not complete")
    return problems, summary


def hour_tag(h):
    return "load-" + h[:4] + h[5:7] + h[8:13] + h[14:16] + "Z"


FETCHER = "fetch_load_attempts.sh v1"
COLLECTOR = "collect_load_evidence.py v8"
FILE_NAME = re.compile(r"^(attempts/)?load-(\d{8}T\d{4}Z)(-\d{8}T\d{6}Z-\d+)?\.json$")


def qualification_expectations(ctx):
    """What a valid fetch and its final rows must name: the frozen fetcher and collector of this checkout, the context,
    and the approvals/terminations the collector hashed (json.dumps(obj or {}, sort_keys=True), as the collector does)."""
    gate_dir = os.path.join(HERE, "load-gate")
    inputs = {k: sha_bytes(json.dumps(json.load(open(os.path.join(gate_dir, f"{k}.json"))) or {}, sort_keys=True).encode())
              for k in ("terminations", "approvals")}
    return {"context": ctx, "fetcher_sha256": sha(os.path.join(HERE, "fetch_load_attempts.sh")),
            "collector_sha256": sha(os.path.join(HERE, "collect_load_evidence.py")), "inputs_sha256": inputs}


def qualification_after_joins(hours, ident):
    """P6 gate 4 (D-1105): a worker that joined during the history is a relevant placement change, so every
    qualification hour must start at or after the latest declared join (k5nbm: 2026-10-09T19:03:20Z → first eligible hour
    20:00Z). Unparsable hours are reported, never skipped."""
    ie = load_ie()
    joins = [ie.parse(w["joined"]) for w in ident["workers"] if isinstance(w, dict)]
    if not joins:
        return []
    latest, out = max(joins), []
    for h in hours:
        try:
            if ie.parse(h) < latest:
                out.append(f"qualification hour {h} starts before the latest worker join {ie.iso(latest)} (P6 gate 4, D-1105)")
        except ValueError:
            out.append(f"qualification hour {h!r} is not a timestamp")
    return out


def gate_qualification(fetched, hours, expect):
    """(problems, summary): the qualification hours from the immutable load-gate attempts fetched by
    fetch_load_attempts.sh v1 (D-1093, Codex r59/r60). The fetch: trailer over the exact bytes; the receipt names exactly
    this fetcher version and its frozen sha256, the expected context and load-evidence-pvc, a reader pod and node;
    receipt entries unique, well-formed and consistent (file name ↔ hour ↔ requested hours), each text matching its
    entry. An hour qualifies when every attempt of it lists exactly the six arms for that hour, every arm has at least
    one final row, and every final row is PASS with qualifies=True and was written by the frozen collector with the
    frozen approvals/terminations: provisional rows are ignored, conflicting final rows fail closed in any order. The
    hours must be two or more consecutive hours."""
    if not fetched or not hours:
        return ["no qualification attempts or hours given"], {}
    try:
        lines = fetched.rstrip(b"\n").split(b"\n")
        trailer = json.loads(lines[-1])["trailer"]
        body = b"\n".join(lines[:-1]) + b"\n"
        if len(body) != trailer["bytes"] or sha_bytes(body) != trailer["sha256"]:
            return ["the qualification fetch does not match its trailer"], {}
        rec = json.loads(lines[0])["receipt"]
        texts, problems = {}, []
        for line in lines[1:-1]:
            d = json.loads(line)
            if d["file"] in texts:
                problems.append(f"{d['file']} fetched twice")
            texts[d["file"]] = d["text"]
    except (ValueError, KeyError, IndexError, TypeError) as e:
        return [f"the qualification fetch is unreadable ({e})"], {}
    if rec.get("extractor") != FETCHER or rec.get("extractor_sha256") != expect["fetcher_sha256"]:
        problems.append(f"fetched by {rec.get('extractor')!r} {str(rec.get('extractor_sha256'))[:12]}, not the frozen "
                        f"{FETCHER} {expect['fetcher_sha256'][:12]}")
    src = rec.get("source") if isinstance(rec.get("source"), dict) else {}
    if src.get("context") != expect["context"] or src.get("pvc") != "load-evidence-pvc" \
            or not str(src.get("pod") or "").startswith("load-attempts-fetch-") or not src.get("node"):
        problems.append(f"the fetch's source {src} is not {expect['context']}/load-evidence-pvc with a reader pod and node")
    listed = {}
    for f in rec.get("files") if isinstance(rec.get("files"), list) else []:
        m = FILE_NAME.match(str(f.get("path"))) if isinstance(f, dict) else None
        if not m or bool(m.group(1)) != bool(m.group(3)) or not isinstance(f.get("bytes"), int) \
                or not re.fullmatch(r"[0-9a-f]{64}", str(f.get("sha256"))):
            problems.append(f"malformed receipt entry {f}")
            continue
        if f["path"] in listed:
            problems.append(f"receipt lists {f['path']} twice")
        if f.get("hour") not in hours or hour_tag(f["hour"]) != "load-" + m.group(2):
            problems.append(f"receipt entry {f['path']} is for hour {f.get('hour')}")
        listed[f["path"]] = f
    if set(listed) != set(texts):
        problems.append("the fetched files differ from the receipt")
    problems += [f"{p} does not match its receipt" for p, f in sorted(listed.items())
                 if p in texts and (sha_bytes(texts[p].encode()) != f.get("sha256") or len(texts[p].encode()) != f.get("bytes"))]
    if not set(hours) <= set(rec.get("hours") or []):
        problems.append(f"the fetch covers {rec.get('hours')}, not {hours}")
    ts = sorted(epoch(h) for h in hours)
    if len(ts) < 2 or any(b - a != 3600 for a, b in zip(ts, ts[1:])):
        problems.append(f"qualification hours {hours} are not two or more consecutive hours")
    summary = {}
    for h in hours:
        attempts = sorted(p for p in texts if p.startswith(f"attempts/{hour_tag(h)}-"))
        finals = {}
        if not attempts:
            problems.append(f"{h}: no attempts fetched")
        for p in attempts:
            try:
                rows = json.loads(texts[p])
            except ValueError:
                problems.append(f"{p}: not JSON")
                continue
            apps = [r.get("app") for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
            if sorted(apps) != sorted(ARMS) or len(apps) != len(rows):
                problems.append(f"{p}: arms {sorted(map(str, apps))}")
                continue
            for r in rows:
                try:
                    same_hour = epoch(r.get("hour_start", "")) == epoch(h)
                except (ValueError, TypeError):
                    same_hour = False
                if not same_hour:
                    problems.append(f"{p}: row for {r.get('hour_start')}")
                fin = r.get("maturity").get("finalized") if isinstance(r.get("maturity"), dict) else None
                if not isinstance(fin, bool):
                    problems.append(f"{p}: {r.get('app')} has no boolean maturity.finalized")   # malformed evidence
                if fin is True:
                    provenance = r.get("collector") == COLLECTOR and r.get("collector_sha256") == expect[
                        "collector_sha256"] and r.get("inputs_sha256") == expect["inputs_sha256"]
                    finals.setdefault(r["app"], []).append((r.get("status"),
                                                            (r.get("qualification") or {}).get("qualifies"),
                                                            "frozen collector" if provenance else "other collector",
                                                            r.get("generator_pod")))
        for a in ARMS:
            f = finals.get(a, [])
            if attempts and not f:
                problems.append(f"{h} {a}: no final evaluation")
            elif any(st != "PASS" or q is not True or pv != "frozen collector" for st, q, pv, _ in f):
                problems.append(f"{h} {a}: final rows {f}")
        summary[h] = {"attempts": attempts, "final_rows": {a: finals.get(a, []) for a in ARMS}}
    return problems, summary


def gate_generators(qualification, hours, pods):
    """D-1097: the qualification hours ran on the generators that run now — per arm exactly one generator pod in the
    final rows of all qualification hours, alive under that name, its k6 container never restarted and started before
    the first qualification hour."""
    problems, out = [], {}
    if not hours or not qualification:
        return ["no qualification evidence to bind the generators to"], out
    first = min(epoch(h) for h in hours)
    live = {p["metadata"]["name"]: p for p in pods if p["metadata"].get("namespace") == "demo"
            and p["metadata"]["name"].startswith("k6-")}
    for a in ARMS:
        names = sorted({str(g) for h in hours for (_, _, _, g) in (qualification.get(h) or {}).get("final_rows", {})
                        .get(a, [])})
        out[a] = names
        if len(names) != 1:
            problems.append(f"{a}: qualification rows name generator pods {names}")
            continue
        p = live.get(names[0])
        if p is None:
            problems.append(f"{a}: qualification generator {names[0]} is not running now")
            continue
        cs = next((c for c in p.get("status", {}).get("containerStatuses", []) if c.get("name") == "k6"), {})
        started = ((cs.get("state") or {}).get("running") or {}).get("startedAt")
        if p.get("status", {}).get("phase") != "Running" or cs.get("restartCount") != 0 or not started \
                or epoch(started) > first:
            problems.append(f"{a}: generator {names[0]} phase {p.get('status', {}).get('phase')}, restarts "
                            f"{cs.get('restartCount')}, k6 started {started} (must be running since before {hours[0]})")
    return problems, out


UNAVAILABLE = re.compile(r"Prediction unavailable, using reactive only.*?\b((?:nginx|myapptwo)[a-z0-9-]*?)-autoscaler\b")
ISSUANCE_WINDOW = 20 * 60          # seconds: every forecasting arm issued fresh within this window before the capture


def gate_fresh_issuances(extract, operator_log, expect_artifact, expect_cutoff, now, ctx, extractor_sha, fc=None):
    """D-1097 (Codex r66/r67): fresh, valid issuance records, not decision lines. `extract` is an extract_decisions.sh
    v3 output taken just before the capture, read with the P9 scorer's own reader (trailer over the exact bytes; receipt
    with hashes, line count, source context/pvc/pod, the exact issuance field list and the issuance count; no malformed
    lines) and judged with its §4 acceptance per arm in log order (complete provenance, anchors, six finite steps,
    targets strictly after issuance and cutoff). The receipt must name the frozen extractor, `ctx` and forecast-log-pvc.
    Per forecasting arm every record issued in the last ISSUANCE_WINDOW must be in namespace demo and accepted, and at
    least one must exist; the newest accepted hybrid and S1 issuance must carry the newest verified training's full
    artifact and its cutoff. Any "Prediction unavailable" fallback of a forecasting arm in the operator's last 20 min
    (stored) fails too."""
    problems, newest = [], {}
    if not extract:
        return ["no issuance extract given (--issuances)"], newest
    fc = fc or load_forecasts()
    with tempfile.NamedTemporaryFile(suffix=".jsonl") as fh:
        fh.write(extract)
        fh.flush()
        try:
            receipt, rows, _ = fc.read_trailed(fh.name, "extract_decisions.sh v3", "I", "issuance_fields",
                                               fc.ISSUANCE_FIELDS)
        except (ValueError, KeyError, IndexError, TypeError, UnicodeDecodeError) as e:
            return [f"the issuance extract is invalid ({str(e).replace(fh.name, '<extract>')})"], newest
    src = receipt["source"]
    if receipt.get("extractor_sha256") != extractor_sha or src.get("context") != ctx or src.get("pvc") != "forecast-log-pvc":
        problems.append(f"the issuance extract was not read from {ctx}/forecast-log-pvc by the frozen extractor")
    lo, hi = (now - ISSUANCE_WINDOW) * 1000, (now + 120) * 1000
    cut_ms = fc.ts(expect_cutoff) if expect_cutoff else None
    for a in FORECASTING_ARMS:
        recs = [r for r in rows if r.get("application") == a]
        acc, rej = fc.accept(recs)
        in_window = lambda r: (fc.ts(r.get("issued_at")) or 0) >= lo and (fc.ts(r.get("issued_at")) or 0) <= hi
        foreign = [r.get("issuance_id") for r in recs if in_window(r) and r.get("namespace") != "demo"]
        bad = [x for x in rej if lo <= (fc.ts(x.get("issued_at")) or lo) <= hi]
        fresh = [r for r in acc if lo <= r["_issued"] <= hi and r.get("namespace") == "demo"]
        if foreign:
            problems.append(f"{a}: issuances outside namespace demo in the window: {foreign[:3]}")
        if bad:
            problems.append(f"{a}: {len(bad)} issuances in the window fail the P9 acceptance: "
                            f"{sorted({x['reason'] for x in bad})}")
        if not fresh:
            problems.append(f"{a}: no accepted issuance in the {ISSUANCE_WINDOW // 60} min before the capture")
            continue
        n = max(fresh, key=lambda r: r["_issued"])
        newest[a] = {k: n.get(k) for k in ("issuance_id", "issued_at", "artifact_sha256", "training_cutoff",
                                           "model_version", "namespace")}
        if a in ("nginx-test", "nginx-seasonal") and (n.get("artifact_sha256") != expect_artifact
                                                      or cut_ms is None or n["_cutoff"] != cut_ms):
            problems.append(f"{a}: newest issuance carries {str(n.get('artifact_sha256'))[:12]} / "
                            f"{n.get('training_cutoff')}, not the verified {str(expect_artifact)[:12]} / {expect_cutoff}")
    fell_back = collections.Counter(m.group(1) for m in UNAVAILABLE.finditer(operator_log or ""))
    problems += [f"{a}: {fell_back[a]} 'Prediction unavailable' fallbacks in the operator's last 20 min"
                 for a in FORECASTING_ARMS if fell_back[a]]
    return problems, {"newest_issuance": newest, "operator_fallbacks": dict(fell_back)}


def gate_arms(deploys, pas, scaled):
    problems = []
    names = {d["metadata"]["name"] for d in deploys}
    problems += [f"deployment {n} missing" for n in list(ARMS) + ["k6-" + a for a in ARMS] if n not in names]
    targets = [p["spec"]["targetDeployment"]["name"] for p in pas]
    want = sorted(a for a in ARMS if a != "myapptwo")
    if sorted(targets) != want:
        problems.append(f"PredictiveAutoscalers target {sorted(targets)}, expected one each for {want}")
    for p in pas:
        if (p["spec"].get("minReplicas"), p["spec"].get("maxReplicas")) != BOUNDS:
            problems.append(f"{p['spec']['targetDeployment']['name']} bounds "
                            f"{p['spec'].get('minReplicas')}..{p['spec'].get('maxReplicas')}")
    so = {s["metadata"]["name"]: s for s in scaled}
    k = so.get("myapptwo-keda-fallback")
    if k is None or (k["spec"].get("minReplicaCount"), k["spec"].get("maxReplicaCount")) != BOUNDS \
            or (k["metadata"].get("annotations") or {}).get("autoscaling.keda.sh/paused") == "true":
        problems.append("myapptwo ScaledObject missing, paused or not bounded 1..12")
    h = so.get("nginx-test-keda-fallback")
    if h is None or (h["metadata"].get("annotations") or {}).get("autoscaling.keda.sh/paused") != "true":
        problems.append("the hybrid's KEDA fallback is not paused")
    return problems


def gate_nodes(nodes, workers, joined=None):
    """joined: {name: (creationTimestamp, uid)} for workers declared as joined during the history (detector v6): the live
    Node must be that object (same UID and creationTimestamp)."""
    problems, out = [], []
    names = {n["metadata"]["name"] for n in nodes}
    problems += [f"frozen worker {w} missing" for w in workers if w not in names]
    for n in nodes:
        decl = (joined or {}).get(n["metadata"]["name"])
        if decl and (n["metadata"].get("creationTimestamp"), n["metadata"].get("uid")) != decl:
            problems.append(f"{n['metadata']['name']}: live creationTimestamp/uid "
                            f"{(n['metadata'].get('creationTimestamp'), n['metadata'].get('uid'))} differ from the declared {decl}")
    for n in nodes:
        cond = {c["type"]: c["status"] for c in n.get("status", {}).get("conditions", [])}
        out.append({"name": n["metadata"]["name"], "frozen_worker": n["metadata"]["name"] in workers,
                    "labels": {k: v for k, v in n["metadata"].get("labels", {}).items()
                               if k.startswith(("predictive-bench/", "node-role.kubernetes.io/"))},
                    "taints": n["spec"].get("taints", []), "conditions": cond,
                    "allocatable": n["status"].get("allocatable"), "capacity": n["status"].get("capacity"),
                    "addresses": n["status"].get("addresses"), "nodeInfo": n["status"].get("nodeInfo")})
        if n["metadata"]["name"] not in workers:
            continue
        if cond.get("Ready") != "True":
            problems.append(f"{n['metadata']['name']} not Ready")
        for c in ("DiskPressure", "MemoryPressure", "PIDPressure"):
            if cond.get(c) != "False":
                problems.append(f"{n['metadata']['name']} {c}={cond.get(c)}")
    return problems, out


def disk_samples(results):
    """{instance ip: {key: value}} from (key, query result) pairs; conflicting values per instance are kept as None and
    reported, never silently overwritten."""
    out, conflicts = {}, []
    for key, result in results:
        for r in result:
            ip = r["metric"].get("instance", "").split(":")[0]
            v = float(r["value"][1])
            slot = out.setdefault(ip, {})
            if key in slot and slot[key] != v:
                conflicts.append(f"{ip} {key}")
                slot[key] = None
            elif key not in slot:
                slot[key] = v
    return out, sorted(set(conflicts))


def root_disk(disk, nodes):
    """Root filesystem use per node (matched by InternalIP); other exporters are kept under their address."""
    ip_to_node = {a["address"]: n["name"] for n in nodes for a in (n.get("addresses") or [])
                  if a.get("type") == "InternalIP"}
    out = {}
    for ip, v in disk.items():
        size, avail = v.get("size"), v.get("avail")
        out[ip_to_node.get(ip, ip)] = {"ip": ip, "size_bytes": size, "avail_bytes": avail,
                                       "size_ts": v.get("size_ts"), "avail_ts": v.get("avail_ts"),
                                       "used_pct": round(100 * (1 - avail / size), 1)
                                       if isinstance(size, float) and isinstance(avail, float) and size > 0
                                       and 0 <= avail <= size else None}
    return out


def gate_telemetry(top_rc, top_text, disk_by_node, workers, series, now, conflicts=()):
    problems = [f"conflicting root-disk series: {c}" for c in conflicts]
    rows = {l.split()[0]: l.split()[1:] for l in top_text.splitlines() if l.strip()}
    if top_rc != 0:
        problems.append(f"kubectl top nodes failed ({top_rc})")
    for w in workers:
        r = rows.get(w)
        if not r or len(r) < 3 or not re.fullmatch(r"\d+m?", r[0]) or not re.fullmatch(r"\d+[KMG]i", r[2]):
            problems.append(f"no usable kubectl top row for {w}")
        d = disk_by_node.get(w) or {}
        vals = [d.get(k) for k in ("size_bytes", "avail_bytes", "size_ts", "avail_ts")]
        if not all(isinstance(v, float) and math.isfinite(v) for v in vals):
            problems.append(f"root-disk samples incomplete for {w}")
            continue
        size, avail, *stamps = vals
        if not (size > 0 and 0 <= avail <= size):
            problems.append(f"root-disk samples for {w} are not physical (size {size}, available {avail})")
        for ts in stamps:
            if ts > now + SKEW or now - ts > FRESH:
                problems.append(f"root-disk sample for {w} at {int(ts)} is outside [now - {FRESH} s, now + {SKEW} s]")
    problems += [f"series {s} absent" for s in REQUIRED_SERIES if s not in series]
    return problems


def p8_code_id(base):
    """p8-job.sh's ConfigMap id: sha256 of the `<sha256>  <file>` lines of its FILES, first 12 hex digits."""
    return sha_bytes("".join(f"{sha(os.path.join(base, f))}  {f}\n" for f in P8_FILES).encode())[:12]


def gate_p8_code(cm_raw, base):
    name = f"p8-code-{p8_code_id(base)}"
    cm = cm_raw.get(("ml-engine", name))
    if cm is None:
        return [], name, False
    want = {f: open(os.path.join(base, f)).read() for f in P8_FILES}
    problems = [] if cm["data"] == want else [f"{name} does not hold the current P8 code"]
    if cm.get("immutable") is not True:
        problems.append(f"{name} is not immutable")
    return problems, name, True


def tool_versions(kube):
    v = json.loads(kube("version", "-o", "json").stdout)
    minor = lambda x: int(re.match(r"\d+", x["minor"]).group(0))
    problems = [] if abs(minor(v["clientVersion"]) - minor(v["serverVersion"])) <= 1 else \
        [f"kubectl {v['clientVersion']['gitVersion']} is outside one minor version of the server "
         f"{v['serverVersion']['gitVersion']}"]
    return problems, {"kubectl": kube.bin, "client": v["clientVersion"]["gitVersion"],
                      "server": v["serverVersion"]["gitVersion"]}


def load_ie():
    spec = importlib.util.spec_from_file_location("infra_events", os.path.join(HERE, "infra_events.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def load_forecasts():
    spec = importlib.util.spec_from_file_location("forecasts", os.path.join(HERE, "scoring", "forecasts.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def checkout_identity():
    git = lambda *x: run(["git", "-C", REPO, *x]).stdout.strip()
    return {"head": git("rev-parse", "HEAD"), "origin_main": git("rev-parse", "origin/main"),
            "dirty": git("status", "--porcelain").splitlines(),
            "ahead": git("rev-list", "origin/main..HEAD").splitlines(),
            "trees": {t: git("rev-parse", f"HEAD:{t}") for t in TREES},
            "files_sha256": {f: sha(os.path.join(REPO, f)) for f in FROZEN_FILES}}


def configuration_identity(ident, rendered_meta, fingerprint, env, vm_static, placement):
    """What must be equal between the main capture and the revalidation before T0 (static settings only: no usage,
    replica counts or training activity)."""
    body = {"head": ident["head"], "files_sha256": ident["files_sha256"],
            "rendered_sha256": {k: v["sha256"] for k, v in rendered_meta.items()}, "config": fingerprint,
            "digests": {k: env[k] for k in ("ML_API_DIGEST", "OPERATOR_DIGEST", "GIT_COMMIT_VALUE")},
            "victoriametrics": vm_static, "placement": placement}
    return {"sha256": sha_bytes(json.dumps(body, sort_keys=True).encode()), "body": body}


def vm_static_identity(monitoring, vm_crs):
    return {"containers": sha_bytes(json.dumps(monitoring, sort_keys=True).encode()),
            "crs": {k: sha_bytes(json.dumps(v.get("spec"), sort_keys=True).encode()) for k, v in vm_crs.items()}}


def placement_identity(nodes):
    return {n["name"]: {"labels": n["labels"], "taints": n["taints"]} for n in nodes}


def verify_capture(d):
    """(capture, problems): an earlier capture package verified end to end. Bytes: COMPLETE.json, a strictly parsed
    SHA256SUMS (64-hex hash, two spaces, a relative path; no duplicates), every listed path a regular file (no link)
    with that hash, no unlisted file, capture.json's own hash. Meaning: a supported capture version, every mandatory
    gate present with PASS and no problems (and matches_previous, if present), a PASS verdict in both files, the
    identity recomputed from its body and equal to COMPLETE's. A failed capture supports only a diagnostic comparison,
    never a freeze baseline."""
    problems = []
    try:
        complete = json.load(open(os.path.join(d, "COMPLETE.json")))
        sums = open(os.path.join(d, "SHA256SUMS"), "rb").read()
        cap_bytes = open(os.path.join(d, "capture.json"), "rb").read()
        cap = json.loads(cap_bytes)
        if not isinstance(complete, dict) or not isinstance(cap, dict):
            raise ValueError("not JSON objects")
    except (OSError, ValueError) as e:
        return None, [f"{d}: not a readable capture package ({e})"]
    if sha_bytes(sums) != complete.get("sha256sums_sha256"):
        problems.append("SHA256SUMS does not match COMPLETE.json")
    listed = {}
    for n, line in enumerate(sums.decode(errors="replace").splitlines(), 1):
        m = re.fullmatch(r"([0-9a-f]{64})  ([^/\s][^\n]*)", line)
        if not m or ".." in m.group(2).split("/"):
            problems.append(f"SHA256SUMS line {n} is malformed")
        elif m.group(2) in listed:
            problems.append(f"SHA256SUMS lists {m.group(2)} twice")
        else:
            listed[m.group(2)] = m.group(1)
    present = {os.path.relpath(os.path.join(r, f), d) for r, _, fs in os.walk(d) for f in fs} - {"SHA256SUMS",
                                                                                                "COMPLETE.json"}
    if set(listed) != present:
        problems.append(f"files differ from SHA256SUMS: {sorted(set(listed) ^ present)[:5]}")
    for rel, h in sorted(listed.items()):
        p = os.path.join(d, rel)
        if os.path.islink(p) or not os.path.isfile(p):
            problems.append(f"{rel} is not a regular file")
        elif sha(p) != h:
            problems.append(f"{rel} does not match its hash")
    if sha_bytes(cap_bytes) != complete.get("capture_sha256"):
        problems.append("capture.json does not match COMPLETE.json")
    if cap.get("freeze_capture") not in SUPPORTED_BASELINES:
        problems.append(f"unsupported capture version {cap.get('freeze_capture')!r}")
    gates = cap.get("gates") if isinstance(cap.get("gates"), dict) else {}
    for g in MANDATORY_GATES + (("matches_previous",) if "matches_previous" in gates else ()):
        v = gates.get(g)
        if not isinstance(v, dict) or v.get("status") != "PASS" or v.get("problems") != []:
            problems.append(f"baseline gate {g} is " + ("missing" if v is None else str(v.get("status"))
                                                       if isinstance(v, dict) else "malformed"))
    ident = cap.get("configuration_identity") if isinstance(cap.get("configuration_identity"), dict) else {}
    if sha_bytes(json.dumps(ident.get("body"), sort_keys=True).encode()) != ident.get("sha256"):
        problems.append("its configuration identity does not match its body")
    if complete.get("configuration_identity") != ident.get("sha256"):
        problems.append("COMPLETE.json names another configuration identity")
    if cap.get("verdict") != "PASS" or complete.get("verdict") != "PASS":
        problems.append(f"baseline verdict {cap.get('verdict')}/{complete.get('verdict')}: a diagnostic comparison "
                        f"only, not a freeze baseline")
    return cap, problems


def compare_identity(prev_cap, identity):
    pb = ((prev_cap or {}).get("configuration_identity") or {}).get("body") or {}
    if pb == identity["body"]:
        return []
    parts = [k for k in sorted(set(pb) | set(identity["body"])) if pb.get(k) != identity["body"].get(k)]
    detail = fingerprint_changes(pb.get("config") or {}, identity["body"].get("config") or {})[:10]
    return [f"configuration identity differs in {parts}" + (f": {detail}" if detail else "")]


def server_diffs(kube, rendered):
    out = {}
    for part, path in rendered.items():
        p = kube("diff", "--server-side", "--field-manager=predictive-bench-deploy", "-f", path, ok=(0, 1))
        out[part] = {"exit": p.returncode, "lines": len(p.stdout.splitlines()), "text": p.stdout}
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="new directory for the capture")
    ap.add_argument("--evidence", action="append", default=[], help="a file to bind by path, size and sha256")
    ap.add_argument("--qualification-attempts", help="fetch_load_attempts.sh output for the qualification hours")
    ap.add_argument("--qualification-hours", default="", help="comma-separated hour starts, e.g. 2026-10-07T21:00:00Z")
    ap.add_argument("--compare-with", help="an earlier PASS capture whose configuration identity must match")
    ap.add_argument("--issuances", help="extract_decisions.sh v3 output taken just before the capture (D-1097)")
    ap.add_argument("--training-check", action="append", default=[],
                    help="training-check.sh v3.5 result.json; give the expected slot and the slot before it (D-1097)")
    a = ap.parse_args(argv)
    ctx = os.environ.get("KUBE_CONTEXT") or sys.exit("set KUBE_CONTEXT")
    vm_url = os.environ.get("VM_URL") or sys.exit("set VM_URL (a reachable vmselect Prometheus API base)")
    missing = [k for k in DEPLOY_ENV if not os.environ.get(k)]
    if missing:
        sys.exit(f"deploy.env not loaded: {missing}")
    if not a.evidence:
        sys.exit("--evidence is required (at least the evaluation protocol)")
    if os.path.exists(a.out):
        sys.exit(f"{a.out} exists; refusing to replace it")
    env = {k: os.environ[k] for k in DEPLOY_ENV}
    kube = Kube(shutil.which(os.environ.get("KUBECTL", "kubectl")) or sys.exit("kubectl not found"), ctx)
    kustomize = shutil.which(os.environ.get("KUSTOMIZE", "kustomize")) or sys.exit("kustomize not found")
    blobs = {}                                                # every evidence file read once: hashed and parsed
    for role, p in [("evidence", p) for p in a.evidence] + [("qualification", a.qualification_attempts)] + \
            [("training_check", p) for p in a.training_check] + [("issuances", a.issuances)]:
        if p:
            blobs.setdefault(role, []).append((os.path.abspath(p), open(p, "rb").read()))
    evidence = [{"role": role, "path": p, "bytes": len(b), "sha256": sha_bytes(b)}
                for role, items in blobs.items() for p, b in items]
    previous, previous_problems = verify_capture(a.compare_with) if a.compare_with else (None, [])
    started, gates = now_iso(), {}
    stage = Staging(a.out)
    try:

        gates["tooling"], tools = tool_versions(kube)
        tools["kustomize"] = {"path": kustomize, "version": run([kustomize, "version"]).stdout.strip()}
        ident0 = checkout_identity()
        gates["clean_checkout"] = ([f"uncommitted: {ident0['dirty'][:5]}"] if ident0["dirty"] else []) + \
                                  ([f"HEAD not in origin/main: {ident0['ahead']}"] if ident0["ahead"] else [])

        with tempfile.TemporaryDirectory(prefix="freeze-render-") as rdir:
            run([os.path.join(HERE, "deploy.sh"), "render"], env=dict(os.environ, RENDER_OUT=rdir))
            rendered = {part: os.path.join(rdir, f"{part}.yaml") for part in NAMESPACES}
            rendered_meta = {part: {"sha256": sha(p), "objects": sum(l.startswith("kind: ") for l in open(p))}
                             for part, p in rendered.items()}
            referenced = "".join(open(p).read() for p in rendered.values())
            diffs0 = server_diffs(kube, rendered)
            live, cm_raw = collect(kube)
            pods_all = kube.get("pods", "-A")["items"]
            nodes_raw = kube.get("nodes")["items"]
            top = kube("top", "nodes", "--no-headers", ok=(0, 1))
            now = time.time()
            sel = '{mountpoint="/",fstype!~"tmpfs|overlay"}'
            disk, conflicts = disk_samples([
                (key, vm(vm_url, "/api/v1/query", query=expr, time=int(now))["result"]) for key, expr in (
                    ("avail", f"node_filesystem_avail_bytes{sel}"), ("size", f"node_filesystem_size_bytes{sel}"),
                    ("avail_ts", f"timestamp(node_filesystem_avail_bytes{sel})"),
                    ("size_ts", f"timestamp(node_filesystem_size_bytes{sel})"))])
            mask_repo = open(os.path.join(HERE, "validity-mask.json")).read()
            hist = calendar.timegm(time.strptime(json.loads(mask_repo)["benchmark_history_start"], "%Y-%m-%dT%H:%M:%SZ"))
            series = vm(vm_url, "/api/v1/label/__name__/values",
                        **{"match[]": '{__name__=~"bench_.*|k6_bench_.*"}', "start": hist, "end": int(now)})
            evicted = kube.get("events", "-A", "--field-selector", "reason=Evicted")["items"]
            monitoring = []
            for kind in ("deployments.apps", "statefulsets.apps"):
                for o in kube.get("-n", "monitoring", kind)["items"]:
                    if o["metadata"]["name"].startswith("vm"):
                        for c in o["spec"]["template"]["spec"]["containers"]:
                            monitoring.append({"object": f"{kind}/{o['metadata']['name']}", "container": c["name"],
                                               "image": c["image"], "args": c.get("args", [])})
            vm_crs = {k: strip(kube.get("-n", "monitoring", k, "vmst")) for k in
                      ("vmcluster.operator.victoriametrics.com", "vmagent.operator.victoriametrics.com")}
            ar, _ = latest_finished(live[("ml-engine", "jobs.batch")], "evidence-archive")
            archive_log = kube("-n", "ml-engine", "logs", f"job/{ar['metadata']['name']}", "--tail=20").stdout if ar else ""
            api_log = kube("-n", "ml-engine", "logs", "deploy/ml-api", "--since=14h").stdout
            operator_log = kube("-n", "ml-engine", "logs", "deploy/predictive-operator", "--since=20m").stdout
            live1, _ = collect(kube)                              # end of collection: the same checks again
            diffs1 = server_diffs(kube, rendered)
        ident1 = checkout_identity()

        ident_raw = json.load(open(os.path.join(HERE, "infra-identities.json")))
        load_ie().worker_entries(ident_raw)                    # the detector's own validation (v6)
        workers = [w if isinstance(w, str) else w["name"] for w in ident_raw["workers"]]
        joined = {w["name"]: (w["joined"], w["uid"]) for w in ident_raw["workers"] if isinstance(w, dict)}
        bench_pods = [p for p in pods_all if p["metadata"]["namespace"] in NAMESPACES]
        fingerprint = config_fingerprint(live)
        gates["clean_checkout"] += [f"checkout changed during the capture: {k}" for k in ("head", "dirty", "files_sha256")
                                    if ident0[k] != ident1[k]]
        gates["live_equals_render"] = [f"{when} {part}: server-side diff has {d['lines']} lines (exit {d['exit']})"
                                       for when, ds in (("start", diffs0), ("end", diffs1)) for part, d in ds.items()
                                       if d["exit"] != 0 or d["lines"]]
        gates["stable_during"] = fingerprint_changes(fingerprint, config_fingerprint(live1))
        mask_live = (cm_raw.get(("ml-engine", "validity-mask")) or {}).get("data", {}).get("validity-mask.json")
        gates["mask"] = [] if mask_live == mask_repo else ["ConfigMap validity-mask differs from validity-mask.json"]
        gates["p8_code"], p8_name, p8_present = gate_p8_code(cm_raw, HERE)
        cms = {name: d["data"] for (ns, name), d in cm_raw.items() if ns == "ml-engine"}
        cfg = load_forecasts().frozen_envelope_config()
        deploys = live[("ml-engine", "deployments.apps")] + live[("demo", "deployments.apps")]
        api_uid = next(d["metadata"]["uid"] for d in deploys if d["metadata"]["name"] == "ml-api")
        api_rs = [rs["metadata"]["uid"] for rs in live[("ml-engine", "replicasets.apps")] if owned_by(rs, api_uid)]
        api_pods = [(p["metadata"]["name"], c) for p in bench_pods if any(owned_by(p, u) for u in api_rs)
                    and p.get("status", {}).get("phase") == "Running" for c in p["spec"]["containers"] if c["name"] == "api"]
        tmpl, gates["envelope_config"], effective = gate_ml_engine(
            live[("ml-engine", "deployments.apps")], live[("ml-engine", "cronjobs.batch")], cms, env, cfg, api_pods)
        if not api_pods:
            gates["envelope_config"].append("no running ml-api pod to resolve")
        runtime, pod_rows = gate_runtime(deploys, live[("ml-engine", "replicasets.apps")] +
                                         live[("demo", "replicasets.apps")], bench_pods, cm_raw)
        gates["runtime"] = tmpl + runtime
        trainer_template = next(c for c in live[("ml-engine", "cronjobs.batch")] if c["metadata"]["name"] == "ml-training")[
            "spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        tcs = []
        for _, b in blobs.get("training_check", []):
            try:
                tcs.append(json.loads(b))
            except ValueError:
                tcs.append("unparsable")
        gates["declared_processes"], processes = gate_processes(live[("ml-engine", "jobs.batch")], bench_pods, env,
                                                                archive_log, api_log, trainer_template, now, tcs)
        qual_bytes = blobs["qualification"][0][1] if "qualification" in blobs else b""
        qual_hours = [h for h in a.qualification_hours.split(",") if h]
        gates["qualification"], qualification = gate_qualification(qual_bytes, qual_hours,
                                                                   qualification_expectations(ctx))
        gates["qualification"] += qualification_after_joins(qual_hours, ident_raw)
        gates["generator_continuity"], generators = gate_generators(qualification, qual_hours, bench_pods)
        gates["fresh_issuances"], issuances = gate_fresh_issuances(
            blobs["issuances"][0][1] if "issuances" in blobs else b"", operator_log,
            (processes.get("artifact") or {}).get("training_check_sha256"),
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime((processes.get("ml-training") or {}).get("expected_slot") or 0)),
            now, ctx, sha(os.path.join(HERE, "scoring", "extract_decisions.sh")))
        gates["arms"] = gate_arms(live[("demo", "deployments.apps")],
                                  live[("demo", "predictiveautoscalers.autoscaler.example.com")],
                                  live[("demo", "scaledobjects.keda.sh")])
        gates["nodes"], nodes = gate_nodes(nodes_raw, workers, joined)
        disk_by_node = root_disk(disk, nodes)
        gates["telemetry"] = gate_telemetry(top.returncode, top.stdout, disk_by_node, workers, series, now, conflicts)
        identity = configuration_identity(ident0, rendered_meta, fingerprint, env, vm_static_identity(monitoring, vm_crs),
                                          placement_identity(nodes))
        if a.compare_with:
            gates["matches_previous"] = previous_problems + compare_identity(previous, identity)

        for (ns, kind), items in live.items():
            stage.json(f"objects/{ns}.{kind}.json", items)
        for ns in NAMESPACES:
            stage.json(f"objects/{ns}.pods.json", [strip(p) for p in bench_pods if p["metadata"]["namespace"] == ns])
        for when, ds in (("start", diffs0), ("end", diffs1)):
            for part, d in ds.items():
                stage.text(f"rendered/{part}.{when}.diff", d["text"])
        if qual_bytes:                                            # the bytes the gate parsed (sha in evidence)
            stage.text("evidence/qualification-attempts.jsonl", qual_bytes.decode(errors="replace"))
        for i, t in enumerate(tcs, 1):
            if isinstance(t, dict):
                stage.json(f"evidence/training-check-{i}.json", t)
        stage.text("evidence/operator-last-20min.log", operator_log)  # the bytes the fallback check read
        if "issuances" in blobs:                                  # the issuance records the gate read (sha in evidence)
            stage.text("evidence/issuances.jsonl", blobs["issuances"][0][1].decode(errors="replace"))
        capture = {
            "freeze_capture": VERSION, "started": started, "finished": now_iso(), "context": ctx,
            "tools": tools, "checkout": {"start": ident0, "end_matches": ident0 == ident1},
            "configuration_identity": identity, "compared_with": a.compare_with and {
                "path": os.path.abspath(a.compare_with), "verified": not previous_problems,
                "identity": ((previous or {}).get("configuration_identity") or {}).get("sha256")},
            "deploy_env": env, "rendered": rendered_meta, "evidence": evidence, "qualification": qualification,
            "generator_continuity": generators, "fresh_issuances": issuances,
            "render_diffs": {when: {part: {k: d[k] for k in ("exit", "lines")} for part, d in ds.items()}
                             for when, ds in (("start", diffs0), ("end", diffs1))},
            "envelope_config": cfg, "effective_env": effective, "runtime_pods": pod_rows,
            "declared_processes": processes, "p8_code": {"configmap": p8_name, "present": p8_present},
            "configmaps_written": {f"{ns}/{n}": {"updated": v["updated"], "provenance": v["provenance"]}
                                   for (ns, n), v in cm_raw.items()},
            "cronjobs": [{"name": c["metadata"]["name"], "schedule": c["spec"]["schedule"],
                          "suspend": c["spec"].get("suspend"), "concurrencyPolicy": c["spec"].get("concurrencyPolicy"),
                          "lastScheduleTime": c.get("status", {}).get("lastScheduleTime")}
                         for c in live[("ml-engine", "cronjobs.batch")]],
            "nodes": nodes, "node_requests_scheduler_rule": node_requests(pods_all),
            "node_usage_top": [l for l in top.stdout.splitlines() if l.strip()], "root_disk": disk_by_node,
            "evicted_events_retained_snapshot": [{"ns": e["metadata"]["namespace"],
                                                  "object": e.get("involvedObject", {}).get("name"),
                                                  "last": e.get("lastTimestamp"), "message": e.get("message")}
                                                 for e in evicted],
            "victoriametrics": {"containers": monitoring, "crs": vm_crs}, "bench_series": series,
            "unreferenced_configmaps": sorted(f"{ns}/{c['metadata']['name']}" for ns in NAMESPACES
                                              for c in live[(ns, "configmaps")] if c["metadata"]["name"] not in referenced),
        }
        redact(capture, stage.found, "capture")
        gates["no_secret_values"] = [f"credential-like literal redacted: {p}" for p in stage.found]
        capture["gates"] = {k: {"status": "PASS" if not v else "FAIL", "problems": v} for k, v in gates.items()}
        missing = [g for g in MANDATORY_GATES if g not in gates]
        if missing:
            raise SystemExit(f"internal error: gates not evaluated: {missing}")
        capture["verdict"] = "PASS" if not any(gates.values()) else "FAIL"
        stage.json("capture.json", capture)
        stage.publish({"verdict": capture["verdict"], "finished": now_iso(),
                       "configuration_identity": identity["sha256"],
                       "capture_sha256": sha(os.path.join(stage.dir, "capture.json"))})
        print(json.dumps({"out": a.out, "verdict": capture["verdict"], "identity": identity["sha256"],
                          "gates": {k: v["status"] for k, v in capture["gates"].items()},
                          "problems": {k: v for k, v in gates.items() if v}}, indent=1))
    except BaseException:
        stage.abort()
        raise
    return 0 if capture["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
