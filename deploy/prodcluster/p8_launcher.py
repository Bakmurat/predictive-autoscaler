#!/usr/bin/env python3
"""In-cluster launcher of the P8 detector (deploy/prodcluster/p8-job.sh; protocol P8 v3.1).

Modes (one run = /evidence/<P8_RUN>/, held by a run-level lock while a mode runs):
  export   raw chunked export + preliminary result; refuses when the run already has an attempt (one attempt per run)
  resume   continues the run's single attempt (verified chunks reused)
  gates    copies the load-gate projections of every hour the window needs (start - 1 h … stop - 1 h) from /gates into
           a temporary directory, validates the COPIED bytes (exactly the six arm rows per hour, each finalized; any
           verdict — PASS, FAIL or INCOMPLETE — is admissible), then publishes them as an immutable, hashed gates-<id>/
  final    re-verifies the newest gates directory against the requested window (exact hour set, no duplicates, hashes,
           every row re-validated), then re-analyses the run's attempt offline with it and, when mounted, the frozen
           attribution file; writes final-<id>.json (unique names; nothing is ever replaced)
The detector runs unchanged (runpy); its result binds its own hash, the identity file and the mask.
"""
import calendar, fcntl, glob, hashlib, json, os, runpy, shutil, sys, tempfile, time

ARMS = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
HOUR = 3600
CODE = "/opt/p8"


def stamp():
    """A unique, sortable revision id (UTC to the microsecond + pid)."""
    t = time.time()
    return time.strftime("%Y%m%dT%H%M%S", time.gmtime(t)) + "%06dZ-%d" % (int(t % 1 * 1e6), os.getpid())


def fresh(path):
    if os.path.exists(path):
        sys.exit(f"{path} exists; refusing to replace it")
    return path


def window_hours(start, stop):
    return list(range(start - start % HOUR - HOUR, stop, HOUR))


def epoch(s):
    return calendar.timegm(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S"))


def hour_name(h):
    return "load-" + time.strftime("%Y%m%dT%H00Z", time.gmtime(h)) + ".json"


def sha(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def check_gate_file(path, h):
    """Exactly the six arms, one row each, all finalized, for hour h; returns problems (empty = admissible)."""
    try:
        rows = json.load(open(path))
    except (OSError, ValueError) as e:
        return [f"{os.path.basename(path)}: unreadable ({e})"]
    apps = [r.get("app") for r in rows]
    problems = []
    if sorted(apps) != sorted(ARMS):
        problems.append(f"{os.path.basename(path)}: arms {sorted(apps)}")
    for r in rows:
        if epoch(r.get("hour_start", "1970-01-01T00:00:00Z")) != h:
            problems.append(f"{os.path.basename(path)}: row for {r.get('hour_start')}")
        if (r.get("maturity") or {}).get("finalized") is not True:
            problems.append(f"{os.path.basename(path)}: {r.get('app')} not finalized")
    return problems


def copy_gates(src, rd, start, stop):
    hours = window_hours(start, stop)
    missing = [hour_name(h) for h in hours if not os.path.exists(os.path.join(src, hour_name(h)))]
    if missing:
        sys.exit("gate rows missing, nothing published: " + ", ".join(missing))
    tmp = tempfile.mkdtemp(dir=rd, prefix=".gates-")
    manifest, problems = [], []
    for h in hours:
        dst = os.path.join(tmp, hour_name(h))
        shutil.copyfile(os.path.join(src, hour_name(h)), dst)
        with open(dst, "rb") as fh:
            os.fsync(fh.fileno())
        problems += check_gate_file(dst, h)                  # the bytes that are kept are the bytes validated
        manifest.append({"file": hour_name(h), "sha256": sha(dst), "bytes": os.path.getsize(dst)})
    if problems:
        shutil.rmtree(tmp)
        sys.exit("gate rows not admissible, nothing published:\n  " + "\n  ".join(problems))
    with open(os.path.join(tmp, "manifest.json"), "w") as fh:
        json.dump({"window": [start, stop], "hours": len(hours), "files": manifest}, fh, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    final = fresh(os.path.join(rd, "gates-" + stamp()))
    os.rename(tmp, final)
    print(json.dumps({"gates": final, "hours": len(hours)}))


def verified_gates(rd, start, stop):
    """The newest gates directory, re-verified for exactly this window: hour set, no duplicates, hashes, rows."""
    dirs = sorted(glob.glob(os.path.join(rd, "gates-*")))
    if not dirs:
        sys.exit("no gates directory; run the gates mode first")
    d = dirs[-1]
    m = json.load(open(os.path.join(d, "manifest.json")))
    hours = window_hours(start, stop)
    names = [f["file"] for f in m.get("files", [])]
    if m.get("window") != [start, stop] or m.get("hours") != len(hours):
        sys.exit(f"{d} was made for window {m.get('window')}, not {[start, stop]}")
    if len(names) != len(set(names)) or sorted(names) != sorted(hour_name(h) for h in hours):
        sys.exit(f"{d} does not hold exactly the window's hours")
    problems = []
    for f, h in zip(sorted(m["files"], key=lambda f: f["file"]), sorted(hours)):
        p = os.path.join(d, f["file"])
        if sha(p) != f["sha256"] or os.path.getsize(p) != f["bytes"]:
            sys.exit(f"gates file {p} does not match its manifest")
        problems += check_gate_file(p, h)
    if problems:
        sys.exit("gate rows not admissible:\n  " + "\n  ".join(problems))
    extra = set(os.listdir(d)) - set(names) - {"manifest.json"}
    if extra:
        sys.exit(f"unexpected files in {d}: {sorted(extra)}")
    return d


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    mode, run = argv[0], os.environ["P8_RUN"]
    start_s, stop_s, prom = os.environ["P8_START"], os.environ["P8_STOP"], os.environ["P8_PROM"]
    evidence = os.environ.get("P8_EVIDENCE", "/evidence")
    rd = os.path.join(evidence, run)
    os.makedirs(rd, exist_ok=True)
    lock = open(os.path.join(rd, ".run.lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(f"another mode holds run {run}")
    attempts = sorted(glob.glob(os.path.join(rd, "attempt-*")))
    common = ["--prom", prom, "--start", start_s, "--stop", stop_s, "--identities", f"{CODE}/infra-identities.json",
              "--mask", f"{CODE}/validity-mask.json", "--archive-root", rd]
    if mode == "gates":
        return copy_gates(os.environ.get("P8_GATES", "/gates"), rd, epoch(start_s), epoch(stop_s))
    if mode == "export":
        if attempts:
            sys.exit(f"run {run} already has {len(attempts)} attempt(s); use resume")
        args = common + ["--json", fresh(os.path.join(rd, f"preliminary-{stamp()}.json"))]
    elif mode in ("resume", "final"):
        if len(attempts) != 1:
            sys.exit(f"run {run} needs exactly one attempt, found {len(attempts)}")
        if mode == "resume":
            args = common + ["--resume", attempts[0], "--json", fresh(os.path.join(rd, f"preliminary-{stamp()}.json"))]
        else:
            args = common + ["--reanalyze", attempts[0], "--load-gate-dir",
                             verified_gates(rd, epoch(start_s), epoch(stop_s)),
                             "--json", fresh(os.path.join(rd, f"final-{stamp()}.json"))]
            if os.path.exists("/opt/p8-attr/attributions.json"):
                args += ["--attributions", "/opt/p8-attr/attributions.json"]
    else:
        sys.exit("mode: export|resume|gates|final")
    sys.argv = [f"{CODE}/infra_events.py"] + args
    runpy.run_path(f"{CODE}/infra_events.py", run_name="__main__")


if __name__ == "__main__":
    main()
