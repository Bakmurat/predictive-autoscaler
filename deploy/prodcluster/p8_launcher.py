#!/usr/bin/env python3
"""In-cluster launcher of the P8 detector (deploy/prodcluster/p8-job.sh; protocol P8 v3.1).

Modes (one run = /evidence/<P8_RUN>/, held by a run-level lock while a mode runs):
  export   raw chunked export + preliminary result; refuses when the run already has an attempt (one attempt per run)
  resume   continues the run's single attempt (verified chunks reused)
  gates    copies, for every hour the window needs (start - 1 h … stop - 1 h), the hour's ONE retained final attempt
           (/gates/attempts/, Codex r89) into a temporary directory, validates the COPIED bytes (exactly the six arm
           rows per hour, each finalized; any verdict — PASS, FAIL or INCOMPLETE — is admissible), then publishes them as
           an immutable, hashed gates-<id>/. An hour with more than one final attempt (conflicting finals) or any
           malformed attempt (unreadable, wrong arms or hour, missing/non-boolean or mixed finality; Codex r90) publishes
           nothing. An hour with no final attempt (all its attempts valid but non-final, or none) is published as an
           explicit "unfinalized" slot without a gate file, so the detector classifies it unverified (Codex r97). The mutable projection load-<hour>.json is never the source; the manifest records whether
           it still equals the final attempt, and the name and hash of every attempt collected after the final one.
  final    re-verifies the newest gates directory against the requested window (exact hour set, no duplicates, hashes,
           every row re-validated), then re-analyses the run's attempt offline with it and, when mounted, the frozen
           attribution file; writes final-<id>.json (unique names; nothing is ever replaced)
The detector runs unchanged (runpy); its result binds its own hash, the identity file and the mask.
"""
import calendar, fcntl, glob, hashlib, json, os, runpy, shutil, sys, tempfile, time

ARMS = ("nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95")
HOUR = 3600
# Collector builds that predate the maturity contract (868ba78, 2026-10-05T11:27Z): their attempts carry no maturity
# field in any row, are never final, and are listed in the gates manifest instead of blocking it (Codex r90; the 11
# attempts of 2026-10-05 11:10-11:14Z). Any other attempt without boolean finality blocks publication.
PRE_MATURITY_COLLECTORS = {"fb725ca60bddb70135cfd5aecf31101fd5d1dbdc5eb05201da026aa1390cca1b"}
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


def attempt_finality(path, h):
    """(finalized, problem) of one retained attempt of hour h (Codex r90: every attempt is validated, fail closed).
    A well-formed attempt holds exactly the six arms once each, all for hour h, each with a boolean
    maturity.finalized, and the same value in every row; anything else is a problem that blocks publication.
    The one exception is an attempt of a PRE_MATURITY_COLLECTORS build (full pinned digest in every row, the maturity
    field absent in EVERY row, valid arms and hour): it is classified "pre-maturity" and never supplies a final."""
    name = os.path.basename(path)
    try:
        rows = json.load(open(path))
    except Exception as e:
        return None, f"{name}: unreadable ({e})"
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        return None, f"{name}: not a list of rows"
    apps = [r.get("app") for r in rows]
    if len(apps) != len(set(apps)) or sorted(apps, key=str) != sorted(ARMS):
        return None, f"{name}: arms {apps}"
    try:
        other_hour = any(epoch(r["hour_start"]) != h for r in rows)
    except Exception:
        other_hour = True
    if other_hour:
        return None, f"{name}: a row for another or no hour"
    if all("maturity" not in r and r.get("collector_sha256") in PRE_MATURITY_COLLECTORS for r in rows):
        return "pre-maturity", None
    flags = [(r.get("maturity") or {}).get("finalized") if isinstance(r.get("maturity"), dict) else None for r in rows]
    if not all(isinstance(f, bool) for f in flags):
        return None, f"{name}: missing or non-boolean maturity.finalized"
    if len(set(flags)) != 1:
        return None, f"{name}: mixed finality"
    return flags[0], None


def hour_attempts(src, h):
    """(final, later, pre, problems) for hour h in <src>/attempts/: final = well-formed attempts whose rows are all
    finalized (the evaluation made in the sixth scheduled window); later = well-formed non-final attempts collected
    after a final one; pre = attempts of a PRE_MATURITY_COLLECTORS build (later and pre are informational: listed with
    their hashes, never selected); problems = malformed attempts."""
    tag = hour_name(h)[:-len(".json")]
    final, other, pre, problems = [], [], [], []
    for p in sorted(glob.glob(os.path.join(src, "attempts", tag + "-*.json"))):
        done, problem = attempt_finality(p, h)
        if problem:
            problems.append(problem)
        elif done == "pre-maturity":
            pre.append(p)
        else:
            (final if done else other).append(p)
    later = [p for p in other if final and os.path.basename(p) > os.path.basename(final[0])]
    return final, later, pre, problems


def copy_gates(src, rd, start, stop):
    hours = window_hours(start, stop)
    tmp = tempfile.mkdtemp(dir=rd, prefix=".gates-")
    manifest, problems = [], []
    for h in hours:
        final, later, pre, bad = hour_attempts(src, h)
        problems += [f"{hour_name(h)}: {b}" for b in bad]
        if len(final) > 1:
            problems.append(f"{hour_name(h)}: {len(final)} final attempts (conflicting finals {[os.path.basename(p) for p in final]})")
        if bad or len(final) > 1:
            continue
        if not final:
            # Codex r97 (P9 amendment 2026-10-15): no final attempt, every retained attempt valid but non-final (or
            # none at all) -> an explicit unfinalized slot. No gate file is published for it, so the detector classifies
            # the hour unverified (all six arms INCOMPLETE); no provisional result is selected and nothing is invented.
            kept = sorted(glob.glob(os.path.join(src, "attempts", hour_name(h)[:-len(".json")] + "-*.json")))
            manifest.append({"file": hour_name(h), "status": "unfinalized",
                             "attempts": [{"file": os.path.basename(p), "sha256": sha(p)} for p in kept]})
            continue
        dst = os.path.join(tmp, hour_name(h))
        shutil.copyfile(final[0], dst)
        with open(dst, "rb") as fh:
            os.fsync(fh.fileno())
        problems += check_gate_file(dst, h)                  # the bytes that are kept are the bytes validated
        proj = os.path.join(src, hour_name(h))
        manifest.append({"file": hour_name(h), "status": "final", "sha256": sha(dst), "bytes": os.path.getsize(dst),
                         "source_attempt": os.path.basename(final[0]),
                         "projection_matches": os.path.exists(proj) and sha(proj) == sha(dst),
                         "later_attempts": [{"file": os.path.basename(p), "sha256": sha(p)} for p in later],
                         "pre_maturity_attempts": [{"file": os.path.basename(p), "sha256": sha(p)} for p in pre]})
    if problems:
        shutil.rmtree(tmp)
        sys.exit("gate rows not admissible, nothing published:\n  " + "\n  ".join(problems))
    with open(os.path.join(tmp, "manifest.json"), "w") as fh:
        json.dump({"window": [start, stop], "hours": len(hours), "files": manifest,
                   "unfinalized": [f["file"] for f in manifest if f["status"] == "unfinalized"]}, fh, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    final_dir = fresh(os.path.join(rd, "gates-" + stamp()))
    os.rename(tmp, final_dir)
    print(json.dumps({"gates": final_dir, "hours": len(hours)}))


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
    slots = sorted(f["file"] for f in m["files"] if f.get("status") == "unfinalized")
    if m.get("unfinalized") != slots:                # the report's list of unfinalized hours must be the manifest's
        sys.exit(f"{d}: the unfinalized list {m.get('unfinalized')} does not match the slots {slots}")
    problems, published = [], []
    for f, h in zip(sorted(m["files"], key=lambda f: f["file"]), sorted(hours)):
        p = os.path.join(d, f["file"])
        if f.get("status") == "unfinalized":
            if os.path.exists(p):                    # an unfinalized slot never carries a gate file (no later rerun)
                sys.exit(f"{p} exists for an unfinalized slot")
            tag = f["file"][:-len(".json")] + "-"
            atts = f.get("attempts")
            if not isinstance(atts, list) or not all(
                    isinstance(a, dict) and isinstance(a.get("file"), str) and a["file"].startswith(tag)
                    and isinstance(a.get("sha256"), str) and len(a["sha256"]) == 64
                    and all(c in "0123456789abcdef" for c in a["sha256"]) for a in atts):
                sys.exit(f"{f['file']}: malformed attempts list in an unfinalized slot")
            continue
        if f.get("status") != "final":
            sys.exit(f"{f['file']}: unknown manifest status {f.get('status')!r}")
        published.append(f["file"])
        if sha(p) != f["sha256"] or os.path.getsize(p) != f["bytes"]:
            sys.exit(f"gates file {p} does not match its manifest")
        problems += check_gate_file(p, h)
    if problems:
        sys.exit("gate rows not admissible:\n  " + "\n  ".join(problems))
    extra = set(os.listdir(d)) - set(published) - {"manifest.json"}
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
