#!/usr/bin/env python3
"""In-cluster evidence archive for the benchmark's rolling retraining.

Runs as a CronJob next to the model volume. Each run does two independent things and records both:

1. Verified-pair archiving. The trainer publishes the artifact and its provenance sidecar by two
   separate renames, so at any instant the two files may describe different trainings. This script
   stages both, requires the sidecar's full artifact_sha256 (and artifact_bytes) to equal the staged
   bytes, re-hashes the source pair after the copy to prove it did not change, and only then renames
   the staging directory into archive/<cutoff>-<sha12>/ and appends one line to INDEX.jsonl.
   A mismatched, incomplete or changing pair is logged to REJECTS.jsonl and never finalized. Re-runs
   are idempotent. A crash between the rename and the index line is repaired on the next run.

2. Log preservation. Completed training Pods: full timestamped log plus Job/Pod UID, terminal status,
   exit code and imageID, written once. API Pods: the "Loaded/Reloaded model ... sha256=" lines with pod
   name and restart count, plus a coverage record per collection (the window it covers) so a restart,
   a replaced pod or a failed collection shows up as a flagged gap rather than as silence.

Polling every 15 minutes cannot guarantee that every intermediate publication is seen; the reader
(reconcile) compares training Jobs, API reload lines and issuance hashes against this archive.
Standard library only.
"""
import datetime
import hashlib
import json
import os
import re
import shutil
import ssl
import sys
import urllib.parse
import urllib.request
import uuid

VERSION = "1"
RELOAD_RE = re.compile(r"(Loaded|Reloaded) model")


class CollectionError(Exception):
    pass


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_ts(s):
    """RFC3339 with any fractional precision (Kubernetes log timestamps carry nanoseconds)."""
    s = s.strip()
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})$", s)
    if not m:
        raise ValueError(f"not an RFC3339 timestamp: {s!r}")
    base, frac, tz = m.groups()
    frac = (frac or ".0")[1:7].ljust(6, "0")
    tz = "+00:00" if tz == "Z" else tz
    return datetime.datetime.fromisoformat(f"{base}.{frac}{tz}")


def mtime_iso(ns):
    return datetime.datetime.fromtimestamp(ns / 1e9, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def fsync_dir(path):
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def append_jsonl(path, rec):
    with open(path, "a") as fh:
        fh.write(json.dumps(rec, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def read_jsonl(path):
    out = []
    if os.path.exists(path):
        with open(path) as fh:
            for ln in fh:
                ln = ln.strip()
                if ln:
                    try:
                        out.append(json.loads(ln))
                    except ValueError:
                        out.append({"__unparsable__": ln})
    return out


def write_atomic(path, data):
    tmp = f"{path}.tmp-{uuid.uuid4().hex[:8]}"
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.rename(tmp, path)
    fsync_dir(os.path.dirname(path))


def sha_file(path):
    h = hashlib.sha256()
    n = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n


def sig(path):
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    return (st.st_ino, st.st_size, st.st_mtime_ns)


def compact_cutoff(cutoff):
    return parse_ts(cutoff).astimezone(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------------------------------
# 1. verified-pair archiving
# ---------------------------------------------------------------------------------------------------

def archive_pair(src, arc, art_name, side_name, ident, after_copy_hook=None, now=utcnow):
    staging_root = os.path.join(arc, ".staging")
    final_root = os.path.join(arc, "archive")
    os.makedirs(staging_root, exist_ok=True)
    os.makedirs(final_root, exist_ok=True)
    index_p = os.path.join(arc, "INDEX.jsonl")
    rejects_p = os.path.join(arc, "REJECTS.jsonl")
    art_p, side_p = os.path.join(src, art_name), os.path.join(src, side_name)
    started = now()

    def reject(reason, **extra):
        rec = {"event": "rejected", "at": now(), "reason": reason, **ident, **extra}
        append_jsonl(rejects_p, rec)
        return {"result": "rejected", "reason": reason, **extra}

    sa0, ss0 = sig(art_p), sig(side_p)
    if sa0 is None or ss0 is None:
        missing = [n for n, s in ((art_name, sa0), (side_name, ss0)) if s is None]
        return reject("incomplete pair: missing " + ", ".join(missing))

    stage = os.path.join(staging_root, f"{os.getpid()}-{uuid.uuid4().hex[:12]}")
    os.makedirs(stage)
    try:
        with open(side_p, "rb") as fh:
            side_bytes = fh.read()
        h = hashlib.sha256()
        size = 0
        with open(art_p, "rb") as fi, open(os.path.join(stage, art_name), "wb") as fo:
            for chunk in iter(lambda: fi.read(1 << 20), b""):
                h.update(chunk)
                size += len(chunk)
                fo.write(chunk)
            fo.flush()
            os.fsync(fo.fileno())
        staged_sha = h.hexdigest()
        with open(os.path.join(stage, side_name), "wb") as fo:
            fo.write(side_bytes)
            fo.flush()
            os.fsync(fo.fileno())
        side_sha = hashlib.sha256(side_bytes).hexdigest()

        if after_copy_hook:
            after_copy_hook()

        # stability of the SOURCE pair across the copy
        sa1, ss1 = sig(art_p), sig(side_p)
        try:
            src_sha_after, _ = sha_file(art_p)
            with open(side_p, "rb") as fh:
                side_after = fh.read()
        except FileNotFoundError:
            return reject("pair changed during copy: a source file disappeared",
                          staged_artifact_sha256=staged_sha)
        if sa1 != sa0 or src_sha_after != staged_sha:
            return reject("artifact changed during copy", staged_artifact_sha256=staged_sha,
                          source_artifact_sha256_after=src_sha_after)
        if ss1 != ss0 or side_after != side_bytes:
            return reject("sidecar changed during copy", staged_artifact_sha256=staged_sha)

        try:
            meta = json.loads(side_bytes)
        except ValueError:
            return reject("sidecar unparsable", staged_artifact_sha256=staged_sha, sidecar_sha256=side_sha)
        if not isinstance(meta, dict):
            return reject("sidecar is not an object", staged_artifact_sha256=staged_sha)
        claimed = meta.get("artifact_sha256")
        if claimed != staged_sha:
            return reject("sidecar artifact_sha256 does not equal the staged artifact's hash",
                          staged_artifact_sha256=staged_sha, sidecar_artifact_sha256=claimed,
                          sidecar_sha256=side_sha)
        if meta.get("artifact_bytes") is not None and meta.get("artifact_bytes") != size:
            return reject("sidecar artifact_bytes does not equal the staged size",
                          staged_artifact_sha256=staged_sha, staged_bytes=size,
                          sidecar_artifact_bytes=meta.get("artifact_bytes"))
        if meta.get("artifact") not in (None, art_name):
            return reject("sidecar names a different artifact file", staged_artifact_sha256=staged_sha)
        try:
            cutoff_c = compact_cutoff(meta.get("training_cutoff") or "")
        except ValueError:
            return reject("sidecar has no parsable training_cutoff", staged_artifact_sha256=staged_sha)

        key = f"{cutoff_c}-{staged_sha[:12]}"
        record = {"key": key, "copied_at": now(), "started_at": started,
                  "artifact": art_name, "artifact_sha256": staged_sha, "artifact_bytes": size,
                  "sidecar_sha256": side_sha, "training_cutoff": meta.get("training_cutoff"),
                  "trained_at_per_sidecar": meta.get("trained_at"),
                  "source_mtime_artifact": mtime_iso(sa0[2]), "source_mtime_sidecar": mtime_iso(ss0[2]),
                  "note": "source mtimes are not publication instants; see the trainer log",
                  "archiver_version": VERSION, **ident}
        final = os.path.join(final_root, key)
        indexed = {(r.get("key"), r.get("sidecar_sha256")) for r in read_jsonl(index_p)}

        if os.path.isdir(final):
            try:
                with open(os.path.join(final, "manifest.json")) as fh:
                    man = json.load(fh)
                have_art, _ = sha_file(os.path.join(final, art_name))
            except (OSError, ValueError):
                return reject("conflict: existing archive directory is unreadable or incomplete", key=key)
            if have_art != staged_sha or man.get("artifact_sha256") != staged_sha:
                return reject("conflict: existing archive differs from the staged artifact (not overwritten)",
                              key=key, staged_artifact_sha256=staged_sha, archived_artifact_sha256=have_art)
            if man.get("sidecar_sha256") == side_sha:
                if (key, side_sha) in indexed:
                    return {"result": "already_archived", "key": key}
                append_jsonl(index_p, {"event": "archived", **man, "index_recovered": True, "recovered_at": now()})
                return {"result": "index_recovered", "key": key}
            # same artifact, different sidecar: keep it, separately and immutably
            vkey = f"{key}-sidecar-{side_sha[:12]}"
            vfinal = os.path.join(final_root, vkey)
            vrec = dict(record, key=vkey, artifact_key=key, event="sidecar_variant")
            if os.path.isdir(vfinal):
                if (vkey, side_sha) in indexed:
                    return {"result": "already_archived", "key": vkey}
                append_jsonl(index_p, {**vrec, "index_recovered": True})
                return {"result": "index_recovered", "key": vkey}
            os.unlink(os.path.join(stage, art_name))
            write_atomic(os.path.join(stage, "manifest.json"), json.dumps(vrec, indent=1, sort_keys=True).encode())
            os.rename(stage, vfinal)
            fsync_dir(final_root)
            append_jsonl(index_p, vrec)
            return {"result": "sidecar_variant", "key": vkey}

        record["event"] = "archived"
        write_atomic(os.path.join(stage, "manifest.json"), json.dumps(record, indent=1, sort_keys=True).encode())
        fsync_dir(stage)
        try:
            os.rename(stage, final)
        except OSError:
            if os.path.isdir(final):   # a concurrent run finalized first; the next run verifies it
                return {"result": "already_archived", "key": key, "note": "concurrent finalize"}
            raise
        fsync_dir(final_root)
        append_jsonl(index_p, record)
        return {"result": "archived", "key": key}
    finally:
        if os.path.isdir(stage):
            shutil.rmtree(stage, ignore_errors=True)


# ---------------------------------------------------------------------------------------------------
# 2. log preservation
# ---------------------------------------------------------------------------------------------------

def _container_status(pod, name=None):
    for cs in (pod.get("status") or {}).get("containerStatuses") or []:
        if name is None or cs.get("name") == name:
            return cs
    return {}


def _terminated(pod):
    css = (pod.get("status") or {}).get("containerStatuses") or []
    return (pod.get("status") or {}).get("phase") in ("Succeeded", "Failed") and css and \
        all("terminated" in (cs.get("state") or {}) for cs in css)


def collect_trainer_logs(api, arc, selector, ident, now):
    d = os.path.join(arc, "logs", "trainer")
    os.makedirs(d, exist_ok=True)
    new, errors = [], []
    for pod in api.list_pods(selector):
        md = pod["metadata"]
        if not _terminated(pod):
            continue
        job = next((o for o in md.get("ownerReferences") or [] if o.get("kind") == "Job"), {})
        jname = job.get("name") or md.get("labels", {}).get("job-name") or "nojob"
        base = f"{jname}-{md['uid'][:8]}"
        meta_p = os.path.join(d, base + ".json")
        if os.path.exists(meta_p):
            continue
        try:
            containers = []
            for cs in pod["status"]["containerStatuses"]:
                text = api.pod_log(md["name"], cs["name"])
                write_atomic(os.path.join(d, f"{base}.{cs['name']}.log"), text.encode())
                prev = None
                if cs.get("restartCount", 0) > 0:
                    try:
                        prev = api.pod_log(md["name"], cs["name"], previous=True)
                        write_atomic(os.path.join(d, f"{base}.{cs['name']}.previous.log"), prev.encode())
                    except CollectionError as e:
                        errors.append(f"previous log of {md['name']}: {e}")
                t = cs["state"]["terminated"]
                containers.append({"name": cs["name"], "exit_code": t.get("exitCode"), "reason": t.get("reason"),
                                   "started_at": t.get("startedAt"), "finished_at": t.get("finishedAt"),
                                   "image": cs.get("image"), "image_id": cs.get("imageID"),
                                   "restart_count": cs.get("restartCount", 0),
                                   "log_sha256": hashlib.sha256(text.encode()).hexdigest(),
                                   "log_lines": len(text.splitlines()),
                                   "previous_log_saved": prev is not None})
            jterm, jdone, juid = None, None, job.get("uid")
            try:
                j = api.get_job(jname)
                juid = (j.get("metadata") or {}).get("uid") or juid
                for c in (j.get("status") or {}).get("conditions") or []:
                    if c.get("status") == "True" and c.get("type") in ("Complete", "Failed"):
                        jterm = c["type"]
                jdone = (j.get("status") or {}).get("completionTime")
            except Exception as e:  # the Job may already be gone; the pod record still stands
                errors.append(f"job {jname}: {e}")
            meta = {"collected_at": now(), "job": jname, "job_uid": juid, "job_terminal": jterm,
                    "job_completion_time": jdone, "pod": md["name"], "pod_uid": md["uid"],
                    "node": (pod.get("spec") or {}).get("nodeName"), "phase": pod["status"]["phase"],
                    "containers": containers, "collector": ident, "archiver_version": VERSION}
            write_atomic(meta_p, json.dumps(meta, indent=1, sort_keys=True).encode())
            append_jsonl(os.path.join(arc, "logs", "trainer-index.jsonl"), meta)
            new.append(base)
        except CollectionError as e:
            errors.append(f"trainer {md['name']}: {e}")
    return new, errors


def _lines_after(text, after_ts):
    out = []
    for ln in text.splitlines():
        if not ln.strip():
            continue
        head = ln.split(" ", 1)
        try:
            t = parse_ts(head[0])
        except ValueError:
            continue
        if after_ts is None or t > after_ts:
            out.append((t, head[0], head[1] if len(head) > 1 else ""))
    return out


def collect_api_reloads(api, arc, selector, container, ident, now):
    d = os.path.join(arc, "logs")
    os.makedirs(d, exist_ok=True)
    state_p = os.path.join(d, "api-state.json")
    state = {}
    if os.path.exists(state_p):
        with open(state_p) as fh:
            state = json.load(fh)
    reload_p, cov_p = os.path.join(d, "api-reload.jsonl"), os.path.join(d, "api-coverage.jsonl")
    collected_at = now()
    errors, new_lines = [], 0
    pods = api.list_pods(selector)
    seen = set()
    for pod in pods:
        md = pod["metadata"]
        uid, name = md["uid"], md["name"]
        seen.add(uid)
        cs = _container_status(pod, container)
        rc = cs.get("restartCount", 0)
        started = ((cs.get("state") or {}).get("running") or {}).get("startedAt")
        st = state.get(uid)
        flags = []
        last_ts = parse_ts(st["last_ts"]) if st and st.get("last_ts") else None
        window_start = st["window_end"] if st else started
        got = []
        try:
            if st and rc > st["restart_count"]:
                if rc == st["restart_count"] + 1:
                    prev = api.pod_log(name, container, previous=True)
                    got += [(t, s, ln, "previous") for t, s, ln in _lines_after(prev, last_ts)]
                    flags.append(f"restart {st['restart_count']}->{rc}: previous instance log recovered")
                else:
                    flags.append(f"GAP: {rc - st['restart_count']} restarts since {st['window_end']}; "
                                 "only the last previous instance is retrievable, earlier instances are lost")
                    try:
                        prev = api.pod_log(name, container, previous=True)
                        got += [(t, s, ln, "previous") for t, s, ln in _lines_after(prev, last_ts)]
                    except CollectionError:
                        pass
                since = None   # the current instance is read from its start
            else:
                # resume from the last LINE timestamp seen (node clock), not from the collector's clock,
                # so skew between the two cannot drop a line; ">" below removes the overlap
                since = st.get("last_ts") if st else None
                if not st and state:
                    flags.append(f"new pod: covered from its container start {started}")
            cur = api.pod_log(name, container, since_time=since)
            got += [(t, s, ln, "current") for t, s, ln in _lines_after(cur, last_ts if since else None)]
        except CollectionError as e:
            errors.append(f"api {name}: {e}")
            flags.append(f"COLLECTION FAILED: {e}")
            append_jsonl(cov_p, {"collected_at": collected_at, "pod": name, "pod_uid": uid, "restart_count": rc,
                                 "container_started_at": started, "window_start": window_start,
                                 "window_end": None, "lines": 0, "reload_lines": 0, "flags": flags,
                                 "collector": ident})
            continue   # state unchanged: the next run retries the same window
        got.sort(key=lambda x: x[0])
        rel = 0
        for t, s, ln, inst in got:
            if RELOAD_RE.search(ln):
                append_jsonl(reload_p, {"ts": s, "pod": name, "pod_uid": uid, "restart_count": rc,
                                        "instance": inst, "line": ln.strip(), "collected_at": collected_at})
                rel += 1
        cur_lines = [g for g in got if g[3] == "current"]
        if cur_lines and window_start:
            try:
                lag = (cur_lines[0][0] - parse_ts(window_start)).total_seconds()
                if since and lag > 1200:
                    flags.append(f"possible log rotation: first line {lag:.0f}s after window start")
            except ValueError:
                pass
        append_jsonl(cov_p, {"collected_at": collected_at, "pod": name, "pod_uid": uid, "restart_count": rc,
                             "container_started_at": started, "window_start": window_start,
                             "window_end": collected_at, "first_line_ts": got[0][1] if got else None,
                             "last_line_ts": got[-1][1] if got else None, "lines": len(got),
                             "reload_lines": rel, "flags": flags, "collector": ident})
        new_lines += rel
        state[uid] = {"pod": name, "restart_count": rc, "window_end": collected_at,
                      "last_ts": got[-1][1] if got else (st or {}).get("last_ts"), "gone": False}
    for uid, st in state.items():
        if uid not in seen and not st.get("gone"):
            append_jsonl(cov_p, {"collected_at": collected_at, "pod": st["pod"], "pod_uid": uid,
                                 "restart_count": st["restart_count"], "window_start": st["window_end"],
                                 "window_end": None, "lines": 0, "reload_lines": 0,
                                 "flags": [f"GAP: pod gone; lines after {st['window_end']} are lost"],
                                 "collector": ident})
            st["gone"] = True
    write_atomic(state_p, json.dumps(state, indent=1, sort_keys=True).encode())
    return new_lines, errors


def collect_logs(api, arc, namespace, trainer_selector, api_selector, api_container, ident, now=utcnow):
    new_tr, e1 = collect_trainer_logs(api, arc, trainer_selector, ident, now)
    new_api, e2 = collect_api_reloads(api, arc, api_selector, api_container, ident, now)
    return {"trainer_logs_new": new_tr, "api_reload_lines_new": new_api, "errors": e1 + e2}


# ---------------------------------------------------------------------------------------------------
# Kubernetes API client (in-cluster service account; get/list pods, pods/log, jobs in one namespace)
# ---------------------------------------------------------------------------------------------------

class KubeAPI:
    SA = "/var/run/secrets/kubernetes.io/serviceaccount"

    def __init__(self, namespace, server="https://kubernetes.default.svc"):
        self.ns, self.server = namespace, server
        with open(os.path.join(self.SA, "token")) as fh:
            self.token = fh.read().strip()
        self.ctx = ssl.create_default_context(cafile=os.path.join(self.SA, "ca.crt"))

    def _get(self, path, params=None, raw=False):
        url = self.server + path + ("?" + urllib.parse.urlencode(params) if params else "")
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + self.token})
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=30) as r:
                body = r.read()
        except Exception as e:
            raise CollectionError(f"GET {path}: {e}")
        return body.decode("utf-8", "replace") if raw else json.loads(body)

    def list_pods(self, selector):
        return self._get(f"/api/v1/namespaces/{self.ns}/pods", {"labelSelector": selector})["items"]

    def pod_log(self, name, container, since_time=None, previous=False):
        p = {"container": container, "timestamps": "true"}
        if since_time:
            p["sinceTime"] = parse_ts(since_time).strftime("%Y-%m-%dT%H:%M:%SZ")
        if previous:
            p["previous"] = "true"
        return self._get(f"/api/v1/namespaces/{self.ns}/pods/{name}/log", p, raw=True)

    def get_job(self, name):
        return self._get(f"/apis/batch/v1/namespaces/{self.ns}/jobs/{name}")


def run(src, arc, art_name, side_name, api, namespace, trainer_selector, api_selector, api_container, ident,
        now=utcnow):
    started = now()
    errors = []
    try:
        pair = archive_pair(src, arc, art_name, side_name, ident, now=now)
    except Exception as e:
        pair = {"result": "error", "reason": repr(e)}
        errors.append(f"pair: {e!r}")
    try:
        logs = collect_logs(api, arc, namespace, trainer_selector, api_selector, api_container, ident, now=now)
        errors += logs["errors"]
    except Exception as e:
        logs = {}
        errors.append(f"logs: {e!r}")
    rec = {"started_at": started, "finished_at": now(), **ident, "pair": pair,
           "trainer_logs_new": logs.get("trainer_logs_new"), "api_reload_lines_new": logs.get("api_reload_lines_new"),
           "errors": errors, "archiver_version": VERSION}
    append_jsonl(os.path.join(arc, "RUNS.jsonl"), rec)
    print(json.dumps(rec, sort_keys=True))
    return 1 if errors else 0


def main():
    env = os.environ.get
    ns = env("NAMESPACE", "ml-engine")
    ident = {"job": env("JOB_NAME", ""), "pod": env("POD_NAME", "")}
    return run(env("SOURCE_DIR", "/models"), env("ARCHIVE_DIR", "/archive"),
               env("ARTIFACT", "lstm_nginx-test_requests.pkl"), env("SIDECAR", "lstm_nginx-test_requests.meta.json"),
               KubeAPI(ns), ns, env("TRAINER_SELECTOR", "app=ml-training"), env("API_SELECTOR", "app=ml-api"),
               env("API_CONTAINER", "api"), ident)


if __name__ == "__main__":
    sys.exit(main())
