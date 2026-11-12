#!/usr/bin/env python3
"""Helpers of smoke/demo-kind.sh --replacement: the generator's restart and replacement smoke on a throwaway kind
cluster. The kind bed is for functionality, never evidence.

  demo_kind.py follow  --kubeconfig K --context C --namespace NS --name GEN --node NODE --out DIR [--interval S]
      Runs until DIR/stop exists. Every kubectl call names the kubeconfig AND the context it was given (the smoke's
      own). It writes, in DIR:
        generator.log     every line of every generator container's log, once (the API serves only the current and the
                          previous container's log, and a deleted pod's log is gone: the log is polled and merged)
        pods-watch.jsonl  the watch events of the generator's pods with the time each was received
        pods-poll.jsonl   a snapshot of the generator's pods at every poll
        memory.jsonl      memory.current and memory.peak of every k6 container's cgroup, read on the node
        ingress-memory.jsonl  (with --ingress-node, every second) the ingress container's cgroup on its node:
                          memory.current, memory.peak, memory.max and the oom_kill counter of memory.events, and the
                          oom_kill counter and memory.current of the POD's cgroup (it outlives a restarted container)
  demo_kind.py analyse --dir DIR --harness H --config CFG --ingress FILE --phases FILE --out SUMMARY
      Pure: restarts and their gaps, the pod timeline of the replacement, memory, the reduction and its verdicts per
      phase. Exit 1 when a required property does not hold (each is named).
  demo_kind.py selftest
      The analysis on synthetic input (offline; smoke/demo-kind.sh --render-only runs it).
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time

NAME_LABEL = "app.kubernetes.io/name"
# Reducer reasons that would mean the generator, its records or the single-generator guarantee are broken. The smoke
# deliberately causes gaps (restarts, a deleted pod), so reasons about absence are expected and not listed here.
FORBIDDEN = ("duplicate_generators", "plan_mismatch", "config_mismatch", "start_record_missing", "start_record_invalid",
             "due_mismatch", "carry_unmatched", "inconsistent_line", "conflicting_lines", "malformed_line",
             "generator_counter_reset", "clock_offset")


# --- follow: the only part that talks to the cluster (the smoke's own kind cluster) --------------------------------

def _kubectl(a):
    return ["kubectl", "--request-timeout=20s", "--kubeconfig", a.kubeconfig, "--context", a.context, "-n", a.namespace]


def _pod_view(p):
    m, st = p.get("metadata") or {}, p.get("status") or {}
    cs = (st.get("containerStatuses") or [{}])[0]
    return {"name": m.get("name"), "uid": m.get("uid"), "resourceVersion": m.get("resourceVersion"),
            "phase": st.get("phase"), "deletionTimestamp": m.get("deletionTimestamp"),
            "creationTimestamp": m.get("creationTimestamp"), "restartCount": cs.get("restartCount"),
            "containerID": cs.get("containerID"), "state": cs.get("state"), "lastState": cs.get("lastState"),
            "owners": [[o.get("kind"), o.get("uid")] for o in m.get("ownerReferences") or []]}


def _watch(a, stop):
    """The generator pods' watch events, each with the local time it arrived (ms). Restarted when the watch ends."""
    path = os.path.join(a.out, "pods-watch.jsonl")
    dec = json.JSONDecoder()
    while not stop.is_set():
        proc = subprocess.Popen(_kubectl(a)[:1] + _kubectl(a)[2:] + ["get", "pods", "-l", f"{NAME_LABEL}={a.name}",
                                "--watch", "--output-watch-events", "-o", "json"],
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        buf = ""
        with open(path, "a") as out:
            out.write(json.dumps({"t_ms": int(time.time() * 1000), "type": "WATCH-START"}) + "\n")
            while not stop.is_set():
                chunk = proc.stdout.readline()
                if not chunk:
                    break
                buf += chunk
                while buf.strip():
                    try:
                        doc, end = dec.raw_decode(buf.lstrip())
                    except ValueError:
                        break
                    buf = buf.lstrip()[end:]
                    out.write(json.dumps({"t_ms": int(time.time() * 1000), "type": doc.get("type"),
                                          **_pod_view(doc.get("object") or {})}) + "\n")
                    out.flush()
        proc.kill()
        time.sleep(1)


MEMORY_SH = r'''
for c in $(crictl ps -q --name '^k6$' 2>/dev/null); do
  pid=$(crictl inspect -o go-template --template '{{.info.pid}}' "$c" 2>/dev/null) || continue
  cg=$(sed -n 's/^0:://p' "/proc/$pid/cgroup" 2>/dev/null) || continue
  [ -n "$cg" ] || continue
  echo "$c $(cat "/sys/fs/cgroup$cg/memory.current" 2>/dev/null || echo -) $(cat "/sys/fs/cgroup$cg/memory.peak" 2>/dev/null || echo -)"
done
'''


INGRESS_SH = r'''
num() { cat "$1" 2>/dev/null || echo -; }
oom() { awk '$1 == "oom_kill" {print $2; f = 1} END {if (!f) print "-"}' "$1" 2>/dev/null || echo -; }
for c in $(crictl ps -q --name '^ingress$' 2>/dev/null); do
  pid=$(crictl inspect -o go-template --template '{{.info.pid}}' "$c" 2>/dev/null) || continue
  cg=$(sed -n 's/^0:://p' "/proc/$pid/cgroup" 2>/dev/null) || continue
  [ -n "$cg" ] || continue
  d="/sys/fs/cgroup$cg"; p="${d%/*}"
  echo "$c $(num "$d/memory.current") $(num "$d/memory.peak") $(num "$d/memory.max") $(oom "$d/memory.events") $(oom "$p/memory.events") $(num "$p/memory.current")"
done
'''


def _ingress_memory(a, stop):
    """The ingress container's cgroup on its node, once a second (a kill happens between two slower samples)."""
    path = os.path.join(a.out, "ingress-memory.jsonl")
    keys = ("current", "peak", "limit", "oom_kill", "pod_oom_kill", "pod_current")
    while not stop.is_set():
        t0 = time.time()
        r = subprocess.run(["docker", "exec", a.ingress_node, "sh", "-c", INGRESS_SH], capture_output=True, text=True)
        if r.returncode == 0:
            with open(path, "a") as fh:
                for line in r.stdout.split("\n"):
                    f = line.split()
                    if len(f) == 7:
                        fh.write(json.dumps({"t_ms": int(t0 * 1000), "container": f[0],
                                             **{k: int(v) if v.isdigit() else None for k, v in zip(keys, f[1:])}}) + "\n")
        time.sleep(max(0.1, 1.0 - (time.time() - t0)))


def follow(a):
    os.makedirs(a.out, exist_ok=True)
    stop = threading.Event()
    threading.Thread(target=_watch, args=(a, stop), daemon=True).start()
    if a.ingress_node:
        threading.Thread(target=_ingress_memory, args=(a, stop), daemon=True).start()
    seen = set()
    log_path = os.path.join(a.out, "generator.log")
    while not os.path.exists(os.path.join(a.out, "stop")):
        t0 = time.time()
        now = int(t0 * 1000)
        r = subprocess.run(_kubectl(a) + ["get", "pods", "-l", f"{NAME_LABEL}={a.name}", "-o", "json"],
                           capture_output=True, text=True)
        pods = []
        if r.returncode == 0:
            try:
                pods = [_pod_view(p) for p in json.loads(r.stdout).get("items", [])]
            except ValueError:
                pods = []
            with open(os.path.join(a.out, "pods-poll.jsonl"), "a") as fh:
                fh.write(json.dumps({"t_ms": now, "pods": pods}) + "\n")
        fresh = []
        for p in pods:
            for extra in ([], ["--previous"]):
                lg = subprocess.run(_kubectl(a) + ["logs", "--timestamps", p["name"], "-c", "k6"] + extra,
                                    capture_output=True, text=True, errors="replace")
                if lg.returncode != 0:
                    continue
                for line in lg.stdout.splitlines():
                    if line and line not in seen:
                        seen.add(line)
                        fresh.append(line)
        if fresh:
            with open(log_path, "a") as fh:
                fh.write("\n".join(fresh) + "\n")
        mem = subprocess.run(["docker", "exec", a.node, "sh", "-c", MEMORY_SH], capture_output=True, text=True)
        if mem.returncode == 0:
            with open(os.path.join(a.out, "memory.jsonl"), "a") as fh:
                for line in mem.stdout.split("\n"):
                    f = line.split()
                    if len(f) == 3:
                        fh.write(json.dumps({"t_ms": now, "container": f[0],
                                             "current": int(f[1]) if f[1].isdigit() else None,
                                             "peak": int(f[2]) if f[2].isdigit() else None}) + "\n")
        time.sleep(max(0.2, a.interval - (time.time() - t0)))
    stop.set()
    return 0


# --- analysis (pure) ------------------------------------------------------------------------------------------------

def read_jsonl(path):
    out = []
    if os.path.exists(path):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
    return out


def incarnations(records, target):
    """The incarnations of one target's records, by start time: start and end records and the minute lines."""
    by = {}
    for r in records:
        if r.get("target") != target:
            continue
        e = by.setdefault(r.get("inc"), {"inc": r.get("inc"), "start": None, "end": None, "lines": []})
        if r.get("event") == "start":
            e["start"] = r
        elif r.get("event") == "end":
            e["end"] = r
        elif "minute" in r and "event" not in r:
            e["lines"].append(r)
    out = [e for e in by.values() if e["start"] is not None]
    for e in out:
        e["lines"].sort(key=lambda l: l["minute"])
    return sorted(out, key=lambda e: e["start"]["started_ms"])


def restarts(incs, cfg, scheduled_arrivals=None):
    """Consecutive incarnations of ONE pod where the earlier one ended at its segment end (an end record): a planned
    restart by the kubelet in the same pod. Gaps in ms:
      process   from the earlier incarnation's end record to the later one's start (the kubelet's restart, its
                back-off included, and k6's start)
      traffic   from the segment end to the instant the later incarnation begins to schedule arrivals
    no_replay: the later incarnation's first line begins exactly where it started and owes exactly the schedule's
    arrivals from there on (nothing of the gap is sent later)."""
    out = []
    for a, b in zip(incs, incs[1:]):
        if a["start"].get("pod_uid") != b["start"].get("pod_uid") or a["end"] is None:
            continue
        row = {"pod_uid": a["start"].get("pod_uid"), "from": a["inc"], "to": b["inc"],
               "segment_end_ms": a["start"]["segment_end_ms"], "ended_ms": a["end"]["ended_ms"],
               "next_started_ms": b["start"]["started_ms"], "next_active_from_ms": b["start"]["active_from_ms"],
               "process_gap_ms": b["start"]["started_ms"] - a["end"]["ended_ms"],
               "traffic_gap_ms": b["start"]["active_from_ms"] - a["start"]["segment_end_ms"],
               "next_phase": b["start"].get("phase"), "no_replay": None}
        if b["lines"]:
            first = b["lines"][0]
            lo = cfg["origin_ms"] + first["minute"] * cfg["minute_ms"]
            ok = first["minute"] == (b["start"]["active_from_ms"] - cfg["origin_ms"]) // cfg["minute_ms"] and \
                first["active"][0] == b["start"]["active_from_ms"] - lo and first["carried"][0] == 0 and \
                not any(l["minute"] < first["minute"] for l in b["lines"])
            if ok and scheduled_arrivals is not None:
                ok = first["due"] == scheduled_arrivals(cfg, first["target"], first["minute"], *first["active"]) and \
                    first["due"] <= first["intended"]
            row["no_replay"] = bool(ok)
            row["first_line"] = {"minute": first["minute"], "active": first["active"], "intended": first["intended"],
                                 "due": first["due"], "dispatched": first["dispatched"]}
        out.append(row)
    return out


def pod_timeline(events, polls):
    """What the watch events and the polls say about the generator's pods: every pod object in order, the largest
    number of pod objects that existed at once, and for each replacement the order of "old object gone" and "new
    object created". A pod object that exists may be running (a terminating pod too), so the guarantee asked of the
    StatefulSet is on OBJECTS: never two at once."""
    alive, order, most, problems = {}, [], 0, []
    info = {}
    for e in events:
        uid, typ = e.get("uid"), e.get("type")
        if typ == "WATCH-START":
            continue                                      # a re-list follows: ADDED for objects already known
        if not uid:
            continue
        d = info.setdefault(uid, {"uid": uid, "name": e.get("name"), "first_seen_ms": e["t_ms"], "running_ms": None,
                                  "terminating_ms": None, "gone_ms": None, "created": e.get("creationTimestamp")})
        if typ == "DELETED":
            alive.pop(uid, None)
            d["gone_ms"] = e["t_ms"]
            continue
        if uid not in alive:
            if d["gone_ms"] is not None:
                continue                                  # a late event of a deleted object
            alive[uid] = True
            if uid not in order:
                order.append(uid)
        if e.get("deletionTimestamp") and d["terminating_ms"] is None:
            d["terminating_ms"] = e["t_ms"]
        if e.get("phase") == "Running" and "running" in (e.get("state") or {}) and d["running_ms"] is None:
            d["running_ms"] = e["t_ms"]
        most = max(most, len(alive))
        if len(alive) > 1:
            problems.append(f"{len(alive)} generator pod objects existed at once at {e['t_ms']}: {sorted(alive)}")
    poll_most = max([len(p.get("pods") or []) for p in polls] or [0])
    if poll_most > 1:
        problems.append(f"a poll saw {poll_most} generator pods at once")
    replacements = []
    for old, new in zip(order, order[1:]):
        o, n = info[old], info[new]
        row = {"old_uid": old, "new_uid": new, "name": n["name"], "old_terminating_ms": o["terminating_ms"],
               "old_gone_ms": o["gone_ms"], "new_first_seen_ms": n["first_seen_ms"], "new_running_ms": n["running_ms"]}
        if o["gone_ms"] is None or n["first_seen_ms"] < o["gone_ms"]:
            problems.append(f"pod {new} appeared before the old pod object {old} was gone")
        else:
            row["old_gone_to_new_created_ms"] = n["first_seen_ms"] - o["gone_ms"]
            if o["terminating_ms"] is not None:
                row["delete_to_gone_ms"] = o["gone_ms"] - o["terminating_ms"]
                if n["running_ms"] is not None:
                    row["delete_to_new_running_ms"] = n["running_ms"] - o["terminating_ms"]
        replacements.append(row)
    return {"pods": [info[u] for u in order], "max_objects_at_once": max(most, poll_most),
            "replacements": replacements, "problems": problems}


def container_exits(events, polls):
    """Every container termination the pod statuses reported (lastState.terminated), once: exit code and reason."""
    seen, out = set(), []
    views = [e for e in events if e.get("uid")] + [p for s in polls for p in (s.get("pods") or [])]
    for v in views:
        t = ((v.get("lastState") or {}).get("terminated")) or {}
        key = (v.get("uid"), t.get("containerID"), t.get("finishedAt"))
        if t and key not in seen:
            seen.add(key)
            out.append({"pod_uid": v.get("uid"), "exit_code": t.get("exitCode"), "reason": t.get("reason"),
                        "started_at": t.get("startedAt"), "finished_at": t.get("finishedAt")})
    return sorted(out, key=lambda x: str(x["finished_at"]))


def memory_summary(samples, limit_bytes, windows=None):
    """The largest memory.current and memory.peak per container and overall; `windows` {name: [from_ms, to_ms]}
    gives the largest memory.current inside each."""
    per = {}
    for s in samples:
        d = per.setdefault(s["container"], {"container": s["container"][:12], "samples": 0, "max_current": 0,
                                            "peak": None, "from_ms": s["t_ms"], "to_ms": s["t_ms"]})
        d["samples"] += 1
        d["to_ms"] = s["t_ms"]
        if s.get("current") is not None:
            d["max_current"] = max(d["max_current"], s["current"])
        if s.get("peak") is not None:
            d["peak"] = max(d["peak"] or 0, s["peak"])
    rows = sorted(per.values(), key=lambda d: d["from_ms"])
    top_current = max([d["max_current"] for d in rows] or [0])
    peaks = [d["peak"] for d in rows if d["peak"] is not None]
    out = {"containers": rows, "samples": len(samples), "max_current_bytes": top_current,
           "max_peak_bytes": max(peaks) if peaks else None, "limit_bytes": limit_bytes}
    top = max([top_current] + peaks)
    out["max_bytes"] = top
    out["max_fraction_of_limit"] = round(top / limit_bytes, 4) if limit_bytes else None
    out["windows"] = {}
    for name, (a, b) in (windows or {}).items():
        inside = [s for s in samples if a <= s["t_ms"] < b and s.get("current") is not None]
        out["windows"][name] = {"samples": len(inside), "max_current_bytes": max([s["current"] for s in inside] or [0]),
                                "max_peak_bytes": max([s["peak"] for s in inside if s.get("peak") is not None] or [0])}
    return out


def phase_verdicts(reduction, phases):
    """Per phase {name: [from_ms, to_ms]} and target: the states and reasons of the reducer's rows in the phase. A
    minute belongs to every phase it OVERLAPS (the minute in which a phase begins or ends is counted in both)."""
    out = {}
    for name, (a, b) in phases.items():
        out[name] = {}
        for target, t in reduction["targets"].items():
            rows = [r for r in t["rows"] if r["start_ms"] < b and r["start_ms"] + 60_000 > a]
            states, reasons = {}, {}
            for r in rows:
                states[r["state"]] = states.get(r["state"], 0) + 1
                for x in r["reasons"]:
                    reasons[x] = reasons.get(x, 0) + 1
            g = [r["generator"] for r in rows]
            out[name][target] = {
                "minutes": len(rows), "states": states, "reasons": dict(sorted(reasons.items())),
                "intended": sum(r["intended"] or 0 for r in rows), "dispatched": sum(x["dispatched"] for x in g),
                "ok": sum(x["ok"] for x in g), "failed": sum(x["failed"] for x in g),
                "no_response": sum(x["no_response"] for x in g),
                "dropped": {k: sum(x["dropped"][k] for x in g) for k in ("late", "inflight", "segment_end")},
                "late_max_ms": max([x.get("late_max_ms", 0) for x in g] or [0]),
                # Minutes the reducer could confirm against the ingress AND in which requests were served: the
                # positive evidence evidence_problems() asks for.
                "observed_served": sum(1 for r in rows if r["state"] == "observed" and r["generator"]["ok"] > 0),
                "forbidden": sorted(x for x in reasons if x in FORBIDDEN)}
    return out


# The phases demo-kind.sh --replacement runs, the ones in which every target must be seen served, and the fewest
# memory samples that count as a measurement of the timeout phase (one sample every 3 s: 30 s of it).
PHASES = ("restarts", "replacement", "steady", "timeout", "recovery")
SERVED_PHASES = ("steady", "recovery")
MIN_MEMORY_SAMPLES = 10


def evidence_problems(verdicts, memory, timeout, phases, targets, unanswered):
    """What the verdict needs to have SEEN, so that evidence that is missing never reads as a pass: the absence of a
    forbidden reason proves nothing when no minute was observed, and a memory maximum of 0 is no measurement.

    - every phase ran;
    - in the steady and recovery phases each target has an observed minute with served requests (the restart and
      replacement phases keep their declared gaps: nothing is asked of them here);
    - in the timeout phase the unanswered target has failed requests and every other target an observed, served minute;
    - the generator's memory limit is known and the timeout phase holds real samples below it."""
    problems = [f"phase {name} is missing from the phases file" for name in PHASES if name not in phases]

    def served(name, target):
        return ((verdicts.get(name) or {}).get(target) or {}).get("observed_served", 0)
    if not targets:
        problems.append("the configuration names no target")
    for name in SERVED_PHASES:
        for target in targets if name in phases else ():
            if served(name, target) < 1:
                problems.append(f"phase {name}, target {target}: no minute was observed with served requests "
                                "(no positive evidence from the generator and the ingress together)")
    if "timeout" in phases:
        if unanswered not in targets:
            problems.append(f"the unanswered target {unanswered!r} is not one of the targets {sorted(targets)}")
        if timeout is None or not timeout["minutes"]:
            problems.append("the timeout phase holds no minute line of the unanswered target")
        elif timeout["failed_total"] == 0:
            problems.append("the timeout phase produced no failed request: the target kept answering")
        for target in targets:
            if target != unanswered and served("timeout", target) < 1:
                problems.append(f"phase timeout, target {target}: no minute was observed with served requests "
                                "(the other target must stay served while one does not answer)")
        limit = memory.get("limit_bytes")
        window = (memory.get("windows") or {}).get("timeout") or {}
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            problems.append(f"the generator's memory limit is unknown ({limit!r}): nothing to compare the memory with")
        if window.get("samples", 0) < MIN_MEMORY_SAMPLES or window.get("max_current_bytes", 0) <= 0:
            problems.append(f"the timeout phase holds {window.get('samples', 0)} memory samples with a largest value of "
                            f"{window.get('max_current_bytes', 0)} bytes (at least {MIN_MEMORY_SAMPLES} samples above "
                            "0 needed): the generator's memory was not measured")
        elif isinstance(limit, int) and limit > 0 and memory["max_bytes"] >= limit:
            problems.append("the generator's memory reached its limit")
    return problems


def timeout_load(lines, cfg, target, window):
    """The unanswered target's lines in the minutes the timeout window overlaps: requests that got no answer in
    time, and the requests in flight that follows from them (each such request is in flight for the whole timeout,
    so a minute with F of them holds F x timeout / 60 s in flight on average)."""
    a, b = window
    inside = [l for l in lines if l["target"] == target
              and cfg["origin_ms"] + l["minute"] * cfg["minute_ms"] < b
              and cfg["origin_ms"] + (l["minute"] + 1) * cfg["minute_ms"] > a]
    timeout_s = cfg["timeout_ms"] / 1000.0
    rows = []
    for l in sorted(inside, key=lambda l: l["minute"]):
        slow = l["failed"]                                   # no response (0) or the ingress's 504 at its own timeout
        rows.append({"minute": l["minute"], "intended": l["intended"], "dispatched": l["dispatched"], "ok": l["ok"],
                     "failed": l["failed"], "failed_by": l["failed_by"], "dropped_inflight": l["dropped"]["inflight"],
                     "lat_max_ms": l["lat"]["max_ms"], "inflight_mean": round(slow * timeout_s / 60.0, 1)})
    worst = max(rows, key=lambda r: r["failed"]) if rows else None
    return {"target": target, "timeout_s": timeout_s, "max_inflight": cfg["max_inflight"], "minutes": rows,
            "inflight_reached": worst["inflight_mean"] if worst else 0,
            "inflight_reached_fraction_of_max": round(worst["inflight_mean"] / cfg["max_inflight"], 3) if worst else 0,
            "failed_total": sum(r["failed"] for r in rows),
            "dropped_inflight_total": sum(r["dropped_inflight"] for r in rows)}


# The ingress's OOM counters must be read from the start of the fault to the end of the recovery: within this of
# each end, and without a longer gap between two readings (the sampler reads once a second).
OOM_EDGE_MS, OOM_MAX_GAP_MS = 5_000, 15_000


def oom_coverage(samples, start_ms, end_ms):
    """The readings of BOTH oom_kill counters (the container's and the pod's cgroup) in [start_ms, end_ms], with
    the last one before and the first one after it: how many, the largest gap between two of them, the largest
    counter, and whether they cover the span from end to end."""
    valid = sorted((s["t_ms"], max(s["oom_kill"], s["pod_oom_kill"])) for s in samples
                   if s.get("oom_kill") is not None and s.get("pod_oom_kill") is not None)
    before = [x for x in valid if x[0] < start_ms][-1:]
    after = [x for x in valid if x[0] > end_ms][:1]
    seen = before + [x for x in valid if start_ms <= x[0] <= end_ms] + after
    times = [t for t, _ in seen]
    gap = max([b - a for a, b in zip(times, times[1:])] or [0])
    covered = (len(seen) >= 2 and times[0] <= start_ms + OOM_EDGE_MS and times[-1] >= end_ms - OOM_EDGE_MS
               and gap <= OOM_MAX_GAP_MS)
    return {"readings": len(seen), "first_ms": times[0] if times else None, "last_ms": times[-1] if times else None,
            "max_gap_ms": gap, "max_kills": max([k for _, k in seen], default=None), "covered": covered}


def _container_name(container_id):
    return (container_id or "").split("://")[-1]


def ingress_evidence_problems(final, before, memory, oom, sampled):
    """What the verdict needs to have SEEN of the fixture's ingress (the same rule as for the generator: missing
    evidence never reads as a pass):

    - its container statuses AFTER THE RECOVERY (`final`) with a restart count of 0;
    - the same pod and container as before the fault (`before`): one of each, UID and container ID unchanged, and
      the memory sampler saw exactly that container from the fault to the end of the recovery (`sampled`, the
      container IDs it saw there). A replacement at any point of the fault or the recovery fails;
    - a known memory limit and real samples of its memory in the timeout phase below it (`memory`);
    - both OOM counters read from the start of the fault to the end of the recovery (`oom`, an oom_coverage), all 0.
      A counter that was not read throughout proves nothing.

    `final` and `before` are [{pod, uid, container_id, restarts, last_terminated}]."""
    problems = []
    if not final:
        problems.append("the ingress's container status after the recovery was not captured: its restart count is unknown")
    for p in final or []:
        if not isinstance(p.get("restarts"), int) or isinstance(p.get("restarts"), bool):
            problems.append(f"ingress pod {p.get('pod')}: no restart count")
        elif p["restarts"] != 0:
            t = p.get("last_terminated") or {}
            problems.append(f"ingress pod {p.get('pod')} restarted {p['restarts']} time(s): last termination "
                            f"{t.get('reason')} (exit {t.get('exitCode')}) at {t.get('finishedAt')}")

    def identity(rows):
        return sorted((p.get("uid"), _container_name(p.get("container_id"))) for p in rows or [])
    if not before:
        problems.append("the ingress's pod and container before the fault were not captured: nothing to compare the "
                        "ingress after the recovery with")
    elif final:
        a, b = identity(before), identity(final)
        if len(a) != 1 or len(b) != 1 or not all(a[0]) or not all(b[0]):
            problems.append(f"the ingress is not exactly one identified pod and container (before {a}, after {b})")
        elif a != b:
            problems.append(f"the ingress pod or container changed between the start of the fault and the end of the "
                            f"recovery (before {a[0]}, after {b[0]})")
        elif sorted(sampled or []) != [b[0][1]]:
            problems.append(f"the memory sampler saw the ingress containers {sorted(sampled or [])} from the fault to "
                            f"the end of the recovery, not exactly the one in its status ({b[0][1]})")
    limit = memory.get("limit_bytes")
    window = (memory.get("windows") or {}).get("timeout") or {}
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        problems.append(f"the ingress's memory limit is unknown ({limit!r})")
    if window.get("samples", 0) < MIN_MEMORY_SAMPLES or window.get("max_current_bytes", 0) <= 0:
        problems.append(f"the timeout phase holds {window.get('samples', 0)} samples of the ingress's memory with a "
                        f"largest value of {window.get('max_current_bytes', 0)} bytes (at least {MIN_MEMORY_SAMPLES} "
                        "above 0 needed): the ingress's memory was not measured")
    elif isinstance(limit, int) and not isinstance(limit, bool) and limit > 0 and memory["max_bytes"] >= limit:
        problems.append(f"the ingress's memory reached its limit ({memory['max_bytes']} of {limit} bytes)")
    if not oom or not oom.get("covered"):
        o = oom or {}
        problems.append(f"the ingress's oom_kill counters were not read from the start of the fault to the end of the "
                        f"recovery ({o.get('readings', 0)} readings, largest gap {o.get('max_gap_ms')} ms): that no "
                        "OOM kill happened is unproven")
    if oom and oom.get("max_kills"):
        problems.append(f"the kernel OOM-killed in the ingress's cgroup ({oom['max_kills']} kill(s))")
    return problems


def analyse(a):
    with open(a.phases) as fh:
        if json.load(fh).get("kind") == "ingress-faults":     # demo-kind.sh --ingress-faults: its own verdict
            import ingress_faults
            return ingress_faults.analyse(a)
    sys.path.insert(0, a.harness)
    from c28 import demo, demo_reduce as DR
    cfg = json.load(open(a.config))
    why = demo.config_problems(cfg)
    if why:
        sys.exit("analyse: the configuration is not intact: " + "; ".join(why))
    with open(os.path.join(a.dir, "generator.log"), errors="replace") as fh:
        records, log_stats = DR.parse_lines(fh.read().splitlines())
    events = read_jsonl(os.path.join(a.dir, "pods-watch.jsonl"))
    polls = read_jsonl(os.path.join(a.dir, "pods-poll.jsonl"))
    mem = read_jsonl(os.path.join(a.dir, "memory.jsonl"))
    ph = json.load(open(a.phases))
    phases = {k: v for k, v in ph["phases"].items()}
    ingress = json.load(open(a.ingress))
    minute = cfg["minute_ms"]
    lo = min(v[0] for v in phases.values()) // minute * minute
    hi = max(v[1] for v in phases.values()) // minute * minute
    red = DR.reduce(cfg, records, ingress, lo, hi)
    red["log"] = log_stats
    with open(os.path.join(a.dir, "reduction.json"), "w") as fh:
        json.dump(red, fh, indent=1, sort_keys=True)
    target0 = cfg["targets"][0]
    incs = incarnations(records, target0)
    rs = restarts(incs, cfg, DR.scheduled_arrivals)
    pods = pod_timeline(events, polls)
    exits = container_exits(events, polls)
    lines = [r for r in records if r.get("schema") == DR.MINUTE]
    verdicts = phase_verdicts(red, phases)
    tl = timeout_load(lines, cfg, ph["unanswered_target"], phases["timeout"]) if "timeout" in phases else None
    memory = memory_summary(mem, ph.get("memory_limit_bytes"), {k: v for k, v in phases.items()})
    ing_mem = read_jsonl(os.path.join(a.dir, "ingress-memory.jsonl"))
    limits = sorted({s["limit"] for s in ing_mem if s.get("limit")})
    ingress_memory = memory_summary(ing_mem, limits[-1] if limits else None, {k: v for k, v in phases.items()})
    kills = [x for s in ing_mem for x in (s.get("oom_kill"), s.get("pod_oom_kill")) if x is not None]
    ingress_memory["oom_kill_max"] = max(kills) if kills else None
    ingress_memory["limits_seen_bytes"] = limits
    # From the start of the fault to the end of the recovery: the OOM counters' coverage and the containers sampled.
    span = (phases["timeout"][0], phases["recovery"][1]) if {"timeout", "recovery"} <= set(phases) else None
    ingress_oom = oom_coverage(ing_mem, *span) if span else None
    ingress_sampled = sorted({s["container"] for s in ing_mem if span and span[0] <= s["t_ms"] <= span[1]})
    ingress_memory["oom_coverage"], ingress_memory["containers_in_fault_and_recovery"] = ingress_oom, ingress_sampled
    problems = list(pods["problems"])
    # The longest run of CONSECUTIVE planned restarts in one pod.
    run = best = 0
    prev = None
    for r in rs:
        run = run + 1 if prev is not None and prev["to"] == r["from"] else 1
        best, prev = max(best, run), r
    if best < ph.get("min_restarts", 3):
        problems.append(f"only {best} consecutive planned restarts in one pod (at least {ph.get('min_restarts', 3)} needed)")
    if any(r["no_replay"] is False for r in rs):
        problems.append("an incarnation after a planned restart did not resume exactly at its start (replay?)")
    planned = [e for e in exits if e["reason"] == "Completed"]
    if any(e["exit_code"] != 0 for e in planned) or len(planned) < ph.get("min_restarts", 3):
        problems.append(f"the planned restarts are not all exit 0 / Completed: {exits}")
    if ph.get("expect_replacement", True) and not pods["replacements"]:
        problems.append("no pod replacement was observed")
    forbidden = sorted({x for p in verdicts.values() for t in p.values() for x in t["forbidden"]})
    if forbidden:
        problems.append(f"the reducer reports {forbidden}")
    if any(red["duplicate_generators"][t] for t in red["duplicate_generators"]):
        problems.append(f"duplicate generators: {red['duplicate_generators']}")
    for i in red["incarnations"]:
        if i["start_record"] != "valid" or i["flags"]:
            problems.append(f"incarnation {i['incarnation']} ({i['target']}): start record {i['start_record']}, "
                            f"flags {i['flags']}")
    problems += evidence_problems(verdicts, memory, tl, phases, list(cfg["targets"]),
                                  ph.get("unanswered_target"))
    problems += ingress_evidence_problems(ph.get("ingress_final"), ph.get("ingress_before"), ingress_memory,
                                          ingress_oom, ingress_sampled)
    out = {"schema": "c28-demo-kind-smoke/v1", "config_digest": cfg["digest"], "label": cfg["label"],
           "origin_utc": cfg["origin_utc"], "segment_minutes": cfg["segment_minutes"],
           "max_inflight": cfg["max_inflight"], "log": log_stats, "records": red["records"],
           "incarnations": [{"inc": e["inc"], "pod_uid": e["start"].get("pod_uid"), "phase": e["start"].get("phase"),
                             "started_ms": e["start"]["started_ms"], "segment_end_ms": e["start"]["segment_end_ms"],
                             "ended": "segment-end" if e["end"] else "no end record", "lines": len(e["lines"])}
                            for e in incs],
           "restarts": rs, "consecutive_restarts": best, "container_exits": exits, "pods": pods, "memory": memory,
           "timeout_load": tl, "ingress": ph.get("ingress"), "ingress_before": ph.get("ingress_before"),
           "ingress_final": ph.get("ingress_final"), "ingress_memory": ingress_memory,
           "blackhole": ph.get("blackhole"), "other_log_lines": log_stats["other"], "phases": {k: {"from_ms": v[0], "to_ms": v[1]} for k, v in phases.items()},
           "verdicts": verdicts, "duplicate_generators": red["duplicate_generators"], "problems": problems,
           "result": "PASS" if not problems else "FAIL"}
    with open(a.out, "w") as fh:
        json.dump(out, fh, indent=1, sort_keys=True)
    gaps = [(r["process_gap_ms"], r["traffic_gap_ms"]) for r in rs]
    print(f"analyse: {len(incs)} incarnations, {len(rs)} planned restarts ({best} consecutive), gaps (process, "
          f"traffic) ms {gaps}; pod objects at once {pods['max_objects_at_once']}, replacements "
          f"{[(r.get('delete_to_gone_ms'), r.get('old_gone_to_new_created_ms')) for r in pods['replacements']]}; "
          f"memory max {memory['max_bytes']} of {memory['limit_bytes']} bytes; "
          f"in flight reached {tl['inflight_reached'] if tl else None} of {cfg['max_inflight']}")
    for name, per in verdicts.items():
        print(f"analyse: phase {name}: " + "; ".join(f"{t}: {v['states']} {v['reasons']}" for t, v in per.items()))
    for p in problems:
        print(f"analyse: PROBLEM {p}")
    return 0 if not problems else 1


# --- selftest (offline) -----------------------------------------------------------------------------------------------

def selftest():
    cfg = {"origin_ms": 0, "minute_ms": 60_000}
    t, uid = "c28-x", "pod-1"

    def start(started, seg_end, pod=uid, active=None):
        return {"schema": "inc", "event": "start", "target": t, "inc": f"{pod}/{started}", "pod_uid": pod,
                "started_ms": started, "active_from_ms": started + 1 if active is None else active,
                "segment_end_ms": seg_end, "phase": "scheduled"}

    def end(started, ended, pod=uid):
        return {"schema": "inc", "event": "end", "target": t, "inc": f"{pod}/{started}", "ended_ms": ended}

    def line(started, minute, a0, due, pod=uid, carried=(0, 0)):
        return {"schema": "min", "target": t, "inc": f"{pod}/{started}", "minute": minute, "active": [a0, 60_000],
                "intended": 100, "due": due, "dispatched": due, "carried": list(carried)}
    recs = [start(10_000, 180_000), line(10_000, 0, 10_001, 80), end(10_000, 182_500),
            start(185_000, 360_000), line(185_000, 3, 5_001, 90), end(185_000, 362_500),
            start(375_000, 540_000), line(375_000, 6, 15_001, 70), end(375_000, 542_500),
            start(565_000, 720_000), line(565_000, 9, 25_001, 55),                       # killed: no end record
            start(760_000, 900_000, "pod-2"), line(760_000, 12, 40_001, 30, "pod-2")]
    incs = incarnations(recs, t)
    assert [e["inc"] for e in incs] == ["pod-1/10000", "pod-1/185000", "pod-1/375000", "pod-1/565000", "pod-2/760000"]
    rs = restarts(incs, cfg)
    assert [(r["process_gap_ms"], r["traffic_gap_ms"]) for r in rs] == [(2_500, 5_001), (12_500, 15_001), (22_500, 25_001)]
    assert all(r["no_replay"] for r in rs) and len(rs) == 3              # the kill and the other pod are no restarts
    replayed = [dict(r) for r in recs]
    replayed[4] = line(185_000, 3, 0, 100)                               # claims the whole minute: not a resume
    assert restarts(incarnations(replayed, t), cfg)[0]["no_replay"] is False
    carried = [dict(r) for r in recs]
    carried[4] = line(185_000, 3, 5_001, 90, carried=(2, 0))
    assert restarts(incarnations(carried, t), cfg)[0]["no_replay"] is False

    def ev(tms, typ, u, phase="Running", deleting=None, running=True):
        return {"t_ms": tms, "type": typ, "uid": u, "name": "g-0", "phase": phase, "deletionTimestamp": deleting,
                "state": {"running": {}} if running else {"waiting": {}}, "creationTimestamp": "x"}
    good = [{"t_ms": 1, "type": "WATCH-START"}, ev(10, "ADDED", "a", "Pending", running=False), ev(20, "MODIFIED", "a"),
            ev(100, "MODIFIED", "a", deleting="d"), ev(160, "DELETED", "a"),
            ev(165, "ADDED", "b", "Pending", running=False), ev(190, "MODIFIED", "b"),
            {"t_ms": 200, "type": "WATCH-START"}, ev(201, "ADDED", "b")]
    tl = pod_timeline(good, [{"t_ms": 150, "pods": [{}]}, {"t_ms": 170, "pods": [{}]}])
    assert tl["problems"] == [] and tl["max_objects_at_once"] == 1, tl
    assert tl["replacements"] == [{"old_uid": "a", "new_uid": "b", "name": "g-0", "old_terminating_ms": 100,
                                   "old_gone_ms": 160, "new_first_seen_ms": 165, "new_running_ms": 190,
                                   "old_gone_to_new_created_ms": 5, "delete_to_gone_ms": 60,
                                   "delete_to_new_running_ms": 90}]
    # What a Deployment does on a plain delete: the new pod exists while the old one is still terminating.
    bad = [ev(10, "ADDED", "a"), ev(100, "MODIFIED", "a", deleting="d"), ev(101, "ADDED", "b", "Pending", running=False),
           ev(160, "DELETED", "a")]
    tl = pod_timeline(bad, [])
    assert tl["max_objects_at_once"] == 2 and any("existed at once" in p for p in tl["problems"])
    assert any("appeared before the old pod object" in p for p in tl["problems"])
    assert pod_timeline(good, [{"t_ms": 150, "pods": [{}, {}]}])["problems"] == ["a poll saw 2 generator pods at once"]
    term = {"terminated": {"exitCode": 0, "reason": "Completed", "containerID": "c1", "finishedAt": "t1"}}
    polls = [{"t_ms": 1, "pods": [{"uid": "a", "lastState": term}]}, {"t_ms": 2, "pods": [{"uid": "a", "lastState": term}]},
             {"t_ms": 3, "pods": [{"uid": "a", "lastState": {"terminated": {"exitCode": 137, "reason": "Error",
                                                                            "containerID": "c2", "finishedAt": "t2"}}}]}]
    assert [(e["exit_code"], e["reason"]) for e in container_exits([], polls)] == [(0, "Completed"), (137, "Error")]
    mem = [{"t_ms": 10, "container": "c1", "current": 100, "peak": 150}, {"t_ms": 20, "container": "c1", "current": 300, "peak": 320},
           {"t_ms": 30, "container": "c2", "current": 200, "peak": None}]
    ms = memory_summary(mem, 1000, {"w": [15, 25]})
    assert (ms["max_current_bytes"], ms["max_peak_bytes"], ms["max_bytes"], ms["max_fraction_of_limit"]) == (300, 320, 320, 0.32)
    assert ms["windows"]["w"] == {"samples": 1, "max_current_bytes": 300, "max_peak_bytes": 320}
    g = {"dispatched": 5, "ok": 5, "failed": 0, "no_response": 0, "dropped": {"late": 0, "inflight": 0, "segment_end": 0}}
    red = {"targets": {t: {"rows": [
        {"start_ms": 0, "state": "observed", "reasons": [], "intended": 5, "generator": g},
        {"start_ms": 60_000, "state": "unknown", "reasons": ["unrecorded_tail"], "intended": 5, "generator": g},
        {"start_ms": 120_000, "state": "unknown", "reasons": ["duplicate_generators"], "intended": 5, "generator": g}]}}}
    v = phase_verdicts(red, {"one": [0, 120_000], "two": [120_000, 180_000], "three": [59_000, 61_000]})
    assert v["one"][t]["states"] == {"observed": 1, "unknown": 1} and v["one"][t]["forbidden"] == []
    assert v["two"][t]["forbidden"] == ["duplicate_generators"]
    assert v["three"][t]["minutes"] == 2                                 # a minute counts in every phase it overlaps
    assert v["one"][t]["observed_served"] == 1 and v["two"][t]["observed_served"] == 0
    # The verdict asks for what was SEEN. A complete run passes; each missing piece of evidence fails, in particular
    # no memory sample at all (a maximum of 0 is below any limit) and no ingress evidence (no minute is observed, so
    # no minute carries a forbidden reason either).
    o = "c28-y"
    names = {"restarts": [0, 60_000], "replacement": [60_000, 120_000], "steady": [120_000, 180_000],
             "timeout": [180_000, 240_000], "recovery": [240_000, 300_000]}
    served = {"start_ms": 0, "state": "observed", "reasons": [], "intended": 5, "generator": g}
    failing = {"start_ms": 0, "state": "partial", "reasons": [], "intended": 5, "generator": dict(g, ok=0, failed=5)}

    def reduction(rows_of):
        return {"targets": {x: {"rows": [dict(r, start_ms=m * 60_000) for m, r in enumerate(rows_of(x))]}
                            for x in (t, o)}}
    full = phase_verdicts(reduction(lambda x: [served, served, served, failing if x == t else served, served]), names)
    mem_ok = memory_summary([{"t_ms": 180_000 + 3_000 * i, "container": "c1", "current": 400 + i, "peak": 500}
                             for i in range(MIN_MEMORY_SAMPLES)], 1000, names)
    load_ok = {"minutes": [{}], "failed_total": 5}
    assert evidence_problems(full, mem_ok, load_ok, names, [t, o], t) == []

    def flagged(needle, verdicts=full, mem=mem_ok, load=load_ok, phases=names, targets=(t, o), unanswered=t):
        found = evidence_problems(verdicts, mem, load, phases, list(targets), unanswered)
        assert any(needle in p for p in found), (needle, found)
        return found
    flagged("memory was not measured", mem=memory_summary([], 1000, names))               # every sample deleted
    flagged("memory was not measured", mem=memory_summary(
        [{"t_ms": 10 + i, "container": "c1", "current": 400, "peak": 500} for i in range(50)], 1000, names))  # none in the phase
    flagged("memory was not measured", mem=memory_summary(
        [{"t_ms": 180_000 + i, "container": "c1", "current": None, "peak": None} for i in range(50)], 1000, names))
    flagged("memory was not measured", mem=memory_summary(
        [{"t_ms": 180_000 + i, "container": "c1", "current": 400, "peak": 500} for i in range(MIN_MEMORY_SAMPLES - 1)],
        1000, names))
    for limit in (None, 0, True):
        flagged("memory limit is unknown", mem=dict(mem_ok, limit_bytes=limit))
    flagged("reached its limit", mem=dict(mem_ok, limit_bytes=500))
    # No ingress evidence: the reducer observes no minute (here every row unknown), and nothing is forbidden.
    blind = phase_verdicts(reduction(lambda x: [dict(served, state="unknown", reasons=["ingress_gap"])] * 5), names)
    assert all(v2[x]["forbidden"] == [] for v2 in blind.values() for x in v2)
    found = flagged("phase steady, target c28-x: no minute was observed", verdicts=blind)
    assert sum("no minute was observed" in p for p in found) == 5, found   # steady and recovery x 2, timeout x the other
    flagged("phase recovery, target c28-y", verdicts=phase_verdicts(
        reduction(lambda x: [served, served, served, failing if x == t else served,
                             dict(served, generator=dict(g, ok=0)) if x == o else served]), names))  # observed, nothing served
    flagged("phase timeout, target c28-y", verdicts=phase_verdicts(
        reduction(lambda x: [served, served, served, failing, served]), names))            # both targets failed
    flagged("kept answering", load={"minutes": [{}], "failed_total": 0})
    flagged("holds no minute line", load={"minutes": [], "failed_total": 0})
    flagged("holds no minute line", load=None)
    flagged("phase steady is missing", phases={k: v2 for k, v2 in names.items() if k != "steady"})
    flagged("is not one of the targets", unanswered="c28-z")
    flagged("names no target", targets=())
    # The ingress: after the recovery the same pod and container as before the fault with a restart count of 0, real
    # memory samples below a known limit, and both OOM counters read at 0 from the start of the fault to the end of
    # the recovery.
    cid = "c" * 64
    imem = dict(memory_summary([{"t_ms": 180_000 + 1_000 * i, "container": cid, "current": 300 + i, "peak": 400}
                                for i in range(MIN_MEMORY_SAMPLES)], 1000, names))
    ipods = [{"pod": "ing-1", "uid": "u-1", "container_id": "containerd://" + cid, "restarts": 0, "last_terminated": None}]
    span = (180_000, 300_000)                                          # the timeout and recovery phases of `names`
    isamples = [{"t_ms": t, "container": cid, "oom_kill": 0, "pod_oom_kill": 0} for t in range(179_000, 302_000, 1_000)]
    ioom = oom_coverage(isamples, *span)
    assert ioom["covered"] and ioom["max_kills"] == 0 and ioom["max_gap_ms"] == 1_000
    assert ingress_evidence_problems(ipods, ipods, imem, ioom, [cid]) == []

    def iflag(needle, final=ipods, before=ipods, mem=imem, oom=ioom, sampled=(cid,)):
        found = ingress_evidence_problems(final, before, mem, oom, list(sampled))
        assert any(needle in p for p in found), (needle, found)
        return found
    iflag("after the recovery was not captured", final=[])
    iflag("after the recovery was not captured", final=None)
    iflag("no restart count", final=[dict(ipods[0], restarts=None)])
    iflag("restarted 1 time(s): last termination OOMKilled (exit 137)", final=[dict(
        ipods[0], restarts=1, last_terminated={"reason": "OOMKilled", "exitCode": 137, "finishedAt": "t"})])
    iflag("before the fault were not captured", before=[])
    iflag("before the fault were not captured", before=None)
    iflag("pod or container changed", final=[dict(ipods[0], container_id="containerd://" + "d" * 64)])
    iflag("pod or container changed", final=[dict(ipods[0], uid="u-2")])        # the pod was replaced, restarts 0
    iflag("not exactly one identified pod and container", final=ipods * 2)
    iflag("not exactly one identified pod and container", before=[dict(ipods[0], uid=None)])
    # A replacement the status captures missed still shows in the samples (one container ID from fault to recovery).
    iflag("memory sampler saw the ingress containers", sampled=(cid, "replacement-container-after-status-capture"))
    iflag("memory sampler saw the ingress containers", sampled=())
    iflag("memory was not measured", mem=memory_summary([], 1000, names))
    iflag("memory limit is unknown", mem=dict(imem, limit_bytes=None))
    iflag("reached its limit", mem=dict(imem, limit_bytes=400))
    # OOM counters: one reading of 0 before the fault proves nothing about the fault and the recovery.
    unread = [dict(x, oom_kill=None, pod_oom_kill=None) for x in isamples[1:]]
    iflag("is unproven", oom=oom_coverage(isamples[:1] + unread, *span))
    iflag("is unproven", oom=oom_coverage([], *span))
    iflag("is unproven", oom=None)
    iflag("is unproven", oom=oom_coverage([x for x in isamples if x["t_ms"] <= 240_000], *span))     # stops mid-recovery
    iflag("is unproven", oom=oom_coverage([x for x in isamples if x["t_ms"] >= 200_000], *span))     # starts mid-fault
    iflag("is unproven", oom=oom_coverage([x for x in isamples if not 200_000 < x["t_ms"] < 230_000], *span))  # a 30 s hole
    iflag("is unproven", oom=oom_coverage([dict(x, pod_oom_kill=None) for x in isamples], *span))   # one counter only
    killed = [dict(x, pod_oom_kill=2) if x["t_ms"] >= 250_000 else x for x in isamples]
    found = iflag("OOM-killed in the ingress's cgroup (2 kill(s))", oom=oom_coverage(killed, *span))
    assert not any("unproven" in p for p in found)                    # read throughout, and it shows the kill
    cfg2 = {"origin_ms": 0, "minute_ms": 60_000, "timeout_ms": 2_000, "max_inflight": 505}
    ls = [{"target": t, "minute": 5, "intended": 9000, "dispatched": 9000, "ok": 0, "failed": 9000,
           "failed_by": {"0": 9000}, "dropped": {"inflight": 0}, "lat": {"max_ms": 0}},
          {"target": "other", "minute": 5, "intended": 9000, "dispatched": 9000, "ok": 9000, "failed": 0,
           "failed_by": {}, "dropped": {"inflight": 0}, "lat": {"max_ms": 9}}]
    load = timeout_load(ls, cfg2, t, [310_000, 350_000])
    assert load["inflight_reached"] == 300.0 and load["failed_total"] == 9000 and len(load["minutes"]) == 1
    print("demo_kind selftest: restarts and their gaps, replay detection, the pod timeline (never two objects), "
          "container exits, memory, phase verdicts, the in-flight arithmetic and the evidence a pass needs (the "
          "generator's and the ingress's): OK")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog="demo_kind.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("follow")
    for name in ("kubeconfig", "context", "namespace", "name", "node", "out"):
        f.add_argument(f"--{name}", required=True)
    f.add_argument("--interval", type=float, default=3.0)
    f.add_argument("--ingress-node", default="")
    an = sub.add_parser("analyse")
    for name in ("dir", "harness", "config", "ingress", "phases", "out"):
        an.add_argument(f"--{name}", required=True)
    sub.add_parser("selftest")
    a = ap.parse_args(argv)
    return {"follow": follow, "analyse": analyse, "selftest": lambda _a: _selftests()}[a.cmd](a)


def _selftests():
    import ingress_faults
    return selftest() or ingress_faults.selftest()


if __name__ == "__main__":
    sys.exit(main())
