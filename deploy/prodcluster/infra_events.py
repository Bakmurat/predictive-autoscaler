#!/usr/bin/env python3
"""Infrastructure-event detector for the prodcluster campaign (evaluation protocol P8), v5 (Codex r30–r35).

Run after the stop and before any score is looked at. Declared sources only, read as RAW samples through the
VictoriaMetrics export API in bounded time chunks; every chunk is streamed into an attempt directory, hashed and
recorded before detection parses the bytes read back from disk. Export lines are merged by label set, sorted,
deduplicated, and rejected when malformed or conflicting; `null` is a staleness marker. State series must hold 0/1
(restart counters non-negative integers) or the run fails. Times are integer milliseconds throughout.

Sources and coverage (a gap in any required series is unknown, never clean):
- workers (frozen identities): kube_node_status_condition{status="true"} for Ready, DiskPressure, MemoryPressure and
  PIDPressure; every condition must be covered (gap > 60 s = 3 scrapes), and states are read only at instants where all
  four were scraped together (one kube-state-metrics scrape carries one timestamp; no value is carried forward).
- benchmark pods (frozen name patterns, filtered server-side). Every series of the namespace's benchmark pods is
  streamed chunk by chunk into per-pod summaries. A pod's lifetime runs from its creation time (kube_pod_created; when
  missing, the start is unresolved back to the window start) to its END, which is established only by a staleness
  marker on its inventory series (kube_pod_info) or by the next inventory scrape of the same exporter instance without
  it; a pod whose inventory stops without such evidence, or any of whose series continue after it, has an unknown tail
  to the window edge, and its series are read over everything observed. Over the lifetime (prefix from creation included): kube_pod_info,
  scheduled[true/false/unknown] (one-hot at each scrape, else unknown) and reason[5] must be covered; a generator pod also
  needs kube_pod_start_time and, from its start, the k6 container's running state and restart counter.
- generators: k6_vus (dense, every 10 s; gap > 30 s) per application. k6 series carry no pod label, so heartbeats are
  bound to incarnations through the k6 containers' running states; heartbeats while no generator of the application is
  observed running are unknown. k6_dropped_iterations_total is sparse (sent only after a drop): where the same
  application's heartbeat is covered, its absence means no drop was reported (this rests on A1′).
- the validity mask, the hourly load-gate rows (P4) and an attribution file for candidates.

Events. Definite (the generator's own evidence, or declared): a k6 container restart; a new generator incarnation (its
container observed running); two generators of one application observed running at the same scrape; a validity-mask
interval. Candidates, unknown until an attribution record resolves them ("infrastructure" → invalid; "system" → the
systems' own outcome, listed and still scored): a worker not Ready or under pressure; a benchmark pod failed with a
kubelet/node reason (the 0 → 1 transition is the event, the terminal reason that stays exported afterwards is not); a
benchmark pod unscheduled for more than 120 s (ambiguous when only its bracket exceeds 120 s); generator pod objects
that coexist without simultaneous running evidence; a generator pod never observed running; a heartbeat gap; dropped
iterations. A restart counter already positive at its first observation is a definite restart when the pod started in
the window and an unresolved (unknown) interval otherwise. Every interval is bracketed by actual observations; a side
without one stays open and its extent to the window edge is unknown. A declared mask interval explains a generator
event within one heartbeat spacing; the parts outside the mask keep their own status.

Classification. Every invalid or unknown interval is extended by the declared 60-minute recovery washout — for coverage
gaps an administrative, conservative assumption whose extra exposure is reported separately. Capacity slots are
[t, t + 10 min) inside [start, stop), each inside one load-gate hour; a forecast target at t depends on [t − 90 s, t] (the
1-min rate plus MetricsQL's preceding sample at a 30-s scrape) and on every gate hour that span touches. Priority:
invalid > unknown > unverified (a gate hour not finalized PASS for exactly the six arms) > gate_fail (finalized, some
arm FAIL: listed for attribution, still scored) > verified_clean. The 20 % budget counts invalid + unknown + unverified.

Archive. A new attempt directory is exclusive and records the run identity; --resume continues an unfinished attempt
with the same identity (one writer, file lock); a chunk that no longer matches its record is moved to raw/rejected/ with
a repair record; only an unterminated final manifest fragment is tolerated; a wall-clock deadline bounds every
download, also while it streams. --reanalyze re-runs detection offline on an archived
attempt (e.g. with a new attribution file) and writes a new result revision; raw data and earlier results stay.

Declared limits: no preemption and no scheduler-reason series exist here; a pod that kube-state-metrics never
exported cannot be seen (both stated with every result).
"""
import argparse, bisect, datetime, fcntl, glob, hashlib, json, math, os, platform, re, sys, tempfile, threading, time
import urllib.parse, urllib.request

MS = 1000
SLOT = 600 * MS
HOUR = 3600 * MS
WASHOUT = 3600 * MS                 # declared capacity-recovery assumption after an event ends
PRE = WASHOUT + HOUR                # history read before the window, so earlier events' washouts are seen
SCHED_ALERT = 120 * MS              # strictly more than this, in milliseconds of unscheduled state
KSM_GAP = 60 * MS                   # > 3 kube-state-metrics scrapes (20 s) missing = coverage gap
HB_GAP = 30 * MS                    # > 3 k6 pushes (10 s) missing = heartbeat gap
TARGET_LOOKBACK = 90 * MS           # 1-min rate window + the sample MetricsQL reads before it (30-s Envoy scrape)
CHUNK = 6 * HOUR                    # export time chunk (VictoriaMetrics export is inclusive at both ends)
RETRIES, RETRY_BACKOFF = 3, 2.0     # per chunk; seconds, doubled per attempt
CONDITIONS = ("Ready", "DiskPressure", "MemoryPressure", "PIDPressure")
SCHEDULED = ("true", "false", "unknown")
POD_REASONS = ("Evicted", "NodeAffinity", "NodeLost", "Shutdown", "UnexpectedAdmissionError")
POD_FAMILIES = ("kube_pod_status_scheduled", "kube_pod_status_reason", "kube_pod_container_status_restarts_total",
                "kube_pod_container_status_running", "kube_pod_start_time")
UNSUPPORTED = ["preemption (no series)", "scheduler Unschedulable reason (no series)",
               "pods never exported by kube-state-metrics"]
ATTRIBUTION_SCHEMA = "p8-attribution-v1"
ATTRIBUTION_KEYS = ("id", "attribution", "evidence", "recorded_by", "recorded_at", "decision_ref")
CLASSES = ("invalid", "unknown", "unverified", "gate_fail", "verified_clean")
BINARY = {0.0, 1.0}


def iso(ms):
    return (datetime.datetime.fromtimestamp(ms // MS, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            + ".%03dZ" % (ms % MS))


def parse(s):
    m = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,3}))?Z", s)
    if not m:
        raise ValueError(f"bad timestamp {s!r}")
    sec = datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    return int(sec.timestamp()) * MS + int((m.group(2) or "0").ljust(3, "0"))


def now_ms():
    return int(time.time() * MS)


# ---------------------------------------------------------------------------------------------------- raw samples
def parse_lines(raw):
    """VictoriaMetrics export JSON lines → [(labels, [(t_ms, value)])]; value None is a staleness marker. Unequal
    arrays, non-integer timestamps and non-finite values are rejected."""
    out = []
    for n, line in enumerate(raw.decode().splitlines(), 1):
        if not line.strip():
            continue
        d = json.loads(line)
        m, ts, vs = d.get("metric"), d.get("timestamps"), d.get("values")
        if not isinstance(m, dict) or not isinstance(ts, list) or not isinstance(vs, list) or len(ts) != len(vs):
            raise ValueError(f"export line {n}: malformed record")
        for t, v in zip(ts, vs):
            if type(t) is not int or not (v is None or (type(v) in (int, float) and math.isfinite(v))):
                raise ValueError(f"export line {n}: bad sample {t!r} {v!r}")
        out.append((m, [(t, None if v is None else float(v)) for t, v in zip(ts, vs)]))
    return out


def merge(samples, what):
    """Sort by time, drop identical duplicates, reject conflicting ones."""
    out = []
    for t, v in sorted(samples, key=lambda x: x[0]):
        if out and out[-1][0] == t:
            if out[-1][1] != v:
                raise ValueError(f"conflicting samples for {what} at {iso(t)}")
            continue
        out.append((t, v))
    return out


def normalize(records):
    """Merge export lines by full label set (one series may come on several lines) → [(labels, samples)]."""
    by = {}
    for m, s in records:
        by.setdefault(tuple(sorted(m.items())), []).extend(s)
    return [(dict(k), merge(s, dict(k))) for k, s in sorted(by.items())]


def by_label(series, name):
    """Merge series that differ only in exporter labels (a kube-state-metrics restart changes them) by one label."""
    out = {}
    for m, s in series:
        out.setdefault(m.get(name), []).extend(s)
    return {k: merge(s, f"{name}={k}") for k, s in out.items()}


def check_domain(series, what, counter=False):
    for k, s in series.items():
        for t, v in s:
            if v is not None and (v < 0 or v != int(v) if counter else v not in BINARY):
                raise ValueError(f"{what}[{k}] value {v} at {iso(t)} outside its domain")


def seen(samples):
    return [t for t, v in samples if v is not None]


def gaps(ts, lo, hi, limit):
    """Intervals inside [lo, hi] not covered by observations at most `limit` apart: (a, b, open_a, open_b); an open
    side is a window edge, not an observation."""
    if hi < lo:
        return []
    ts = [t for t in ts if lo <= t <= hi]
    if not ts:
        return [(lo, hi, True, True)]
    out = []
    if ts[0] - lo > limit:
        out.append((lo, ts[0], True, False))
    out += [(a, b, False, False) for a, b in zip(ts, ts[1:]) if b - a > limit]
    if hi - ts[-1] > limit:
        out.append((ts[-1], hi, False, True))
    return out


def exact_join(series):
    """Instants at which every series has a finite sample (one kube-state-metrics scrape carries one timestamp)."""
    maps = {n: {t: v for t, v in s if v is not None} for n, s in series.items()}
    common = set.intersection(*(set(m) for m in maps.values())) if maps else set()
    return [(t, {n: maps[n][t] for n in maps}) for t in sorted(common)]


def bad_runs(obs):
    """obs: sorted [(t, healthy)] → runs (first_bad, last_bad, healthy_before, healthy_after); None = no observation."""
    out, i = [], 0
    while i < len(obs):
        if obs[i][1]:
            i += 1
            continue
        j = i
        while j + 1 < len(obs) and not obs[j + 1][1]:
            j += 1
        out.append((obs[i][0], obs[j][0], obs[i - 1][0] if i > 0 else None, obs[j + 1][0] if j + 1 < len(obs) else None))
        i = j + 1
    return out


def chunks(src, selector, lo, hi):
    if hasattr(src, "fetch_chunks"):
        yield from src.fetch_chunks(selector, lo, hi)
    else:
        yield src.fetch(selector, lo, hi)


# ---------------------------------------------------------------------------------------------------- events
def validate_attributions(doc):
    if not doc:
        return {}
    if doc.get("schema") != ATTRIBUTION_SCHEMA or not isinstance(doc.get("records"), list):
        raise ValueError(f"attribution file must be {{'schema': '{ATTRIBUTION_SCHEMA}', 'records': [...]}}")
    out = {}
    for r in doc["records"]:
        missing = [k for k in ATTRIBUTION_KEYS if not r.get(k)]
        if missing:
            raise ValueError(f"attribution record {r.get('id')!r} lacks {missing}")
        if r["attribution"] not in ("infrastructure", "system"):
            raise ValueError(f"attribution record {r['id']!r}: attribution must be infrastructure or system")
        if not isinstance(r["evidence"], list) or not all(isinstance(x, str) and x.strip() for x in r["evidence"]):
            raise ValueError(f"attribution record {r['id']!r}: evidence must be a non-empty list of references")
        parse(r["recorded_at"])
        if r["id"] in out:
            raise ValueError(f"duplicate attribution record {r['id']!r}")
        out[r["id"]] = r
    return out


class Events:
    def __init__(self, lo, hi, attributions):
        self.lo, self.hi, self.att, self.used = lo, hi, attributions, set()
        self.events, self.unknown = [], []

    def gap(self, source, a, b):
        if b >= a:
            self.unknown.append({"source": source, "start": a, "end": b})

    def add(self, cls, kind, subject, first, last, before, after, evidence, candidate, definite=True):
        """first/last: the observations showing the event; before/after: the observations bracketing it (None = open:
        its extent to the window edge is unknown)."""
        cid = f"{cls}:{kind}:{subject}:{iso(first)}"
        status, rec = "invalid", None
        if candidate:
            rec = self.att.get(cid)
            status = {"infrastructure": "invalid", "system": "system_outcome"}[rec["attribution"]] if rec else "unknown"
            if rec:
                self.used.add(cid)
        a = before if before is not None else first
        b = after if after is not None else last
        segs = [] if status == "system_outcome" else [[a, b, status]]
        if before is None and self.lo < first:
            segs.append([self.lo, first, "unknown"])
        if after is None and last < self.hi:
            segs.append([last, self.hi, "unknown"])
        self.events.append({"id": cid, "class": cls, "kind": kind, "subject": subject, "start": a, "end": b,
                            "open_start": before is None, "open_end": after is None, "status": status,
                            "candidate": candidate, "definite": definite, "evidence": evidence,
                            "attribution": rec, "segments": segs})


def pod_selector(ns, pod):
    return f'{{__name__=~"{"|".join(POD_FAMILIES)}",namespace="{ns}",pod="{pod}"}}'


def server_regex(pattern):
    """The identity pattern without its anchors (VictoriaMetrics anchors label regexes itself)."""
    return pattern[1 if pattern.startswith("^") else 0:-1 if pattern.endswith("$") else None]


def inventory(src, ev, ns, pattern, lo, hi):
    """Stream every series of the namespace's benchmark pods (server-side filtered) chunk by chunk into per-pod
    summaries: the inventory series (kube_pod_info) with its gaps, staleness marker and exporter instance; the creation
    time; and the earliest and latest observation of any other pod series (evidence the pod existed)."""
    rx, pod_re = server_regex(pattern), re.compile(pattern)
    names = "|".join(("kube_pod_info", "kube_pod_created") + POD_FAMILIES)
    pods, scrapes = {}, {}
    new = lambda: {"first": None, "last": None, "gaps": [], "marker": None, "inst": None, "created": None,
                   "any_first": None, "any_last": None, "null_first": None, "null_last": None}
    for records in chunks(src, f'{{__name__=~"{names}",namespace="{ns}",pod=~"{rx}"}}', lo, hi):
        rows = {}
        for m, s in normalize(records):
            if not pod_re.match(m.get("pod", "")):
                continue
            p = pods.setdefault(m["pod"], new())
            nulls = [t for t, v in s if v is None]
            if nulls:
                p["null_first"] = min(p["null_first"] if p["null_first"] is not None else nulls[0], nulls[0])
                p["null_last"] = max(p["null_last"] if p["null_last"] is not None else nulls[-1], nulls[-1])
            if m.get("__name__") != "kube_pod_info":
                ts = seen(s)
                if ts:
                    p["any_first"] = min(p["any_first"] if p["any_first"] is not None else ts[0], ts[0])
                    p["any_last"] = max(p["any_last"] if p["any_last"] is not None else ts[-1], ts[-1])
                if m.get("__name__") == "kube_pod_created" and p["created"] is None:
                    vals = [v for _, v in s if v is not None]
                    if vals:
                        p["created"] = int(round(vals[0] * MS))
                continue
            inst = f'{m.get("job")}/{m.get("instance")}'
            scrapes.setdefault(inst, set()).update(seen(s))
            rows.setdefault(m["pod"], []).extend((t, v, inst) for t, v in s)
        for pod, xs in rows.items():
            p = pods[pod]
            for t, v, inst in sorted(xs, key=lambda x: (x[0], x[1] is not None)):
                if v is None:
                    if p["last"] is not None and t > p["last"] and p["marker"] is None:
                        p["marker"] = t
                    continue
                if p["marker"] is not None and t > p["marker"]:
                    p["marker"] = None                       # the series came back
                if p["last"] is not None and t - p["last"] > KSM_GAP:
                    p["gaps"].append((p["last"], t))
                if p["first"] is None:
                    p["first"] = t
                if p["last"] is None or t >= p["last"]:
                    p["last"], p["inst"] = t, inst
    out = {}
    scr = {k: sorted(v) for k, v in scrapes.items()}
    for pod, p in sorted(pods.items()):
        subject = f"{ns}/{pod}"
        if p["first"] is None:
            if p["any_first"] is not None:                   # other series without an inventory
                ev.gap(f"pod series without kube_pod_info {subject}", p["any_first"], p["any_last"])
            else:                                            # only staleness markers: it existed before, extent unknown
                ev.gap(f"pod seen only through staleness markers {subject}", lo, p["null_last"])
            continue
        seen_first = min(p["first"], p["any_first"] if p["any_first"] is not None else p["first"])
        seen_last = max(p["last"], p["any_last"] if p["any_last"] is not None else p["last"])
        if p["created"] is None:                             # the start of the lifetime is not established
            if seen_first - lo > KSM_GAP:
                ev.gap(f"kube_pod_created missing, lifetime start unresolved {subject}", lo, seen_first)
            begin, known = seen_first, False
        else:
            begin, known = min(p["created"], seen_first), True
        alive = hi - p["last"] <= KSM_GAP
        end = None
        if not alive:
            if p["marker"] is not None:
                end = p["marker"]
            else:                                            # the same exporter's next inventory scrape without the pod
                own = scr.get(p["inst"], [])
                i = bisect.bisect_right(own, p["last"])
                end = own[i] if i < len(own) else None
        contradicted = seen_last > p["last"] + KSM_GAP
        if contradicted:
            ev.gap(f"pod series continue after kube_pod_info ends {subject}", p["last"], hi)
            end = None
        elif not alive and end is None:
            ev.gap(f"pod end not established {subject}", p["last"], hi)
        s0 = max(lo, begin)
        if p["first"] - s0 > KSM_GAP:
            ev.gap(f"kube_pod_info prefix {subject}", s0, p["first"])
        for a, b in p["gaps"]:
            if b >= lo and a <= hi:
                ev.gap(f"kube_pod_info {subject}", max(a, lo), min(b, hi))
        out[pod] = dict(p, begin=begin, end=end, alive=alive, truncated=begin < lo or not known,
                        fetch_from=max(lo, min(begin, seen_first)), life_end=seen_last)
    return out


def detect(src, start, stop, ident, mask=None, attributions=None):
    """src.fetch(selector, lo, hi) → raw export records (src.fetch_chunks optional). Returns (events, unknown)."""
    lo, hi = start - PRE, stop
    ev = Events(lo, hi, validate_attributions(attributions))

    # (a) workers: every condition covered; states only at joint scrapes; unhealthy runs are candidates
    for node in ident["workers"]:
        sel = f'kube_node_status_condition{{node="{node}",status="true",condition=~"{"|".join(CONDITIONS)}"}}'
        conds = by_label(normalize(src.fetch(sel, lo, hi)), "condition")
        conds = {c: conds.get(c, []) for c in CONDITIONS}
        check_domain(conds, f"node {node}")
        for c, s in conds.items():
            for a, b, _, _ in gaps(seen(s), lo, hi, KSM_GAP):
                ev.gap(f"kube-state-metrics node {node} condition {c}", a, b)
        joined = exact_join(conds)
        for a, b, _, _ in gaps([t for t, _ in joined], lo, hi, KSM_GAP):
            ev.gap(f"kube-state-metrics node {node} joint scrape", a, b)
        obs = [(t, v["Ready"] == 1 and not any(v[c] for c in CONDITIONS[1:])) for t, v in joined]
        for first, last, before, after in bad_runs(obs):
            ev.add("a", "node not ready or under pressure", node, first, last, before, after, sel, candidate=True)

    gen_re = {app: re.compile(p) for app, p in ident["generators"].items()}
    gens = {app: [] for app in gen_re}
    for ns, pattern in ident["pod_patterns"].items():
        for pod, p in inventory(src, ev, ns, pattern, lo, hi).items():
            subject = f"{ns}/{pod}"
            life = (max(lo, p["begin"]), min(hi, p["life_end"]))
            fam = normalize(src.fetch(pod_selector(ns, pod), p["fetch_from"], min(hi, life[1] + KSM_GAP)))
            pick = lambda name: [x for x in fam if x[0].get("__name__") == name]
            sched = by_label(pick("kube_pod_status_scheduled"), "condition")
            sched = {k: sched.get(k, []) for k in SCHEDULED}
            reasons = by_label(pick("kube_pod_status_reason"), "reason")
            reasons = {k: reasons.get(k, []) for k in POD_REASONS}
            check_domain(sched, f"scheduled {subject}")
            check_domain(reasons, f"reason {subject}")
            for fam_name, got in (("kube_pod_status_scheduled", sched), ("kube_pod_status_reason", reasons)):
                for k, s in got.items():
                    for a, b, _, _ in gaps(seen(s), *life, KSM_GAP):
                        ev.gap(f"{fam_name}[{k}] {subject}", a, b)

            def bracket(first, last, before, after, obs):
                if before is None and not p["truncated"] and first == obs[0][0]:
                    before = p["begin"]                      # the run starts with the pod: its creation closes it
                if after is None and last == obs[-1][0] and p["end"] is not None:
                    after = p["end"]                         # the run lasts until the pod's established end
                return before, after

            # (c) unscheduled for more than 120 s; scheduled states must be one-hot at each scrape
            joined = exact_join(sched)
            for a, b, _, _ in gaps([t for t, _ in joined], *life, KSM_GAP):
                ev.gap(f"kube_pod_status_scheduled joint scrape {subject}", a, b)
            obs = []
            for i, (t, v) in enumerate(joined):
                if sum(v.values()) != 1:
                    ev.gap(f"kube_pod_status_scheduled not one-hot {subject}",
                           joined[i - 1][0] if i else t, joined[i + 1][0] if i + 1 < len(joined) else t)
                    continue
                obs.append((t, v["true"] == 1))
            for first, last, before, after in bad_runs(obs):
                before, after = bracket(first, last, before, after, obs)
                upper = (after if after is not None else hi) - (before if before is not None else lo)
                if last - first > SCHED_ALERT:
                    ev.add("c", "unscheduled > 120 s", subject, first, last, before, after, "kube_pod_status_scheduled", True)
                elif upper > SCHED_ALERT:
                    ev.add("c", "unscheduled, ambiguous vs 120 s", subject, first, last, before, after,
                           "kube_pod_status_scheduled", True, definite=False)
            # (b) kubelet / node reasons: the transition is the event; a terminal reason stays exported afterwards
            for reason in POD_REASONS:
                obs = [(t, v == 0) for t, v in reasons[reason] if v is not None]
                for first, last, before, after in bad_runs(obs):
                    before, _ = bracket(first, last, before, None, obs)
                    if after is None:                        # terminal: closed at the first observation of the failed state
                        ev.add("b", reason, subject, first, first, before, first, "kube_pod_status_reason", True)
                    else:
                        ev.add("b", reason, subject, first, last, before, after, "kube_pod_status_reason", True)

            for app, gre in gen_re.items():
                if ns != "demo" or not gre.match(pod):
                    continue
                k6 = lambda name: merge([q for m, x in pick(name) if m.get("container") == "k6" for q in x], f"{name} {subject}")
                restarts, running = k6("kube_pod_container_status_restarts_total"), k6("kube_pod_container_status_running")
                check_domain({"k6": restarts}, f"restarts {subject}", counter=True)
                check_domain({"k6": running}, f"running {subject}")
                sts = merge([q for _, x in pick("kube_pod_start_time") for q in x], f"start time {subject}")
                vals = sorted({v for _, v in sts if v is not None})
                st = int(round(vals[0] * MS)) if vals else None
                if st is None:
                    ev.gap(f"kube_pod_start_time {subject}", *life)
                elif len(vals) > 1:                          # a pod's start time is a constant
                    ev.gap(f"kube_pod_start_time changes {subject}", *life)
                cfrom = (max(lo, st), life[1]) if st is not None else life
                for name, s in (("start time", sts), ("restarts", restarts), ("running", running)):
                    for a, b, _, _ in gaps(seen(s), *cfrom, KSM_GAP):
                        ev.gap(f"k6 container {name} {subject}", a, b)
                run_obs = [(t, v == 1) for t, v in running if v is not None]
                runs = [(f, l, b, a) for f, l, b, a in bad_runs([(t, not r) for t, r in run_obs])]
                gens[app].append(dict(p, pod=pod, st=st, restarts=restarts, runs=runs,
                                      run_times={t for t, r in run_obs if r}, obs_times=[t for t, _ in run_obs]))

    # (d) generators
    for app, pods in gens.items():
        hb = sorted({t for _, s in normalize(src.fetch(f'k6_vus{{testid="{app}"}}', lo, hi)) for t in seen(s)})
        pods.sort(key=lambda p: p["begin"])
        running_at = lambda q, a, b: any(a <= t <= b for t in q["run_times"])
        for i, p in enumerate(pods):
            for q in pods[i + 1:]:
                common = sorted(p["run_times"] & q["run_times"])
                if common:                                   # both observed running at the same scrapes
                    near = sorted(set(p["obs_times"]) | set(q["obs_times"]))
                    before = max((t for t in near if t < common[0]), default=None)
                    after = min((t for t in near if t > common[-1]), default=None)
                    ev.add("d", "simultaneous generators", f"{p['pod']}+{q['pod']}", common[0], common[-1],
                           before, after, "kube_pod_container_status_running", candidate=False)
                elif q["begin"] <= p["last"]:                # objects coexist; running not shown together
                    ev.add("d", "generator pod objects overlap", f"{p['pod']}+{q['pod']}", q["begin"], p["last"],
                           q["begin"], p["end"], "kube_pod_info", True)
        starts = []
        for p in pods:
            anchor = p["st"] if p["st"] is not None else p["begin"]
            alone = lambda a, b: not any(q is not p and running_at(q, a - HB_GAP, b + HB_GAP) for q in pods)
            if not p["truncated"] and lo < anchor < hi:
                if p["runs"]:
                    f_run = p["runs"][0][0]
                    starts.append(anchor)
                    ev.add("d", "new generator incarnation", p["pod"], anchor, f_run,
                           max((h for h in hb if h <= anchor), default=None), min((h for h in hb if h >= f_run), default=None),
                           "kube_pod_start_time + kube_pod_container_status_running", False)
                else:
                    ev.add("d", "generator pod never observed running", p["pod"], p["begin"], p["last"],
                           p["begin"], p["end"], "kube_pod_container_status_running", True)
            mine = [h for h in hb if any(f - KSM_GAP <= h <= l + KSM_GAP for f, l, _, _ in p["runs"])]
            vals = [(t, v) for t, v in p["restarts"] if v is not None]
            if vals and vals[0][1] > 0:                      # already restarted at its first observation
                if not p["truncated"] and anchor >= lo:
                    ev.add("d", "k6 container restart", p["pod"], anchor, vals[0][0], anchor, vals[0][0],
                           "kube_pod_container_status_restarts_total (positive at its first sample)", False)
                else:
                    ev.gap(f"k6 restart count positive at first observation {p['pod']}", max(lo, anchor), vals[0][0])
            for (t0, v0), (t1, v1) in zip(vals, vals[1:]):
                if v1 > v0:
                    if alone(t0, t1):
                        before = max((h for h in mine if h <= t0), default=t0)
                        after = min((h for h in mine if h >= t1), default=None)
                    else:                                    # heartbeats cannot be attributed to this incarnation
                        before, after = t0, t1
                    ev.add("d", "k6 container restart", p["pod"], t0, t1, before, after,
                           "kube_pod_container_status_restarts_total", False)
                    starts.append(t1)
                elif v1 < v0:
                    ev.gap(f"k6 restart counter decreased {p['pod']}", t0, t1)
        # heartbeats while no generator of the application is observed running
        cover = sorted((f - KSM_GAP, (a if a is not None else l + KSM_GAP)) for p in pods for f, l, _, a in p["runs"])
        orphan = [h for h in hb if not any(x <= h <= y for x, y in cover)]
        group = []
        for h in orphan + [None]:
            if group and (h is None or h - group[-1] > HB_GAP):
                ev.gap(f"k6 heartbeats without a running generator {app}", group[0], group[-1])
                group = []
            if h is not None:
                group.append(h)
        for a, b, oa, ob in gaps(hb, lo, hi, HB_GAP):
            ev.add("d", "k6 heartbeat gap", app, a, b, None if oa else a, None if ob else b, "k6_vus", True)
        starts += [f for p in pods for f, _, _, _ in p["runs"]]
        starts.sort()
        for m, s in normalize(src.fetch(f'k6_dropped_iterations_total{{testid="{app}"}}', lo, hi)):
            check_domain({"k6": s}, f"dropped iterations {app}", counter=True)
            prev = None
            for t, v in s:
                if v is None:
                    prev = None                              # staleness marker: the counter ended
                    continue
                if (prev is None or v < prev[1]) and v > 0:  # first sample, or a new counter after a reset
                    inc = max((x for x in starts if x <= t), default=None)
                    ev.add("d", "k6 dropped iterations", app, t, t, inc, t, "k6_dropped_iterations_total", True)
                elif prev is not None and v > prev[1]:
                    ev.add("d", "k6 dropped iterations", app, t, t, prev[0], t, "k6_dropped_iterations_total", True)
                prev = (t, v)

    # (e) validity-mask intervals (declared), including those before the window
    masks = []
    for iv in (mask or {}).get("intervals", []):
        a, b = parse(iv["start"]), parse(iv["end"])
        if b > lo and a < hi:
            ev.add("e", "validity-mask interval", "all arms", a, b, a, b, iv.get("reason", ""), candidate=False)
            masks.append((ev.events[-1]["id"], a, b))
    # a declared mask interval explains a generator event within one heartbeat spacing; the parts outside it keep
    # their status (the mask itself covers the inside)
    for e in ev.events:
        if e["class"] != "d":
            continue
        for mid, a, b in masks:
            if a - HB_GAP <= e["start"] and e["end"] <= b + HB_GAP:
                e["explained_by"] = mid
                segs = []
                for sa, sb, st in e["segments"]:
                    if sa < a:
                        segs.append([sa, min(sb, a), st])
                    if sb > b:
                        segs.append([max(sa, b), sb, st])
                e["segments"] = segs
                break

    stale = sorted(set(ev.att) - ev.used)
    if stale:
        raise ValueError(f"attribution records match no candidate: {stale}")
    return ev.events, ev.unknown


# ---------------------------------------------------------------------------------------------------- classification
def gate_status(rows, apps):
    """verified: exactly the six arms, each a finalized qualifying PASS; gate_fail: all finalized, some FAIL;
    otherwise unverified."""
    if rows is None or set(rows) != set(apps):
        return "unverified"
    fin = lambda r: (r.get("maturity") or {}).get("finalized") is True
    ok = lambda r: r.get("status") == "PASS" and fin(r) and (r.get("qualification") or {}).get("qualifies") is True
    if all(ok(r) for r in rows.values()):
        return "verified"
    if all(ok(r) or (r.get("status") == "FAIL" and fin(r)) for r in rows.values()):
        return "gate_fail"
    return "unverified"


class Classifier:
    def __init__(self, segments, gate_hours):
        """segments: [(a, b, status)] already extended by the washout; gate_hours: {hour_start_ms: gate status}."""
        self.segs, self.gates = segments, gate_hours

    @staticmethod
    def build(events, unknown, gates, apps, coverage_washout=True):
        segs = [(a, b + WASHOUT, st) for e in events for a, b, st in e["segments"]]
        segs += [(u["start"], u["end"] + (WASHOUT if coverage_washout else 0), "unknown") for u in unknown]
        return Classifier(segs, {h: gate_status(r, apps) for h, r in (gates or {}).items()})

    def _worst(self, s, e, closed, hours):
        hit = {st for a, b, st in self.segs if a <= e and b >= s and (closed or a < e)}
        for c in ("invalid", "unknown"):
            if c in hit:
                return c
        g = {self.gates.get(h, "unverified") for h in hours}
        return "unverified" if "unverified" in g else "gate_fail" if "gate_fail" in g else "verified_clean"

    def target(self, t):
        """A forecast target at t: its value depends on [t − 90 s, t] and on every gate hour that span touches."""
        s = t - TARGET_LOOKBACK
        return self._worst(s, t, True, {s - s % HOUR, t - t % HOUR})

    def capacity(self, a, b):
        """A capacity interval [a, b)."""
        return self._worst(a, b, False, set(range(a - a % HOUR, b, HOUR)))


def classify(events, unknown, start, stop, apps, gates=None):
    """Capacity slots [t, t + 10 min) in [start, stop) and forecast targets at the same grid instants."""
    if start % SLOT or stop % SLOT or stop <= start:
        raise ValueError("start and stop must be ten-minute grid instants with start < stop")
    cl = Classifier.build(events, unknown, gates, apps)
    bare = Classifier.build(events, unknown, gates, apps, coverage_washout=False)
    grid = range(start, stop, SLOT)
    slots = {t: cl.capacity(t, t + SLOT) for t in grid}
    targets = {t: cl.target(t) for t in grid}
    n = {c: sum(1 for v in slots.values() if v == c) for c in CLASSES}
    total = len(slots)
    budget = n["invalid"] + n["unknown"] + n["unverified"]
    extra = sum(1 for t in grid if slots[t] == "unknown" and bare.capacity(t, t + SLOT) not in ("invalid", "unknown"))
    return slots, targets, dict(n, total=total, budget_slots=budget, compromised=budget > 0.2 * total,
                                coverage_washout_extra_slots=extra,
                                targets={c: sum(1 for v in targets.values() if v == c) for c in CLASSES}), cl


def gate_hours(directory):
    """Latest load-gate projections {hour_start_ms: {app: row}} and the files read, with their hashes."""
    out, files = {}, []
    for f in sorted(glob.glob(os.path.join(directory, "load-2*.json"))):
        data = open(f, "rb").read()
        files.append({"file": os.path.basename(f), "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)})
        for r in json.loads(data):
            out.setdefault(parse(r["hour_start"]), {})[r["app"]] = r
    return out, files


# ---------------------------------------------------------------------------------------------------- archive
def write_atomic(path, obj, exclusive=False):
    if exclusive and os.path.exists(path):
        raise FileExistsError(path)
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp-")
    with os.fdopen(fd, "w") as fh:
        json.dump(obj, fh, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def append_record(path, rec):
    with open(path, "a") as fh:
        fh.write(json.dumps(rec) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


class Archive:
    """Chunked raw export into one attempt directory (modes: new, resume, offline). Each chunk is streamed to a
    temporary file, fsynced and renamed, then recorded with its hash and byte count in an append-only, fsynced
    `manifest.jsonl`. One writer per attempt (file lock); the attempt is bound to its run identity."""

    def __init__(self, base, attempt, identity, mode="new", opener=None, deadline=None):
        if mode == "new":
            os.makedirs(os.path.join(attempt, "raw"))
        elif not os.path.isdir(attempt):
            raise ValueError(f"no attempt at {attempt}")
        self.lock = open(os.path.join(attempt, ".lock"), "w")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f"another process holds {attempt}")
        self.run_path = os.path.join(attempt, "run.json")
        if mode == "new":
            write_atomic(self.run_path, identity, exclusive=True)
        else:
            have = json.load(open(self.run_path))
            keys = [k for k in identity if not (mode == "offline" and k == "detector_sha256")]
            diff = [k for k in keys if have.get(k) != identity[k]]
            if diff:
                raise ValueError(f"run identity differs from the attempt's: {diff}")
            if mode == "resume" and os.path.exists(os.path.join(attempt, "result.json")):
                raise ValueError("the attempt is complete; use --reanalyze for a new result revision")
        self.base, self.attempt, self.mode, self.deadline = base.rstrip("/"), attempt, mode, deadline
        self.opener = opener or (lambda url, timeout: urllib.request.urlopen(url, timeout=timeout))
        self.log = os.path.join(attempt, "manifest.jsonl")
        self.path = os.path.join(attempt, "manifest.json")
        self.entries, self.repairs, self.torn = {}, [], False
        text = open(self.log).read() if os.path.exists(self.log) else ""
        lines = text.split("\n")
        tail = lines.pop()                                   # "" when the file ends with a newline
        for i, line in enumerate(lines):
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                raise ValueError(f"manifest.jsonl line {i + 1} is malformed")
            if "repair" in e:
                self.repairs.append(e["repair"])
            else:
                self.entries[(e["selector"], e["start_ms"], e["end_ms"])] = e
        self.torn = tail != ""                               # an unterminated final fragment: an interrupted append
        if self.torn:
            with open(self.log, "w") as fh:                  # drop only the torn tail, keep the rest verbatim
                fh.write("".join(l + "\n" for l in lines))
        self.planned, self.reused = [], 0

    def fetch_chunks(self, selector, lo, hi):
        c = lo
        while c <= hi:
            e = min(c + CHUNK - 1, hi)
            yield parse_lines(self._chunk(selector, c, e))
            c = e + 1

    def fetch(self, selector, lo, hi):
        return [r for part in self.fetch_chunks(selector, lo, hi) for r in part]

    def _chunk(self, selector, a, b):
        key = (selector, a, b)
        self.planned.append(key)
        name = f"raw/{hashlib.sha256(selector.encode()).hexdigest()[:16]}-{a}-{b}.jsonl"
        path = os.path.join(self.attempt, name)
        ent = self.entries.get(key)
        if ent and os.path.exists(path):
            data = open(path, "rb").read()
            if hashlib.sha256(data).hexdigest() == ent["sha256"] and len(data) == ent["bytes"]:
                self.reused += 1
                return data
            if self.mode == "offline":
                raise ValueError(f"archived chunk {name} does not match its record")
            os.makedirs(os.path.join(self.attempt, "raw", "rejected"), exist_ok=True)
            kept = os.path.join("raw", "rejected", os.path.basename(name) + "." + iso(now_ms()))
            os.replace(path, os.path.join(self.attempt, kept))
            rep = {"file": name, "kept_as": kept, "at": iso(now_ms()), "expected_sha256": ent["sha256"],
                   "found_sha256": hashlib.sha256(data).hexdigest(), "found_bytes": len(data)}
            append_record(self.log, {"repair": rep})
            self.repairs.append(rep)
        if self.mode == "offline":
            raise ValueError(f"chunk not in the archive: {selector} {iso(a)}–{iso(b)}")
        url = self.base + "/api/v1/export?" + urllib.parse.urlencode(
            {"match[]": selector, "start": "%d.%03d" % divmod(a, MS), "end": "%d.%03d" % divmod(b, MS)})
        for attempt in range(RETRIES):
            left = lambda: (self.deadline - now_ms()) / MS if self.deadline is not None else 300.0
            if left() <= 0:
                raise TimeoutError("export deadline passed")
            try:
                fd, tmp = tempfile.mkstemp(dir=os.path.join(self.attempt, "raw"), prefix=".part-")
                with os.fdopen(fd, "wb") as fh, self.opener(url, min(300.0, left())) as resp:
                    fired = threading.Event()               # a watchdog closes the response at the deadline,
                    dog = threading.Timer(max(0.0, left()), lambda: (fired.set(), resp.close()))   # even mid-read
                    dog.daemon = True
                    dog.start()
                    try:
                        while True:
                            try:
                                block = resp.read(1 << 20)
                            except Exception:
                                if fired.is_set():
                                    raise TimeoutError("export deadline passed while streaming")
                                raise
                            if fired.is_set() or left() <= 0:
                                raise TimeoutError("export deadline passed while streaming")
                            if not block:
                                break
                            fh.write(block)
                    finally:
                        dog.cancel()
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, path)
                break
            except Exception as exc:
                if os.path.exists(tmp):
                    os.remove(tmp)
                if attempt == RETRIES - 1 or isinstance(exc, TimeoutError):
                    raise
                time.sleep(RETRY_BACKOFF * 2 ** attempt)
        data = open(path, "rb").read()
        ent = {"file": name, "selector": selector, "start_ms": a, "end_ms": b, "start": iso(a), "end": iso(b),
               "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        append_record(self.log, ent)
        self.entries[key] = ent
        return data

    def finish(self):
        """Every planned chunk was fetched or verified; the final manifest lists exactly the planned chunks."""
        missing = [k for k in self.planned if k not in self.entries]
        if missing or len(set(self.planned)) != len(self.planned):
            raise ValueError(f"manifest does not match the plan ({len(missing)} missing)")
        if self.mode == "offline":
            return
        write_atomic(self.path, [self.entries[k] for k in self.planned])


def sha_file(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def main(argv=None, opener=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--stop", required=True)
    ap.add_argument("--identities", required=True)
    ap.add_argument("--mask", required=True)
    ap.add_argument("--load-gate-dir")
    ap.add_argument("--attributions")
    ap.add_argument("--archive-root", required=True, help="a new attempt directory is created inside it")
    ap.add_argument("--resume", help="an unfinished attempt with the same run identity (verified chunks are reused)")
    ap.add_argument("--reanalyze", help="a complete attempt: detect offline from its raw data, write a new revision")
    ap.add_argument("--deadline-minutes", type=float, default=240)
    ap.add_argument("--json", required=True)
    a = ap.parse_args(argv)
    start, stop = parse(a.start), parse(a.stop)
    identity = {"prom": a.prom.rstrip("/"), "start": iso(start), "stop": iso(stop),
                "detector_sha256": sha_file(os.path.abspath(__file__)), "identities_sha256": sha_file(a.identities),
                "mask_sha256": sha_file(a.mask), "chunk_ms": CHUNK, "pre_ms": PRE}
    if a.reanalyze:
        attempt, mode = a.reanalyze, "offline"
    elif a.resume:
        attempt, mode = a.resume, "resume"
    else:
        attempt, mode = os.path.join(a.archive_root, "attempt-" + datetime.datetime.now(
            datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")), "new"
    arch = Archive(a.prom, attempt, identity, mode, opener, deadline=now_ms() + int(a.deadline_minutes * 60 * MS))
    try:
        ident = json.load(open(a.identities))
        mask = json.load(open(a.mask))
        att = json.load(open(a.attributions)) if a.attributions else None
        events, unknown = detect(arch, start, stop, ident, mask, att)
        arch.finish()
        gates, gate_files = gate_hours(a.load_gate_dir) if a.load_gate_dir else (None, [])
        slots, targets, summary, cl = classify(events, unknown, start, stop, list(ident["generators"]), gates)
    except Exception as exc:
        if mode != "offline":
            write_atomic(os.path.join(attempt, "failure-" + iso(now_ms()).replace(":", "") + ".json"), {
                "at": iso(now_ms()), "error": repr(exc), "planned_chunks": len(arch.planned),
                "recorded_chunks": len(arch.entries), "repairs": arch.repairs})
        raise
    ser = lambda x: dict(x, start=iso(x["start"]), end=iso(x["end"]), start_ms=x["start"], end_ms=x["end"])
    revision = 0 if mode != "offline" else 1 + len(glob.glob(os.path.join(attempt, "result-r*.json")))
    out = {"detector": "infra_events.py v5", "detector_sha256": identity["detector_sha256"],
           "python": platform.python_version(), "attempt": os.path.basename(os.path.normpath(attempt)),
           "revision": revision, "run_identity": identity,
           "inputs_sha256": {k: sha_file(v) for k, v in (("identities", a.identities), ("mask", a.mask),
                                                        ("attributions", a.attributions)) if v},
           "load_gate_files": gate_files, "manifest_sha256": sha_file(arch.path),
           "chunks": {"planned": len(arch.planned), "reused": arch.reused, "repairs": arch.repairs,
                      "torn_manifest_line_dropped": arch.torn},
           "start": iso(start), "stop": iso(stop), "unsupported": UNSUPPORTED,
           "constants_ms": {"washout": WASHOUT, "pre": PRE, "sched_alert": SCHED_ALERT, "ksm_gap": KSM_GAP,
                            "hb_gap": HB_GAP, "target_lookback": TARGET_LOOKBACK, "slot": SLOT, "chunk": CHUNK},
           "assumptions": ["60-min recovery washout after every invalid/unknown interval (declared)",
                           "the washout after coverage gaps is administrative and conservative; its extra exposure is "
                           "summary.coverage_washout_extra_slots"],
           "summary": summary,
           "events": [dict(ser(e), segments=[[iso(x), iso(y), s] for x, y, s in e["segments"]],
                           segments_ms=e["segments"]) for e in events],
           "unknown": [ser(u) for u in unknown],
           "segments_with_washout_ms": cl.segs,
           "gate_hours": {iso(h): g for h, g in sorted(cl.gates.items())},
           "load_gate_fail_hours_for_attribution": sorted(
               {(iso(h), app) for h, g in (gates or {}).items() for app, r in g.items() if r.get("status") == "FAIL"}),
           "capacity_slots": {iso(t): c for t, c in sorted(slots.items())},
           "targets": {iso(t): c for t, c in sorted(targets.items())}}
    name = "result.json" if mode != "offline" else f"result-r{revision}.json"
    write_atomic(os.path.join(attempt, name), out, exclusive=True)
    write_atomic(a.json, out)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
