"""Per-minute accounting of the demonstration generator, binned from the RECORDED schedule origin (LIVE-DEMO-PLAN
section 6). It reads the generator's own records (load/c28-demo.js: one JSON line per target and minute in the pod log,
independent of remote-write delivery) and the ingress's raw counter samples, and never changes reduce.py's scoring.

Every bin is minute m = floor((t - origin) / 60 s) of the ONE origin in the generator's configuration; the reducer has
no other notion of a start. A generator that starts late, or restarts, therefore costs only the minutes it really did
not cover: its first minute is PARTIAL (the record says exactly which part of the minute it was active), and every
later minute is binned exactly as if it had started on time (the lesson of review finding R2-01).

Which timestamp each count uses
  intended     the plan (per_minute[m mod 1440]); no timestamp
  due          the arrival's SCHEDULED time, inside the incarnation's active span
  dispatched   the generator's clock immediately before the send (dispatch time)
  dropped      the scheduled time of the arrival the generator did not send (late / inflight)
  ok, failed   the terminal outcome of the requests DISPATCHED in the minute (attributed to the dispatch minute, never
               to the completion time)
  received     the ingress's clock when the request arrived (c28_ingress_requests_total), from RAW samples with their
               own timestamps, interpolated at the minute boundaries between the two adjacent samples
Received and dispatched are never required to match inside one minute: requests in flight across a boundary move
between neighbours. They are reconciled as a running balance instead (see _reconcile).

Row states
  observed   one incarnation covered the whole minute, its record is self-consistent and equals the plan, the ingress
             count is known, the clocks agree within the limit, and the running balance reconciles
  partial    the record is trustworthy but does not describe a whole, fully observed minute: an incarnation started
             or ended inside it (the active span says where), the generator was provably absent (between an end record
             and the next start), or the ingress count is unknown (a scrape gap, a counter reset, no series)
  unknown    the generator's side cannot be trusted or is missing: no record while a generator may have been running
             (a killed incarnation's tail, a lost line), records of two incarnations overlapping (duplicate
             generators), an inconsistent record, a clock offset beyond the limit or no clock evidence, or a balance
             that does not reconcile. Unknown is never zero and never healthy.
`reasons` names every cause; `generator_state` and `ingress_state` keep the two sides apart.
"""

from __future__ import annotations

import json
import math
import re
from typing import Dict, Iterable, List, Optional, Tuple

from . import frozen as F
from . import reduce as _reduce

SCHEMA = "c28-demo-reduction/v1"
MINUTE = "c28-demo-minute/v1"
INCARNATION = "c28-demo-incarnation/v1"
MAX_LINE_BYTES = 16_384
DEFAULT_MAX_SCRAPE_GAP_S = 2 * F.SCRAPE_INTERVAL_S + 5        # two missed scrapes are a gap
DEFAULT_CLOCK_LIMIT_MS = 2_000
CLOCK_NEIGHBOUR_MINUTES = 5

_K8S_TIME = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+(Z|[+-][0-9:]+)\s+")
_LOGFMT_MSG = re.compile(r'\bmsg="((?:[^"\\]|\\.)*)"')


# --- reading the pod log --------------------------------------------------------------------------------------------

def parse_lines(lines: Iterable[str]) -> Tuple[List[Dict], Dict]:
    """The generator's records in a pod log, whatever k6 wrapped them in: the bare JSON line (--log-format raw), k6's
    JSON log line ({"msg": "<the record>"}), or its text format (msg="<escaped record>"); a leading `kubectl logs
    --timestamps` time is dropped. Other lines (k6's own messages) are counted, never interpreted."""
    out, stats = [], {"lines": 0, "records": 0, "other": 0, "oversize": 0}
    for raw in lines:
        stats["lines"] += 1
        line = raw.strip()
        if len(line) > MAX_LINE_BYTES:
            stats["oversize"] += 1
            continue
        line = _K8S_TIME.sub("", line, count=1)
        rec = _record(line)
        if rec is None:
            stats["other"] += 1
        else:
            stats["records"] += 1
            out.append(rec)
    return out, stats


def _record(line: str) -> Optional[Dict]:
    doc = None
    if line.startswith("{"):
        try:
            doc = json.loads(line)
        except ValueError:
            return None
        if isinstance(doc, dict) and "schema" not in doc and isinstance(doc.get("msg"), str):
            try:
                doc = json.loads(doc["msg"])
            except ValueError:
                return None
    else:
        m = _LOGFMT_MSG.search(line)
        if not m:
            return None
        try:
            doc = json.loads(json.loads('"' + m.group(1) + '"'))
        except ValueError:
            return None
    if isinstance(doc, dict) and doc.get("schema") in (MINUTE, INCARNATION):
        return doc
    return None


# --- the generator's side ---------------------------------------------------------------------------------------------

def _int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _line_problems(cfg: Dict, r: Dict) -> List[str]:
    """Why a minute record cannot be trusted (empty when it is self-consistent and equals the plan)."""
    try:
        fields = [r["minute"], r["day_minute"], r["intended"], r["due"], r["dispatched"], r["ok"], r["failed"],
                  r["unresolved"], r["carried"][0], r["carried"][1], r["dropped"]["late"], r["dropped"]["inflight"],
                  r["active"][0], r["active"][1], r["n"]]
        counts = r["lat"]["counts"]
    except (KeyError, IndexError, TypeError):
        return ["malformed_line"]
    if not all(_int(v) for v in fields) or any(v < 0 for v in fields[2:]) or not isinstance(counts, list) \
            or not all(_int(c) and c >= 0 for c in counts):
        return ["malformed_line"]
    out = []
    m = r["minute"]
    if r["day_minute"] != m % cfg["day_minutes"] or r["intended"] != cfg["per_minute"][m % cfg["day_minutes"]]:
        out.append("plan_mismatch")
    a0, a1 = r["active"]
    if not 0 <= a0 <= a1 <= cfg["minute_ms"]:
        out.append("inconsistent_line")
    elif r["due"] != r["dispatched"] - r["carried"][0] + r["carried"][1] + r["dropped"]["late"] + r["dropped"]["inflight"] \
            or r["dispatched"] != r["ok"] + r["failed"] + r["unresolved"] or sum(counts) != r["ok"] \
            or r["due"] > r["intended"]:
        out.append("inconsistent_line")
    if r["unresolved"]:
        out.append("unresolved_outcomes")
    return out


class _Stream:
    """One incarnation's records for one target."""

    def __init__(self, inc: str, target: str):
        self.inc, self.target = inc, target
        self.start: Optional[Dict] = None
        self.end: Optional[Dict] = None
        self.minutes: Dict[int, Dict] = {}
        self.problems: Dict[int, List[str]] = {}
        self.flags: List[str] = []
        self.seen: Dict[int, str] = {}                 # line number n -> canonical content
        self.duplicates = 0

    def add(self, cfg: Dict, r: Dict) -> None:
        body = json.dumps(r, sort_keys=True)
        n = r.get("n")
        if _int(n) and n in self.seen:
            if self.seen[n] == body:
                self.duplicates += 1                   # the same line read twice (overlapping log pulls)
            elif "conflicting_lines" not in self.flags:
                self.flags.append("conflicting_lines")
            return
        if _int(n):
            self.seen[n] = body
        if r["schema"] == INCARNATION:
            if r.get("event") == "start":
                self.start = r
            elif r.get("event") == "end":
                self.end = r
            return
        probs = _line_problems(cfg, r)
        if "malformed_line" in probs:
            if "malformed_line" not in self.flags:
                self.flags.append("malformed_line")
            return
        m = r["minute"]
        if m in self.minutes:
            self.flags.append("conflicting_lines")
            return
        self.minutes[m] = r
        if probs:
            self.problems[m] = probs

    def finish(self, cfg: Dict) -> None:
        if self.start is not None and self.start.get("config_digest") != cfg.get("digest"):
            self.flags.append("config_mismatch")
        ordered = [self.minutes[m] for m in sorted(self.minutes)]
        for a, b in zip(ordered, ordered[1:]):
            if any(b["cum"].get(k, 0) < a["cum"].get(k, 0) for k in ("due", "dispatched", "dropped", "ok", "failed")) \
                    or b["n"] <= a["n"]:
                if "generator_counter_reset" not in self.flags:
                    self.flags.append("generator_counter_reset")

    @property
    def started_ms(self) -> Optional[int]:
        if self.start is not None and _int(self.start.get("active_from_ms")):
            return self.start["active_from_ms"]
        return None

    @property
    def first_minute(self) -> Optional[int]:
        return min(self.minutes) if self.minutes else None

    @property
    def last_minute(self) -> Optional[int]:
        return max(self.minutes) if self.minutes else None

    def lost_lines(self) -> int:
        ns = sorted(self.seen)
        return (ns[-1] - ns[0] + 1 - len(ns)) if ns else 0

    def order(self, cfg: Dict) -> float:
        """A time that orders the streams of a target: the start, else the first recorded minute."""
        if self.started_ms is not None:
            return float(self.started_ms)
        if self.minutes:
            return float(cfg["origin_ms"] + self.first_minute * cfg["minute_ms"])
        return math.inf


def _streams(cfg: Dict, records: List[Dict]) -> Tuple[Dict[str, List[_Stream]], Dict]:
    by: Dict[Tuple[str, str], _Stream] = {}
    stats = {"foreign_run": 0, "foreign_target": 0, "duplicate_lines": 0}
    for r in records:
        if r.get("run") != cfg["run_id"]:
            stats["foreign_run"] += 1
            continue
        t, inc = r.get("target"), r.get("inc")
        if t not in cfg["targets"] or not isinstance(inc, str):
            stats["foreign_target"] += 1
            continue
        by.setdefault((t, inc), _Stream(inc, t)).add(cfg, r)
    out: Dict[str, List[_Stream]] = {t: [] for t in cfg["targets"]}
    for (t, _), s in by.items():
        s.finish(cfg)
        stats["duplicate_lines"] += s.duplicates
        out[t].append(s)
    for t in out:
        out[t].sort(key=lambda s: (s.order(cfg), s.inc))
    return out, stats


def _minute_of(cfg: Dict, t_ms: float) -> int:
    return int(math.floor((t_ms - cfg["origin_ms"]) / cfg["minute_ms"]))


def _generator_minute(cfg: Dict, streams: List[_Stream], m: int) -> Dict:
    """The generator's side of minute m for one target: state complete | partial | gap | unknown, reasons, counts."""
    lo = cfg["origin_ms"] + m * cfg["minute_ms"]
    hi = lo + cfg["minute_ms"]
    lines, reasons = [], []
    for i, s in enumerate(streams):
        if m in s.minutes:
            lines.append((s, s.minutes[m]))
            for flag in s.flags:
                if flag not in reasons:
                    reasons.append(flag)
            reasons += [x for x in s.problems.get(m, []) if x not in reasons]
            continue
        # No line of this incarnation for minute m: could it have been running, unrecorded?
        first, last = s.first_minute, s.last_minute
        began = _minute_of(cfg, s.started_ms) if s.started_ms is not None else first
        if began is None or m < began:
            continue                                               # not started yet (as far as the record says)
        if last is not None and m < last:
            if "generator_record_missing" not in reasons:
                reasons.append("generator_record_missing")       # a lost line before its last recorded minute
            continue
        if s.end is not None:
            if _int(s.end.get("segment_end_ms")) and s.end["segment_end_ms"] <= lo:
                continue                                           # proven ended before this minute
            if "generator_record_missing" not in reasons:
                reasons.append("generator_record_missing")       # inside its segment, yet no line
            continue
        # It has no end record (killed, or still running): its tail is unrecorded until the next incarnation starts.
        later = [x.order(cfg) for x in streams[i + 1:] if math.isfinite(x.order(cfg))]
        bound = _minute_of(cfg, min(later)) if later else None
        if (bound is None or m <= bound) and "unrecorded_tail" not in reasons:
            reasons.append("unrecorded_tail")
    out = {"state": "unknown", "reasons": reasons, "incarnations": [s.inc for s, _ in lines], "active_ms": 0}
    blocking = ("unrecorded_tail", "generator_record_missing", "conflicting_lines", "malformed_line",
                "config_mismatch", "generator_counter_reset", "plan_mismatch", "inconsistent_line",
                "unresolved_outcomes")
    spans = sorted((r["active"][0], r["active"][1]) for _, r in lines)
    if any(b[0] < a[1] for a, b in zip(spans, spans[1:])):
        reasons.append("duplicate_generators")
    for key in ("due", "dispatched", "ok", "failed", "unresolved"):
        out[key] = sum(r[key] for _, r in lines)
    out["dropped"] = {k: sum(r["dropped"][k] for _, r in lines) for k in ("late", "inflight")}
    out["no_response"] = sum(int((r.get("failed_by") or {}).get("0", 0)) for _, r in lines)
    out["active_ms"] = sum(b - a for a, b in spans)
    out["late_max_ms"] = max([r.get("late_max_ms", 0) for _, r in lines] or [0])
    out["latency"] = _latency(cfg, [r for _, r in lines])
    if any(x in blocking or x == "duplicate_generators" for x in reasons):
        return out
    if not lines:
        proven = [s for s in streams if s.end is not None and _int(s.end.get("segment_end_ms"))
                  and s.end["segment_end_ms"] <= lo]
        nxt = [s for s in streams if s.started_ms is not None and s.started_ms >= hi]
        if proven and nxt:
            out["state"] = "gap"
            reasons.append("generator_gap")
        else:
            reasons.append("before_first_record" if not any(s.order(cfg) < hi for s in streams) else
                           "generator_record_missing")
        return out
    if len(lines) == 1 and spans[0] == (0, cfg["minute_ms"]):
        out["state"] = "complete"
    else:
        out["state"] = "partial"
        reasons.append("incarnation_boundary")
    return out


def _latency(cfg: Dict, lines: List[Dict]) -> Optional[Dict]:
    """p95/p99 of the ok responses from the MERGED bucket counts (never an average of percentiles), in ms."""
    bounds = cfg["latency_bounds_ms"]
    counts = [0] * (len(bounds) + 1)
    for r in lines:
        c = r["lat"]["counts"]
        if len(c) != len(counts):
            return None
        counts = [a + b for a, b in zip(counts, c)]
    if not sum(counts):
        return None
    cum, total = [], 0
    for le, c in zip(list(bounds) + [math.inf], counts):
        total += c
        cum.append((float(le), float(total)))
    return {"p95_ms": _reduce.quantile(0.95, cum), "p99_ms": _reduce.quantile(0.99, cum),
            "max_ms": max(r["lat"].get("max_ms", 0) for r in lines), "ok": total}


def _clock(streams_by_target: Dict[str, List[_Stream]]) -> Dict[str, Dict[int, Tuple[float, float]]]:
    """{incarnation: {minute: (lo, hi)}}: bounds in ms of (generator clock - ingress clock) from each minute's sample
    (any target: the clock is the pod's). The ingress wrote its Date header (whole seconds) between the request's
    dispatch and its receipt, so the offset lies in [recv - rt - date - 1000, recv - date]."""
    out: Dict[str, Dict[int, Tuple[float, float]]] = {}
    for streams in streams_by_target.values():
        for s in streams:
            for m, r in s.minutes.items():
                c = r.get("clock")
                if not (isinstance(c, dict) and all(_int(c.get(k)) for k in ("date_s", "recv_ms", "rt_ms"))):
                    continue
                lo = c["recv_ms"] - c["rt_ms"] - c["date_s"] * 1000 - 1000
                hi = c["recv_ms"] - c["date_s"] * 1000
                cur = out.setdefault(s.inc, {}).get(m)
                out[s.inc][m] = (lo, hi) if cur is None else (max(cur[0], lo), min(cur[1], hi))
    return out


def _clock_minute(clock: Dict, incs: List[str], m: int, limit_ms: float) -> Tuple[Optional[List[float]], Optional[str]]:
    """The offset bounds that apply to minute m (its own sample, else the nearest within CLOCK_NEIGHBOUR_MINUTES of
    the same incarnation) and the reason the timing cannot be trusted, if any."""
    best = None
    for inc in incs:
        series = clock.get(inc, {})
        for d in range(CLOCK_NEIGHBOUR_MINUTES + 1):
            for k in ((m,) if d == 0 else (m - d, m + d)):
                if k in series:
                    best = series[k] if best is None else (min(best[0], series[k][0]), max(best[1], series[k][1]))
                    break
            else:
                continue
            break
    if best is None:
        return None, "clock_unverified"
    if best[0] > limit_ms or best[1] < -limit_ms:
        return [best[0], best[1]], "clock_offset"
    return [best[0], best[1]], None


# --- the ingress's side -----------------------------------------------------------------------------------------------

def _samples(raw) -> List[Tuple[float, float]]:
    out = []
    for item in raw or []:
        t, v = float(item[0]), float(item[1])
        if math.isfinite(t) and math.isfinite(v):
            out.append((t, v))
    out.sort()
    return out


def _slope(p: Tuple[float, float], q: Tuple[float, float], max_gap: float) -> Optional[float]:
    """The counter's rate between two consecutive samples, or None across a gap or a reset."""
    dt = q[0] - p[0]
    if dt <= 0 or dt > max_gap or q[1] < p[1]:
        return None
    return (q[1] - p[1]) / dt


def _at_boundary(samples: List[Tuple[float, float]], t: float, max_gap: float) -> Dict:
    """The counter at time t (seconds) from RAW samples: exact at a sample; otherwise between the two adjacent samples
    (va at ta <= t < tb at vb, the hard bounds), estimated as the mean of the extrapolation from the left (va plus the
    rate of the sample pair before ta) and from the right (vb minus the rate of the pair after tb), each clamped to
    the bounds. The program's rate is constant within a minute and changes AT the boundaries, where a straight line
    through the bracket is off by up to a quarter of the scrape interval's arrivals; the two-sided estimate is exact
    there. `err` is half the disagreement of the two sides (plus one request of counter granularity); with only one
    side, or neither (a plain interpolation), it is half the bracket's range. Unknown (with the reason) without a
    bracket, across a gap or across a reset."""
    import bisect
    i = bisect.bisect_right(samples, (t, math.inf))
    if i and samples[i - 1][0] == t:
        v = samples[i - 1][1]
        return {"value": v, "lo": v, "hi": v, "width": 0.0, "err": 0.0}
    if i == 0 or i == len(samples):
        return {"unknown": "ingress_gap" if samples else "ingress_absent"}
    (ta, va), (tb, vb) = samples[i - 1], samples[i]
    if tb - ta > max_gap:
        return {"unknown": "ingress_gap", "gap_s": tb - ta}
    if vb < va:
        return {"unknown": "ingress_reset"}
    clamp = lambda v: min(max(v, va), vb)  # noqa: E731
    left = _slope(samples[i - 2], samples[i - 1], max_gap) if i >= 2 else None
    right = _slope(samples[i], samples[i + 1], max_gap) if i + 1 < len(samples) else None
    sides = ([clamp(va + left * (t - ta))] if left is not None else []) + \
        ([clamp(vb - right * (tb - t))] if right is not None else [])
    if len(sides) == 2:
        value, err = sum(sides) / 2.0, abs(sides[0] - sides[1]) / 2.0 + 1.0
    else:
        value = sides[0] if sides else va + (vb - va) * (t - ta) / (tb - ta)
        err = (vb - va) / 2.0 + 1.0
    return {"value": value, "lo": va, "hi": vb, "width": tb - ta, "err": err}


def _ingress_minute(samples: List[Tuple[float, float]], lo_s: float, hi_s: float, max_gap: float) -> Dict:
    """The requests the ingress received in [lo_s, hi_s): state ok | gap | reset | absent."""
    if not samples:
        return {"state": "absent", "reasons": ["ingress_absent"]}
    a, b = _at_boundary(samples, lo_s, max_gap), _at_boundary(samples, hi_s, max_gap)
    inside = [s for s in samples if lo_s <= s[0] <= hi_s]
    bad = [x["unknown"] for x in (a, b) if "unknown" in x]
    if any(y[1] < x[1] for x, y in zip(inside, inside[1:])):
        bad.append("ingress_reset")
    gaps = [y[0] - x[0] for x, y in zip(inside, inside[1:])]
    if any(g > max_gap for g in gaps) and "ingress_gap" not in bad:
        bad.append("ingress_gap")
    if bad:
        kind = "reset" if "ingress_reset" in bad else "absent" if set(bad) == {"ingress_absent"} else "gap"
        return {"state": kind, "reasons": sorted(set(bad))}
    return {"state": "ok", "reasons": [], "received": round(max(0.0, b["value"] - a["value"]), 3),
            "received_bounds": [max(0.0, b["lo"] - a["hi"]), b["hi"] - a["lo"]],
            "received_err": round(a["err"] + b["err"], 3),
            "bracket_s": [a["width"], b["width"]], "max_sample_gap_s": max(gaps + [a["width"], b["width"]])}


# --- reconciliation -----------------------------------------------------------------------------------------------------

def _reconcile(cfg: Dict, rows: List[Dict]) -> None:
    """A running balance of dispatched minus received over consecutive minutes with both sides known. Requests in
    flight across a boundary move between neighbours, so no single minute must match; the balance, however, may only
    hold what can be in flight (the reducer's own allowance: the timeout plus a second of arrivals, plus two) and the
    estimation error of the ingress counter at the minute's boundaries (received_err). Requests that got NO HTTP response
    (timeouts, connection failures) may or may not have reached the ingress: they explain a positive imbalance up to
    their number. Beyond that the minute is `reconciled: false`, the balance restarts, and the row is unknown."""
    balance = 0.0
    for row in rows:
        g, ing = row["generator"], row["ingress"]
        if g["state"] == "unknown" or ing["state"] != "ok":
            balance = 0.0
            row["reconciled"] = None
            continue
        n = row["intended"]
        allowance = n * (cfg["timeout_ms"] / 1000.0 + 1.1) / 60.0 + 2.0 + ing["received_err"]
        diff = g["dispatched"] - ing["received"]
        diff -= min(max(diff, 0.0), float(g["no_response"]))
        balance += diff
        row["balance"] = round(balance, 3)
        row["allowance"] = round(allowance, 3)
        row["reconciled"] = abs(balance) <= allowance
        if not row["reconciled"]:
            balance = 0.0


# --- the reduction ----------------------------------------------------------------------------------------------------

def reduce(cfg: Dict, records: List[Dict], ingress: Dict[str, List], start_ms: int, end_ms: int,
           max_scrape_gap_s: float = DEFAULT_MAX_SCRAPE_GAP_S, clock_limit_ms: float = DEFAULT_CLOCK_LIMIT_MS) -> Dict:
    """Per-minute rows of every target for [start_ms, end_ms), binned from cfg["origin_ms"].

    cfg: the generator's configuration (demo.generator_config; its digest must be the one the incarnations report).
    records: parse_lines() of the pod logs (any number of pulls, in any order; a line read twice is dropped).
    ingress: {target: [[t_seconds, value], ...]} RAW samples of c28_ingress_requests_total{target} with their own
             timestamps (a range-vector query or an export; never a stepped range query, which hides scrape gaps)."""
    minute = cfg["minute_ms"]
    if (start_ms - cfg["origin_ms"]) % minute or (end_ms - cfg["origin_ms"]) % minute or end_ms <= start_ms:
        raise ValueError("start and end must be minute boundaries of the recorded origin, start before end")
    streams, stats = _streams(cfg, records)
    clock = _clock(streams)
    m0, m1 = _minute_of(cfg, start_ms), _minute_of(cfg, end_ms)
    out = {"schema": SCHEMA, "run_id": cfg["run_id"], "origin_utc": cfg.get("origin_utc"), "origin_ms": cfg["origin_ms"],
           "config_digest": cfg.get("digest"), "start_ms": start_ms, "end_ms": end_ms, "minutes": m1 - m0,
           "limits": {"max_scrape_gap_s": max_scrape_gap_s, "clock_limit_ms": clock_limit_ms},
           "records": stats, "targets": {}, "incarnations": []}
    for t in cfg["targets"]:
        samples = _samples((ingress or {}).get(t))
        rows = []
        for m in range(m0, m1):
            lo = cfg["origin_ms"] + m * minute
            g = _generator_minute(cfg, streams[t], m)
            ing = _ingress_minute(samples, lo / 1000.0, (lo + minute) / 1000.0, max_scrape_gap_s)
            rows.append({"minute": m, "start_ms": lo, "day_minute": m % cfg["day_minutes"],
                         "intended": cfg["per_minute"][m % cfg["day_minutes"]], "generator": g, "ingress": ing})
        _reconcile(cfg, rows)
        for row in rows:
            g, ing = row["generator"], row["ingress"]
            reasons = list(g["reasons"]) + [r for r in ing["reasons"] if r not in g["reasons"]]
            state = "unknown" if g["state"] == "unknown" else \
                "observed" if g["state"] == "complete" and ing["state"] == "ok" else "partial"
            if g["state"] in ("complete", "partial"):
                bounds, why = _clock_minute(clock, g["incarnations"], row["minute"], clock_limit_ms)
                row["clock_offset_ms"] = bounds
                if why:
                    state = "unknown"
                    reasons.append(why)
            if row.get("reconciled") is False:
                state = "unknown"
                reasons.append("unreconciled")
            row["state"], row["reasons"] = state, reasons
            row["generator_state"], row["ingress_state"] = g["state"], ing["state"]
        out["targets"][t] = {"rows": rows, "summary": _summary(rows)}
        for s in streams[t]:
            out["incarnations"].append({
                "target": t, "incarnation": s.inc, "started_ms": s.started_ms,
                "pod_uid": (s.start or {}).get("pod_uid"), "first_minute": s.first_minute,
                "last_minute": s.last_minute, "minute_records": len(s.minutes),
                "ended": "segment-end" if s.end is not None else "no end record",
                "lost_lines": s.lost_lines(), "flags": sorted(set(s.flags))})
    return out


def _summary(rows: List[Dict]) -> Dict:
    states = {"observed": 0, "partial": 0, "unknown": 0}
    reasons: Dict[str, int] = {}
    totals = {k: 0.0 for k in ("intended", "due", "dispatched", "dropped", "ok", "failed", "received")}
    for row in rows:
        states[row["state"]] += 1
        for r in row["reasons"]:
            reasons[r] = reasons.get(r, 0) + 1
        if row["state"] == "observed":
            g = row["generator"]
            totals["intended"] += row["intended"]
            for k in ("due", "dispatched", "ok", "failed"):
                totals[k] += g[k]
            totals["dropped"] += g["dropped"]["late"] + g["dropped"]["inflight"]
            totals["received"] += row["ingress"]["received"]
    return {"minutes": len(rows), "states": states, "reasons": dict(sorted(reasons.items())),
            "observed_totals": {k: round(v, 3) for k, v in totals.items()},
            "note": "totals cover observed minutes only; partial and unknown minutes are listed, never zero-filled"}
