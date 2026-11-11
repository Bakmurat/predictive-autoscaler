// C2.8 demonstration generator: one continuous, open-loop, UTC-aligned FINITE schedule for several targets
// (LIVE-DEMO-PLAN section 6). A NEW script: c28.js and c28-calibrate.js stay pinned and are not used here.
//
//   k6 run --quiet --no-color --no-usage-report --no-summary --no-thresholds --log-format raw \
//     --out experimental-prometheus-rw --tag c28_run=<run> -e C28D_CONFIG=/plan/config.json /plan/c28-demo.js
//
// The configuration (schema c28-demo-generator/v2) is written by the harness (`python -m c28 demo-plan`): ONE
// immutable schedule origin (a UTC midnight, epoch ms), the schedule's whole UTC days (28 for a demonstration), the
// seed of every day (consecutive, predeclared), and per_minute_days: for EVERY day its own row of 1,440 intended
// arrivals per minute (the same for every target). Python computed every number; this script reads the table and
// never draws a random number. The schedule is FINITE: it never repeats and never wraps.
//
// The configuration that RUNS is the configuration that was reviewed: at start the script recomputes the digest of
// the parsed configuration exactly as the harness does (schedule.digest: SHA-256 over the JSON with sorted keys,
// separators "," and ":", non-ASCII escaped, the digest key left out) and refuses to start when it differs from the
// configuration's own digest, so a table edited after rendering cannot run under the old digest. The configuration
// holds only objects, arrays, strings, booleans, null and integers (no float: the two languages write them
// differently), which checkConfig enforces.
//
// How it runs
//   * One k6 scenario per target, each with ONE virtual user whose single iteration runs an event loop: arrivals are
//     timed by setTimeout against wall-clock time and sent with http.asyncRequest, so no arrival ever waits for a
//     response (open loop), and a slow or failing target cannot delay another target's arrivals (separate VUs, separate
//     event loops, separate connection pools).
//   * The program is a pure function of wall-clock time and the origin. Minute m = floor((t - origin) / 60 s), its day
//     d = floor(m / 1440) (whole UTC days from the origin), its intended count n = per_minute_days[d][m - 1440 d], its
//     arrival j at origin + m * 60 s + floor((j + phase) * 60 s / n). A process that starts at any time RESUMES at the
//     current position: arrivals scheduled before its start are not sent (no replay), and a restart never changes the
//     day (the day comes from the clock and the origin, never from a counter of this process).
//   * Arrivals inside a minute are EVENLY SPACED (deterministic spacing, NOT a Poisson process): phase = (target index
//     + 0.5) / targets, so targets interleave and never fire in the same millisecond. Nothing inside a minute is
//     random. What differs from day to day is the per-minute count: each day's row carries that day's own seeded
//     +-5 % perturbation of the diurnal shape.
//   * BEFORE the origin nothing is sent and nothing is intended: an incarnation that starts at most pre_origin_max_ms
//     before the origin says so in its start record (phase "pre-origin"), waits, and begins with minute 0; an earlier
//     one only idles, exits 0 before the origin, and is restarted. AFTER the last day (schedule_end_ms = origin + days
//     x 1440 minutes) the schedule is EXHAUSTED: the generator stops scheduling, writes ONE record (event "exhausted",
//     reason "schedule-exhausted") and sends nothing more; an incarnation that starts after the end writes its start
//     record (phase "schedule-exhausted") and that record, sends nothing, idles to its segment end and exits 0 (no
//     hot loop: the next incarnation does the same, one segment later). There is NO per-minute line before the
//     origin or after the end: intended is undefined there, not zero.
//   * A process is one INCARNATION: id "<pod UID>/<start, epoch ms>". It runs one SEGMENT, to the next multiple of
//     segment_minutes from the origin (a UTC midnight for whole-day segments), writes its end record and k6 exits 0;
//     the Deployment's container is then restarted and the next incarnation resumes. Memory is bounded per segment.
//   * Nothing is retried. An arrival more than max_late_ms late (a stalled process, a forward clock step) is DROPPED,
//     reason "late", never sent in a burst; an arrival that would exceed max_inflight requests in flight is DROPPED,
//     reason "inflight"; and an arrival that is owed but whose send would happen AT OR AFTER the end of this
//     incarnation's span (its segment end or the schedule's end: the process was delayed across it) is DROPPED, reason
//     "segment-end": nothing is ever sent at or after the span's end, so every request this incarnation sends is in
//     one of the minute lines it writes. All three are the generator's drops (this script, not k6's arrival-rate
//     executors, schedules the arrivals, so k6's own dropped_iterations metric does not apply and stays absent). The
//     position in the schedule
//     only moves forward: after a BACKWARD clock step the generator waits until the clock reaches the next arrival
//     again and never sends an arrival twice. It sleeps at most MAX_SLEEP_MS, so a clock step is seen within a minute.
//
// What it records, per target and per minute, as ONE JSON line (schema c28-demo-minute/v3), written timeout_ms +
// flush_grace_ms after the minute ends, when every request dispatched in it has a terminal outcome. The records are
// written with console.log: k6 writes console.log to STDERR (not stdout), so they are in the process's stderr; a
// pod's log carries both streams.
//   day, day_minute   the minute's day (0-based from the origin) and its position in that day
//   intended     the table's count for the whole minute
//   active       [from, to] ms of the minute during which this incarnation was scheduling ([0, 60000] = complete)
//   due          arrivals of this minute scheduled inside the active span
//   dispatched   requests whose DISPATCH timestamp (taken immediately before the send) fell in this minute
//   carried      [in, out]: dispatched here but scheduled in the previous minute / due here but dispatched in the next
//   dropped      {late, inflight, segment_end}: due arrivals this generator did not send
//   ok, failed, failed_by, unresolved   terminal outcomes of the requests DISPATCHED in this minute (by status;
//                0 = no HTTP response: timeout or connection failure); never a completion-stamped count
//   lat          latency bucket counts of the ok responses (bounds in the start record) and their maximum
//   late_max_ms  the worst dispatch lateness against the schedule
//   clock        the Date header (seconds) of the minute's response with the SHORTEST round trip, with the local
//                receive time and that round trip: the tightest evidence the minute has of the offset between this
//                pod's clock and the ingress's; the schedule is NEVER corrected by it
//   cum          cumulative counts of this incarnation and target (a lost line or a reset is detectable)
// Identity of every line: due = dispatched - carried[0] + carried[1] + dropped.late + dropped.inflight +
// dropped.segment_end, and a carry never leaves the incarnation's span: what one line carries out is what the next
// line of the same incarnation carries in, and its first line carries nothing in, its last nothing out. Also
// dispatched = ok + failed + unresolved. A start and an end record (schema c28-demo-incarnation/v2) frame each
// incarnation; the start record names its phase and the span [send_from_ms, send_to_ms) it will schedule (null when
// it sends nothing). The lines do not depend on remote-write delivery. The custom counters below go to the
// remote-write output for dashboards only; a missing remote-write series means unknown, never zero.
import crypto from 'k6/crypto';
import http from 'k6/http';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';
import { setTimeout } from 'k6/timers';

// --- pure logic (no k6 API below this line until "k6 wiring"; exercised under node by harness/tests) ---------------

const MAX_SLEEP_MS = 60000;   // the longest single sleep: a wall-clock step is noticed within a minute

// The canonical JSON of a value, byte for byte what Python's json.dumps(value, sort_keys=True, separators=(",", ":"))
// writes (ensure_ascii, its default): object keys sorted by code point, no white space, every character outside
// " ".."~" escaped as \uXXXX (lower-case hex, UTF-16 units). Integers only: a non-integer or unsafe number throws,
// because Python and JavaScript format floats differently (2.0 against 2, 1e-07 against 1e-7).
function canonicalJson(v) {
  if (v === null) return 'null';
  if (v === true) return 'true';
  if (v === false) return 'false';
  if (typeof v === 'number') {
    if (!Number.isSafeInteger(v)) throw new Error(`c28-demo config: ${v} is not a safe integer (no floats in the configuration)`);
    return String(v);
  }
  if (typeof v === 'string') {
    return JSON.stringify(v).replace(/[\u007f-\uffff]/g, (c) => `\\u${(`000${c.charCodeAt(0).toString(16)}`).slice(-4)}`);
  }
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(',')}]`;
  if (typeof v === 'object') {
    const keys = Object.keys(v).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${keys.map((k) => `${canonicalJson(k)}:${canonicalJson(v[k])}`).join(',')}}`;
  }
  throw new Error(`c28-demo config: a ${typeof v} cannot be part of the configuration`);
}

// The configuration's digest as the harness computes it (schedule.digest): SHA-256, hex, of the canonical JSON of
// the configuration without its "digest" key. sha256hex(text) -> lower-case hex (k6/crypto here, node's crypto in the
// tests).
function configDigest(cfg, sha256hex) {
  const body = {};
  for (const k of Object.keys(cfg)) if (k !== 'digest') body[k] = cfg[k];
  return sha256hex(canonicalJson(body));
}

// checkConfig(cfg, sha256hex): the structural checks, then the digest of what was actually parsed.
function checkConfig(cfg, sha256hex) {
  const fail = (why) => { throw new Error(`c28-demo config: ${why}`); };
  const int = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
  if (!cfg || cfg.schema !== 'c28-demo-generator/v2') fail('schema must be c28-demo-generator/v2');
  if (cfg.label !== 'demonstration' && cfg.label !== 'smoke') fail('label must be demonstration or smoke');
  if (cfg.minute_ms !== 60000 || cfg.day_minutes !== 1440) fail('minute_ms must be 60000 and day_minutes 1440');
  // The origin is a UTC midnight. SMOKE ONLY: a configuration labelled "smoke" may start on any whole minute, so a
  // smoke can put a day boundary of the table a few minutes ahead; a "demonstration" can never use that.
  if (!int(cfg.origin_ms, 0, 4102444800000) || cfg.origin_ms % cfg.minute_ms !== 0 ||
      (cfg.origin_ms % 86400000 !== 0 && cfg.label !== 'smoke')) {
    fail('origin_ms must be a UTC midnight (a whole minute only under the label smoke)');
  }
  if (!int(cfg.days, 1, 28)) fail('days must be an integer in 1..28');
  if (cfg.label === 'demonstration' && cfg.days !== 28) fail('a demonstration has exactly 28 days');
  if (cfg.schedule_minutes !== cfg.days * cfg.day_minutes ||
      cfg.schedule_end_ms !== cfg.origin_ms + cfg.days * cfg.day_minutes * cfg.minute_ms) {
    fail('schedule_minutes and schedule_end_ms must be days x 1440 minutes from the origin');
  }
  if (!Array.isArray(cfg.per_minute_days) || cfg.per_minute_days.length !== cfg.days) {
    fail('per_minute_days must have exactly one row per day');
  }
  cfg.per_minute_days.forEach((row, d) => {
    if (!Array.isArray(row) || row.length !== cfg.day_minutes || !row.every((n) => int(n, 0, 600000))) {
      fail(`per_minute_days[${d}] must be 1440 arrival counts in 0..600000`);
    }
  });
  if (!Array.isArray(cfg.seeds) || cfg.seeds.length !== cfg.days || !int(cfg.seeds[0], 1, 2147483647) ||
      !cfg.seeds.every((s, i) => s === cfg.seeds[0] + i)) fail('seeds must be one seed per day, consecutive');
  if (!cfg.program || cfg.program.seed !== cfg.seeds[0] || cfg.program.days !== cfg.days) {
    fail('program.seed and program.days must be the base seed and the day count');
  }
  if (!int(cfg.segment_minutes, 2, 1440 * 28)) fail('segment_minutes must be an integer in 2..40320');
  if (!int(cfg.min_segment_ms, 0, cfg.segment_minutes * cfg.minute_ms / 2)) fail('min_segment_ms is out of range');
  if (!int(cfg.pre_origin_max_ms, 0, 600000)) fail('pre_origin_max_ms must be 0..600000');
  if (!Array.isArray(cfg.targets) || cfg.targets.length < 1 || cfg.targets.length > 8 ||
      !cfg.targets.every((t) => typeof t === 'string' && /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(t)) ||
      new Set(cfg.targets).size !== cfg.targets.length) fail('targets must be 1..8 distinct DNS labels');
  if (typeof cfg.base_url !== 'string' || !/^http:\/\/[A-Za-z0-9.:-]+$/.test(cfg.base_url)) fail('base_url must be http://host[:port]');
  if (!int(cfg.timeout_ms, 100, 60000)) fail('timeout_ms must be 100..60000');
  if (!int(cfg.max_late_ms, 1, cfg.minute_ms - 1)) fail('max_late_ms must be below one minute');
  if (!int(cfg.flush_grace_ms, 0, 10000)) fail('flush_grace_ms must be 0..10000');
  if (!int(cfg.max_inflight, 1, 100000)) fail('max_inflight must be 1..100000');
  if (!Array.isArray(cfg.latency_bounds_ms) || cfg.latency_bounds_ms.length < 1 || cfg.latency_bounds_ms.length > 32 ||
      !cfg.latency_bounds_ms.every((b, i, a) => int(b, 1, 600000) && (i === 0 || b > a[i - 1]))) {
    fail('latency_bounds_ms must be increasing');
  }
  if (typeof cfg.run_id !== 'string' || typeof cfg.digest !== 'string') fail('run_id and digest are required');
  if (typeof sha256hex !== 'function') fail('no SHA-256 function: the configuration cannot be verified');
  const actual = configDigest(cfg, sha256hex);
  if (actual !== cfg.digest) {
    fail(`the configuration is not the one its digest names (recomputed ${actual}, stated ${cfg.digest}): refusing to start`);
  }
  return cfg;
}

function minuteIndex(cfg, tMs) { return Math.floor((tMs - cfg.origin_ms) / cfg.minute_ms); }
function minuteStart(cfg, m) { return cfg.origin_ms + m * cfg.minute_ms; }
// Minute m belongs to the finite schedule: not before the origin, not after the last day. Never wrapped.
function scheduled(cfg, m) { return m >= 0 && m < cfg.schedule_minutes; }
function dayIndex(cfg, m) { return Math.floor(m / cfg.day_minutes); }
function dayMinute(cfg, m) { return m - dayIndex(cfg, m) * cfg.day_minutes; }
// The intended arrivals of minute m: its day's row of the table. null outside the schedule: UNDEFINED, not zero.
function intended(cfg, m) {
  return scheduled(cfg, m) ? cfg.per_minute_days[dayIndex(cfg, m)][dayMinute(cfg, m)] : null;
}
function targetPhase(cfg, index) { return (index + 0.5) / cfg.targets.length; }

// The scheduled time of arrival j (0-based) of a scheduled minute m: strictly inside the minute, evenly spaced.
function arrivalTime(cfg, m, j, phase) {
  return minuteStart(cfg, m) + Math.floor(((j + phase) * cfg.minute_ms) / intended(cfg, m));
}

// The first arrival index of a scheduled minute m at or after tMs (n when there is none left in the minute).
function firstIndexAtOrAfter(cfg, m, tMs, phase) {
  const n = intended(cfg, m);
  let j = Math.max(0, Math.min(n, Math.ceil(((tMs - minuteStart(cfg, m)) * n) / cfg.minute_ms - phase) - 1));
  while (j < n && arrivalTime(cfg, m, j, phase) < tMs) j += 1;
  while (j > 0 && arrivalTime(cfg, m, j - 1, phase) >= tMs) j -= 1;
  return j;
}

// The end of the segment an incarnation started at startMs runs: the next multiple of segment_minutes from the origin,
// or the one after it when that is closer than min_segment_ms (so a start just before a boundary is not two restarts).
// The same rule after the schedule's end (an exhausted incarnation idles to it). A start BEFORE the origin: at most
// pre_origin_max_ms before it, the incarnation waits for the origin and runs the whole first segment; earlier, it only
// idles (never longer than one segment) and ends half of pre_origin_max_ms before the origin, so that the restarted
// incarnation is one that waits.
function segmentEnd(cfg, startMs) {
  const seg = cfg.segment_minutes * cfg.minute_ms;
  if (startMs < cfg.origin_ms) {
    if (cfg.origin_ms - startMs <= cfg.pre_origin_max_ms) return cfg.origin_ms + seg;
    return Math.min(startMs + seg, cfg.origin_ms - Math.floor(cfg.pre_origin_max_ms / 2));
  }
  let end = cfg.origin_ms + (Math.floor((startMs - cfg.origin_ms) / seg) + 1) * seg;
  if (end - startMs < cfg.min_segment_ms) end += seg;
  return end;
}

function incarnationId(podUid, startedMs) { return `${podUid || 'no-pod-uid'}/${startedMs}`; }

function okStatus(status) { return status >= 200 && status < 400; }

// An HTTP Date header (IMF-fixdate, "Sun, 06 Nov 1994 08:49:37 GMT", what Go's net/http writes) in epoch seconds, or
// null. Parsed here, field by field, so the result never depends on the JavaScript engine's Date.parse.
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
function httpDateSeconds(text) {
  if (typeof text !== 'string') return null;
  const m = /^[A-Z][a-z]{2}, ([0-9]{2}) ([A-Z][a-z]{2}) ([0-9]{4}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) GMT$/.exec(text.trim());
  if (!m) return null;
  const month = MONTHS.indexOf(m[2]);
  if (month < 0) return null;
  const ms = Date.UTC(Number(m[3]), month, Number(m[1]), Number(m[4]), Number(m[5]), Number(m[6]));
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
}

// One target's generator. deps: now() -> epoch ms; setTimeout(fn, ms); send(url, timeoutMs, done(status, durationMs,
// dateHeader)); emit(object) writes one record; count(name, tags, n) adds n (default 1) to a counter (remote write,
// optional).
// shared: { started_ms, segment_end_ms, pod_uid, pod } - the same for every target of the incarnation.
function createGenerator(cfg, index, shared, deps) {
  const target = cfg.targets[index];
  const phase = targetPhase(cfg, index);
  const url = `${cfg.base_url}/t/${target}/work`;
  const inc = incarnationId(shared.pod_uid, shared.started_ms);
  const flushAfter = cfg.timeout_ms + cfg.flush_grace_ms;
  const endMs = shared.segment_end_ms;
  const scheduleEnd = cfg.schedule_end_ms;
  const B = cfg.latency_bounds_ms.length;
  const minutes = new Map();
  const cum = { due: 0, dispatched: 0, dropped: 0, ok: 0, failed: 0, late_completions: 0 };
  let activeFrom = 0;         // this incarnation's own start (its clock)
  let sendFrom = 0;           // it schedules the arrivals in [sendFrom, sendTo): never before the origin, never at
  let sendTo = 0;             // or after the schedule's end or its segment's end
  let cursor = null;          // { m, j, t }: the next arrival; it only ever moves forward
  let nextEmit = 0;           // the next minute index to write
  let lastMinute = -1;        // the last minute index this incarnation writes (below nextEmit: none)
  let exhaustedNoted = false; // the "exhausted" record is written once
  let inflight = 0;
  let seq = 0;
  let finished = false;
  let onDone = null;

  function rec(m) {
    let r = minutes.get(m);
    if (!r) {
      r = { due: 0, dispatched: 0, in: 0, out: 0, late: 0, inflight: 0, end: 0, ok: 0, failed: 0, failed_by: {},
        lat: new Array(B + 1).fill(0), lat_max: 0, late_max: 0, clock: null };
      minutes.set(m, r);
    }
    return r;
  }

  function seek(m, j) {
    // The arrival (m, j), moved forward past finished and empty minutes; null at or beyond sendTo (the segment's end
    // or the schedule's end, whichever is first): the schedule is never wrapped.
    for (;;) {
      if (minuteStart(cfg, m) >= sendTo) return null;
      if (j < intended(cfg, m)) {
        const t = arrivalTime(cfg, m, j, phase);
        return t >= sendTo ? null : { m, j, t };
      }
      m += 1; j = 0;
    }
  }

  const cumNow = () => ({ due: cum.due, dispatched: cum.dispatched, dropped: cum.dropped, ok: cum.ok,
    failed: cum.failed, late_completions: cum.late_completions });

  function complete(mDispatch, status, durationMs, dateHeader, recvMs) {
    inflight -= 1;
    const ok = okStatus(status);
    if (deps.count) deps.count('completed', { c28_target: target, outcome: ok ? 'ok' : 'failed' });
    if (ok) cum.ok += 1; else cum.failed += 1;
    const r = minutes.get(mDispatch);
    if (!r) { cum.late_completions += 1; return; }    // its minute's line is already written
    if (ok) {
      r.ok += 1;
      let b = 0;
      while (b < B && durationMs > cfg.latency_bounds_ms[b]) b += 1;
      r.lat[b] += 1;
      if (durationMs > r.lat_max) r.lat_max = durationMs;
    } else {
      r.failed += 1;
      const key = String(status);
      if (key in r.failed_by || Object.keys(r.failed_by).length < 8) r.failed_by[key] = (r.failed_by[key] || 0) + 1;
      else r.failed_by.other = (r.failed_by.other || 0) + 1;
    }
    // Clock evidence: keep the response with the shortest round trip (the narrowest bounds on the offset).
    const rt = Math.round(durationMs);
    if (r.clock === null || rt < r.clock.rt_ms) {
      const d = httpDateSeconds(dateHeader);
      if (d !== null) r.clock = { date_s: d, recv_ms: recvMs, rt_ms: rt };
    }
  }

  function arrive(a, now) {
    const r = rec(a.m);
    r.due += 1; cum.due += 1;
    if (deps.count) deps.count('scheduled', { c28_target: target });
    const late = now - a.t;
    if (late > cfg.max_late_ms) {
      r.late += 1; cum.dropped += 1;
      if (deps.count) deps.count('dropped', { c28_target: target, reason: 'late' });
      return;
    }
    const stamp = deps.now();                       // the DISPATCH timestamp
    if (stamp >= sendTo) {
      // The span is over (the process was delayed across its segment end or the schedule's end): the arrival is owed
      // but is NOT sent. A send here would fall into a minute this incarnation never writes a line for.
      r.end += 1; cum.dropped += 1;
      if (deps.count) deps.count('dropped', { c28_target: target, reason: 'segment-end' });
      return;
    }
    if (inflight >= cfg.max_inflight) {
      r.inflight += 1; cum.dropped += 1;
      if (deps.count) deps.count('dropped', { c28_target: target, reason: 'inflight' });
      return;
    }
    // stamp < sendTo, so the dispatch minute is at most this incarnation's last minute: its line will be written.
    const md = Math.max(a.m, minuteIndex(cfg, stamp));   // never an earlier (already written) minute
    const rd = md === a.m ? r : rec(md);
    rd.dispatched += 1; cum.dispatched += 1;
    if (md !== a.m) { r.out += 1; rd.in += 1; }
    if (late > rd.late_max) rd.late_max = late;
    inflight += 1;
    if (deps.count) deps.count('dispatched', { c28_target: target });
    deps.send(url, cfg.timeout_ms, (status, durationMs, dateHeader) => {
      complete(md, status, durationMs, dateHeader, deps.now());
    });
  }

  function line(m) {
    const r = minutes.get(m) || rec(m);
    minutes.delete(m);
    const start = minuteStart(cfg, m);
    const from = Math.max(0, sendFrom - start);
    const to = Math.min(cfg.minute_ms, sendTo - start);
    deps.emit({ schema: 'c28-demo-minute/v3', run: cfg.run_id, inc, n: seq, target, minute: m,
      day: dayIndex(cfg, m), day_minute: dayMinute(cfg, m), intended: intended(cfg, m), active: [from, to],
      due: r.due, dispatched: r.dispatched, carried: [r.in, r.out],
      dropped: { late: r.late, inflight: r.inflight, segment_end: r.end },
      ok: r.ok, failed: r.failed, failed_by: r.failed_by, unresolved: r.dispatched - r.ok - r.failed,
      lat: { counts: r.lat, max_ms: Math.round(r.lat_max) }, late_max_ms: Math.round(r.late_max), clock: r.clock,
      cum: cumNow() });
    seq += 1;
  }

  function tick() {
    if (finished) return;
    let now = deps.now();
    while (cursor !== null && cursor.t <= now) {
      const nextStart = minuteStart(cfg, cursor.m + 1);
      if (now - nextStart >= cfg.max_late_ms && nextStart <= sendTo) {
        // Every remaining arrival of this minute is more than max_late_ms late (a long stall, a forward clock step):
        // all of them are counted at once as due and dropped "late". Nothing is sent, however long the jump.
        const left = intended(cfg, cursor.m) - cursor.j;
        const r = rec(cursor.m);
        r.due += left; r.late += left; cum.due += left; cum.dropped += left;
        if (deps.count) {
          deps.count('scheduled', { c28_target: target }, left);
          deps.count('dropped', { c28_target: target, reason: 'late' }, left);
        }
        cursor = seek(cursor.m + 1, 0);
      } else {
        arrive(cursor, now);
        cursor = seek(cursor.m, cursor.j + 1);
      }
      now = deps.now();
    }
    // Write every minute whose requests all have a terminal outcome by now (in order, empty minutes included).
    while (nextEmit <= lastMinute && now >= minuteStart(cfg, nextEmit + 1) + flushAfter) {
      line(nextEmit);
      nextEmit += 1;
    }
    // The schedule's end: said once, explicitly. From here on nothing is scheduled and no minute line is written.
    if (!exhaustedNoted && now >= scheduleEnd) {
      exhaustedNoted = true;
      deps.emit({ schema: 'c28-demo-incarnation/v2', event: 'exhausted', reason: 'schedule-exhausted', run: cfg.run_id,
        inc, target, n: seq, at_ms: now, schedule_end_ms: scheduleEnd, days: cfg.days,
        last_minute: cfg.schedule_minutes - 1, segment_end_ms: endMs, cum: cumNow() });
      seq += 1;
    }
    if (nextEmit > lastMinute && now >= endMs) {
      finished = true;
      deps.emit({ schema: 'c28-demo-incarnation/v2', event: 'end', run: cfg.run_id, inc, target, n: seq,
        ended_ms: now, reason: 'segment-end', segment_end_ms: endMs, exhausted: exhaustedNoted, inflight,
        cum: cumNow() });
      if (onDone) onDone();
      return;
    }
    // Sleep to the next thing to do: the next arrival, the next line to write, the schedule's end, else the segment's
    // end (an idle incarnation: before the origin or after the schedule's end).
    let wake = nextEmit <= lastMinute ? minuteStart(cfg, nextEmit + 1) + flushAfter : endMs;
    if (cursor !== null && cursor.t < wake) wake = cursor.t;
    if (!exhaustedNoted && scheduleEnd < wake) wake = scheduleEnd;
    deps.setTimeout(tick, Math.max(0, Math.min(wake - deps.now(), MAX_SLEEP_MS)));
  }

  return {
    target,
    inc,
    // Begin at the current wall-clock position: nothing scheduled before `now` is sent, nothing is reset, and the
    // day is the clock's (a restart never changes it). Before the origin and after the schedule's end nothing is sent.
    start(done) {
      onDone = done || null;
      activeFrom = deps.now();
      sendFrom = Math.max(activeFrom, cfg.origin_ms);
      sendTo = Math.min(endMs, scheduleEnd);
      const sends = sendFrom < sendTo;
      const where = activeFrom >= scheduleEnd ? 'schedule-exhausted' : activeFrom < cfg.origin_ms ? 'pre-origin' : 'scheduled';
      const m0 = sends ? minuteIndex(cfg, sendFrom) : null;
      if (sends) {
        nextEmit = m0;
        lastMinute = minuteIndex(cfg, sendTo - 1);
        cursor = seek(m0, firstIndexAtOrAfter(cfg, m0, sendFrom, phase));
      }
      deps.emit({ schema: 'c28-demo-incarnation/v2', event: 'start', run: cfg.run_id, inc, target, n: seq,
        pod_uid: shared.pod_uid || null, pod: shared.pod || null, started_ms: shared.started_ms,
        active_from_ms: activeFrom, origin_ms: cfg.origin_ms, phase: where, minute: m0,
        day: sends ? dayIndex(cfg, m0) : null, send_from_ms: sends ? sendFrom : null, send_to_ms: sends ? sendTo : null,
        segment_end_ms: endMs, schedule_end_ms: scheduleEnd, days: cfg.days,
        config_schema: cfg.schema, config_digest: cfg.digest, label: cfg.label || null, timeout_ms: cfg.timeout_ms,
        max_late_ms: cfg.max_late_ms, max_inflight: cfg.max_inflight, lat_bounds_ms: cfg.latency_bounds_ms });
      seq += 1;
      tick();
    },
    state() { return { inflight, cursor, nextEmit, finished, cum: Object.assign({}, cum) }; },
  };
}

// --- k6 wiring ------------------------------------------------------------------------------------------------------

const cfg = checkConfig(JSON.parse(open(__ENV.C28D_CONFIG)), (text) => crypto.sha256(text, 'hex'));

const counters = {
  scheduled: new Counter('c28d_scheduled'),       // arrivals that came due (dispatched or dropped)
  dispatched: new Counter('c28d_dispatched'),     // added at dispatch
  dropped: new Counter('c28d_dropped'),           // tag reason: late | inflight | segment-end
  completed: new Counter('c28d_completed'),       // added at COMPLETION (tag outcome); never a dispatch count
};

const scenarios = {};
cfg.targets.forEach((_, i) => {
  scenarios[`t${i}`] = { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'run',
    // One segment, and room for the wait before the origin (pre_origin_max_ms <= 600 s) and the last flush.
    maxDuration: `${cfg.segment_minutes * 60 + 900}s`, gracefulStop: '10s' };
});

export const options = {
  discardResponseBodies: true,
  // A request that fails without an HTTP response (a timeout, a refused or cut connection) REJECTS its promise
  // instead of resolving with status 0 and a "Request Failed" warning in the log: the rejection handler below
  // records it exactly the same way (status 0), and the log stays the records. Without this k6 writes one warning
  // line per failed request (seen on the kind bed: 16,683 such lines in two minutes with one target not answering),
  // which at the peak rate rotates the container's log within minutes and takes the minute lines with it.
  throw: true,
  // One VU keeps up to max_inflight requests in flight: keep that many idle connections instead of k6's 20/6.
  batch: cfg.max_inflight,
  batchPerHost: cfg.max_inflight,
  setupTimeout: '30s',
  scenarios,
  tags: { c28_pod: __ENV.POD_UID || 'no-pod-uid' },
};

export function setup() {
  const started = Date.now();
  return { started_ms: started, segment_end_ms: segmentEnd(cfg, started), pod_uid: __ENV.POD_UID || '',
    pod: __ENV.POD_NAME || '' };
}

export function run(shared) {
  const index = Number(String(exec.scenario.name).slice(1));
  const timeout = cfg.timeout_ms % 1000 === 0 ? `${cfg.timeout_ms / 1000}s` : `${cfg.timeout_ms}ms`;
  const gen = createGenerator(cfg, index, shared, {
    now: () => Date.now(),
    setTimeout,
    emit: (o) => console.log(JSON.stringify(o)),
    count: (name, tags, n) => counters[name].add(n === undefined ? 1 : n, tags),
    send: (url, _timeoutMs, done) => {
      const t0 = Date.now();
      http.asyncRequest('GET', url, null, { timeout, tags: { name: 'work', c28_target: cfg.targets[index] } }).then(
        (res) => done(res.status || 0, res.timings ? res.timings.duration : Date.now() - t0,
          res.headers ? res.headers.Date : undefined),
        () => done(0, Date.now() - t0, undefined),
      );
    },
  });
  // The iteration lasts until the event loop is empty: the generator's last timer fires at the segment end (an idle
  // incarnation, before the origin or after the schedule's end, also sleeps to its segment end before k6 exits 0).
  gen.start();
}
