// C2.8 demonstration generator: one continuous, open-loop, UTC-aligned diurnal program for several targets
// (LIVE-DEMO-PLAN section 6). A NEW script: c28.js and c28-calibrate.js stay pinned and are not used here.
//
//   k6 run --quiet --no-color --no-usage-report --no-summary --no-thresholds --log-format raw \
//     --out experimental-prometheus-rw --tag c28_run=<run> -e C28D_CONFIG=/plan/config.json /plan/c28-demo.js
//
// The configuration (schema c28-demo-generator/v1) is written by the harness (`python -m c28 demo-plan`): ONE
// immutable schedule origin (a UTC midnight, epoch ms), the intended arrivals of each of the 1,440 minutes of the day
// (the same for every target), the targets, the timeout and the limits below.
//
// How it runs
//   * One k6 scenario per target, each with ONE virtual user whose single iteration runs an event loop: arrivals are
//     timed by setTimeout against wall-clock time and sent with http.asyncRequest, so no arrival ever waits for a
//     response (open loop), and a slow or failing target cannot delay another target's arrivals (separate VUs, separate
//     event loops, separate connection pools).
//   * The program is a pure function of wall-clock time and the origin. Minute m = floor((t - origin) / 60 s), its
//     intended count n = per_minute[m mod 1440], its arrival j at origin + m * 60 s + floor((j + phase) * 60 s / n)
//     (evenly spaced; phase = (target index + 0.5) / targets, so targets interleave and never fire in the same
//     millisecond). A process that starts at any time RESUMES at the current position: arrivals scheduled before its
//     start are not sent (no replay), the day is never reset.
//   * A process is one INCARNATION: id "<pod UID>/<start, epoch ms>". It runs one SEGMENT, to the next multiple of
//     segment_minutes from the origin (a UTC midnight for whole-day segments), writes its end record and k6 exits 0;
//     the Deployment's container is then restarted and the next incarnation resumes. Memory is bounded per segment.
//   * Nothing is retried. An arrival more than max_late_ms late (a stalled process, a clock step) is DROPPED, reason
//     "late"; an arrival that would exceed max_inflight requests in flight is DROPPED, reason "inflight". Both are the
//     generator's drops (this script, not k6's arrival-rate executors, schedules the arrivals, so k6's own
//     dropped_iterations metric does not apply and stays absent).
//
// What it records, per target and per minute, as ONE JSON line (console.log; schema c28-demo-minute/v1), written
// timeout_ms + flush_grace_ms after the minute ends, when every request dispatched in it has a terminal outcome:
//   intended     the plan's count for the whole minute
//   active       [from, to] ms of the minute during which this incarnation was scheduling ([0, 60000] = complete)
//   due          arrivals of this minute scheduled inside the active span
//   dispatched   requests whose DISPATCH timestamp (taken immediately before the send) fell in this minute
//   carried      [in, out]: dispatched here but scheduled in the previous minute / due here but dispatched in the next
//   dropped      {late, inflight}: due arrivals this generator did not send
//   ok, failed, failed_by, unresolved   terminal outcomes of the requests DISPATCHED in this minute (by status;
//                0 = no HTTP response: timeout or connection failure); never a completion-stamped count
//   lat          latency bucket counts of the ok responses (bounds in the start record) and their maximum
//   late_max_ms  the worst dispatch lateness against the schedule
//   clock        the first response's Date header (seconds) with the local receive time and its round trip: evidence
//                of the offset between this pod's clock and the ingress's; the schedule is NEVER corrected by it
//   cum          cumulative counts of this incarnation and target (a lost line or a reset is detectable)
// Identity of every line: due = dispatched - carried[0] + carried[1] + dropped.late + dropped.inflight, and
// dispatched = ok + failed + unresolved. A start and an end record (schema c28-demo-incarnation/v1) frame each
// incarnation. The lines do not depend on remote-write delivery. The custom counters below go to the remote-write
// output for dashboards only; a missing remote-write series means unknown, never zero.
import http from 'k6/http';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';
import { setTimeout } from 'k6/timers';

// --- pure logic (no k6 API below this line until "k6 wiring"; exercised under node by harness/tests) ---------------

function checkConfig(cfg) {
  const fail = (why) => { throw new Error(`c28-demo config: ${why}`); };
  const int = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
  if (!cfg || cfg.schema !== 'c28-demo-generator/v1') fail('schema must be c28-demo-generator/v1');
  if (!int(cfg.origin_ms, 0, 4102444800000) || cfg.origin_ms % 86400000 !== 0) fail('origin_ms must be a UTC midnight');
  if (cfg.minute_ms !== 60000 || cfg.day_minutes !== 1440) fail('minute_ms must be 60000 and day_minutes 1440');
  if (!int(cfg.segment_minutes, 2, 1440 * 28)) fail('segment_minutes must be an integer in 2..40320');
  if (!int(cfg.min_segment_ms, 0, cfg.segment_minutes * cfg.minute_ms / 2)) fail('min_segment_ms is out of range');
  if (!Array.isArray(cfg.per_minute) || cfg.per_minute.length !== cfg.day_minutes ||
      !cfg.per_minute.every((n) => int(n, 0, 600000))) fail('per_minute must be 1440 arrival counts');
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
  return cfg;
}

function minuteIndex(cfg, tMs) { return Math.floor((tMs - cfg.origin_ms) / cfg.minute_ms); }
function minuteStart(cfg, m) { return cfg.origin_ms + m * cfg.minute_ms; }
function dayMinute(cfg, m) { return ((m % cfg.day_minutes) + cfg.day_minutes) % cfg.day_minutes; }
function intended(cfg, m) { return cfg.per_minute[dayMinute(cfg, m)]; }
function targetPhase(cfg, index) { return (index + 0.5) / cfg.targets.length; }

// The scheduled time of arrival j (0-based) of minute m: strictly inside the minute, evenly spaced.
function arrivalTime(cfg, m, j, phase) {
  return minuteStart(cfg, m) + Math.floor(((j + phase) * cfg.minute_ms) / intended(cfg, m));
}

// The first arrival index of minute m scheduled at or after tMs (n when there is none left in the minute).
function firstIndexAtOrAfter(cfg, m, tMs, phase) {
  const n = intended(cfg, m);
  let j = Math.max(0, Math.min(n, Math.ceil(((tMs - minuteStart(cfg, m)) * n) / cfg.minute_ms - phase) - 1));
  while (j < n && arrivalTime(cfg, m, j, phase) < tMs) j += 1;
  while (j > 0 && arrivalTime(cfg, m, j - 1, phase) >= tMs) j -= 1;
  return j;
}

// The end of the segment an incarnation started at startMs runs: the next multiple of segment_minutes from the origin,
// or the one after it when that is closer than min_segment_ms (so a start just before a boundary is not two restarts).
function segmentEnd(cfg, startMs) {
  const seg = cfg.segment_minutes * cfg.minute_ms;
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
// dateHeader)); emit(object) writes one record; count(name, tags) adds 1 to a counter (remote write, optional).
// shared: { started_ms, segment_end_ms, pod_uid, pod } - the same for every target of the incarnation.
function createGenerator(cfg, index, shared, deps) {
  const target = cfg.targets[index];
  const phase = targetPhase(cfg, index);
  const url = `${cfg.base_url}/t/${target}/work`;
  const inc = incarnationId(shared.pod_uid, shared.started_ms);
  const flushAfter = cfg.timeout_ms + cfg.flush_grace_ms;
  const endMs = shared.segment_end_ms;
  const B = cfg.latency_bounds_ms.length;
  const minutes = new Map();
  const cum = { due: 0, dispatched: 0, dropped: 0, ok: 0, failed: 0, late_completions: 0 };
  let activeFrom = 0;
  let cursor = null;          // { m, j, t }: the next arrival
  let nextEmit = 0;           // the next minute index to write
  let inflight = 0;
  let seq = 0;
  let finished = false;
  let onDone = null;

  function rec(m) {
    let r = minutes.get(m);
    if (!r) {
      r = { due: 0, dispatched: 0, in: 0, out: 0, late: 0, inflight: 0, ok: 0, failed: 0, failed_by: {},
        lat: new Array(B + 1).fill(0), lat_max: 0, late_max: 0, clock: null };
      minutes.set(m, r);
    }
    return r;
  }

  function seek(m, j) {
    // The arrival (m, j), moved forward past exhausted and empty minutes; null at or beyond the segment end.
    for (;;) {
      if (minuteStart(cfg, m) >= endMs) return null;
      if (j < intended(cfg, m)) {
        const t = arrivalTime(cfg, m, j, phase);
        return t >= endMs ? null : { m, j, t };
      }
      m += 1; j = 0;
    }
  }

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
    if (r.clock === null) {
      const d = httpDateSeconds(dateHeader);
      if (d !== null) r.clock = { date_s: d, recv_ms: recvMs, rt_ms: Math.round(durationMs) };
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
    if (inflight >= cfg.max_inflight) {
      r.inflight += 1; cum.dropped += 1;
      if (deps.count) deps.count('dropped', { c28_target: target, reason: 'inflight' });
      return;
    }
    const stamp = deps.now();                       // the DISPATCH timestamp
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
    const from = Math.max(0, activeFrom - start);
    const to = Math.min(cfg.minute_ms, endMs - start);
    deps.emit({ schema: 'c28-demo-minute/v1', run: cfg.run_id, inc, n: seq, target, minute: m,
      day_minute: dayMinute(cfg, m), intended: intended(cfg, m), active: [from, to], due: r.due,
      dispatched: r.dispatched, carried: [r.in, r.out], dropped: { late: r.late, inflight: r.inflight },
      ok: r.ok, failed: r.failed, failed_by: r.failed_by, unresolved: r.dispatched - r.ok - r.failed,
      lat: { counts: r.lat, max_ms: Math.round(r.lat_max) }, late_max_ms: Math.round(r.late_max), clock: r.clock,
      cum: { due: cum.due, dispatched: cum.dispatched, dropped: cum.dropped, ok: cum.ok, failed: cum.failed,
        late_completions: cum.late_completions } });
    seq += 1;
  }

  function tick() {
    if (finished) return;
    let now = deps.now();
    while (cursor !== null && cursor.t <= now) {
      arrive(cursor, now);
      cursor = seek(cursor.m, cursor.j + 1);
      now = deps.now();
    }
    // Write every minute whose requests all have a terminal outcome by now (in order, empty minutes included).
    const lastMinute = minuteIndex(cfg, endMs - 1);
    while (nextEmit <= lastMinute && now >= minuteStart(cfg, nextEmit + 1) + flushAfter) {
      line(nextEmit);
      nextEmit += 1;
    }
    if (nextEmit > lastMinute) {
      finished = true;
      deps.emit({ schema: 'c28-demo-incarnation/v1', event: 'end', run: cfg.run_id, inc, target, n: seq,
        ended_ms: now, reason: 'segment-end', segment_end_ms: endMs, inflight,
        cum: { due: cum.due, dispatched: cum.dispatched, dropped: cum.dropped, ok: cum.ok, failed: cum.failed,
          late_completions: cum.late_completions } });
      if (onDone) onDone();
      return;
    }
    const flushAt = minuteStart(cfg, nextEmit + 1) + flushAfter;
    const wake = cursor !== null ? Math.min(cursor.t, flushAt) : flushAt;
    deps.setTimeout(tick, Math.max(0, wake - deps.now()));
  }

  return {
    target,
    inc,
    // Begin at the current wall-clock position: nothing scheduled before `now` is sent, nothing is reset.
    start(done) {
      onDone = done || null;
      activeFrom = deps.now();
      const m0 = minuteIndex(cfg, activeFrom);
      nextEmit = m0;
      cursor = activeFrom >= endMs ? null : seek(m0, firstIndexAtOrAfter(cfg, m0, activeFrom, phase));
      deps.emit({ schema: 'c28-demo-incarnation/v1', event: 'start', run: cfg.run_id, inc, target, n: seq,
        pod_uid: shared.pod_uid || null, pod: shared.pod || null, started_ms: shared.started_ms,
        active_from_ms: activeFrom, origin_ms: cfg.origin_ms, minute: m0, segment_end_ms: endMs,
        config_digest: cfg.digest, label: cfg.label || null, timeout_ms: cfg.timeout_ms,
        max_late_ms: cfg.max_late_ms, max_inflight: cfg.max_inflight, lat_bounds_ms: cfg.latency_bounds_ms });
      seq += 1;
      tick();
    },
    state() { return { inflight, cursor, nextEmit, finished, cum: Object.assign({}, cum) }; },
  };
}

// --- k6 wiring ------------------------------------------------------------------------------------------------------

const cfg = checkConfig(JSON.parse(open(__ENV.C28D_CONFIG)));

const counters = {
  scheduled: new Counter('c28d_scheduled'),       // arrivals that came due (dispatched or dropped)
  dispatched: new Counter('c28d_dispatched'),     // added at dispatch
  dropped: new Counter('c28d_dropped'),           // tag reason: late | inflight
  completed: new Counter('c28d_completed'),       // added at COMPLETION (tag outcome); never a dispatch count
};

const scenarios = {};
cfg.targets.forEach((_, i) => {
  scenarios[`t${i}`] = { executor: 'per-vu-iterations', vus: 1, iterations: 1, exec: 'run',
    maxDuration: `${cfg.segment_minutes * 60 + 900}s`, gracefulStop: '10s' };
});

export const options = {
  discardResponseBodies: true,
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
    count: (name, tags) => counters[name].add(1, tags),
    send: (url, _timeoutMs, done) => {
      const t0 = Date.now();
      http.asyncRequest('GET', url, null, { timeout, tags: { name: 'work', c28_target: cfg.targets[index] } }).then(
        (res) => done(res.status || 0, res.timings ? res.timings.duration : Date.now() - t0,
          res.headers ? res.headers.Date : undefined),
        () => done(0, Date.now() - t0, undefined),
      );
    },
  });
  // The iteration lasts until the event loop is empty: the generator's last timer fires at the segment end.
  gen.start();
}
