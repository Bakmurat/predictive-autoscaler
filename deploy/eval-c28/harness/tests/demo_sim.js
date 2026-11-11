// Runs the REAL deploy/eval-c28/load/c28-demo.js under node, without k6: the script's k6 imports are replaced by
// stubs, its init code runs against a configuration file, and its generator (createGenerator: position in the day,
// arrival scheduling, per-minute bookkeeping, the records it writes) is driven by a discrete-event simulation with a
// virtual clock. Nothing here proves anything about k6 itself (its event loop, http.asyncRequest, timers, exit code).
//
//   node demo_sim.js <c28-demo.js> <config.json> <scenario.json>
//
// scenario.json:
//   {"incarnations": [{"pod_uid": "...", "start_ms": T, "kill_ms": T | null, "skew_ms": 0,
//                      "stalls": [{"at_ms": T, "for_ms": D}],            start/kill/stalls in TRUE time
//                      "clock_steps": [{"at_ms": T, "by_ms": D}]}],      its clock steps by D (+ forward, - back) at T
//    "latency_ms": 20,                              response time of a healthy request
//    "latencies": [{"from_ms": T, "to_ms": T, "latency_ms": D}],   another response time for requests sent in a span
//    "faults": [{"target": "...", "from_ms": T, "to_ms": T, "mode": "timeout" | "status", "status": 503}],
//    "network_ms": 1,                               dispatch -> ingress receipt
//    "scrape": {"interval_ms": 15000, "offset_ms": 3000, "from_ms": T, "to_ms": T,
//               "gaps": [{"from_ms": T, "to_ms": T}]},          ingress counter samples (raw, with their timestamps)
//    "ingress_restarts": [T, ...],                  the ingress's counters restart (no series until the next request)
//    "ingress_initial": 100}                        the counters' value at the beginning (absent: no series yet)
// An incarnation's own clock reads true time + skew_ms + every clock step so far (it schedules by its own clock; its
// timers are delays and do not move with a step, like k6's); the ingress's Date header and the scrape timestamps are
// true time. kill_ms ends an incarnation abruptly: nothing more is written (SIGKILL).
//
// Output (stdout, JSON): {"lines": [every record the script wrote, in order], "truth": {"<target>": {"<minute>":
//   {"dispatched": n, "received": n}}} (by TRUE dispatch / receipt time, minute index from the origin), "sends":
//   {"<target>": total requests sent}, "ingress": {"<target>": [[t_seconds, counter], ...]}, "functions": {...results
//   of the pure functions for "probes"...}}
'use strict';
const fs = require('fs');
const nodeCrypto = require('crypto');

// What k6/crypto's sha256(text, 'hex') returns: the SHA-256 of the text's UTF-8 bytes, lower-case hex.
const sha256hex = (text) => nodeCrypto.createHash('sha256').update(text, 'utf8').digest('hex');

function loadScript(scriptPath, configPath) {
  let src = fs.readFileSync(scriptPath, 'utf8');
  const edits = [
    ["import crypto from 'k6/crypto';", 'const crypto = __stubs.crypto;'],
    ["import http from 'k6/http';", 'const http = __stubs.http;'],
    ["import exec from 'k6/execution';", 'const exec = __stubs.exec;'],
    ["import { Counter } from 'k6/metrics';", 'const Counter = __stubs.Counter;'],
    ["import { setTimeout } from 'k6/timers';", 'const setTimeout = __stubs.setTimeout;'],
    ['export const options =', 'const options ='],
    ['export function setup()', 'function setup()'],
    ['export function run(shared)', 'function run(shared)'],
  ];
  for (const [from, to] of edits) {
    if (src.split(from).length !== 2) throw new Error(`c28-demo.js no longer contains exactly one '${from}'`);
    src = src.replace(from, to);
  }
  if (/^\s*(import|export)\s/m.test(src)) throw new Error('c28-demo.js has an import or export the loader does not know');
  const stubs = {
    crypto: { sha256: (text, enc) => { if (enc !== 'hex') throw new Error('only hex is stubbed'); return sha256hex(text); } },
    http: { asyncRequest: () => { throw new Error('http is not available under node'); } },
    exec: { scenario: { name: 't0' } },
    Counter: function Counter(name) { this.name = name; this.add = () => null; },
    setTimeout: () => { throw new Error('timers are not available at init'); },
  };
  const open = (p) => fs.readFileSync(p, 'utf8');
  const body = `${src}\nreturn { canonicalJson, configDigest, checkConfig, minuteIndex, minuteStart, scheduled, dayIndex, dayMinute, intended,
    targetPhase, arrivalTime, firstIndexAtOrAfter, segmentEnd, incarnationId, okStatus, httpDateSeconds,
    createGenerator, cfg, options, setup, run };`;
  // eslint-disable-next-line no-new-func
  return new Function('open', '__ENV', '__stubs', body)(open, { C28D_CONFIG: configPath, POD_UID: 'init-pod' }, stubs);
}

class Queue {                                   // a stable min-heap of {t, seq, fn}
  constructor() { this.h = []; this.seq = 0; }
  push(t, fn, owner) {
    const e = { t, seq: this.seq += 1, fn, owner };
    this.h.push(e);
    let i = this.h.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.less(this.h[i], this.h[p])) { [this.h[i], this.h[p]] = [this.h[p], this.h[i]]; i = p; } else break;
    }
  }
  less(a, b) { return a.t < b.t || (a.t === b.t && a.seq < b.seq); }
  pop() {
    const top = this.h[0];
    const last = this.h.pop();
    if (this.h.length) {
      this.h[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1; const r = l + 1; let s = i;
        if (l < this.h.length && this.less(this.h[l], this.h[s])) s = l;
        if (r < this.h.length && this.less(this.h[r], this.h[s])) s = r;
        if (s === i) break;
        [this.h[i], this.h[s]] = [this.h[s], this.h[i]]; i = s;
      }
    }
    return top;
  }
  get size() { return this.h.length; }
}

function simulate(mod, sc) {
  const cfg = mod.cfg;
  const q = new Queue();
  let T = 0;                                       // TRUE time, ms
  const lines = [];
  const truth = {};
  const sends = {};
  const counters = {};                             // the ingress's counters (reset by a restart)
  const samples = {};
  // ingress_initial: the counters' value when the simulation begins (earlier traffic); absent = no series yet.
  const initial = sc.ingress_initial === undefined ? null : sc.ingress_initial;
  for (const t of cfg.targets) { truth[t] = {}; sends[t] = 0; counters[t] = initial; samples[t] = []; }
  const bump = (target, tMs, key) => {
    const m = String(mod.minuteIndex(cfg, tMs));
    const row = truth[target][m] || (truth[target][m] = { dispatched: 0, received: 0 });
    row[key] += 1;
  };
  const network = sc.network_ms === undefined ? 1 : sc.network_ms;
  const latency = sc.latency_ms === undefined ? 20 : sc.latency_ms;
  const fault = (target, t) => (sc.faults || []).find((f) => f.target === target && t >= f.from_ms && t < f.to_ms);
  const latencyAt = (t) => {
    const w = (sc.latencies || []).find((x) => t >= x.from_ms && t < x.to_ms);
    return w ? w.latency_ms : latency;
  };

  for (const t of sc.ingress_restarts || []) {
    q.push(t, () => { for (const k of Object.keys(counters)) counters[k] = null; }, null);
  }
  if (sc.scrape) {
    const s = sc.scrape;
    const gap = (t) => (s.gaps || []).some((g) => t >= g.from_ms && t < g.to_ms);
    for (let t = s.from_ms + s.offset_ms; t <= s.to_ms; t += s.interval_ms) {
      // A target that has had no request since the ingress started has NO series (the fixture's lazy counter).
      if (!gap(t)) q.push(t, () => { for (const k of cfg.targets) if (counters[k] !== null) samples[k].push([t / 1000, counters[k]]); }, null);
    }
  }

  (sc.incarnations || []).forEach((inc, n) => {
    // The incarnation's clock offset at true time t: its skew and every clock step that has happened by then.
    const skewAt = (t) => (inc.skew_ms || 0) +
      (inc.clock_steps || []).reduce((sum, c) => sum + (t >= c.at_ms ? c.by_ms : 0), 0);
    const owner = { alive: true, n };
    const stalled = (t) => {                       // the end of the stall covering true time t, else t
      const s = (inc.stalls || []).find((x) => t >= x.at_ms && t < x.at_ms + x.for_ms);
      return s ? s.at_ms + s.for_ms : t;
    };
    const at = (t, fn) => q.push(stalled(t), fn, owner);
    const deps = {
      now: () => T + skewAt(T),
      setTimeout: (fn, ms) => at(T + Math.max(0, ms), fn),
      emit: (o) => lines.push(o),
      count: () => null,
      send: null,
    };
    q.push(inc.start_ms, () => {
      const started = T + skewAt(T);
      const shared = { started_ms: started, segment_end_ms: mod.segmentEnd(cfg, started), pod_uid: inc.pod_uid, pod: `gen-${n}` };
      cfg.targets.forEach((target, i) => {
        const d = Object.assign({}, deps, {
          send: (url, timeoutMs, done) => {
            const sent = T;
            if (url !== `${cfg.base_url}/t/${target}/work`) throw new Error(`unexpected URL ${url}`);
            bump(target, sent, 'dispatched');
            sends[target] += 1;
            const f = fault(target, sent);
            if (f && f.mode === 'unreachable') {            // never reaches the ingress: no HTTP response at all
              at(sent + timeoutMs, () => done(0, timeoutMs, undefined));
              return;
            }
            q.push(sent + network, () => { counters[target] = (counters[target] || 0) + 1; bump(target, T, 'received'); }, null);
            if (f && f.mode === 'timeout') {
              at(sent + timeoutMs, () => done(0, timeoutMs, undefined));
            } else {
              const status = f ? f.status : 200;
              const lat = f ? Math.min(latencyAt(sent), timeoutMs) : latencyAt(sent);
              at(sent + lat, () => done(status, lat, new Date(Math.floor((sent + network) / 1000) * 1000).toUTCString()));
            }
          },
        });
        mod.createGenerator(cfg, i, shared, d).start();
      });
    }, owner);
    if (inc.kill_ms !== null && inc.kill_ms !== undefined) q.push(inc.kill_ms, () => { owner.alive = false; }, null);
  });

  let guard = 0;
  while (q.size) {
    const e = q.pop();
    if (sc.until_ms !== undefined && e.t > sc.until_ms) break;
    T = Math.max(T, e.t);
    if (e.owner === null || e.owner.alive) e.fn();
    if ((guard += 1) > 50_000_000) throw new Error('the simulation does not terminate');
  }
  return { lines, truth, sends, ingress: samples };
}

function probes(mod, list) {
  const cfg = mod.cfg;
  return (list || []).map((p) => {
    const phase = mod.targetPhase(cfg, p.target_index || 0);
    if (p.fn === 'position') {
      const m = mod.minuteIndex(cfg, p.t_ms);
      const inside = mod.scheduled(cfg, m);
      return { minute: m, scheduled: inside, day: mod.dayIndex(cfg, m), day_minute: mod.dayMinute(cfg, m),
        intended: mod.intended(cfg, m), first_index: inside ? mod.firstIndexAtOrAfter(cfg, m, p.t_ms, phase) : null,
        segment_end_ms: mod.segmentEnd(cfg, p.t_ms) };
    }
    if (p.fn === 'arrivals') {
      const n = mod.intended(cfg, p.minute);
      const ts = [];
      for (let j = 0; j < n; j += 1) ts.push(mod.arrivalTime(cfg, p.minute, j, phase));
      return { minute: p.minute, times: ts };
    }
    if (p.fn === 'httpdate') return { seconds: mod.httpDateSeconds(p.text) };
    if (p.fn === 'digest') return { digest: mod.configDigest(cfg, sha256hex), stated: cfg.digest };
    if (p.fn === 'canonical') {
      try { return { text: mod.canonicalJson(p.value) }; } catch (e) { return { error: String(e.message) }; }
    }
    if (p.fn === 'check') {
      // override: fields to replace; day_rows: {"<day>": row} replaces single rows of the table (the config is large).
      // The digest is recomputed for the edited configuration unless keep_digest is set (then the OLD digest stays,
      // as when a table is altered after rendering).
      try {
        const c = Object.assign({}, cfg, p.override);
        if (p.day_rows) {
          c.per_minute_days = c.per_minute_days.slice();
          for (const [d, row] of Object.entries(p.day_rows)) c.per_minute_days[Number(d)] = row;
        }
        if (p.cells) {                              // [[day, minute, value], ...]: single counts of the table
          c.per_minute_days = c.per_minute_days.map((row) => row.slice());
          for (const [d, m, v] of p.cells) c.per_minute_days[d][m] = v;
        }
        if (!p.keep_digest) { try { c.digest = mod.configDigest(c, sha256hex); } catch (e) { /* checkConfig reports */ } }
        mod.checkConfig(c, p.no_hash ? undefined : sha256hex);
        return { ok: true };
      } catch (e) { return { ok: false, error: String(e.message) }; }
    }
    return { error: `unknown probe ${p.fn}` };
  });
}

const [scriptPath, configPath, scenarioPath] = process.argv.slice(2);
try {
  const mod = loadScript(scriptPath, configPath);
  const sc = JSON.parse(fs.readFileSync(scenarioPath, 'utf8'));
  const out = simulate(mod, sc);
  out.functions = probes(mod, sc.probes);
  out.options = mod.options;
  process.stdout.write(`${JSON.stringify(out)}\n`);
} catch (e) {
  process.stdout.write(`${JSON.stringify({ error: String(e && e.message) })}\n`);
  process.exit(1);
}
