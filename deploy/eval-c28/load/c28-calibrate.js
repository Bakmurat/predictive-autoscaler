// C2.8 capacity calibration load (EVAL-C28-MANIFEST section 3; staging/calibrate.py, stage S5a). One trial = one k6 run:
// a `settle` scenario (five minutes) then a `measure` scenario (fifteen minutes), both constant-arrival-rate at the
// same total rate through the evaluation ingress, one request per iteration, open loop (arrivals never wait for
// completions). The 2 s timeout and no retries are frozen, as in c28.js.
//
//   k6 run --no-usage-report -e C28CAL_TRIAL=t001 -e C28CAL_TARGET=c28cal-... -e C28CAL_BASE_URL=http://... \
//     -e C28CAL_RATE_PER_MIN=1200 -e C28CAL_SETTLE_S=300 -e C28CAL_MEASURE_S=900 -e C28CAL_SEED=... \
//     -e C28CAL_PREALLOCATED_VUS=N -e C28CAL_MAX_VUS=M -e C28CAL_TIMEOUT_S=2 c28-calibrate.js
//
// Each scenario's ACTUAL start (exec.scenario.startTime, after k6 initialization and VU allocation) is recorded as
// the min/max of a per-scenario Trend and reported as start_ms, so calibrate.py measures the observer window from the
// real measurement start, never from the container start plus the nominal settling.
//
// The per-scenario counts and latencies leave the pod as its termination message (/dev/termination-log, part of the
// Pod's status), so calibrate.py reads them from the API without logs, a sidecar or an extra output. Submetrics exist
// only for metrics with a threshold, hence the always-true thresholds below. A metric without samples is absent from
// the summary (k6 never emits a zero dropped_iterations): calibrate.py then reconciles scheduled = dropped + dispatched
// and treats any other absence as missing evidence, never as zero.
import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { Trend } from 'k6/metrics';

const scenarioStart = new Trend('c28cal_scenario_start_ms');

function intEnv(name, lo, hi) {
  const v = Number(__ENV[name]);
  if (!Number.isInteger(v) || v < lo || v > hi) {
    throw new Error(`${name} must be an integer in ${lo}..${hi}`);
  }
  return v;
}

const trial = __ENV.C28CAL_TRIAL;
const target = __ENV.C28CAL_TARGET;
const base = __ENV.C28CAL_BASE_URL;
if (!/^t[0-9]{3}$/.test(trial || '') || !/^[a-z0-9-]+$/.test(target || '') || !/^http:\/\//.test(base || '')) {
  throw new Error('C28CAL_TRIAL, C28CAL_TARGET and C28CAL_BASE_URL (http://...) are required');
}
const rate = intEnv('C28CAL_RATE_PER_MIN', 1, 10000000);
const settle = intEnv('C28CAL_SETTLE_S', 1, 86400);
const measure = intEnv('C28CAL_MEASURE_S', 1, 86400);
const seed = intEnv('C28CAL_SEED', 1, 2147483647);
const preVUs = intEnv('C28CAL_PREALLOCATED_VUS', 1, 100000);
const maxVUs = intEnv('C28CAL_MAX_VUS', preVUs, 200000);
const timeoutS = intEnv('C28CAL_TIMEOUT_S', 1, 60);

const SCENARIOS = ['settle', 'measure'];
const METRICS = ['iterations', 'dropped_iterations', 'http_reqs', 'http_req_failed', 'http_req_duration'];
const thresholds = {};
for (const s of SCENARIOS) {
  thresholds[`iterations{scenario:${s}}`] = ['count>=0'];
  thresholds[`dropped_iterations{scenario:${s}}`] = ['count>=0'];
  thresholds[`http_reqs{scenario:${s}}`] = ['count>=0'];
  thresholds[`http_req_failed{scenario:${s}}`] = ['rate>=0'];
  thresholds[`http_req_duration{scenario:${s}}`] = ['max>=0'];
  thresholds[`c28cal_scenario_start_ms{scenario:${s}}`] = ['max>=0'];
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    settle: { executor: 'constant-arrival-rate', rate, timeUnit: '1m', duration: `${settle}s`,
      preAllocatedVUs: preVUs, maxVUs, gracefulStop: `${timeoutS + 3}s` },
    measure: { executor: 'constant-arrival-rate', rate, timeUnit: '1m', duration: `${measure}s`, startTime: `${settle}s`,
      preAllocatedVUs: preVUs, maxVUs, gracefulStop: `${timeoutS + 3}s` },
  },
  thresholds,
  summaryTrendStats: ['avg', 'min', 'p(50)', 'p(95)', 'p(99)', 'max', 'count'],
  tags: { c28cal_trial: trial, c28cal_target: target },
};

// Seeded per-VU xorshift32: a small arrival jitter (0-100 ms dispatch offset) that does not change the arrival count.
let state = 0;
function rnd() {
  if (state === 0) {
    state = ((seed ^ Math.imul(__VU, 2654435761)) >>> 0) || 1;
  }
  state ^= state << 13; state >>>= 0;
  state ^= state >>> 17;
  state ^= state << 5; state >>>= 0;
  return state / 4294967296;
}

let startRecorded = '';
export default function () {
  if (startRecorded !== exec.scenario.name) {       // once per VU and scenario
    scenarioStart.add(exec.scenario.startTime);
    startRecorded = exec.scenario.name;
  }
  sleep(rnd() * 0.1);
  http.get(`${base}/t/${target}/work`, { timeout: `${timeoutS}s`, tags: { name: 'work' } });
}

export function handleSummary(data) {
  const pick = (name) => (data.metrics[name] ? data.metrics[name].values : null);
  const out = { schema: 'c28-calibration-k6/v1', trial, target, rate_per_min: rate, settle_s: settle,
    measure_s: measure, seed, scenarios: {} };
  for (const s of SCENARIOS) {
    out.scenarios[s] = {};
    for (const m of METRICS) {
      out.scenarios[s][m] = pick(`${m}{scenario:${s}}`);
    }
    const st = pick(`c28cal_scenario_start_ms{scenario:${s}}`);
    out.scenarios[s].start_ms = st ? { min: st.min, max: st.max } : null;
  }
  return { '/dev/termination-log': JSON.stringify(out) };
}
