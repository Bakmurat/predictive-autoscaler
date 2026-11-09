// Loads the REAL deploy/eval-c28/load/c28.js init code under node with minimal k6 stubs, against a plan file, and
// prints the resulting k6 options (or the error c28.js throws). Used by test_azure_functions_2019.py (A2): it proves
// the emitted trace plan passes c28.js's own validator and yields a well-formed ramping-arrival-rate scenario.
//   node c28js_loader.js <c28.js> <plan.json>
'use strict';
const fs = require('fs');
const [script, planPath] = process.argv.slice(2);
let src = fs.readFileSync(script, 'utf8');
const edits = [
  ["import http from 'k6/http';", "const http = { get: () => null };"],
  ["import { sleep } from 'k6';", "const sleep = () => null;"],
  ['export const options =', 'const options ='],
  ['export default function ()', 'function __default()'],
  ['export function handleSummary(', 'function handleSummary('],
];
for (const [from, to] of edits) {
  if (src.split(from).length !== 2) {
    console.log(JSON.stringify({ loader_error: `c28.js no longer contains exactly one '${from}'` }));
    process.exit(3);
  }
  src = src.replace(from, to);
}
const open = (p) => fs.readFileSync(p, 'utf8');
const env = { C28_PLAN: planPath, C28_BASE_URL: 'http://c28-ingress.invalid' };
try {
  const mod = new Function('open', '__ENV', '__VU', src + '\nreturn { options, handleSummary };')(open, env, 1);
  console.log(JSON.stringify({ ok: true, options: mod.options }));
} catch (e) {
  console.log(JSON.stringify({ ok: false, error: String(e && e.message) }));
}
