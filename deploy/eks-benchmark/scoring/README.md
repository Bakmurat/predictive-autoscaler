# Verified forecast-log inputs

For the separate descriptive RPC/cache ledger reader, see
[ATTEMPT-RECORDS.md](ATTEMPT-RECORDS.md). It uses the same verified bytes and reports
formal forecast availability as `not_evaluated`.

The scoring CLI requires the log **and its collector receipt**, including for
`--participation-only`. Existing function-level fixture tests do not need one.

Use the intended cluster context/profile before collecting (the benchmark uses
`AWS_PROFILE=personal`, `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` under
`~/Desktop/Home/.aws`, and `KUBE_CONTEXT=predictive-bench`):

```sh
bash read-forecast-log.sh --all --out /new/evidence/forecasts.jsonl
python3 score.py --forecast-log /new/evidence/forecasts.jsonl \
  --forecast-receipt /new/evidence/forecasts.jsonl.receipt.json \
  --prom http://localhost:9090 --start <UTC-start> --end <UTC-end>
```

The reader requires exactly one operator pod, mounts the forecast PVC read-only,
probes byte length N and SHA-256 of the first N bytes, and verifies the transfer
and saved staging file. It publishes the saved file and then its receipt using
exclusive hard links from temporary files on the destination filesystem. Existing
destinations are refused. A receipt failure leaves an unreceipted file, which the
scorer refuses; retry into a new destination. The temporary reader pod is removed
on exit. Operator and PVC identities are checked again after transfer; a change
fails closed. Probe time is taken before remote execution, conservatively bounding
the issuance window. Rewriting, truncating or rotating the source violates the
append-only precondition. Save collector stdout/stderr alongside the evidence.

The receipt records source PVC UID/PV and operator pod UID, probe and collection
times, and the reader's source hash. It proves equality to a remote **prefix**
under the append-only assumption, not that every reconcile was logged. Coverage
checks remain necessary. It is an integrity record, **not authentication**: anyone
with write access could forge a self-consistent receipt. Preserve the original
collector evidence; do not create receipts from local hashes for old logs.

The scorer reads the log once, verifies that immutable byte buffer, and uses it
for both decision and issuance parsing. Replacing the path afterward cannot
change the scored inputs. Output contains the receipt and its own SHA-256.
The issuance window must end no later than the remote probe. Targets from those
recorded issuances can mature later; `--as-of` therefore remains independent
(default: scoring time). `--last` and `--count` produce no receipt.

Tests may explicitly pass `--allow-fixture-receipt` for a receipt with
`kind: fixture`; output then carries `forecast_log_provenance.fixture: true`.
Production capture must never use that switch. Missing, malformed, mismatched,
or unapproved fixture receipts fail before querying Prometheus.

Scorer `--out` destinations must also be new. Results are staged completely and
published exclusively; an existing result or concurrent writer is never replaced.
Use the default `--out -` to print to stdout.

The forecast reader transfers the probed prefix in 1 MiB chunks, each with its
own remote length and SHA-256, before checking the original whole-prefix hash.
Each chunk has at most three attempts, a 45-second subprocess timeout and a
30-second kubectl request timeout. The helper enforces a 900-second budget using
both monotonic and wall time, including a final check before success. Failed
bytes are never appended to the accepted prefix. Reader pod UID/restart count
must stay unchanged from before the probe through completion. Appends after the
probe are allowed; the final chunk stops exactly at the probed byte count.

`--out FILE` also exclusively creates `FILE.transfer` before cluster access.
It retains the remote probe, reader identity, per-attempt stderr, exit status,
chunk index/offset and elapsed times. Failed reads retain `prefix.partial`
(possibly incomplete); successful output removes this duplicate copy.
Without `--out`, clean success removes temporary diagnostics; failure or recovery
after retries retains them, with the directory printed on stderr. Use a
new destination after failure; neither a partial prefix nor successful transport
alone is a valid scored input. The complete file still needs its receipt and
the scorer's parsing/coverage checks. A probe can intersect a partially written
JSONL line; this reader preserves those exact bytes, and the parser fails closed.
Newline-aligned probing is a separate framing change, not part of transport retries.
Repeated failures can accumulate retained partial copies, including under the
temporary directory; preserve or inspect this failure evidence before removing it.

New `reader_fingerprint` values hash this fixed ordered UTF-8 string:
`v2\n<SHA256(read-forecast-log.sh)>\n<SHA256(forecast_transfer.py)>\n`.
Earlier receipts retain their single-script fingerprint meaning. The receipt
schema and source-prefix semantics are unchanged. Keep both scripts together
when copying the reader. Collection tooling does not require a serving rollout.

### Recorded component comparison

Normal scoring also emits `recorded_components`: the served forecast and its
recorded `component-v1` seasonal pattern, joined to the **same canonical actual
rows** already used by the raw scorer. This is neither the previous-day point
query nor a different application's forecast. No additional Prometheus queries
are made. Legacy, malformed, misaligned and unavailable component evidence is
retained as unavailable; zero remains a valid value.

For each step, globally and per artifact, `issued_steps_matured` is the conditional
availability denominator. `actual_available`, `hybrid_finite`, `pattern_usable`
and `network_finite` are separate counts. `paired_n` counts the intersection with
finite actual, served and pattern values; both paired MAE and signed bias use
that same intersection. Positive bias means overprediction. Outstanding targets
are excluded from matured denominators. Empty comparisons are null, not zero.
The unrounded `targets` retain alignment, availability and effective lead time.
Unique paired target/input timestamps are reported because repeated observations
and input windows are not independent samples; absent input anchors are counted
as unknown. Blend-weight discrepancies are non-fatal diagnostics, not filters.

This is **conditional availability among accepted issued targets**, not protocol
availability across all attempted or scheduled prediction origins. Refusals and
missing issuance records require separate evidence; decision records are not a
one-to-one attempt log. The section does not implement pilot sizing, whole-day
uncertainty, a pooled improvement estimate or a winner decision. Its
`formal_comparison_complete` remains false. Existing acceptance, raw scores,
coverage exit gates and the participation-only path are unchanged.

Component-v1 validation supports six ten-minute steps. Unsupported geometry is
unavailable component evidence, while raw scoring remains unchanged. Finite
inputs can still overflow when subtracting; `paired_loss_nonfinite` counts those
pairs and affected loss summaries are null rather than infinite or zero.

Duplicate issued/actual target identities invalidate only the component section
(`status: invalid`, reason and duplicate counts, no paired metrics); they never
cause a silent join or change the raw scoring gates. Synthetic unit fixtures
do not prove deployed operator serialization. Verify that boundary against a
privately retained, receipt-bound operator log before using a new operator build.
