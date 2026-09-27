# Descriptive forecast attempt report

`attempt_report.py` describes the operator's `forecast-attempt-v1` evidence in
the same verified forecast-log prefix used by the existing scorer. It does not
change forecast scoring, component comparisons, participation, controller
behavior, or qualification thresholds.

```sh
python3 attempt_report.py \
  --forecast-log /new/evidence/forecasts.jsonl \
  --forecast-receipt /new/evidence/forecasts.jsonl.receipt.json \
  --app nginx-test --namespace demo \
  --start 2030-01-01T00:00:00Z --end 2030-01-01T01:00:00Z \
  --out /new/evidence/attempt-report.json
```

Replace the example dates with the actual UTC window. The end must be no later
than the receipt's remote probe. The reader calls `forecast_log.load_verified`
and parses its exact immutable bytes; it never reopens the log after verification.
Receipts establish equality to the captured prefix under the collector's
append-only assumption, not authentication or complete operational capture.
See [README.md](README.md) for collection and receipt requirements.

No Prometheus connection is needed. Output defaults to stdout. A file destination
must be new and is published exclusively. Malformed JSON, duplicate JSON members,
nonfinite JSON numbers, truncated records, or invalid receipts stop the command.
Schema/link defects are retained in a JSON report as invalid or incomplete
evidence. Exit zero means that a report was written, **not** that its evidence is
consistent or complete. Test fixtures require the explicit
`--allow-fixture-receipt` switch and retain the fixture label in provenance.

Completion outcome, HTTP status, error class/stage, and served-field diagnostics
must agree with a possible producer branch. A contradictory completion is retained
in an `invalid_completion_semantics` issue and is excluded from valid completion
counts. Its linked start can also report `missing_completion` because no valid
completion resolved it. Those two diagnostics describe one underlying invalid
record; they are not two separate failed HTTP calls.

## What is joined

An operator incarnation has a random `operator_run_id`; lookup, attempt, and
issuance IDs derive from that run and the lookup counter. One attempt means one
invocation of the existing HTTP client's `Post`, not a count of internal redirects
or wire retries. The report validates request identity/hash agreement across
start and completion; it does not reconstruct the request body from its hash.

The reader joins the **entire prefix before selecting an app or time window**:

- `forecast_ledger_session`, `forecast_attempt_started`, and
  `forecast_attempt_completed` establish process/attempt observations.
- Existing issuance rows link a nonempty fresh forecast to its attempt.
- The existing decision's `forecast_lookup` explains cache use and links the
  fresh attempt, prior issuance, and returned issuance separately.

A successful issuance can appear before its completion record because completion
evidence is appended after the existing cache and issuance clock observations.
The start must still precede completion, and the decision must follow completion.
Issuance `namespace` retains the exact wire `request_namespace`, including an
empty value; attempt and decision `namespace` carry the effective target.

Missing sessions, starts, completions, expected nonempty issuances, or linked
decisions remain explicit. Duplicate records remain present, including identical
duplicates; conflicting identity/link fields and unsupported schemas are invalid.
Whole-prefix integrity can therefore be invalid because of another application's
evidence even when selected counts are otherwise usable as diagnostics.

## Reading the output

Attempt selection uses start time in `[start,end)` at nanosecond precision. A
completion after `end` can resolve that attempt if captured in the same prefix.
Decision summaries independently select decision time in `[start,end)`, so their
population can differ. Source line numbers and raw records are retained.

`counts.starts` counts raw selected start rows, while `attempt_identities` counts
their distinct IDs. `completion_records` retains completion multiplicity for
selected IDs. `completed_unique` and `outcomes` include only IDs with exactly one
valid completion record; the report retains all duplicate/invalid evidence and
does not present these counts as a reliability denominator. A pending start is
`pending_or_unresolved`, never an invented API failure.

Raw completion outcome and lookup resolution answer different questions. A
failed fresh HTTP call can have `stale_after_error` resolution and return a prior
issuance. A `cache_hit` has no fresh attempt. `fresh_response` includes an empty
decoded response, which creates no issuance. HTTP refusal with a timeout while
reading its body remains an HTTP refusal, with `error_class: timeout`.

`served_predictions_status`, `served_step_count`, and `served_step_status` are
observations of the raw served response field. Null, malformed, nonfinite, and
finite elements are distinct. A decoded HTTP response does not imply six finite
forecast steps, input validity, or accurate predictions. Network-component flags
are not a proxy for the served response.

Served diagnostics inspect the captured first JSON document, matching the core
decoder's boundary. During a failed HTTP200 decode, captured bytes can be partial
(for example, after a response-body timeout). A `malformed` diagnostic then means
the captured document could not be parsed; it does not prove the server sent a
malformed complete response.

Lookup resolution also precedes decision eligibility. A returned forecast can be
rejected later by sanity or horizon checks. Existing decision `forecast_status`
remains the source for actual participation; this report does not change it.

## Limits that remain explicit

`formal_availability.status` is always `not_evaluated`, and coverage is null.
Successful responses are cached while failures can be retried at a different
cadence. Counting HTTP attempts cannot supply the protocol's independent,
all-predictor opportunity denominator.

`event_seq` covers only session/start/completion records. Gaps and duplicates are
visible, but a missing cache-hit-only decision is not detectable from those
sequences. Missing final tails, entire unobserved processes, throttled/early-return
reconciles, and API component outcomes hidden by refusals remain unknown. An
otherwise consistent prefix is not proof that every call or reconcile was logged.

Historical rows without these IDs are `not_instrumented`; mixed windows remain
labelled mixed. Evidence-disabled and initialization-failed observations are
listed separately. Missing historical attempts are never reconstructed as zero
failures or perfect availability. No report here declares qualification or T0.

## Tests

`python3 attempt_report_test.py` exercises synthetic evidence, including the saved
Go-generated fixture. `score_test.py` includes this module through its unittest
loader, so the repository's existing scoring test gate runs these checks too.
Mixed-log regression tests verify unchanged raw/component scoring and unchanged
participation output. No test invokes Go, a model, Kubernetes, or a cloud API.
