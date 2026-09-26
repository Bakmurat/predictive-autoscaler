# Forecast component records

Fresh forecast issuance records may contain a `components` object with
`schema: "component-v1"`. The object comes from the same API response as the
recorded forecast, without a second inference request. Cached reuse does not
create a new issuance. Application, namespace, model version, artifact hash,
issuance time and input anchor remain on the enclosing record.

Only `status: "ok"` exposes component values. This means the diagnostic has
valid structure and matches the recorded targets and served final values; it
is not an accuracy or completeness verdict. Version 1 supports six 10-minute
steps over a 60-minute horizon.

| Field | Meaning |
| --- | --- |
| `target_timestamps` | Six UTC RFC3339 timestamps, exactly matching the enclosing forecast targets as instants. |
| `pattern`, `lstm`, `blended`, `final` | Six nullable numbers, preserved at API precision. Raw values may be negative. `final` is required and finite. |
| `pattern_available_per_step` | Six required booleans; false corresponds exactly to a null pattern value. Zero is an available value. |
| `network_finite_per_step` | Six required booleans; false corresponds exactly to a null LSTM value. |
| `pattern_weights` | Six finite numbers in [0, 1], retained even when a pattern step is unavailable. |
| `pattern_source` | Nullable source description supplied by the API. |
| `network_failed` | Explicit null on normal inference, or the API's exception description string. It is not a boolean. A missing field is invalid. |
| `reason` | Null for valid diagnostics; otherwise an explanatory code. |

The final value rounded to two decimals must equal the served prediction.
The conversion rounds the binary float to decimal with ties to even; it does
not multiply by 100 before rounding. The generic schema does not require
`blended == final`, because a model can apply a post-blend adjustment.

`absent` means no component object was supplied. `invalid` means malformed,
missing, nonfinite, inconsistent or unsupported diagnostics. Both contain only
schema, status and reason. `misaligned` contains those fields plus normalized
API target timestamps, with no numeric arrays. Naive API timestamps mean UTC;
fractional seconds are preserved when checking them. A fractional timestamp
that differs from the core log's whole-second target is misaligned.

Optional diagnostic errors do not refuse an otherwise valid forecast, change
replicas or discard its core issuance record. Core decode errors and refusal
handling remain unchanged. Invalid JSON itself remains a core decode error.
Unknown diagnostic keys are ignored. Raw response JSON is never written to
the forecast log.

Legacy records lack this object and have unavailable component evidence.
Existing raw-forecast scoring continues to accept old and new records, including
valid raw forecasts that the controller later rejects. Component comparisons
must select status `ok`, retain per-step availability and pair the pattern with
the enclosing forecast's target and actual observation. Missing values must not
be replaced with zero. The existing previous-day point-query diagnostic is
not the recorded seasonal component. This schema does not implement a formal
paired statistical report or change qualification thresholds.

The controller test fixture `testdata/component-api-responses.json` contains
offline API responses using synthetic traffic and a preserved model checkpoint;
it is not a scored observation. It covers both hybrid and seasonal responses.
