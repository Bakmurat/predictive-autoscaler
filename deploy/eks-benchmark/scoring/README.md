# Verified forecast-log inputs

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
