# Evidence archive (benchmark instrumentation)

The benchmark retrains the forecasting model every six hours and replaces the published artifact in
place. Evidence of each training used to be captured from a workstation, which fails whenever that
machine is asleep. This directory adds a small in-cluster CronJob that preserves the evidence
without an outside observer. It is declared instrumentation: it reads, copies and records; it does
not change the operator, the API, the trainer or the demo applications.

## What it keeps

On the archive volume (`evidence-archive-pvc`, 1 GiB gp3), every 15 minutes (`5,20,35,50`):

| Path | Content |
|---|---|
| `archive/<cutoff>-<sha12>/` | the published artifact, its provenance sidecar and a manifest, finalized by one rename only after the pair is verified |
| `INDEX.jsonl` | one line per finalized pair: copy time, full artifact and sidecar SHA-256, training cutoff, source mtimes, archiving Job and Pod |
| `REJECTS.jsonl` | every pair that was not archived, with the reason |
| `logs/trainer/` | each completed training Pod's full timestamped log, with Job/Pod UID, terminal status, exit code and imageID |
| `logs/api-reload.jsonl` | the API's `Loaded/Reloaded model ... sha256=` lines, with pod name and restart count |
| `logs/api-coverage.jsonl` | the log window each collection covers, with flags for restarts, replaced pods and failed collections |
| `RUNS.jsonl` | one line per archiver run, for detecting missed runs |

## How a pair is verified

The trainer publishes the artifact and the sidecar with two separate renames, so for a moment they
can describe different trainings. The archiver stages both files, requires the sidecar's
`artifact_sha256` and `artifact_bytes` to equal the staged bytes, then re-reads the source pair to
prove neither changed during the copy. Only then does it rename the staging directory into place and
append the index line. Anything else is recorded in `REJECTS.jsonl` and retried on the next run.
Re-runs do not duplicate entries; a crash between the rename and the index line is repaired on the
next run; an existing archive directory is never overwritten.

## What it cannot establish

- Neither file mtimes nor the sidecar record the moment of publication (the sidecar has no such
  field). Publication times come from the trainer's own log lines, when those were preserved.
- Polling every 15 minutes cannot guarantee that every publication is seen: two publications inside
  one interval would leave only the second. Completeness is checked afterwards by reconciling
  training Jobs, API reload lines and the operator's issuance records against the index.
- An API reload line shows when the API noticed a new file, not when the operator first used it.

## Placement and privileges

The model volume is ReadWriteOnce, so the pod is scheduled next to the API by pod affinity and
mounts that volume read-only. The archive volume is created in the same zone at first use
(`WaitForFirstConsumer`). The service account may only get and list pods, pod logs and Jobs in
`ml-engine`. No Istio sidecar is injected. Requests 20m CPU / 48Mi, limits 200m / 96Mi, at most four
minutes per run.

## Install and test

```sh
python3 archive_test.py            # also part of `make verify`
kubectl apply -k .                 # installs into ml-engine
```

Storage cost at the published gp3 rate for ap-southeast-1 (USD 0.096 per GB-month, AWS Price List
API, 2026-09-23) is about USD 0.10 per month for 1 GiB. Each archived pair is about 4.2 MB, so 1 GiB holds roughly 200
trainings, about 50 days at four per day.
