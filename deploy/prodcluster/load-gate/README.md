# Load-gate records (prodcluster campaign)

Inputs of the hourly load-evidence CronJob (`../ml-engine/load-evidence.yaml`), versioned so every change is a commit:

- `settings.env` — `INVENTORY_START`: the moment every arm pod ran the final-snapshot hook v2 (2026-10-05T08:14:00Z);
  pods before it are closed by the campaign's one-time record.
- `approvals.json` — the user's recorded approvals of the gate's conditional assumptions, e.g.
  `{"A1'": {"decision": "approved", "by": "user", "at": "2026-10-05T09:00:00Z", "ref": "U-30"}}`. Empty: every row
  reports `qualifies: false` with A1' pending. Only the user approves.
- `terminations.json` — manual termination records for pods without an accepted final snapshot:
  `{"<pod>": {"terminated_before": UTC, "pod_start": UTC, "pod_uid": "...", "evidence": "...", "recorded_by": "...",
  "at": UTC}}`. Each closes one incarnation (name + pod start) and must cite its evidence. A pod that never ran uses
  `{"never_scheduled": true, "created": UTC, "deleted_before": UTC, "pod_uid", "evidence", "recorded_by", "at"}`; the
  collector re-checks it against kube-state-metrics (one creation time, empty node label in every sample, scheduled
  condition never true, no start time, no Envoy target, no istio series, nothing after deleted_before). Current
  records: the eleven arm pods that stayed Pending on 2026-10-05 10:02–10:30Z (topology-spread cap, D-1080).
