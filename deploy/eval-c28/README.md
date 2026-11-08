# C2.8 evaluation fixture and harness

Offline code for the C2.8 study (`agent-coordination/09-real-world-research/EVAL-C28-MANIFEST.md`, sections 3–9 and
the prodcluster adaptation in section 11). Nothing here runs an experiment, deploys to a lab cluster or produces C2.8
evidence by itself. The executor uses it under a locked run-lock and the change gates of manifest section 11.4.

| Path | What it is |
|---|---|
| `fixture/` | The HTTP fixture: one binary in three modes. `server` is the workload: fixed CPU work, a fixed response, a bounded queue, a measured startup delay and `/ready`, `/metrics`. `ingress` counts offered demand per target before the target can reject it, with a 2 s timeout and no retries. `tcpproxy` is the evaluation-owned metrics and forecast routes, with scheduled outages. `Dockerfile` builds a static, non-root image. |
| `load/c28.js` | The k6 open-loop generator (ramping-arrival-rate, one request per iteration), driven by a plan from `python -m c28 plan`. |
| `harness/c28/` | `render` (fixture, ingress, routes, quota and the five arms for both startup profiles, with an exclusive-writer audit), `schedule` (programs, k6 plans, fault timetable, the frozen session order), `runlock`, `collect` (hashed evidence bundle), `reduce` (per-window metrics), `score` (paired day-block bootstrap, Holm correction, the section 8 classification), `inject` (the guarded quota fault). `frozen.py` holds every value the manifest freezes. |
| `params/` | `kind-smoke.json` for the kind smoke. `prodcluster.template.json` lists what the run-lock must resolve; it cannot be rendered until every placeholder is replaced. |
| `smoke/kind.sh` | The aspire kind-bed smoke, on its own cluster and kubeconfig only. `--render-only` renders and audits offline; `scripts/verify.sh` runs that mode. |

```sh
cd deploy/eval-c28/harness
python -m c28 render --params ../params/kind-smoke.json --out /tmp/c28.yaml
python -m c28 plan --kind scored --seed 201 --capacity 35 --target c28-pa-n-s20 --out plan.json
python -m c28 order
python -m c28 runlock lock runlock.json && python -m c28 runlock verify runlock.json --manifest-root <dir>
python -m c28 release-values --params P --out release-values.yaml   # the evaluation release's routes and TLS name
python -m c28 collect --runlock runlock.json --targets c28-pa-n-s20 --start 2026-11-20T00:00:00Z \
  --end 2026-11-21T00:00:00Z --out bundle/ --file plan=plan-201-c28-pa-n-s20.json --file ledgers=decisions.jsonl
python -m c28 reduce --runlock runlock.json --bundle bundle/ --target c28-pa-n-s20 --block-start 2026-11-20T00:00:00Z
python -m c28 score --runlock runlock.json --study study.json
```

Frozen inputs are enforced, not just recorded.

- **collect:** runs exactly the run-lock's query definitions against its query API and namespace, for its targets and inside its schedule.
- **reduce:** works only on a bundle whose hashes verify, that was collected under that run-lock, with the locked queries and a locked confirmation plan. Capacity comes from the run-lock, and the output carries its binding.
- **score:** re-derives every block from the bundle it names: the seed's locked plan, a date the run-lock permits for that arm/profile/seed, an identical binding, windows, day and measured validity. Evidence reused for another block, or an edited derived metric, is refused. It refuses malformed studies outright: an incomplete prerequisite schema, non-boolean flags, non-finite or inconsistent counts, or duplicate records.

The error endpoint's denominator is the plan's **scheduled** arrivals.

- Every scheduled arrival has one client-side terminal outcome: dropped by k6, failed (including timeouts and no response), or succeeded.
- Arrivals with no observed outcome count as failures.
- Each window reconciles scheduled = dropped + dispatched within 1 %. An unreconciled block makes the result INCOMPLETE.
- Per minute, the running balance of scheduled minus terminal outcomes may only be what is still in flight. Beyond that, the minute is unknown, never healthy.
- Dispatched minus admitted at the ingress gives the pre-ingress failures. They are reported and never counted twice.

The windows tile the day: the manifest's omitted 03:00–06:00 is an explicit ordinary window.

Arms. Every arm runs as its own Deployment (`c28-<arm>-<s20|s90>`, so each has a new UID) and has exactly one writer:

- **H-CPU:** an autoscaling/v2 HPA on CPU utilization.
- **H-RPS:** a KEDA ScaledObject on the target's offered RPS, `AverageValue` = c, `ignoreNullValues=false`. Its generated HPA is the writer.
- **PA-R, PA-N, PA-E:** PredictiveAutoscalers on the statistical engine with capacity c, scale-down headroom 0, the frozen calibration and caps, and the profile's startup budget.
  - PA-R sets the predictive extra to 0, so it is reactive on the same Go policy, grid and scheduling. Its forecasting keeps running, with its overhead (shadow issuances); the forecasts just never add replicas.
  - Each PA arm sets the real `spec.prediction.families` (C2.8a, #27): PA-N `[seasonalNaive]` (E1-product is never fitted or offered), PA-R and PA-E `[seasonalNaive, statistical]`. The run-lock records each arm's families and effective candidate set; every PA block reports the families observed at confirmation and the candidates its issuances carried. A missing record is INCOMPLETE; any other set is a contract failure (FAIL).

All PredictiveAutoscalers render in Recommend mode; activation is the executor's gated step. HPA and KEDA keep their native cadence on prodcluster (section 11.3).

Faults are scheduled. A ConfigMap written before each block lists absolute UTC windows, so ConfigMap propagation never moves a boundary.

- **Metrics and forecast outages:** the `tcpproxy` routes refuse and cut connections during their windows.
- **Slow start:** decided by each server process at start.
- **Quota block:** the one live mutation. The `quota` command patches the PriorityClass-scoped ResourceQuota after checking the cluster UID; without `--apply` it is a dry run.

Faults and their limits.

- **Forecast route:** a TCP pass-through. The evaluation release dials it by name (`operator.forecasterAddress`), and that name is in the forecaster's certificate (`forecaster.tls.extraDNSNames`), so TLS verification stays on. The chart refuses an address whose host is not in the certificate. The fixture tests verify through the proxy; the smoke checks a forecast answer over the route.
- **H-CPU's CPU-metric outage is N/A, not tested (manifest section 12).** CPU HPA reads the resource-metrics API through the shared metrics-server, and the protocol never intervenes there. H-CPU runs the full 24-hour program, the 12:20 step included, with normal CPU telemetry. Its 12:00–14:00 window is an unperturbed control that stays in every total; it is never zero-filled and never counted as fault coverage.
  - **Request-metric arms:** the 12:15–12:30 RPS-query outage stays required for H-RPS, PA-R, PA-N and PA-E. The `rps_metric_fault` prerequisite and a per-block `metric_outage: verified` record are needed. Anything else keeps the study NOT READY or INCOMPLETE.
  - **Applicability declaration:** the run-lock and the study carry exactly the frozen declaration (`frozen.APPLICABILITY`), bound to the amended manifest's digest, and the cohort is locked before its first scored run. There is no `cpu_metric_fault` flag and no caller-supplied waiver.
  - **Comparisons:** the eight-test Holm family is unchanged, and H-CPU is in no inferential comparison.
  - **Disclosure:** every score carries this sentence: "CPU HPA metric-outage behavior was not tested; its corresponding interval retained normal CPU telemetry. Outage comparisons concern request-metric controllers only."
- **Quota fault:** the injector binds to the locked cluster UID, quota UID, run label and PriorityClass scope before patching (the patch carries the UID). It counts only the run's pods with the run's PriorityClass, so the ingress is never counted. Restore uses only the approved original limit recorded in the run-lock (`fault_scope.pods_normal`).
- **New certificate names need a rotation.** Adding `forecaster.tls.extraDNSNames` does not renew an existing self-signed certificate on upgrade: delete the forecaster TLS Secret and `helm upgrade` (docs/helm.md).

Other open items:

- The default queries are a starting point. They must be adapted to the observer (k6 remote-write series, kube-state-metrics) and frozen in the run-lock, and the collector then uses exactly those.
- The E1 policy-cost binding and the lab-qualification mechanism are implemented by C2.8a (#27). The study still records them as section 1 prerequisites, which are true only for the reviewed evaluation build of the pinned tree.
