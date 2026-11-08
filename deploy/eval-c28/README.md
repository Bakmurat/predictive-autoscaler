# C2.8 evaluation fixture and harness

Offline code for the C2.8 study (`agent-coordination/09-real-world-research/EVAL-C28-MANIFEST.md`, sections 3–9 and
the prodcluster adaptation in section 11). Nothing here runs an experiment, deploys to a lab cluster or produces C2.8
evidence by itself. The executor uses it under a locked run-lock and the change gates of manifest section 11.4.

| Path | What it is |
|---|---|
| `fixture/` | The HTTP fixture: one binary in three modes. `server` is the workload: fixed CPU work, a fixed response, a bounded queue, a measured startup delay and `/ready`, `/metrics`. `ingress` counts offered demand per target before the target can reject it, with a 2 s timeout and no retries. `tcpproxy` is the evaluation-owned metrics and forecast routes, with scheduled outages. `Dockerfile` builds a static, non-root image. |
| `load/c28.js` | The k6 open-loop generator (ramping-arrival-rate, one request per iteration), driven by a plan from `python -m c28 plan`. |
| `harness/c28/` | `render` (fixture, ingress, routes, quota and the five arms for both startup profiles, with an exclusive-writer audit, a sidecar-free audit and the residency budget), `schedule` (programs, k6 plans, fault timetable, the frozen session order), `runlock`, `collect` (hashed evidence bundle), `reduce` (per-window metrics), `score` (paired day-block bootstrap, Holm correction, the section 8 classification), `inject` (the guarded quota fault). `frozen.py` holds every value the manifest freezes. |
| `params/` | `kind-smoke.json` for the kind smoke. `prodcluster.template.json` lists what the run-lock must resolve; it cannot be rendered until every placeholder is replaced (the calibrated fixture resources and the W/S residency inputs included). |
| `smoke/kind.sh` | The aspire kind-bed smoke, on its own cluster and kubeconfig only. `--render-only` renders and audits offline; `scripts/verify.sh` runs that mode. |

```sh
cd deploy/eval-c28/harness
python -m c28 render --params ../params/kind-smoke.json --out /tmp/c28.yaml
python -m c28 plan --kind scored --seed 201 --capacity 35 --target c28-pa-n-s20 --out plan.json
python -m c28 order
python -m c28 runlock lock runlock.json && python -m c28 runlock verify runlock.json --manifest-root <dir>
python -m c28 release-values --params P --out release-values.yaml   # the evaluation release's routes and TLS name
python -m c28 residency --params P          # W/S maximum resident requests against 75 % of allocatable (exit 1 when over)
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

## Sidecar-free evaluation

The evaluation runs in prodcluster's `demo`, which has Istio sidecar injection enabled namespace-wide
(`istio-injection=enabled`; the injector webhook's objectSelector is `sidecar.istio.io/inject NotIn [false]`). That
policy is not changed. Instead every Pod template rendered for the evaluation carries `sidecar.istio.io/inject: "false"`
both as a pod **label** (the webhook's objectSelector then excludes the pod, so the API server never calls the injector)
and as a pod **annotation** (the injector's own per-pod opt-out), on the pod template's metadata, never only on the
workload's own metadata or in a selector. That covers the ten fixture Deployments, the ingress, the two fault routes
(`render`), the generator Jobs (`staging/session.py`) and the evaluation release's operator and forecaster (`podLabels`
and `podAnnotations` in `release-values` and in `prepare.py`'s staging overlay, which `values_problems` checks; the
chart gained `operator.podLabels` and `forecaster.podLabels`, which may never set its selector keys). `render` refuses a
rendering in which any Pod template lacks the label or the annotation (`audit_sidecar_free`); `session.py` audits its
generator Job the same way; `prepare.py verify` reports any admitted evaluation pod whose regular or init containers
include `istio-proxy`/`istio-init` (`render.unexpected_injection`, reusable for the live checks). A default sidecar
(100m CPU / 128Mi requested per pod) would otherwise make the workload node unschedulable.

**Claim limit.** Results concern this sidecar-free fixture, not mesh-enabled workloads. They are not directly comparable
with the old prodcluster benchmark, whose `demo` pods ran with sidecars.

## Fixture resources and the residency budget

The fixture's CPU/memory requests and limits are a calibrated, reviewed parameter (manifest section 3), identical for
every arm. They are never invented or lowered to make the budget fit. `validate` refuses a missing or placeholder value
(`<...>`) and a request above its limit. The prodcluster template keeps them as placeholders; the kind smoke keeps its
own values.

When the params carry a `residency` section, `render` computes the evaluation's maximum resident requests on each
reserved node and refuses when evaluation plus node-system requests exceed **75 % of allocatable** (the manifest's
>= 25 % headroom), CPU and memory checked separately, listing every contribution. The prodcluster staging helpers
(`execlib.load_params`) require the section; the kind smoke may omit it. `prepare.py` runs the complete residency and
sidecar audits over the whole evaluation (every phase's objects plus the staging overlay) before every phase's plan,
and so again when `apply` recomputes the plan: no phase is prepared while the evaluation as a whole would be refused.

| Field | Meaning |
|---|---|
| `residency.workload` / `residency.support` | `allocatable` {cpu, memory} from the fresh dated node inventory; `system_requests` {cpu, memory}: the summed requests of the node-level pods that stay on that node (DaemonSets, allowlisted agents). Required. |
| `overlap_pods` | Extra pods of the loaded arm counted on W for startup/termination overlap. Default 2 (= the frozen scale-down step of two pods: terminating pods keep their requests on the node until gone while scale-up, stabilization 0, already creates replacements); at least 1. |
| `idle_arm_replicas` | Replica count of every arm that is not loaded. Default 2, the fixed starting count every target is created and verified at (no parking). A higher, conservative value is allowed. A lower value is refused (see below). |
| `operator`, `forecaster` | Declared budgets on S. Defaults: the chart's operator request 100m/128Mi; the forecaster at 1 CPU/512Mi, which is also its floor (STAGING S2). The evaluation release requests exactly these (`release-values` and the staging overlay set `operator.resources` and `forecaster.statistical.resources`; limits below a request are raised to it). The accounting reads what the final chart values (values.yaml merged with the overlay) request, and refuses a chart pod requesting less than its budget; a test checks that model against `helm template`. |
| `generator` | `concurrent` generator Jobs (warm-up/history generators included) and the requests of each. Default one Job at 500m/256Mi (session.py's default). `session.py` refuses a generator that requests more. |
| `support_extra` | Every other evaluation pod on S (recorder, adapters): `[{name, replicas, cpu_request, memory_request}]`, or `[]`. |

How it is computed, from the rendered pod templates (each pinned to its role by required node affinity):

- **W:** nine idle arms × `idle_arm_replicas` + the loaded arm × maxReplicas (12) + `overlap_pods` of the loaded arm
  (the arm with the largest per-pod request, so the worst case) + the ingress at its replicas + W's system requests.
  One arm is loaded at a time (confirmation is sequential; `session.py` refuses a second open session or generator).
- **S:** the two fault routes + operator and forecaster (as the chart values request them) + `concurrent` generators +
  `support_extra` + S's system requests.

Example (W allocatable 4000m / 7.75Gi, system 400m / 512Mi, fixture 50m / 32Mi, ingress 100m / 64Mi, defaults):
CPU 9 × 2 × 50 + 12 × 50 + 2 × 50 + 100 + 400 = 2100m = 52.5 % (limit 3000m); memory 32 × 32Mi + 64Mi + 512Mi =
1600Mi = 20.2 % of 7936Mi. The same node with a 100m fixture needs 3700m = 92.5 % and is refused.

**Idle-arm parking is not implemented.** Parking inactive arms at fewer replicas between sessions would need their
writers stopped, the common starting count restored before each session, history collection preserved and the loaded
arm's frozen bounds unchanged. Nothing in the harness or `session.py` does that (sessions never scale a target, and a
stopped session leaves its target at whatever count its writer last set). So `idle_arm_replicas` below the starting
count is refused. `session.py` admits a session only when every other target has converged to its idle residency: desired
replicas at most `idle_arm_replicas`, status (observed generation, replicas, updated, ready, available) equal to it, and
exactly that many Pods owned through its ReplicaSets (bound by controller UID to the Deployment's UID), terminating Pods
included. A target left scaled up by an earlier session must be restored first. That restore (writer shutdown
confirmed, a UID/resourceVersion-protected scale, waiting until the excess Pods are gone) is not implemented yet and is
a hard prerequisite before S5.
