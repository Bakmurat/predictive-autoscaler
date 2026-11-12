# C2.8 evaluation fixture and harness

Offline code for the C2.8 study (`agent-coordination/09-real-world-research/EVAL-C28-MANIFEST.md`, sections 3–9 and
the prodcluster adaptation in section 11). Nothing here runs an experiment, deploys to a lab cluster or produces C2.8
evidence by itself. The executor uses it under a locked run-lock and the change gates of manifest section 11.4.

| Path | What it is |
|---|---|
| `fixture/` | The HTTP fixture: one binary in three modes. `server` is the workload: fixed CPU work, a fixed response, a bounded queue, a measured startup delay and `/ready`, `/metrics`. `ingress` counts offered demand per target before the target can reject it, with a 2 s timeout and no retries (a connection attempt to an upstream is bounded by the same timeout, so attempts to an address that never answers cannot pile up after their requests have ended); with `ROUTING=endpoints-rr` (what every evaluation and calibration rendering sets) it sends each request to the next READY pod of the target in a strict per-request round-robin, discovered by a namespaced LIST + WATCH of the target Services' EndpointSlices (`endpoints.go`: per-pod dispatch counters, endpoint-set generation and ready count, discovery errors, last-sync age and a stale flag; a stale discovery keeps the last known set and says so), instead of through the Service ClusterIP, where the cluster balances per kept-alive connection. `ROUTING=service` (the binary's default, no API access needed) is the earlier behaviour. `tcpproxy` is the evaluation-owned metrics and forecast routes, with scheduled outages. `Dockerfile` builds a static, non-root image. |
| `load/c28.js` | The k6 open-loop generator (ramping-arrival-rate, one request per iteration), driven by a plan from `python -m c28 plan`. |
| `load/c28-demo.js` | The demonstration's continuous generator (k6; a NEW script, the two above stay pinned): one single-VU scenario per target runs an event loop that times arrivals against wall-clock time from ONE recorded origin and sends them asynchronously (open loop, targets independent), follows a FINITE schedule of 28 predeclared days (one table row and one seed per day, never repeated or wrapped), resumes at the current position after any restart, and writes one JSON record per target and minute. See "The demonstration stage" below. |
| `load/c28-calibrate.js` | The k6 script of the S5a capacity calibration (`staging/calibrate.py`): settle and measure scenarios at one constant arrival rate, per-scenario counts and latencies in the pod's termination message. |
| `harness/c28/` | `render` (fixture, ingress with its namespaced EndpointSlice-reader ServiceAccount/Role/RoleBinding, routes, quota and the five arms for both startup profiles, with an exclusive-writer audit, a sidecar-free audit, an RBAC least-privilege audit and the residency budget), `schedule` (programs, k6 plans, fault timetable, the frozen session order), `runlock`, `collect` (hashed evidence bundle), `reduce` (per-window metrics), `score` (paired day-block bootstrap, Holm correction, the section 8 classification), `inject` (the guarded quota fault). `frozen.py` holds every value the manifest freezes. |
| `trace/` | PROPOSAL (draft contract v2, pending Codex review): the pre-declared selection and transformation of the Azure Functions 2019 trace (CC BY 4.0) for a separate public-trace demonstration. Not part of C2.8 scoring and no replacement for the manifest's ordinary warm-up. `azure_functions_2019.py select|prepare|plan|rules` (standard library, one file, run with `python -I`): each stage checks the reviewed sha256 of its input and records the hashes it was built from (source, selection, prepared, plan), validates schema, membership, length and transformation consistency, and never overwrites an output. `plan` emits exactly the `c28-k6-plan/v1` that `load/c28.js` accepts (kind `trace-demo`), one constant rate per trace minute, so each minute's scheduled count equals its intended count; `tests/c28js_loader.js` runs the real `c28.js` validator on it under node. Generated requests are "derived from per-minute invocation counts", never real arrival timestamps or real traffic. `k6_points.py` sums k6 counter samples into per-minute "metric-point buckets" anchored to a declared scenario start (emission/completion time, so not dispatch or ingress receipt times). It never runs a forecaster. |
| `params/` | `kind-demo.json` and `demonstration.template.json` for the demonstration stage (no `c0_rps`, HPA target or KEDA setting). `kind-smoke.json` for the kind smoke. `prodcluster.template.json` lists what the run-lock must resolve; it cannot be rendered until every placeholder is replaced (the calibrated fixture resources and the W/S residency inputs included). |
| `smoke/kind.sh` | The aspire kind-bed smoke, on its own cluster and kubeconfig only. `--render-only` renders and audits offline; `scripts/verify.sh` runs that mode. `--images images.json` runs the exact reviewed digests of a `c28-images/v1` file (pulled by digest, no build) and fails on any other running image. `--shadow` (with or without `--images`) installs the release as the S3 shadow install runs it (`operator.rbac.scaleWrite: false`, the forecast ledger on) and adds checks S1–S5: the live scale rule is `get` only; the API server's authorizer answers no to `update` and `patch` (yes to `get`) on `deployments/scale` for the operator's ServiceAccount, asked without a name and for the Deployment name of every PredictiveAutoscaler target (a grant restricted by `resourceNames` answers only the named question), an error counting as unproven, never as no; and no RoleBinding or ClusterRoleBinding of the cluster that names that ServiceAccount, its username or one of its groups, whatever its labels, resolves to a rule that may write a scale subresource. S1 then tests itself: a Role without the release's labels granting `update` on `deployments/scale` for one named target, bound to the operator's ServiceAccount, must make it refuse, and after the Role and RoleBinding are deleted it must pass again, all before any load. Under a second accelerated load a Recommend arm recommends at least 4 replicas while its Deployment keeps 2 replicas and its generation, with `ScalingActive=False/RecommendMode` (an unavailable forecast is expected and only reported); the operator log has no `deployments/scale` line at any time and no `forbidden` line up to the point before the quota check (check 6), after which the only `forbidden` lines allowed are the smoke's own quota denial as the operator's ScaleOutBlocked event quotes it (`pods … is forbidden: exceeded quota: c28-fixture-quota`), counted separately and reported, any other failing; the ledger advances and holds only `recommend` decisions; and at the end no autoscaler left Recommend or records a write. `--render-only --shadow` renders the chart with and without the shadow values (`helm template`, no cluster) and self-tests those checks' classifiers, the name-restricted outside grant included; `scripts/verify.sh` runs it where helm is installed (always in CI). |
| `smoke/demo-kind.sh` | The demonstration-stage smoke. `--render-only` (offline; `scripts/verify.sh` runs it): the params stage and its refusals, the rendering audits, and the real generator script under node through `demo-reduce`. `--k6-local`: the real k6 binary against a dummy server on 127.0.0.1. Without a flag: a throwaway kind cluster with its own kubeconfig. `--images images.json`: the full mode with the reviewed fixture and k6 images of a `c28-images/v1` record, pulled by digest on the nodes (no build, no local registry). `--replacement`: the generator's restart and replacement smoke (three consecutive planned restarts with their gaps, `kubectl delete pod` with its grace period, one target that stops answering with the generator's memory read from its cgroup, and the reducer over the continuously collected log of the whole smoke, with verdicts per phase); `smoke/demo_kind.py` holds its log follower and its analysis, self-tested offline by `--render-only`. |
| `smoke/calibrate-kind.sh` | The kind smoke of the calibration path (`--images images.json --out report.json`, own cluster and kubeconfig only, reviewed digests pulled on the nodes): non-root summary delivery through the termination message, the actual scenario timing, zero drops reconciled against the ingress's own counters, reported drops with starved VUs, refusal of truncated or malformed summaries, UID-bound stop, `activeDeadlineSeconds` expiry, cleanup and image identity (`calibrate_kind.py`). Required before any prod calibration; not C2.8 evidence. |

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
generator Job the same way. Any admitted evaluation pod whose regular or init containers include
`istio-proxy`/`istio-init` (`render.unexpected_injection` via `execlib.injection_problems`: the run's pods, the
release's pods and any pod controlled by the session's generator Job, replacements included) is a problem for
`prepare.py verify`, refuses a session start, and is a stop for every `session.py verify` and watchdog check, which
then takes the existing stop/unresolved path. A default sidecar
(100m CPU / 128Mi requested per pod) would otherwise make the workload node unschedulable.

**Claim limit.** Results concern this sidecar-free fixture, not mesh-enabled workloads. They are not directly comparable
with the old prodcluster benchmark, whose `demo` pods ran with sidecars.

## Params stages: reservation before calibration

The full c28-params/v1 file can only be completed after capacity calibration on the reserved W (`c0_rps`, fixture
resources, `work_iterations`, the HPA target, routes, ...), so the S2 reservation validates only the fields it
consumes: `render.validate_stage(params, "reservation")` (`execlib.load_params_stage`) checks `schema`, `run_id`,
`nodes.workload`/`nodes.support`, `reservation.taint_key`/`taint_value`/`role_label` and `control_namespace` +
`release` (occupant classification on S) with the full validation's strictness (no placeholders, DNS names,
qualified label keys, the taint value equal to the run id); every other field may still be a placeholder.
`reserve.py` plan/apply/rollback-plan/rollback/verify use it, so rollback never needs calibrated parameters.
Passing a stage validation says nothing about readiness: `render.full_validation_problems(params)` (and
`execlib.experiment_params_problems`, which adds the residency requirement) list why the params are incomplete. `reserve.py
verify` reports `reservation: PASS|FAIL`, `placement: PASS|FAIL|NOT READY` (the rendered placement is checked only
on complete params), `ready_for_experiment` and `experiment_readiness`, which follows the combined verdict and names
every failing part (`reservation FAIL (...)`, `params incomplete (...)`, `rendered placement FAIL (...)`); exit 0
ready, 3 reserved but not ready, 1 fail; its `problems` include the NOT READY item. `validate` (the full validation,
used by `load_params` in prepare, freeze and session) includes the reservation stage and refuses any remaining
placeholder anywhere in the file.

Two gates read that verdict. `reserve.require_ready_for_experiment` needs the complete experiment parameters on a
passing reservation: `prepare.py` requires it live in every phase's plan and again in apply's recomputation under the
shared lock, so a reservation lost between review and apply creates nothing. It is NOT the calibration gate (it needs
the calibrated values themselves, which would recreate the c0/HPA-target cycle). Capacity calibration on W
(`staging/calibrate.py`, below) needs `reserve.require_reservation_pass` (the reservation part alone) plus its own
separately validated calibration contract and admission checks. `residency` stays outside the reservation stage and
mandatory for workload admission (`load_params`, prepare's residency audit).

## The metrics route's query path

The metrics route is a plain TCP proxy to `routes.metrics_upstream`, so the upstream's Prometheus-API prefix travels in
the URL: `routes.metrics_query_path` is required, `/select/0/prometheus` for the cluster VictoriaMetrics (vmselect,
tenant 0), `/` for a non-tenant backend (the kind smoke's single-node VictoriaMetrics). It must start with `/` and has
no query string, fragment, `..`, empty segment or trailing slash (except the root). `render.metrics_query_url` builds
the one URL the evaluation release's `prometheus.url` and KEDA's `serverAddress` use (the bare route for `/`); the
operator appends `/api/v1/query`, the forecaster `/api/v1/query_range`. `keda.server_address` must equal it exactly.

## Fixture resources and the residency budget

The fixture's CPU/memory requests and limits are a calibrated, reviewed parameter (manifest section 3), identical for
every arm. They are never invented or lowered to make the budget fit. `validate` refuses a missing or placeholder value
(`<...>` or `__...__`) and a request above its limit. The prodcluster template keeps them as placeholders; the kind smoke keeps its
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

**Idle-arm parking below the starting count is not implemented**, so `idle_arm_replicas` below the starting count is
refused (that would need the writers stopped, the count restored before each session, history preserved and the loaded
arm's frozen bounds unchanged). `session.py` admits a session only when EVERY target, the selected one included, exists
with its recorded UID, sits at exactly the common starting count and has converged there (`idle_arm_replicas` is only
the budget's upper bound): desired replicas equal to the count, status (observed generation, replicas, updated, ready,
available) equal to it, and exactly that many Pods owned through its ReplicaSets (bound by controller UID to the
Deployment's UID), none of them terminating and every one Running and Ready. A session never scales a target, and a
stopped session leaves its target at whatever count its writer last set; the start refusal names those targets and
points to the restore.

## The demonstration stage

"Predictive scaling on live synthetic diurnal traffic" (`LIVE-DEMO-PLAN.md` sections 5 and 6): two fixture targets
under their own run id, fed the same finite schedule (28 days of the ordinary diurnal program, each day with its own
predeclared seed) by one continuous generator. It shows a running system.
It is not real business demand, not a comparison result, and it earns no confirmation or qualification credit for the
study. It is a stage of these tools (`harness/c28/demo.py`, `demo_reduce.py`, `load/c28-demo.js`,
`staging/prepare.py` with `prepare.stage: "demonstration"`), with no experiment-only field and no dummy value.

**Not in this stage:** the Active transition, the demonstration qualification and its mount, the chart's
`operator.rbac.scaleTargets`, the dashboard, and the admission exercise's executor. Every PredictiveAutoscaler is
rendered in Recommend and the release overlay withholds the scale write.

### Params stage `demonstration`

`render.validate_stage(params, "demonstration")` (`python -m c28 validate --params P --stage demonstration`) checks
exactly `render.STAGE_FIELDS["demonstration"]` with the full validation's strictness: schema, run id, namespaces,
release, the fixture image and its calibrated resources, W and S, the reservation keys, ingress and proxy resources,
routes and query path, priority class and quota, the scrape kind, `release_network_policies`, `residency` (required),
and the `demonstration` block. It never reads `c0_rps`, `hpa_cpu_target_percent` or `keda.*`
(`render.EXPERIMENT_ONLY`): they may be absent or still placeholders. Passing it says nothing about the comparison
experiment (`full_validation_problems`).

| Field of `demonstration` | Rule |
|---|---|
| `run_id` | The params' `run_id` (objects, receipts and journals use it; `c28.devkuban.com/run` carries it). |
| `reservation_ref` | `{run_id, taint_value, nodes: {workload: {name, uid}, support: {name, uid}}}`: the reservation's OWNER. `taint_value` is the owner's run id and equals `reservation.taint_value`; the node names equal `nodes.*`; the UIDs are UUIDs and differ. The stage never retags a node. |
| `targets` | Exactly two, exactly the roles `forecast` and `reactive`: `{name, role, families}` and `{name, role}`. Names are `c28-*` DNS labels, different, and never a comparison arm's or a run-wide object's name. `families` must be `["seasonalNaive"]`; the reactive reference runs the same families. |
| `profile` | `"s20"`: one startup profile. |
| `bounds`, `shadow_replicas` | Bounds exactly `{min: 1, max: 12}`; the fixed shadow replica count an integer within them. |
| `capacity_per_replica_rps`, `capacity_label`, `capacity_source_calibration` | A positive number (the PredictiveAutoscaler's `capacityPerReplica`, already 0.875 x the reviewed rate), the literal `"provisional"`, and the calibration id it comes from (`c28cal-<10 hex>`). |
| `cadence` | Exactly `{grid_seconds: 60, update_interval_seconds: 60}`. Any other combination is refused, and the rendering is checked against it. |
| `min_lead_seconds` | Exactly `600`: with the cadence, risk 1000 bps and `["seasonalNaive"]` this is the ONE forecast profile that was qualified (`ml-engine/forecasting/contracts.py` `VALIDATED_PROFILES`). Any other lead is refused. It is rendered as `spec.prediction.minLeadSeconds`; without it the s20 startup profile would let the operator select a 180 s lead, which was never qualified. |
| `schedule_origin_utc` | ONE UTC midnight (`YYYY-MM-DDT00:00:00Z`), set once at the start of history. Every reader bins from it. |
| `peak_multiple` | `9`, the ordinary program's peak in units of c. |
| `program` | `{kind: "ordinary", seed, days}`: a FINITE schedule of `days` whole UTC days from the origin (an integer 1..28; exactly 28 for a demonstration, fewer only render as a smoke). `seed` is the BASE seed: day d (0-based) is `schedule.program("ordinary", seed + d)`, so the seeds are consecutive and predeclared, and none of them (`seed` … `seed + days − 1`) may be a tuning or confirmation seed of the comparison. No day repeats: every day has its own ±5 % per-minute perturbation. After the last day the generator sends nothing. |
| `generator` | `{image, k6_version, resources, segment_hours, remote_write_url}`: the reviewed k6 image by digest (`sha256:70af91f8…`, any mirror repository), `0.57.0`, requests at most limits, a whole number of days in hours (24 … 672), an http(s) URL without user-info. The memory limit must cover the estimate of `demo-sizing`. |

Unknown fields are refused; so is any `<...>` or `__...__` placeholder in a consumed field.

The stage also sets a floor for the ingress (`demo.ingress_problems`): `ingress.memory_limit` at least 128Mi, and
`ingress.memory_request` equal to it, so that the residency count (which reads requests) holds what the limit allows.
Both targets' traffic passes through that one container, so if it is killed both lose requests. On kind
(2026-11-11) an ingress with bounded dials peaked at 41.3 MiB with one target unreachable at the schedule's peak
rate; two unreachable targets and longer faults were not measured. The floor is a rule for admission, not evidence
that 128Mi is enough: that is the fault test on the final images. `params/kind-demo.json` uses the floor, and the
replacement smoke no longer raises the ingress limit on its own (`DEMO_INGRESS_MEMORY` overrides it for a
comparison run).

`demo.active_values_problems(values, params)` validates the scale-write part of the release's chart values for both
configurations and produces neither: shadow (`scaleWrite: false`) may name no `scaleTargets`; Active
(`scaleWrite: true`) must name exactly the two targets, and an empty or missing list (the chart's namespace-wide
default) is refused.

### What is rendered

`python -m c28 render --params P --stage demonstration [--part common|targets|pas|generator]`. Every name is `c28-*`
and every object and pod carries `c28.devkuban.com/run=<the demonstration's run id>`; pods tolerate the OWNER's taint.

| Phase | Objects |
|---|---|
| common | PriorityClass and scoped ResourceQuota (the params' names), the ingress with its ServiceAccount, Role and RoleBinding (routing `/t/<target>/` to the two targets only), `c28-metrics-route` and `c28-forecast-route` with their EMPTY fault ConfigMaps, the route NetworkPolicies when the release enforces them, the scrape object (`render.common_objects`). |
| targets | Per target an EMPTY fault ConfigMap, the Deployment at `shadow_replicas` and the Service, exactly as the s20 arms are rendered (`render.target_objects`: image by digest, the calibrated resources, probes, placement). |
| pas | Two PredictiveAutoscalers in Recommend through the arms' own rendering (`render.pa_object`), pinned to the ONE qualified forecast profile: engine statistical, `sampleIntervalSeconds: 60`, `updateIntervalSeconds: 60`, `minLeadSeconds: 600`, `families: ["seasonalNaive"]`, risk 1000 bps, settlement 120 s, outcome grace 600 s, and NO `calibration` or `trust` block (the defaults apply; `demo.qualified_prediction_problems` refuses anything else at render time). A test resolves the rendered objects with the forecaster's own `contracts.resolve_settings` and requires `contracts.validated_profile(settings, 600)` to be the allowlist entry. The reactive reference is the same object with `predictiveExtra` an explicit `{maxReplicas: 0, maxPercent: 0}`, as PA-R. |
| generator | ConfigMap `c28-demo-generator` (the script and its configuration), a headless Service and StatefulSet `c28-demo-generator` with ONE replica, on S, `k6 run` of the reviewed image. Its only pod is `c28-demo-generator-0`. |

`python -m c28 residency --params P --stage demonstration` is the stage's admission audit, on
`render.residency_report`'s machinery and the same 75 % rule: on W both targets at `maxReplicas` plus `overlap_pods`
each and the ingress; on S the two routes, the rendered generator, and the operator and forecaster as the release's
final chart values request them; each node's system requests on top.

### `prepare.py` for the stage

The record's `prepare` section gets `"stage": "demonstration"` and `"reservation": {"record", "sha256"}`, the
reservation OWNER's preparation record. The phases are the experiment's six (`namespaces`, `storage`, `values`,
`common`, `targets`, `pas`) plus `generator`. Every existing refusal stays: an existing name (cluster-scoped ones
included), a name outside `c28-*`, a writer that references a target, a PredictiveAutoscaler not in Recommend, a
non-empty fault schedule, the residency audit and the reservation gate in every plan and again under the lock at apply.

- **The reservation is another run's.** `demonstration_gate` loads the owner's record (bound by sha256) and checks the
  reference against it: owner run id, taint key and value, role label, both node names and UIDs, the same cluster and
  lock directory, the same release and control namespace. Then it requires the owner's LIVE verdict with the
  demonstration as an authorized occupant (`reserve.verify(owner, occupant={run_id, namespaces})`: a pod with the
  demonstration's run label in its namespaces classifies as `authorized-occupant`; any other pod is still unplanned)
  and the placement of the demonstration's rendered pods. Without that reference, reserve.py's own `verify`,
  calibration and sessions see the demonstration's pods as unplanned occupants, as before.
- **Values.** The overlay as before (`operator.rbac.scaleWrite: false`, no lab qualification), plus the release's own
  scrape object from `params.scrape`: `VMPodScrape` → `metrics.vmServiceScrape.enabled: true`, `PodMonitor` →
  `metrics.serviceMonitor.enabled: true`, `annotations` refused for the release. `values_problems` requires exactly
  one. Stage `experiment` writes the overlay it wrote before, unchanged.
- **Generator.** Created only after `targets` and `pas` (this run's objects, same UIDs, both PAs in Recommend), with the
  schedule origin not in the future and the finite schedule not yet over (an expired schedule is refused: the
  generator would send nothing), and only when no other Deployment, StatefulSet or Pod carries the generator's name
  label (strict listings: a failed list is never "no generator"). The clock is a parameter of `plan`, `apply` and
  `main` (`now`), so tests never read the real one. `verify` checks the fixed shadow count, that the generator is
  still a StatefulSet with one replica, that at most ONE generator pod may be running and that it is the
  StatefulSet's own (`c28-demo-generator-0`, controlled by it). A TERMINATING pod counts as possibly running (a
  deletion timestamp only asks it to stop); only phase Succeeded or Failed is not a sender.

### The continuous generator (`load/c28-demo.js`)

- **Program: a finite schedule of 28 days.** `python -m c28 demo-plan --params P` writes its configuration
  (`c28-demo-generator/v2`): the origin, `days`, the seed of every day (`seeds`, consecutive from `program.seed`) and
  `per_minute_days`, one row per day with the intended arrivals of each of its 1,440 minutes,
  `round(u × capacity_per_replica_rps × 60)` of that day's own ordinary program, the same for every target. Python is
  the only source of the numbers: the script reads the table and never draws a random number. Minute
  m = floor((t − origin) / 60 s), day d = floor(m / 1440) (whole UTC days from the one origin), count
  `per_minute_days[d][m − 1440 d]`. The day comes from the clock and the origin, so a restart never changes it.
  The configuration's digest covers the whole table, the seed list, the origin and the schema; the script's
  `checkConfig` refuses a table whose day count, any day's length (1,440) or any count's range is wrong, a seed list of
  another length, or a schedule length that is not days × 1,440 minutes.
- **Deterministic spacing, not Poisson.** Inside a minute the arrivals are EVENLY SPACED (arrival j at
  `floor((j + phase) × 60 s / n)`, the targets interleaved by a phase); nothing inside a minute is random. The
  day-to-day variation is the per-minute count: each day's row carries its own seeded ±5 % perturbation of the
  diurnal shape.
- **Before the origin and after the last day.** The schedule never wraps. Before the origin nothing is sent: an
  incarnation that starts at most 10 minutes early says so in its start record (`phase: "pre-origin"`), waits and
  begins with minute 0; an earlier one only idles and exits 0 five minutes before the origin. At the schedule's end
  (origin + days × 1,440 minutes) the generator stops scheduling and writes ONE record (`event: "exhausted"`,
  `reason: "schedule-exhausted"`); an incarnation that starts after the end writes its start record
  (`phase: "schedule-exhausted"`) and that record, sends nothing, idles to its segment end and exits 0 (no hot loop).
  There is no per-minute line before the origin or after the end: intended is undefined there, not zero.
- **Smoke-only origin.** A configuration labelled `smoke` (`generator_config(..., smoke_segment_minutes=N,
  smoke_origin_ms=T)`, on the command line `--smoke-segment-minutes N --smoke-origin-utc YYYY-MM-DDTHH:MM:00Z`) may
  start on any whole minute, so a smoke can put a day boundary of the table a few minutes ahead of its start, or
  sit at the daily peak whatever the wall clock says. A configuration labelled `demonstration` always has a UTC-midnight origin and exactly 28 days;
  Python and the script both refuse anything else.
- **Open loop, independent targets.** One k6 scenario per target with ONE virtual user, whose single iteration runs
  an event loop: arrivals are timed by `setTimeout` against wall-clock time and sent with `http.asyncRequest`, so no
  arrival waits for a response and a slow or failing target cannot delay the other (separate VUs and event loops).
  The script, not a k6 arrival-rate executor, schedules the arrivals: per-minute accounting needs state shared by all
  of a target's requests, which k6's VUs do not have, and the VU count would otherwise grow with rate × timeout.
- **Resume, no replay, no reset.** The position is a pure function of wall-clock time and the origin. A process that
  starts at any time sends only what is scheduled from then on. An arrival more than 1 s late (a stalled process, a
  forward clock step) is dropped with reason `late`, never sent in a burst (a whole skipped minute is accounted in
  one step); one that would exceed `max_inflight` requests in flight is dropped with reason `inflight`; and one that
  is still owed when the incarnation's span is over (the process was delayed across its segment end or the
  schedule's end) is dropped with reason `segment-end`: nothing is ever sent at or after the end of the span, so
  every request an incarnation sends is in one of the minute lines it writes. Nothing is retried or caught up. The position only moves forward: after a backward clock step the generator waits until its
  clock reaches the next arrival again and never sends an arrival twice.
- **Segments and incarnations.** One k6 process is one incarnation, id `<pod UID>/<start, epoch ms>`. It runs to the
  next multiple of `segment_hours` from the origin (a UTC midnight, the program's lowest demand; the one after it when
  closer than 5 minutes), writes its end record, and k6 exits 0; the kubelet restarts the container and the next
  incarnation resumes. That bounds anything that could grow with run time.
- **Single generator: at most one pod.** A StatefulSet with one replica. Its only pod is always
  `c28-demo-generator-0`, and the API server never holds two pods of one name: a replacement (an update, a deleted or
  evicted pod, a drained node) is created only after the old pod object is gone. A Deployment does not give that:
  strategy Recreate orders an update, but when a pod is merely deleted the ReplicaSet creates its replacement at
  once while the old pod may still run for its termination grace period. The price is availability, the right side
  to err on: with S unreachable no replacement starts until the old pod is confirmed gone. The planned restart at a
  segment end is the kubelet restarting the container inside the same pod. `prepare.py` refuses a second generator
  and `verify` counts every pod that may still run, terminating ones included. Detection does not rely on that: every record
  names its incarnation, records of two incarnations whose active spans overlap in a minute are
  `duplicate_generators` (the minute is unknown and listed in the reduction's `duplicate_generators`, never summed
  silently), and a generator whose log is missing shows as received ≫ dispatched (`unreconciled`).
- **Limits (review finding R2-18).** `python -m c28 demo-sizing --params P` states the arithmetic: 3 VUs (one per
  target and k6's setup VU), `max_inflight = ceil(peak arrivals per second × 2 s × 1.5) + 10` per target, and a memory
  ESTIMATE from assumed constants (128 MiB base, 16 MiB per VU, 128 KiB per request in flight, 64 B per table entry
  and VU) that the memory limit must cover. The peak is the MAXIMUM minute of the WHOLE table (all days, each day's
  perturbation included; the report names the day and its seed), never one day's. For 17.5 rps per replica the peak
  is about 165 arrivals per second per target, `max_inflight` 505, the estimate about 310 MiB against a 512 MiB
  limit. The constants are assumptions, not measurements. One measurement exists (2026-11-11, real k6 under docker
  with a 512 MiB limit, a local stand-in server, every minute at the peak of 9,898 arrivals per target, 330 per
  second in total, `docker stats` every 5 s over two 2-minute incarnations): at most 25.7 MiB. The stand-in answers
  in about 5 ms, so few requests were in flight and no remote-write output ran: it is a floor for a healthy target,
  not a worst case, and the estimate stays the admission rule. The kind smoke's full mode prints the generator's
  `memory.current`. The ConfigMap carries the script and the whole table (about 0.2 MiB for
  28 days); rendering refuses more than three quarters of the 1 MiB limit, and the kind smoke applies it server-side
  (a client-side apply would copy it into an annotation limited to 256 KiB).
- **A failed request is a record, not a log line.** The script sets k6's `throw` option: a request without an HTTP
  response (a timeout, a refused or cut connection) rejects its promise and is recorded as status 0, exactly as
  before, but k6 no longer writes a "Request Failed" warning for each. On the kind bed one target that did not answer
  produced 16,683 such lines in two minutes; at the peak rate that is about 9 kB of log per second, enough to
  rotate the container's log within minutes and take the minute lines with it.
- **The configuration that runs is the one reviewed.** At start the script recomputes the configuration's digest
  from what it parsed, exactly as `schedule.digest` does (SHA-256 over the JSON with sorted keys, separators `,` and
  `:`, non-ASCII escaped, the `digest` key left out; `k6/crypto`), and refuses to start on a mismatch: a table edited
  after rendering cannot run under its old digest. The configuration holds only integers, strings, booleans, arrays
  and objects (the per-replica capacity is its decimal string): Python and JavaScript write floats differently, so
  both sides refuse them. The start record carries `config_schema` and `config_digest`.
- **Records.** One JSON line per target and minute (`c28-demo-minute/v3`, under 800 bytes, with the minute's `day`
  and `day_minute`), written 2.5 s after the minute ends, when every request dispatched in it has its outcome; a
  start and an end record (`c28-demo-incarnation/v2`) frame each incarnation (the start record names its `phase` and
  the span it will schedule), and the `exhausted` record marks the schedule's end. Two targets write at most
  2 × 1,440 × 800 B ≈ 2.3 MiB a day, 64 MiB in 28 days. k6 writes `console.log` to STDERR, so the records are on the
  process's stderr; the pod's log carries both streams and does not depend on remote-write delivery. **The API serves only the
  current and the previous container's log**, and each segment is a new container: with 24-hour segments the logs
  must be collected at least daily (or kept by the cluster's log pipeline).
- **Remote write** (dashboards only): counters `c28d_scheduled`, `c28d_dispatched`, `c28d_dropped{reason}` (`late`, `inflight`, `segment-end`) and
  `c28d_completed{outcome}` per `c28_target`, tagged `c28_run` and `c28_pod`. `c28d_completed` and k6's own
  `http_reqs` are stamped at completion and are never dispatch counts. A missing series is unknown, never zero.
- **Clock.** Each minute's record carries the `Date` header of its response with the SHORTEST round trip, with the
  local receive time and that round trip: the tightest evidence the minute has of the offset between the generator's
  and the ingress's clocks. The schedule is never corrected by it.

| Count | Timestamp it uses |
|---|---|
| `intended` | none: the plan's count for the minute |
| `due` | the arrival's scheduled time, inside the incarnation's active span (`active: [from, to]` ms of the minute) |
| `dispatched` | the generator's clock immediately before the send (dispatch time) |
| `carried` | `[in, out]`: dispatched in this minute but scheduled in the previous one / due here but dispatched in the next |
| `dropped.late`, `dropped.inflight`, `dropped.segment_end` | the scheduled time of an arrival the generator did not send |
| `ok`, `failed`, `failed_by`, `unresolved` | the outcome of the requests dispatched in the minute (never the completion time); status `0` is no HTTP response |
| received (reducer) | the ingress's clock at arrival (`c28_ingress_requests_total`) |

Every line satisfies `due = dispatched − carried[0] + carried[1] + dropped.late + dropped.inflight +
dropped.segment_end`; a carry never leaves the incarnation (a line's carry-out is the next line's carry-in, the first
line carries nothing in, the last nothing out); and
`dispatched = ok + failed + unresolved`.

### The reducer (`python -m c28 demo-reduce`)

`demo_reduce.reduce(config, records, ingress, start, end)` returns per-minute rows of every target, binned from the
configuration's recorded origin and nothing else, so a late or restarted generator costs only the minutes it did not
cover (review finding R2-01). Every record's `day`, `day_minute` and `intended` are checked against the table entry
of that ABSOLUTE minute (`per_minute_days[m // 1440][m % 1440]`), never against one repeated day; the CLI refuses a
configuration whose digest, table shape, seed list or schedule length is not intact. Its inputs are the pod logs (any number of pulls; a line read twice is dropped; k6's
raw, JSON and text log formats and `kubectl logs --timestamps` are read) and the ingress's RAW counter samples with
their own timestamps, `{"<target>": [[t_seconds, value], ...]}` (a range-vector query such as
`c28_ingress_requests_total{target="…"}[1d]` or an export; a stepped range query would hide scrape gaps, R2-19).
`reduce.py` and the study's scoring are unchanged.

| State | Meaning |
|---|---|
| `observed` | One incarnation covered the whole minute; that incarnation has a valid start record bound to this configuration; its record is self-consistent, equals the table and accounts for every arrival the schedule places in the minute (`due` is exactly that number); the ingress count is known; the WHOLE interval the clock evidence allows lies inside the limit; and the running balance reconciles. |
| `partial` | Trustworthy but not a whole, fully observed minute: an incarnation started or ended inside it (`incarnation_boundary`, with the exact active span and counts), the generator was provably absent between an end record and the next start (`generator_gap`), or the ingress count is unknown (`ingress_gap`, `ingress_reset`, `ingress_absent`). |
| `unscheduled` | The minute is outside the finite schedule (`before_origin`, `schedule_exhausted`) and no generator line claims it: `intended` is null (undefined, never zero), nothing is judged. A line that claims such a minute is a `plan_mismatch`. |
| `unknown` | The generator's side is missing or cannot be trusted: no record yet for the range's first minutes (`before_first_record`), a killed incarnation's unrecorded tail within the span it declared at its start, up to the next start (`unrecorded_tail`), a lost line (`generator_record_missing`), two incarnations whose active spans overlap in the minute (`duplicate_generators`, also listed per target in the output's `duplicate_generators`), `inconsistent_line` (also a line outside the span its start record declared), `plan_mismatch`, `config_mismatch`, `conflicting_lines`, `generator_counter_reset`, `unresolved_outcomes`, no valid start record for the incarnation (`start_record_missing`, `start_record_invalid`), a `due` that is not the schedule's arrivals in the active span (`due_mismatch`), a carry without its counterpart in the same incarnation (`carry_unmatched`), a proven clock offset (`clock_offset`), clock evidence that is missing for a contributing incarnation, contradicts itself or only overlaps the limit (`clock_unverified`), or a balance that does not reconcile (`unreconciled`). Never zero, never healthy. |

- The ingress count of a minute is the counter's difference between its two boundaries, each estimated from the
  adjacent raw samples (`received`, with hard `received_bounds` and a stated `received_err`); two missed scrapes
  (more than 35 s between samples) are a gap, a decrease is a reset, no series is `ingress_absent`.
- Received and dispatched are not required to match inside a minute. A running balance of dispatched minus received
  may hold what can be in flight (the timeout plus 1.1 s of arrivals, plus two) and the ingress estimate's error;
  requests without any HTTP response explain a positive imbalance up to their number. Beyond that the minute is
  `unreconciled` and the balance restarts.
- **Provenance.** A minute line counts only under its incarnation's START record, and only when that record is this
  configuration's: run id, `config_schema`, `config_digest`, origin, schedule end, day count, a segment end that
  follows from its start time by the script's rule, an incarnation id of `<pod UID>/<start>`, and a declared span
  that follows from those. Each line must lie in that span with exactly the active part it gives the minute. With
  the incarnation records removed, no minute is observed.
- **Accounting.** In its active part of a minute an incarnation owes every arrival the schedule places there: the
  table row's count, the target's phase and the span give the number exactly (`demo_reduce.scheduled_arrivals`, the
  script's own arithmetic), and `due` must equal it. A complete minute that reports due = dispatched = dropped = 0
  against an intended count is `due_mismatch`, not observed, however flat the ingress.
- **Conservation.** A carry-out must be matched by the carry-in of the following minute of the SAME incarnation
  (and a carry-in by the previous line's carry-out). An unmatched carry is `carry_unmatched` and neither minute is
  observed: it means requests were dispatched that no line records. The script cannot produce one (it never sends
  at or after the end of its span and records what it still owes there as `dropped.segment_end`); the records of the
  script before that fix (a final row with `carried: [0, 2]`) are caught by this rule.
- **Clock.** The evidence of a minute bounds (generator clock − ingress clock) to an interval at least one second
  wide (the `Date` header has whole seconds). The minute is trusted only when the WHOLE interval lies inside the limit
  (default ±2 s, recorded in the output) for EVERY contributing incarnation: bounds of [−2500, 500] are
  `clock_unverified`, an interval entirely outside is `clock_offset`, and an incarnation without evidence of its own
  (its sample of the minute or within five minutes) or with samples that cannot both be true is `clock_unverified`.
- The newest minutes of a running incarnation are `unrecorded_tail` until their lines exist: end the range at least
  three minutes before the log was pulled.
- Summary totals cover observed minutes only. What is missing is never zero intended traffic: a scheduled minute
  always carries the table's intended count. In a restart gap the DELIVERY is missing (`generator_gap`: dispatched 0
  against that count); with a lost line the EVIDENCE is missing (`unknown`). The summary states
  `scheduled_intended` (all scheduled minutes of the range) and `unobserved_intended` (those not observed).

### Smoke and what was run

`smoke/demo-kind.sh --render-only` runs in `scripts/verify.sh`. The generator's logic (position in the schedule, the
day change inside an incarnation and across a restart, the start before the origin, the schedule's end, forward and
backward clock steps, arrival times, resume, segments, bookkeeping, drops) and the reducer are tested on the real
script under node with a virtual clock (`harness/tests/demo_sim.js`). k6 itself ran this script only in local
accounting smokes without a cluster (the reviewed k6 image under docker against a counting stand-in server on
127.0.0.1, 2026-11-11): unequal adjacent days with a restart across their boundary and with the boundary inside one
incarnation (both with the script as it was before the digest check), and, with the present script, every minute at
the peak rate with the memory sampled, plus an altered table under its old digest, which k6 refused at start
(`k6/crypto`). In all of them the per-minute dispatched counts equal the server's own counts, with no drops. That
exercises k6's event loop, `http.asyncRequest`, `k6/timers`, `k6/crypto`, the stderr record stream and the exit
status at a segment end; it is functionality, not evidence. **Not run for the 28-day schedule:**
`demo-kind.sh --k6-local` (needs k6 on PATH) and the full mode on a throwaway kind cluster. Nothing on a cluster has
therefore exercised the default full mode's own checks. The generator's restart and replacement were run on kind
(next paragraph).

**On a kind cluster (2026-11-11, `demo-kind.sh --replacement --images`, the laptop bed; functionality, not
evidence).** The reviewed fixture and k6 images by digest, no operator and no forecaster, the schedule placed at its
daily peak with 17.5 rps per replica (about 157 arrivals a second per target), 3-minute smoke segments, remote write
into an in-cluster VictoriaMetrics.
- *Apply.* The stage's 24 objects applied; the generator's ConfigMap (234 kB as JSON) through server-side apply; the
  StatefulSet with one replica and its headless Service (no port) were accepted and the pod `c28-demo-generator-0`
  ran on S.
- *Planned restarts.* Seven in the run, every one exit 0 (`Completed`) in the same pod, each next incarnation
  resuming exactly at its start with nothing replayed. The gap from the segment end to the first scheduled arrival
  of the next incarnation was 4.0 s, 15.3 s and 26.5 s in the first pod and 4.6 s, 15.4 s, 33.4 s and 51.5 s in
  the second: the old incarnation waits 2.5 s for its last outcomes before it exits; the first restart of a pod
  then takes 1.5 to 2.1 s (the kubelet restarting the container, k6 starting); every further one also waits for the
  kubelet's restart back-off (10 s, 20 s, 40 s, ... up to 5 minutes),
  which is only reset after a container has run for 10 minutes. With the smoke's 3-minute segments it never is;
  with the demonstration's 24-hour segments every restart should be a first one, which this smoke does not show.
- *Pod deletion with its grace period (never forced).* The old pod object was gone 1.1 s after the delete (k6 stops
  at once on SIGTERM, so the 15 s grace period was not used up), the replacement was created 24 ms after that and
  ran 2.1 s after the delete; never two pod objects at once (watch events and polls). The reducer shows no
  `duplicate_generators`; the minute of the deletion is `unknown` (`unrecorded_tail`: the killed incarnation wrote
  no line for it), the next ones `partial` (`incarnation_boundary`) and `observed`.
- *One target not answering, remote write on.* Its pods' responses were dropped for 110 s. In the whole minute inside
  that span all 9,429 requests to it failed (9,133 at the generator's own 2 s timeout, 296 as the ingress's 504),
  314 requests in flight on average against `max_inflight` 505, none dropped; the other target was served
  completely (9,429 of 9,429, dispatch lateness under 10 ms). The generator container's memory (cgroup `memory.current` and
  `memory.peak` on the node) reached 50.2 MB (47.9 MiB), 9.4 % of its 512 MiB limit; about 26 MB otherwise.
- *Reducer over the whole smoke* (19 minutes per target, the log collected continuously): 11 `observed`, 7
  `partial` (`incarnation_boundary` at each restart, `ingress_gap` in the first minute of the series), 1 `unknown`
  (the deletion); no forbidden reason, every incarnation with a valid start record, no lost line.
- *What a pass needs to have seen* (`demo_kind.py`, `evidence_problems`; added after review, the run above passes
  it unchanged). Missing evidence fails instead of reading as "nothing wrong": every phase ran; in the steady and
  recovery phases each target has an `observed` minute with served requests; in the timeout phase the unanswered
  target has failed requests and the other target an `observed`, served minute; the memory limit is known and the
  timeout phase holds at least 10 memory samples above 0. With the run's memory samples removed, or its ingress
  evidence replaced by `{}`, the analysis now fails (both passed before).
- *Seen in the first run and changed.* k6's per-request warnings (above). And the fixture's INGRESS container was
  restarted about a minute after the packets TO a target's pods were dropped (a network blackhole); its exit reason
  was not captured. The smoke's default drops the target's responses instead (the ingress stayed up).
- *Blackhole direction and the ingress's own verdict.* `DEMO_BLACKHOLE=requests` makes the timeout phase drop the
  packets TO the target's pods again (the SYN never arrives; `responses`, the default, is the direction above). The
  follower samples the ingress container's cgroup on the workload node once a second (`memory.current`,
  `memory.peak`, `memory.max`, the `oom_kill` counter of `memory.events`, and the pod cgroup's, which outlives a
  restarted container), and the smoke keeps the ingress pod's statuses (before the fault, right after it and after
  the recovery), previous log, events and description. A pass now also needs, as positive evidence: the ingress
  status AFTER THE RECOVERY with a restart count of 0; the same pod UID and container ID as before the fault, and
  exactly that container in the samples from the fault to the end of the recovery; real samples of its memory in the
  timeout phase below its limit; and both OOM counters read at 0 from the start of the fault to the end of the
  recovery (within 5 s of each end, no gap over 15 s). A counter that was read once, or a status taken before the
  recovery, proves nothing about what came after and fails. The two archived comparison runs predate the captures
  before the fault and after the recovery, so this verifier reports them as not captured; their samples show one
  container and counters at 0 throughout for the new build. The fixture's ingress bounds its upstream dials by the request
  timeout since PR #76; the two runs that compare the reviewed image with a build of that source under
  `DEMO_BLACKHOLE=requests` are recorded with the evidence, not here.
- *Not shown.* A pod that needs its whole grace period to stop, a node failure, the `--k6-local` mode and the
  default full mode of this script, 24-hour segments, the operator and the forecaster (not installed), and any rate
  above the one used.

## Restore to the common starting count

```sh
session.py --record R restore-plan                        # read-only: writes the plan file, prints its digest
session.py --record R restore --plan F --plan-digest D    # restore exactly that reviewed plan
session.py --record R recover-plan | recover --plan F --plan-digest D   # after a refusal, timeout or crash
```

`restore-plan` lists every arm whose desired replicas differ from the **common starting count** (or that has not
converged at it), each bound by name, UID (from `targets.json`), generation, resourceVersion and current replicas. The
count is frozen, never a CLI value: every arm's rendered Deployment `spec.replicas` (`render.FIXED_REPLICAS`, 2), the
same for every arm and at most `residency.idle_arm_replicas`; for a confirmation record the arm renderings must equal
the signed run-lock's (`arms.<arm/profile>.rendered_sha256`). The plan refuses while any session is open or unresolved,
any intent or earlier restore is unresolved, or any generator Job or Pod of the run remains (terminating and finished
ones included). Per arm, the writer must be shut down or provably write nothing, otherwise that arm is listed under
`refused` with the reason and left alone:

| Arm | Writer inactive when |
|---|---|
| PA-R, PA-N, PA-E | the writer inventory (HPA, KEDA, both PA API groups, any namespace) is exactly the arm's own PredictiveAutoscaler (the UID `prepare.py` created), its `spec.mode` is `Recommend`, and the operator reports `status.mode: Recommend` at `observedGeneration` >= `generation`. In Recommend the operator decides and records but never writes the target. |
| H-CPU, H-RPS | no writer at all: their HPA/ScaledObject exists only during their session (`session.py` creates it at start and deletes it, and waits for KEDA's generated HPA to go, at stop). An HPA cannot be paused, so any HPA, ScaledObject or `keda-hpa-<target>` refuses the arm. |

`restore` recomputes the plan under the shared evaluation lock and runs only the reviewed digest. Each arm is one
journalled step (intent before, outcome after): the cluster UID re-check, a re-check of the bound Deployment, `PUT
.../deployments/<name>/scale` with the read resourceVersion and the UID (a Scale response without `spec.replicas` means
0), then a bounded wait (record `restore.timeout_s`, default 600) until desired, status and the owned Pod census through
the ReplicaSets (terminating Pods included, the admission's accounting) equal the count with every Pod Running and
Ready, the writer still inactive and the desired count unchanged. The writer and the bound Deployment are re-checked
immediately before each arm's write (after the earlier arms' waits), both before its intent and inside it. The write's
progress is persisted as `restore-write` receipts bound to the step: `not-submitted` when the pre-write check refuses,
`submitting` durably before the request, then applied (with the resulting generation), refused or unknown. Only a
request marked `submitting` can be recovered as a lost response; a change after an unsubmitted or refused write is
someone else's and stays unresolved. A refusal, a timeout or a crash leaves the step unresolved and the lock held;
`recover` accepts only a transition consistent with that intent and receipt, with the writer provably inactive again:
done when the write applied (or a lost response took exactly one generation step to the count) and the arm converged;
not-done, state `applied-convergence-pending`, when it applied but has not converged (admission keeps refusing the arm,
selected or not); not-done, state `not-applied`, when nothing was written and the Deployment is at its planned
generation and replicas; unresolved for anything else (gone, replaced, an unexplained generation change, another count,
an active writer). It then closes the restore record (`restore-closed`, `resolved` true or false, each unfinished arm's
state). A session start refuses while a restore is open.

A restore never touches a writer (mode, min/max bounds), a fault schedule, the quota, the recorder or any history path:
the PredictiveAutoscalers stay in Recommend and keep collecting their forecast and decision history, the ingress and
recorder keep observing every target, and the receipts (`restore-begin`, the per-arm intent/step, `restore-closed`)
record when and why a target's replica count changed, so the replica series around a restore stays attributable.

## Capacity calibration (S5a): `staging/calibrate.py`

```sh
calibrate.py --record R [plan]                            # read-only: plan file + digest (objects, bounds, admission)
calibrate.py --record R apply --plan F --plan-digest D    # create, select work_iterations, search c0, ceiling probe, close
calibrate.py --record R status | verify                   # journal summary | live contract check (closed: offline recompute)
calibrate.py --record R request-stop --reason TEXT        # ask the running executor to stop (arrivals first)
calibrate.py --record R stop-plan | stop --plan F --plan-digest D       # after a crash or refusal
calibrate.py --record R recover-plan | recover --plan F --plan-digest D
calibrate.py reduce --bundle evidence/calibrate/<id>      # offline: recompute c0 from the evidence bundle alone
```

Manifest section 3's capacity calibration at fixed replicas with no autoscaler, never against a confirmation target.
The record's `calibrate` section names a **calibration contract** (`c28-calibration-contract/v4`; v1 contracts, which predate the endpoints-rr ingress and its discovery gate, are refused, while recorded v1 bundles still reduce; v2 contracts, whose dispatch identity omits the ingress's no-ready rejections, and v3 contracts, whose quiet readings end at the query time and do not rest on distinct raw observations, are read-only: status, verify, stop and recover of their own calibration and the reduction of their bundles are unchanged, while plan and apply refuse them), frozen independently of
the unfinished params (only their reservation stage is read): cluster and node UIDs, namespace, source commit and the
hash of `load/c28-calibrate.js`, the S1 `images.json` with its fixture and generator digests (taken from the record,
never hardcoded), the candidate (CPU/memory requests and limits, max_inflight, max_queue, queue timeout, GOMAXPROCS,
response size, startup delay), the work_iterations selection, ingress and generator resources, the observer (query API,
the evaluation's 15 s scrape cadence, staleness, scrape kind, the exact queries, and `observer.discovery`: the ingress's synced and stale gauges, its scrape timestamp, its endpoint membership, per-endpoint dispatch and (v3) `no_ready`, the `c28_ingress_no_ready_endpoints_total` of the target), the criteria (the manifest's numbers,
which cannot be changed, the queue rule, the limit bounds, the 1.2 ceiling factor), the search bounds and limits
(maximum trials, total deadline, seed, timeouts), the admission budget and the rollout guard. Placeholders, non-finite
numbers and unknown fields are refused. The calibration ID is `c28cal-<10 hex of the contract digest>`: any change to
the candidate is a new calibration, an ID is used once, and results are never combined across IDs.

Stages, all inside one reviewed plan (it authorizes the algorithm and its limits, never an arbitrary load):

1. **work_iterations selection.** Two fixture variants (`<id>-w0`, `<id>-w1`, one per predeclared value), each alone at
   one replica: idle CPU over `idle_s`, then one trial at the declared rate below saturation. Cost = (fixture CPU in the
   window - idle CPU) / successful requests per second. A value qualifies when its trial passes every criterion and its
   cost is inside the declared range; the cost closest to the target wins, a tie goes to fewer iterations. If neither
   qualifies the calibration stops NOT READY for a revised candidate. The choice is a receipt before any c0 trial, and
   both measurements stay in the evidence.
2. **c0 search** on the selected variant for 1, 2 and 4 replicas: three 15-minute measurements after 5 minutes of
   settling per plateau, a bracket (factor `bracket_factor`) and bisection to <= 5 %, every choice a receipt. A pass at the
   arrival ceiling is an unresolved lower bound, a fail at the floor no qualifying rate; any incomplete, limited or
   stopped trial halts the calibration (never bisected across). c0 = min over counts and repetitions of Q/n, c = 0.875 c0,
   and every repetition's 4-replica/1-replica linearity must be >= 0.90.
3. **Ceiling probe**: one trial at 4 replicas at 1.2 x Q4, inside the authorized maximum, whose arrivals must be
   uncensored (no generator drops beyond the bound, every dispatched arrival received by the ingress, generator and
   ingress throttling within bounds). Generator drops are never attributed to the fixture.

Every trial goes through a pinned calibration ingress on W (offered demand is counted before the fixture can reject
it), one k6 Job on S (`load/c28-calibrate.js`: settle and measure scenarios, its summary in the pod's termination
message, a finite `activeDeadlineSeconds`). Scheduled, dispatched, received and terminal outcomes are reconciled;
missing outcomes are unknown (never successes), any dropped arrival in either scenario is a limit, more arrivals at the ingress than the generator dispatched is
incomplete evidence, and the dispatch reconciliation never hides a missing dispatch: k6's dropped + dispatched must be
the scheduled count or k6's one boundary iteration more (zero dropped arrivals is the criterion; an absent
`dropped_iterations` counts as zero only when every scheduled arrival was dispatched). The observer window is the
measure scenario's actual start as k6 recorded it, never the container start plus the nominal settling. Guest steal
above 5 % for five consecutive one-minute bins on W or S, or a missing steal observation, stops the trial. The
work-selection idle interval must be proven idle (ingress and fixture request counters unchanged, waiting queue and
in-flight 0). **Queue rule** (on the WAITING queue; in-flight work is recorded separately): with the window cut into
60 s bins, END = mean of the last `edge_bins` bins and
INCREASE = END - mean of the first `edge_bins`, a repetition fails when END / replicas > `max_end_backlog_per_replica`, or
when INCREASE > `max_increase` AND the least-squares slope > `max_slope_per_min`; bounded growth below those numbers
passes.

The rollout guard judges OnDelete DaemonSets and StatefulSets (pods deliberately kept on an older template, such as
prod's velero node-agent or the VictoriaMetrics StatefulSets) by observed generation, expected pod count and all their
pods Ready and non-terminating, not by revision lag; RollingUpdate objects keep the strict rules. Each plateau records
the guarded owners' generations and pod UID/revision/restart sets, and any replacement, restart, revision or template
change during it stops the trial. Every expected source (each fixture pod, the ingress, the generator while it runs,
W and S for steal) must show a fresh raw sample (a `timestamp()` query per source, `observer.freshness`) at every
evaluation (every step present, the newest raw sample judged against the gate time), so one pod's missing series cannot
hide inside a sum. New pods get a bounded observer warm-up (`observer.readiness_timeout_s`, safety guards active)
before any idle interval or load begins. The ingress's endpoint discovery is a gate (contract v2): before any load it
must report synced 1 and stale 0 from a fresh scrape and route to exactly the variant's live fixture pods (its
`c28_ingress_endpoint` series from that same scrape), or the trial is refused when the warm-up times out; during the
measurement a stale flag, a missing or old discovery metric, or a membership mismatch stops the trial as incomplete
evidence, and the result re-checks every evaluation of the measurement window from the recorded series (a blip between
two live polls invalidates it too). Each trial records the per-endpoint dispatch (`c28_ingress_dispatched_total` by
`endpoint_pod`): every arrival dispatched exactly once or, with a v3 contract, answered 503 by the ingress because the
target had no READY endpoint (sum + the `no_ready` delta = received, read with the same aligned before/after readings;
v2: sum = received) and only to the trial's fixture pods, with the round-robin's spread reported, never judged. The
no-ready 503s stay inside the failure fraction and latency that k6 measures and are reported in the dispatch detail,
never judged again; an absent `no_ready` counter is 0 only from a successful query of the proven fresh, unchanged
ingress, while a failed or stale reading, a series that disappears or a reset is incomplete evidence, also across
trials (once an earlier trial saw the counter on the same ingress pod identity, a later absence or a lower value is
incomplete, derived from the earlier trials' recorded results, so the reducer replays it). Under v3 every ingress
counter reading and its supporting `up` response must be a successful range response (status `success`, resultType
`matrix`) before it is read, so an error answer is never an absent counter. No discovery, membership or
stray-endpoint check is waived. Dispatched, received and answered arrivals reconcile
with tolerance 0/0. The idle CPU baseline is averaged only after a washout of the 60 s rate window plus one CPU scrape
interval.

Readings in time (contract v4). The query API may answer the points newer than now minus its latency offset with
older samples (VictoriaMetrics `-search.latencyOffset`; in the recorded evidence the last two 15 s points of a query
that ended at the query time repeat the third-last). The contract declares that offset
(`observer.latency_offset_s`, at least 30) and the executor keeps two kinds of reading apart. Completeness readings
(the ingress counters before and after a trial, the per-endpoint dispatch, the selection's idle proof, the window
queries) are read as history, their range ending at least `latency_offset_s + 2 x step_s` before the query; the
after-reading is still at least `completion_grace_s` after the Job completed (asserted), and a quiet counter needs
three distinct raw observations (the reading's raw scrape timestamps), never three equal values of repeated points.
Live gates (pods up, freshness, discovery membership) query up to now and judge the newest raw sample in the answer,
which must be within `latency_offset_s + max_staleness_s` of now; each trial's `gates.json` and `verify` say what
instant every gate judged. The validator requires `completion_grace_s >= 2 + latency_offset_s + 2 x step_s` and
`max_staleness_s >= 2 x` the slowest declared scrape interval. A dispatch series that vanishes between the two
readings counts 0 only when its endpoint is proven retired from the trial's dispatch span (not one of the trial's
pods, never in the endpoint set during the trial, its counter never moved); anything else stays incomplete. Equal
per-endpoint totals do not prove a constant endpoint set.

The writer census uses fresh discovery every time: a writer API group that a successful discovery does not serve (on
prod today, the new PredictiveAutoscaler group) is recorded as "API discovery-proven absent" in the reviewed plan and
the receipts, a failed or malformed discovery refuses, and any change of that availability during the calibration
stops it. CronHorizontalPodAutoscalers count as replica writers (also in the shared inventory session and
prepare use); a VerticalPodAutoscaler on a calibration object is allowed only in updateMode Off and recorded.

Before and during every trial, and once more before a completed trial is accepted: the measured configuration equal
to the reviewed rendering, compared strictly (owned objects, the trial's Job, every admitted pod: every container and pod
field, a field the rendering omits must be absent unless it is a listed API default, and admission may add only the
not-ready/unreachable NoExecute 300 s tolerations; only the controlled replica count may differ), the observer queries proven to select exactly the live pods
with fresh data, the interference stop, the reservation with this calibration's own objects recognized only through their
journal-recorded UIDs (`reserve.verify(calibration=...)`: the pod's ReplicaSet must exist with exactly the referenced UID
and belong to one of the journal's Deployment UIDs, or the pod's Job must be one of its Job UIDs; no name prefix), the
calibration footprint plus every other live pod within 75 % of each node's live allocatable, no rollout in progress for
any workload with a pod on W or S or for the contract's extra owners (capacity is never measured during platform
rollouts), no writer of the fixture or ingress, the owned objects at their UIDs, no session or run generator active, no
injected sidecar, a fresh observer, no stop request and the total deadline. A violation stops the trial: the Job is
deleted and its pods proven gone before anything else is touched. Mutations hold the shared evaluation lock with
intents and outcomes; an unknown outcome or an unproven cleanup keeps the lock until `recover`. An interrupted
measurement is recorded as interrupted and never resumed. A journal whose last line is a torn append (the executor
killed inside a receipt's write) is refused by every mutating command of the part-2 helpers before a lock is taken;
`status` and `request-stop` read past it and `status` reports it (`journal.torn_tail`); `recover-plan` lists it and
`recover`, once the owning executor is proven ended, copies the torn bytes into the evidence
(`receipts/<helper>.jsonl.torn-<offset>-<sha12>`), cuts them off the log, records `journal-tail-repaired` and
reconciles the half-recorded step like any unresolved intent. A live journal is never cut or rewritten.

Reads are retried, writes never: every kubectl read (`KubectlApi.get`, for every staging helper) and every calibration
observer query makes up to three attempts over 20 s (`common_records.READ_RETRY`; a refusal such as 403 or an HTTP 4xx
is an answer and is not retried). Each failed attempt is kept as a fixed category (`timeout`, `connection`, ...; never
the error text) and the calibration journals them (`read-retried`; a retried evidence query also carries
`failed_attempts`). An observer query that still fails stays a missing observation; a Kubernetes read that still
fails inside a trial stops that trial as incomplete evidence (`trial-stopped` / `trial-not-started`, halt
`incomplete`) through the usual arrivals-first path, never as an executor error. A failed mutation keeps its unknown
outcome and the lock. The session watchdog's query reads use the same policy, so one failed read no longer stops a
block; a read that stays failed still does.

Lists are strict everywhere (`execlib.Kube.list`): "the collection exists and is empty" is an `items: []` answer;
"not found" or any other non-list answer for a collection is a failed read (retried, then `ReadFailed`) and is never
read as "no objects", because every such listing gates a safety decision (occupants of W and S, generators of the
run, a session's pods, the pods of a calibration, the writer census). A caller that lists an optional API asks
discovery first. A single object's GET is "absent" only on the API server's own `NotFound` answer, never on the words
"not found" in a client-side failure. (`common_records.list_items`, used by the already-executed part-1 helpers
`legacy.py` and `inventory.py`, is unchanged.)

The plan, the `calibration-begin` receipt and the certificate record the environment that was measured on: for W and S
the node name and UID and, from `status.nodeInfo`, the kernel, OS image, container runtime and kubelet versions, the
boot ID and the machine ID. It binds a capacity to the environment that produced it and is never a gate, except that
`apply` refuses when it differs from the reviewed plan's (said field by field). `status` prints the recorded one,
`verify` the recorded and the live one with their differences.

The evidence bundle (`evidence/calibrate/<id>/`) holds the contract, the rendered objects, per trial the Job, pod
snapshots with their image identities, the raw observer responses and the derived result, then the manifest of hashes,
the certificate (READY or NOT READY with every reason; the 1.2 x Q4 probe is the calibration's own ceiling, and the
experiment's maximum-arrival admission stays PENDING, curves with CPU and throttling, every unsuccessful trial) and the
hash-chained journal. `reduce` recomputes everything from that bundle and refuses any difference. The certificate says
what it does not cover: the CPU HPA target, the startup profiles, the twelve-replica layout and single-pod bursts are not
qualified by it, and no prospective history or confirmation evidence starts during calibration.
