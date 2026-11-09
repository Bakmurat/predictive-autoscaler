# C2.8 evaluation fixture and harness

Offline code for the C2.8 study (`agent-coordination/09-real-world-research/EVAL-C28-MANIFEST.md`, sections 3–9 and
the prodcluster adaptation in section 11). Nothing here runs an experiment, deploys to a lab cluster or produces C2.8
evidence by itself. The executor uses it under a locked run-lock and the change gates of manifest section 11.4.

| Path | What it is |
|---|---|
| `fixture/` | The HTTP fixture: one binary in three modes. `server` is the workload: fixed CPU work, a fixed response, a bounded queue, a measured startup delay and `/ready`, `/metrics`. `ingress` counts offered demand per target before the target can reject it, with a 2 s timeout and no retries. `tcpproxy` is the evaluation-owned metrics and forecast routes, with scheduled outages. `Dockerfile` builds a static, non-root image. |
| `load/c28.js` | The k6 open-loop generator (ramping-arrival-rate, one request per iteration), driven by a plan from `python -m c28 plan`. |
| `load/c28-calibrate.js` | The k6 script of the S5a capacity calibration (`staging/calibrate.py`): settle and measure scenarios at one constant arrival rate, per-scenario counts and latencies in the pod's termination message. |
| `harness/c28/` | `render` (fixture, ingress, routes, quota and the five arms for both startup profiles, with an exclusive-writer audit, a sidecar-free audit and the residency budget), `schedule` (programs, k6 plans, fault timetable, the frozen session order), `runlock`, `collect` (hashed evidence bundle), `reduce` (per-window metrics), `score` (paired day-block bootstrap, Holm correction, the section 8 classification), `inject` (the guarded quota fault). `frozen.py` holds every value the manifest freezes. |
| `trace/` | PROPOSAL (draft contract v2, pending Codex review): the pre-declared selection and transformation of the Azure Functions 2019 trace (CC BY 4.0) for a separate public-trace demonstration. Not part of C2.8 scoring and no replacement for the manifest's ordinary warm-up. `azure_functions_2019.py select|prepare|plan|rules` (standard library, one file, run with `python -I`): each stage checks the reviewed sha256 of its input and records the hashes it was built from (source, selection, prepared, plan), validates schema, membership, length and transformation consistency, and never overwrites an output. `plan` emits exactly the `c28-k6-plan/v1` that `load/c28.js` accepts (kind `trace-demo`), one constant rate per trace minute, so each minute's scheduled count equals its intended count; `tests/c28js_loader.js` runs the real `c28.js` validator on it under node. Generated requests are "derived from per-minute invocation counts", never real arrival timestamps or real traffic. It never runs a forecaster. |
| `params/` | `kind-smoke.json` for the kind smoke. `prodcluster.template.json` lists what the run-lock must resolve; it cannot be rendered until every placeholder is replaced (the calibrated fixture resources and the W/S residency inputs included). |
| `smoke/kind.sh` | The aspire kind-bed smoke, on its own cluster and kubeconfig only. `--render-only` renders and audits offline; `scripts/verify.sh` runs that mode. |
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

**Idle-arm parking below the starting count is not implemented**, so `idle_arm_replicas` below the starting count is
refused (that would need the writers stopped, the count restored before each session, history preserved and the loaded
arm's frozen bounds unchanged). `session.py` admits a session only when EVERY target, the selected one included, exists
with its recorded UID, sits at exactly the common starting count and has converged there (`idle_arm_replicas` is only
the budget's upper bound): desired replicas equal to the count, status (observed generation, replicas, updated, ready,
available) equal to it, and exactly that many Pods owned through its ReplicaSets (bound by controller UID to the
Deployment's UID), none of them terminating and every one Running and Ready. A session never scales a target, and a
stopped session leaves its target at whatever count its writer last set; the start refusal names those targets and
points to the restore.

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
The record's `calibrate` section names a **calibration contract** (`c28-calibration-contract/v1`), frozen independently of
the unfinished params (only their reservation stage is read): cluster and node UIDs, namespace, source commit and the
hash of `load/c28-calibrate.js`, the S1 `images.json` with its fixture and generator digests (taken from the record,
never hardcoded), the candidate (CPU/memory requests and limits, max_inflight, max_queue, queue timeout, GOMAXPROCS,
response size, startup delay), the work_iterations selection, ingress and generator resources, the observer (query API,
the evaluation's 15 s scrape cadence, staleness, scrape kind, the exact queries), the criteria (the manifest's numbers,
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
before any idle interval or load begins. Dispatched, received and answered arrivals reconcile
with tolerance 0/0. The idle CPU baseline is averaged only after a washout of the 60 s rate window plus one CPU scrape
interval.

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
measurement is recorded as interrupted and never resumed.

The evidence bundle (`evidence/calibrate/<id>/`) holds the contract, the rendered objects, per trial the Job, pod
snapshots with their image identities, the raw observer responses and the derived result, then the manifest of hashes,
the certificate (READY or NOT READY with every reason; the 1.2 x Q4 probe is the calibration's own ceiling, and the
experiment's maximum-arrival admission stays PENDING, curves with CPU and throttling, every unsuccessful trial) and the
hash-chained journal. `reduce` recomputes everything from that bundle and refuses any difference. The certificate says
what it does not cover: the CPU HPA target, the startup profiles, the twelve-replica layout and single-pod bursts are not
qualified by it, and no prospective history or confirmation evidence starts during calibration.
