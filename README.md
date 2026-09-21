# Predictive Autoscaler

**Scale before the traffic arrives, not after.**

Predictive Autoscaler is an open-source Kubernetes autoscaler that forecasts request demand and provisions capacity ahead of the load. Conventional autoscalers react to metrics that have already crossed a threshold, which means the first users of every traffic surge pay for it in latency and errors. Predictive Autoscaler is built to close that gap: it learns each workload's daily rhythm, predicts the next hour, and aims to have pods ready before the wave hits. A reactive floor means the forecast can only add capacity on top of what live metrics require.

Built by [Bakmurat Kubanaliev](#author). Apache-2.0.

## Why it matters

- **Latency budgets are lost in the lag.** Between a metric crossing its threshold, the autoscaler noticing, new pods being scheduled, and the application becoming ready, minutes pass. For workloads with sharp daily peaks that lag is the whole incident.
- **Over-provisioning is the expensive workaround.** Teams pin replica minimums high enough to survive the morning peak and pay for that capacity all night. Forecasting lets capacity follow demand in both directions.
- **Forecasting must be safe to adopt.** A predictor that can scale a service *down* on a bad forecast will never be trusted. Predictive Autoscaler is additive by design: it can only bring capacity forward, never withhold it.

## What you get

| Capability | What it does |
|---|---|
| **Demand forecasting** | A three-layer bidirectional LSTM (128/64/32 units) forecasts the next 60 minutes of request rate in six 10-minute steps from the last 24 hours of history. |
| **Asymmetric training objective** | The loss penalizes under-prediction twice as heavily as over-prediction, because scaling too late costs more than scaling too early. |
| **Hybrid forecast** | Model output is blended with the workload's own previous-day time-of-day pattern, looked up by timestamp over up to seven prior days (pattern weight 0.70 at the first step rising to 0.908 at the sixth), so forecasts keep the shape of the daily peak instead of regressing to the mean. When less than a day of history is available the pattern is unavailable and the network is served alone, which the response reports. |
| **Continuous learning** | A scheduled job retrains every six hours on the previous seven days and publishes the model to the forecasting service without downtime. |
| **Declarative operator** | A Go operator (controller-runtime) driven by a `PredictiveAutoscaler` custom resource: target Deployment, replica bounds, per-pod targets, horizon, lead time, and reconcile interval. |
| **Additive safety model** | Replicas are set to the **highest** of the forecast baseline, the live reactive requirement, and the configured minimum. The forecast can only add capacity. |
| **Guard rails** | Overestimate detection, scale-down stabilization (a five-minute hold after any scale-up and a further five-minute hold after demand first drops below the current count; then at most one step per two minutes, each removing at most 10% of the pods or two pods, whichever is greater; an active overestimate override skips the holds but keeps the cooldown), and confidence dampening keep a wrong forecast from causing churn or cost. A forecast whose first step exceeds ten times the live rate is discarded as diverged and the reactive rule applies. |
| **Reactive fallback** | When the forecasting service is unreachable (a transport failure), the operator keeps using its last forecast while that forecast is strictly less than ten minutes old, excluding steps whose target time has already passed, and then falls back to its own reactive calculation from live metrics. If the service *refuses* the request as invalid input (HTTP 4xx, for example its freshness guard), no cached forecast stands in for it and the reactive rule applies at once; a refusal takes effect when the service is actually queried, which happens when the five-minute prediction cache expires, not during an unexpired cache hit. Whenever no usable forecast participates in a reconcile, any overestimate override from earlier forecasts is cleared, so the normal scale-down holds apply. A KEDA `ScaledObject` can be kept on the same Deployment as an independent backstop; pause it while the operator is active (KEDA's `autoscaling.keda.sh/paused` annotation) so the two controllers do not compete. |
| **Observability** | Prometheus metrics from both components and three Grafana dashboards: forecast versus actual, replica decisions, and guard-rail state. |

## How it works

```
VictoriaMetrics ──► Collector ──► Training job (every 6 h) ──► Model store
   (Istio RPS,          │                                           │
    CPU, memory)        └──────────► Forecasting service (FastAPI) ◄┘
                                              │  next 60 min, 6 steps
                                              ▼
                        Go operator ── max(forecast, reactive, min) ──► Deployment replicas
                                              │
                                    KEDA ScaledObject (dormant backstop)
```

1. **Collect.** Per-pod request rate from the Istio service mesh, plus CPU and memory, are pulled from VictoriaMetrics at ten-minute resolution.
2. **Train.** Every six hours a CronJob retrains the model on the previous seven days and writes it to a shared volume; the service reloads it by modification time. Per-model locks and the job-based design eliminated the concurrent-retraining failures of early versions.
3. **Forecast.** The service returns the expected demand for the next hour in six steps, blended with the seven-day pattern, with a confidence signal.
4. **Scale.** On every reconcile (60 seconds by default) the operator converts the forecast at the configured lead time (20 minutes by default) into a replica count and applies the additive rule.
5. **Guard.** Overestimate detection compares recent forecasts with actuals and shrinks the forecast's influence when it runs hot; stabilization and cooldown bound every scale-down; confidence dampening discounts low-confidence forecasts.

## Evaluation approach

Predictive Autoscaler was developed and evaluated in a dedicated Kubernetes environment with a purpose-built harness: a k6 traffic generator replaying a full 24-hour daily traffic profile against a stock nginx Deployment with a 1 to 40 replica range, run side by side with an identical Deployment scaled by KEDA alone as the control. The harness ships in `examples/` so anyone can reproduce the setup. Across a dozen operator releases (v3.1.1 to v4.3.0) this loop drove every design decision listed under [Lessons learned](#lessons-learned).

Accuracy figures are deliberately not published yet. A controlled, repeatable benchmark with held-out days and a documented metric is the first item on the roadmap, and numbers will be published with it.

## Getting started

Both components ship as container images:

```sh
cd ml-engine    && docker build -t <your-registry>/predictive-autoscaler-ml-api:dev .
cd k8s-operator && docker build -t <your-registry>/predictive-autoscaler-operator:dev .
```

Set the image names in `k8s-manifests/base/kustomization.yaml` and the Deployment manifests, choose a storage class in `03-pvc.yaml`, point `02-configmap-victoriametrics.yaml` at your VictoriaMetrics query endpoint, then:

```sh
kubectl apply -k k8s-manifests/base
```

Prerequisites: VictoriaMetrics with Istio request metrics, and KEDA for the reactive backstop. The API group `autoscaler.example.com` is a placeholder; rename it (CRD, RBAC, Go types, samples) to a domain you control.

### Declare an autoscaler

```yaml
apiVersion: autoscaler.example.com/v1alpha1
kind: PredictiveAutoscaler
metadata:
  name: web-autoscaler
  namespace: demo
spec:
  targetDeployment:
    name: web
    namespace: demo
    container: web
  minReplicas: 2
  maxReplicas: 40
  metrics:
    requests:
      enabled: true
      targetRPS: 300          # requests per second per pod
  prediction:
    horizonMinutes: 60
    leadTimeMinutes: 20
    updateIntervalSeconds: 60
```

`spec.metrics.cpu` and `spec.metrics.memory` are reserved in the schema; the request-rate model is the one trained today.

### Tests

```sh
cd k8s-operator && go test -race ./...
cd ml-engine    && pip install -r requirements.txt && python -m pytest tests
```

**Publication is gated on `make verify` returning 0.** `scripts/verify.sh` is the one checked
verification entry point: it runs `go vet`, `go test -race` and `pytest` and preserves each
suite's exit status through its own logging (`set -o pipefail`, status read from
`PIPESTATUS[0]` before anything else runs). It exists because a pytest run piped through
`grep` for a tidy summary once swallowed a red test into a green push; `make verify-selftest`
proves the wrapper returns non-zero for a deliberately failing test and zero for a passing one.
Nothing is pushed or deployed on a run that did not go through it.

**The commit gate verifies the tree being committed, not the working directory.** Enable it
once with `make setup` (`git config core.hooksPath .githooks`). `.githooks/pre-commit` runs
`scripts/precommit-verify.sh`, which exports the *staged* tree with `git checkout-index` into a
temporary directory and verifies that — never `git stash`, never `git reset`, never touching
files you have not staged. The pass is bound to the staged tree hash plus the hashes of the
verification script, the Makefile and the dependency manifests, and the staged hash is
re-checked immediately before the commit is allowed, so a stage that changed while the suite
ran does not inherit the pass. Partial-suite flags and an inherited `PYTEST_ARGS` are refused:
a gate runs the whole suite or it is not a gate. It exists because a commit chain once ran the
suite over a working directory holding a later change's tests against an earlier change's
source, saw failures, and committed anyway. `make precommit-selftest` proves both halves: a
clean staged tree commits even with an unstaged failing test present, and a staged failing test
is refused.

A local hook is bypassable (`git commit --no-verify`); it narrows the window rather than
closing it. Protected CI on the remote is the stronger guarantee and is not yet configured.

Both suites pass. Last full run 2026-09-20: Go `go vet` + `go test -race` green; Python 170 passed, 0 failed, executed inside a Kubernetes cluster on the runtime image (arm64) via `ml-engine/Dockerfile.tests`, which adds `requirements-dev.txt` (pytest, httpx) to the ml-api image and runs `python -m pytest -q ml-engine/tests` from the repository root. Build it with `docker build -f ml-engine/Dockerfile.tests --build-arg BASE=<ml-api image> .`.

## Repository layout

| Path | Contents |
|---|---|
| `ml-engine/` | Forecasting service: FastAPI application (`api/`), model code (`models/lstm_model.py`), VictoriaMetrics collector (`data/`), training scripts (`training/`), unit tests (`tests/`), `Dockerfile`. |
| `k8s-operator/` | Go operator: CRD types (`api/v1alpha1/`), reconciler and overestimate logic (`controllers/`), sample custom resources (`config/samples/`), `Dockerfile`. |
| `k8s-manifests/base/` | Kustomize base: namespace, CRD, RBAC, both Deployments, the training CronJob, the model volume, and VictoriaMetrics scrape objects. |
| `grafana/dashboards/` | Three dashboards (overview, forecast components, guard mechanisms). |
| `examples/nginx-test/` | The reference workload scaled by Predictive Autoscaler, with its k6 traffic generator and dormant KEDA backstop. |
| `examples/myapptwo/` | The identical KEDA-only control workload. |

## Lessons learned

- **Forecasts regress to the mean.** A plain LSTM flattened toward the average, useless for anticipating a daily peak. Blending with the workload's own seven-day pattern, weighted more heavily at longer horizons, restored the shape of the forecast.
- **Under-prediction hurts more than over-prediction.** The asymmetric loss was the single most valuable change to the training objective.
- **Additive by construction.** Taking the maximum of forecast, reactive, and minimum is what makes a predictive layer safe to run continuously next to a reactive autoscaler.
- **Retraining is an operational feature, not a script.** Moving training into a CronJob with per-model locks, and reloading by file modification time, removed a whole class of resource-exhaustion incidents.
- **Scale-down must be slow and bounded.** Stabilization windows, a cooldown, and a bounded step size eliminated oscillation on the way down.
- **Version-pin the training image to the serving image.** Manifest-consistency tests (`ml-engine/tests/test_kustomize_sync.py`) now catch mismatches that once let a stale model overwrite a good one.

## Roadmap

1. One bounded stabilisation experiment, properly controlled: `tanh` activation as one arm
   and gradient clipping as a separate arm, never both at once, with seeded initialisation
   and a production-equivalent training configuration, judged against the strongest baseline.
   Reject invalid forecasts through a documented fallback and report raw and guarded outputs
   separately.
2. Validation of the newest forecasting model (five input features, direct six-step output, robust scaling) against the benchmark.
3. Adaptive blend weights in place of the fixed 0.70-to-0.908 schedule.
4. A training-time validation gate so a new model can never replace a better one.
5. Multi-tenant model management, CPU and memory forecasting paths, CRD validation and defaults via webhooks, Helm chart, end-to-end operator tests on kind.

## Status

Predictive Autoscaler is an actively developed personal research project (October 2025 to present). It is built to run alongside KEDA, which stays in place as the reactive backstop, and it is published so the design, code, and lessons are available to everyone working on the same problem. Evaluate it in your own environment with the shipped harness before relying on it.

Note on numbers: request-rate figures in the simulator configuration (for example a 60,000 requests-per-minute peak) are the synthetic load generator's settings, not measured throughput.

## What the measurements say

A production-equivalent evaluation on 2026-09-22 drove this repository's own training and
inference code over rolling origins across five scenario families, with decisions taken by
the real Go controller ([`eval/RESULTS-2026-09-22-corrected.md`](eval/RESULTS-2026-09-22-corrected.md)).

**Neither neural arm reached a 10% improvement over the strongest baseline in those ten runs.**
In one run of the ten the served blend was the single best predictor, ahead of the strongest
baseline by 1.7% — below the bar, but the network is not uniformly beaten.

**The best forecaster depends on the workload.** Steady daily traffic favours the seasonal
pattern, this project's own arithmetic component; trend, level shift and weekly cycles favour
a simple trend-adaptive rule. A forecaster that selects between those two automatically is the
obvious thing to build from this and **has not been tested**.

Treat the neural component as experimental and optional. It is not the reason this system
works, and the project should not be described as though it were.

Every number is from synthetic traffic: the benchmark cluster has not accumulated enough real
history to score. Nothing here measures latency — the replay models capacity arriving, not
response time. An earlier version of this section, built on a run whose stress scenarios never
entered the scored window, has been withdrawn.

## Limitations

- Not deployed to production anywhere; evaluated so far only against synthetic traffic in the author's own environments. The first controlled evaluation is published in `eval/RESULTS-2026-09-22-corrected.md` (the 2026-09-21 run was withdrawn) and it does not support the neural component; a live benchmark is running (`deploy/eks-benchmark/`) and its results will be published the same way, whatever they show.
- The LSTM's training is not numerically stable: on identical data, separate runs produced errors ten orders of magnitude apart. The cause is not established -- the `relu` activation in the LSTM layers is one hypothesis, and the unseeded initialisation is another. Until it is understood, an invalid forecast has no documented rejection path.
- One target metric in practice (request rate per pod); CPU and memory paths exist in the schema but are not trained.
- Model files live on a ReadWriteOnce volume, so a single forecasting replica is supported.
- The API group `autoscaler.example.com` is a placeholder to rename before use.
- Integration with a node-level autoscaler has not been tested; the operator manages replicas only.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`. The operator scaffolding was generated with Kubebuilder, whose boilerplate header is retained in `k8s-operator/hack/boilerplate.go.txt` and the generated deep-copy file.

## Author

Bakmurat Kubanaliev. This is a personal project; it is not affiliated with, endorsed by, or derived from the work of any employer.
