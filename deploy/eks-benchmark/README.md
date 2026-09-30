# EKS benchmark environment (Terraform)

A small, reproducible Kubernetes environment for evaluating the predictive autoscaler against a KEDA-only twin with synthetic traffic. Everything is created by Terraform and `deploy.sh`; everything is removed by `deploy.sh destroy`.

## What it creates
- VPC `10.50.0.0/16` with two **public** subnets in two availability zones, **no NAT gateway** (nodes get public IPs; egress via the internet gateway).
- EKS **1.34** (standard support until 2026-12-01 per the EKS release calendar; 1.31–1.33 were in extended support, which bills the control plane at about six times the standard rate — check `aws eks describe-cluster-versions` before choosing), public endpoint, IRSA enabled; addons vpc-cni (prefix delegation on, so small instances are not capped at 17 pods), coredns, kube-proxy, metrics-server, aws-ebs-csi-driver (IRSA role), versions pinned per minor in `variables.tf`. One managed node group: **two `t4g.medium`** (arm64, AL2023, on-demand; 2 vCPU / 4 GiB each), fixed at 2 (no autoscaling headroom), 30 GiB gp3 root volumes. No KMS key, no control-plane logs.
- `gp3` default StorageClass.
- Istio (base + istiod), KEDA, kube-prometheus-stack (Prometheus with 15-day retention on 20 GiB gp3, Grafana on 2 GiB gp3, no alerting); namespaces `istio-system`, `keda`, `monitoring`, and `demo` (sidecar injection enabled).
- Two ECR repositories: `predictive-autoscaler/ml-api` and `predictive-autoscaler/operator`.
- `deploy.sh app` then adds: the Istio PodMonitor and the ServiceMonitors, the three Grafana dashboards, the ml-engine stack from `../../k8s-manifests/base` through the kustomize overlay (images from ECR, Prometheus as the metrics backend, gp3, trimmed resource requests, training targets), and the demo namespace with three identical nginx Deployments scaled three ways — `nginx-test` by the operator with forecasting on, `nginx-reactive` by the same operator with forecasting off (`spec.prediction.enabled: false`, the matched control), and `myapptwo` by KEDA with a single Prometheus trigger on the same request-count definition (the system comparison) — each with `min 1 / max 12` replicas and the same per-replica target (10 req/s), plus one long-running k6 generator per app that follows the daily pattern as a wall-clock-aligned step schedule and pushes its own metrics to Prometheus.

No load balancers are created; Grafana, Prometheus, and the ml-api are reached with `kubectl port-forward`.

## Cost (ap-southeast-1, on-demand, list prices)
Per day: EKS control plane (standard support) 2.40 + two t4g.medium 2.04 + two public IPv4 addresses 0.24 + gp3 (33 GiB of PVCs and two 30 GiB root volumes) ≈ 0.27 ≈ **about 5 USD/day** before variable charges (CPU surplus credits under the t4g Unlimited mode at 0.04 USD per vCPU-hour, cross-AZ traffic, ECR storage, data transfer), which are read from Cost Explorer after a run. Spot capacity is deliberately not used: a multi-day forward-looking evaluation must not be interrupted.

## Usage
```sh
cp terraform.tfvars.example terraform.tfvars   # set grafana_admin_password and tags
export AWS_PROFILE=<your profile> AWS_REGION=ap-southeast-1
./deploy.sh all        # ≈ 25 minutes: infra (two-stage terraform apply), images, app
./deploy.sh status
```
Adopting ECR repositories that already exist:
```sh
terraform import aws_ecr_repository.ml_api   predictive-autoscaler/ml-api
terraform import aws_ecr_repository.operator predictive-autoscaler/operator
```
Images are built for the node architecture (`IMAGE_ARCH=arm64` by default; set `amd64` with an x86 instance type) and tagged `bench-<git short sha>` (override with `IMAGE_TAG`).

## Differences from the manifests in `examples/`
The demo manifests under `demo/` are the repository examples adapted to two small nodes and to a controlled comparison:
- Traffic: the 24-hour pattern is divided by ten (night 150–300 req/min, peak 6,000 req/min at 15:00 UTC) and keyed by **UTC hour**; each app has one long-running k6 **Deployment** (`ramping-arrival-rate`, a 1-second ramp at every UTC hour boundary then a hold, schedule computed from the wall clock at start so restarts re-align) that pushes its metrics to Prometheus by remote write (`experimental-prometheus-rw`; the Prometheus receiver is enabled in `helm/kps-values.yaml`). Hourly load summaries (delivered vs planned, dropped iterations, failures, destination-observed requests) are derived from Prometheus, never from logs: `scripts/collect-k6-summaries.py --port-forward` prints the last completed UTC hour's gate (add `--hour H --date YYYY-MM-DD` for another hour, `--generator-start` for a partial first hour). Hourly Jobs were replaced because their restart at the top of each hour left a traffic gap that biased every `:00` sample of the ten-minute series low. Generator pods carry no sidecar; requests are counted by the destination sidecar.
- One request-count definition everywhere: `sum(rate(istio_requests_total{reporter="destination",destination_workload="<app>",destination_workload_namespace="demo"}[1m])) * 60` — in the forecasting service's collector, the operator's reactive rule, the KEDA trigger, and the scorer.
- Bounds and targets identical for all three apps: min 1, max 12, 10 requests/s per replica (so the daily pattern needs 1 to 10 replicas). The KEDA comparison uses only the Prometheus trigger; its CPU and memory triggers were removed on purpose.
- The `nginx-test` ScaledObject is present but paused so the operator is the only controller of that Deployment.
- nginx pods request 10m CPU / 16Mi; the forecasting service, operator, and training job carry measured requests set in the overlay; the training CronJob is co-located with the forecasting service (shared ReadWriteOnce model volume) by pod affinity.
- The VictoriaMetrics endpoints in the base manifests are pointed at Prometheus (the collector and the operator use only the Prometheus-compatible `/api/v1/query` and `/api/v1/query_range` endpoints).

## Evaluation
The operator records every forecast at issuance — Prometheus series `predictive_autoscaler_forecast_rpm{step}`, `..._forecast_target_timestamp_seconds{step}`, `..._forecast_issued_timestamp_seconds`, `..._model_trained_timestamp_seconds`, `..._model_info{model_name,model_version}` and a JSONL line on its own small volume (`FORECAST_LOG`) — so each horizon step can be scored against the observation at its own target time. The protocol, the scorer (`score.py` with known-answer tests), and the recorded runs live outside this repository until a run is complete.


### Hybrid arm: validation-selected blend weight (2026-09-28)

The hybrid forecaster serves a blend of its network and a previous-day pattern lookup. The trainer now
attaches the series as the lookup's history, scores the *served* blend on the outer held-out partition
for four candidate pattern weights (the deployed ramp 0.70→0.908, 0.85, 0.95, pattern-only) and
persists the lowest-MAE weight on the artifact (`pattern_weight_override`, honoured by `predict()`).
Ties keep the earlier candidate, so the deployed ramp is only replaced by a strict improvement. The
provenance sidecar records every candidate (`blend_selection`) and the evaluation before selection;
the status line reports `scored` (served_blend or raw_network), `network_only_mae` and `blend`.
Motivation: on the benchmark series the network's held-out MAE swung between 232 and 2,318 rpm across
trainings while the pattern alone scored tens of rpm, so at 9–30 % weight the network was adding error
(`training/blend_selection.py`, tests `ml-engine/tests/test_blend_selection.py`).

#### 2026-09-30: stable selection rule `margin-lb-pooled-hysteresis-1`

The lowest-MAE rule above chose on one held-out partition of ~50 strongly autocorrelated origins and moved between
pattern_only, 0.95 and the deployed ramp across the live trainings of 2026-09-28/29; the model lab's block bootstrap put
the 12Z and 18Z choices near coin tosses, and the network's gains were level shrinkage and a chance bias cancellation,
not forecasting. `training/blend_selection.py` now ports the lab's rule (13 vectors reproduced to 1e-9): pattern_only
is the default; a network candidate is chosen only if it beats pattern_only by at least 5 % of the MAE pooled over the
current and the previous two held-out partitions **and** the one-sided 90 % moving-block-bootstrap lower bound of that
gain is above zero; an incumbent network blend is kept while its pooled gain stays non-negative unless a challenger
beats it the same way. The per-origin evidence comes from `evaluate(..., return_origin_errors=True)` and is chained
through the provenance sidecar (`blend_selection.stability.origin_errors` / `pooled_history`); a sidecar written
under another rule contributes nothing. The same change fixes a latent `evaluate()` bug: after a sequence with an
imputed target was dropped, later pattern lookups used an origin shifted back by the number of drops (no live
partition had an imputed test slot). On the five live partitions of 09-28/29 the rule would have chosen pattern_only
every time, so the hybrid arm is expected to serve its pattern until a network earns weight on pooled evidence.

#### 2026-09-30: the network trains with `tanh` (trainer setting, D-1068)

Of 26 live trainings with the shipped ReLU BiLSTM (no gradient clipping, unseeded), 7 ended with non-finite loss and 4
collapsed (final training loss 0.31-0.40, network MAE 1,470-2,318 rpm). A declared experiment on three live windows x
four seeds reproduced a non-finite ReLU run; gradient clipping (clipnorm 1.0) did not prevent it; `tanh` had zero
failures and the lowest mean network MAE (192.5 vs 279.3 rpm). The trainer now passes `activation="tanh"`
(`training_activation()`, override `TRAIN_ACTIVATION`); the model class default is unchanged for other callers. Even
healthy networks did not beat the plain pattern on these windows, so the stable blend rule keeps deciding whether the
network gets any weight.

### Arm R1: relative-residual profile-AR forecaster (declared 2026-09-29)

A third declared forecaster for the ensemble route, `models/relative_profile_ar.py`
(`relative-profile-ar-1.0.0`): relative residual `q = y / profile - 1` against the seven-day same-slot
profile, one least-squares AR(3) per 6-hourly generation on the trailing seven days of `q` (training
copy clipped to +/- 50 %, unclipped seed at the origin), `y_hat = max(0, profile x (1 + q_hat))`; no
Holt-Winters, no optimiser, one component. It is the model lab's `pr_ar_rob`; the lab's test vectors
(three eight-day series, five origins, AR coefficients per generation) are reproduced to 1e-6 by
`ml-engine/tests/test_relative_profile_ar.py`. An `ENSEMBLE_EXPERIMENT` entry selects it with
`"forecaster": "relative-profile-ar"`; its margin (`margin_mode`, `margin_quantile`) is computed from
its own past leads (`seasonal_ensemble.margin_at(raw_fn=...)`), never from the ensemble's. The
declared arm configuration is `experiments/relative-profile-ar-v1/` (R1 = relative q90, replacing S1
on `nginx-seasonal` after one complete hybrid = S1 A/A day). The issuance log line carries
`"forecaster"`, and the E1/E2 configuration hashes are unchanged by the new field (pinned in the tests).

## Versions
Pinned and used for the recorded runs (2026-09-20): Terraform aws provider 5.100, kubernetes 2.38, helm 2.17; modules terraform-aws-vpc 5.21, terraform-aws-eks 20.37, iam-role-for-service-accounts-eks 5.60; charts istio 1.30.4, keda 2.20.2, kube-prometheus-stack 91.4.1. Latest available on that date, not adopted because they change module inputs or are not yet published as stable charts: terraform-aws-eks 21.25, terraform-aws-vpc 6.7, aws provider 6.65, Istio 1.31.0 (charts only at rc/beta in the Istio Helm index).

## Teardown
```sh
./deploy.sh destroy
```
Deletes the PVC-bearing namespaces first so the EBS volumes are released, then destroys everything Terraform created, including the ECR repositories (`force_delete = true`).
