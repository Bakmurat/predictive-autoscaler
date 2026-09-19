# EKS benchmark environment (Terraform)

A small, reproducible Kubernetes environment for evaluating the predictive autoscaler against a KEDA-only twin with synthetic traffic. Everything is created by Terraform and `deploy.sh`; everything is removed by `deploy.sh destroy`.

## What it creates
- VPC `10.50.0.0/16` with two **public** subnets in two availability zones, **no NAT gateway** (nodes get public IPs; egress via the internet gateway).
- EKS 1.32, public endpoint, IRSA enabled; addons vpc-cni, coredns, kube-proxy, metrics-server, aws-ebs-csi-driver (IRSA role). One managed node group: `t4g.large` (arm64, AL2023, on-demand), min 2 / desired 3 / max 3, 40 GiB gp3 root volumes. No KMS key, no control-plane logs.
- `gp3` default StorageClass.
- Istio (base + istiod), KEDA, kube-prometheus-stack (Prometheus with 15-day retention on 20 GiB gp3, Grafana on 2 GiB gp3, no alerting); namespaces `istio-system`, `keda`, `monitoring`, and `demo` (sidecar injection enabled).
- Two ECR repositories: `predictive-autoscaler/ml-api` and `predictive-autoscaler/operator`.
- `deploy.sh app` then adds: the Istio PodMonitor and the ServiceMonitors, the three Grafana dashboards, the ml-engine stack from `../../k8s-manifests/base` through the kustomize overlay (images from ECR, Prometheus as the metrics backend, gp3), and the demo namespace with `nginx-test` (predictive-scaled; its KEDA ScaledObject is present but paused), `myapptwo` (KEDA-only twin), and one k6 traffic generator per app.

No load balancers are created; Grafana, Prometheus, and the ml-api are reached with `kubectl port-forward`.

## Cost (ap-southeast-1, on-demand, approximate)
EKS control plane 0.10/h + 3 × t4g.large ≈ 0.25/h + ≈ 140 GiB gp3 ≈ 0.015/h ≈ **0.37 USD/hour ≈ 9 USD/day**. Spot capacity is deliberately not used: a multi-day held-out evaluation must not be interrupted.

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
The demo manifests under `demo/` are the repository examples adapted to a three-node cluster: traffic pattern and per-pod thresholds divided by ten (peak 6,000 requests/min; `targetRPS` 30; KEDA Prometheus trigger 30/15), nginx CPU request 20m, both apps capped at 40 replicas, one k6 pod per app, traffic sent to the in-cluster Services instead of an ingress gateway, and the `nginx-test` ScaledObject paused so the operator is the only controller of that Deployment. The VictoriaMetrics endpoints in the base manifests are pointed at Prometheus (the collector and the operator use only the Prometheus-compatible `/api/v1/query` and `/api/v1/query_range` endpoints).

## Evaluation
The protocol, the scoring script, and the recorded runs live outside this repository until a run is complete; `score.py`-style scoring reads Prometheus through a port-forward and reports MAPE and MAE per 10-minute step on held-out days, plus replica time series and pod-minutes for both apps.

## Teardown
```sh
./deploy.sh destroy
```
Deletes the PVC-bearing namespaces first so the EBS volumes are released, then destroys everything Terraform created, including the ECR repositories (`force_delete = true`).
