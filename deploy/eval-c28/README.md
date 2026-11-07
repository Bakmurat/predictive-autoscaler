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
python -m c28 collect --runlock runlock.json --query-api http://<observer> --namespace demo --targets c28-pa-n-s20 \
  --start 2026-11-20T00:00:00Z --end 2026-11-21T00:00:00Z --out bundle/ --file ledgers=decisions.jsonl
python -m c28 reduce --bundle bundle/ --target c28-pa-n-s20 --block-start 2026-11-20T00:00:00Z --capacity 35
python -m c28 score --study study.json
```

Arms. Every arm runs as its own Deployment (`c28-<arm>-<s20|s90>`, so each has a new UID) and has exactly one writer:

- **H-CPU:** an autoscaling/v2 HPA on CPU utilization.
- **H-RPS:** a KEDA ScaledObject on the target's offered RPS, `AverageValue` = c, `ignoreNullValues=false`. Its generated HPA is the writer.
- **PA-R, PA-N, PA-E:** PredictiveAutoscalers on the statistical engine with capacity c, scale-down headroom 0, the frozen calibration and caps, and the profile's startup budget.
  - PA-R sets the predictive extra to 0, so it is reactive on the same Go policy and grid.
  - PA-N carries the `c28.devkuban.com/family-restriction: seasonal_naive` annotation. It needs the C2.8a mechanism, and the scorer reports NOT READY without it.

All PredictiveAutoscalers render in Recommend mode; activation is the executor's gated step. HPA and KEDA keep their native cadence on prodcluster (section 11.3).

Faults are scheduled. A ConfigMap written before each block lists absolute UTC windows, so ConfigMap propagation never moves a boundary.

- **Metrics and forecast outages:** the `tcpproxy` routes refuse and cut connections during their windows.
- **Slow start:** decided by each server process at start.
- **Quota block:** the one live mutation. The `quota` command patches the PriorityClass-scoped ResourceQuota after checking the cluster UID; without `--apply` it is a dry run.

Open items before a confirmation run (also in the PR):

- The forecast route is a TCP pass-through. With forecaster TLS on, the operator's forecaster URL must be a name in the certificate.
- Kubernetes object traces (kube-state-metrics series) and resource/cgroup samples are queried with the default query set. Adjust the queries to the observer and freeze them in the run-lock.
- The family restriction (PA-N), the E1 policy-cost binding and the lab-qualification mechanism are prerequisites owned elsewhere (C2.8a, manifest section 1).
