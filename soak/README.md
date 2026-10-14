# Soak: extended controlled mechanics validation

The soak runs the Helm chart on a small, long-lived Kubernetes cluster for days, under generated traffic. It checks
that the autoscaler's mechanics hold up over time and through injected failures. **It makes no claim about forecast
accuracy, cost or performance against other autoscalers**, and it says nothing about high availability of the
infrastructure. One laptop running kind cannot establish either. The research benchmark is a separate, predeclared
study (see `docs/` and the project's evaluation protocol).

These checks are **declared before the first soak run**. A check that fails is reported as failed, together with its
evidence. A check is never redefined after its result is seen.

## Setup
- **Cluster:** kind (Kubernetes 1.35.x), 1 control plane + 2 workers, on one machine (4 threads, 11 GiB). The chart is
  installed from a recorded commit, and the images are built from that commit and loaded into kind (no registry).
- **Metrics:** a single VictoriaMetrics instance (the Prometheus query API). The demo replicas' request counters are
  imported every 15 s. Every soak sample is labelled `origin="soak"`. Injected synthetic history is labelled
  `origin="synthetic"` and is never mixed into a check's observed series.
- **Workloads** in namespace `pa-soak`, all the same HTTP demo app:

  | Workload | Autoscaler mode |
  |---|---|
  | `active` | Active |
  | `recommend` | Recommend |
  | `conflict` | Active, plus a deliberate HPA during check C2 |

- **Traffic:** a 24-hour diurnal cycle with two short spikes per day, at a demonstration capacity (`targetRPS`). Its
  purpose is to exercise scale-up and scale-down repeatedly, not to model a real service.
- **Training:** serialized and at most one at a time. Models live on the chart's PVC.

## Recorded throughout
- **Decisions:** for every decision on the `active` workload: decision time → `/scale` write → all replicas Ready.
- **Model identity:** for every model the forecaster serves, the artifact sha256, training cutoff and query hash. Each
  training run's outcome.
- **Signals:** the operator and forecaster errors, conditions and events.
- **Resources:** container CPU throttling, memory and restarts; PVC and node disk growth.
- **Evidence gaps:** any interval where the harness or the metrics store was not recording is listed explicitly.
  Gaps are never filled.

## Checks (pass criteria fixed in advance)
| ID | What is checked | Injection | PASS when |
|---|---|---|---|
| C1 | Recommend makes zero writes | none; the whole soak | `recommend`'s `spec.replicas` and generation change only through the harness's own setup, and the operator's `/scale` writes on it are 0 |
| C2 | Refusal next to another replica writer, and recovery | create an HPA on `conflict`; delete it 30 min later | within 2 reconciles: `ConflictDetected=True` and no operator write while the HPA exists. After deletion: `ConflictResolved`, and writes resume within 2 reconciles |
| C3 | Telemetry loss → hold | scale VictoriaMetrics to 0 for 15 min | no replica change on `active` during the loss; `TelemetryAvailable=False`; normal decisions resume within 2 reconciles of recovery |
| C4 | Restart continuity of the holds | delete the operator pod during a downward ramp | no scale-down on `active` within 10 min of the new pod's first reconcile (PR #11); scale-up is not delayed |
| C5 | Leadership handover | 2 operator replicas; delete the leader | the other replica acquires the Lease within the lease duration plus one reconcile; no interval in which both write (write audit by pod) |
| C6 | Training failure and retry | make one scheduled training fail (metrics unreachable from the trainer for its run) | the failed Job leaves the served model and its identity unchanged; the next scheduled training succeeds and publishes a new artifact |
| C7 | Query change is refused until retrained | change `active`'s query template | forecasts for the old query are no longer used (the condition says why); after a training for the new query, forecasts resume under the new query hash |
| C8 | Persistent model reload | delete the forecaster pod | the restarted forecaster loads the same artifact from the PVC (same sha256); forecasts resume |
| C9 | Resource stability over the soak | none | no OOMKill or crash-loop of the operator or forecaster; PVC and disk growth reported per day |

Injections run in a fixed order, one at a time, with at least 2 hours of undisturbed operation between them. The
order and times are logged.

## Not checked here
Forecast accuracy, comparison with HPA/KEDA, latency under real production load, multi-node failure, or anything
that depends on the lab's benchmark cluster.
