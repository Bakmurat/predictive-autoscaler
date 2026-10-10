# Documentation

| Page | For |
|---|---|
| [getting-started.md](getting-started.md) | Build, install and uninstall the current version from source. |
| [concepts.md](concepts.md) | Components, how one scaling decision is made, units, the evidence ledger. |
| [configuration.md](configuration.md) | Every `PredictiveAutoscaler` field (and which ones are ignored), effective defaults, operator settings. |
| [coexistence.md](coexistence.md) | Running next to HPA/KEDA/VPA, the paused-KEDA fallback pattern, GitOps. |
| [helm.md](helm.md) | Installing with the Helm chart: required values, permissions, the CRD under Helm and Argo CD, network policies. |
| [upgrading.md](upgrading.md) | Moving autoscalers from the legacy API group `autoscaler.example.com` to `autoscaling.devkuban.com`. |
| [limitations.md](limitations.md) | What the current version does not do, most important first. |
| [RESEARCH-2026-09-22.md](RESEARCH-2026-09-22.md) | Literature review behind the design. |

Status: research prototype, tested only in test environments with generated load; no published performance figures.
