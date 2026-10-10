# Local quickstart

Run the source Helm chart on its own kind cluster. It uses a tiny Python HTTP application, a request generator and
a single-node VictoriaMetrics store implementing the Prometheus query API. No Istio, KEDA, registry or lab access is
required. API authentication, TLS and the chart's NetworkPolicies stay enabled. kind v0.33+ provides policy enforcement.

Install Docker, kind v0.33+, kubectl, Helm and Python 3. Allocate at least 6 GiB memory and 15 GiB free Docker disk.
The TensorFlow image is large: the first source build/download may take several minutes; there is no ten-minute
guarantee. The script builds local images from this checkout. To reuse known local images, explicitly set both
`OPERATOR_IMAGE` and `FORECASTER_IMAGE` to `repository:tag` references.

## Fresh install

From the repository root:

```sh
quickstart/kind.sh up
quickstart/kind.sh status
```

The workload starts at one replica. The generator sends about eight actual requests per second, and the scraper
imports each replica's request counter every 15 seconds. The PA query sums per-pod rates, with `origin="demo"`.
`targetRPS: 4` is a demonstration capacity, not a measured throughput limit. The operator recommends at least two
replicas while **Recommend leaves the Deployment at one**. `ForecastAvailable=False` is expected: there is no model
or historical data. Roughly a week of usable history and successful training are needed for forecasts; meanwhile
the reactive recommendation works.

```sh
quickstart/kind.sh active  # explicitly allow this demo's operator to write /scale
quickstart/kind.sh stop    # return to Recommend, retaining the current replica count
```

`active` waits for the applied count to match the calculation and for all demo replicas to become Ready. This fresh
path demonstrates reactive scaling. It does not pretend a forecast already exists.

## Optional immediate forecast

Use a different private state directory for a separate cluster:

```sh
export PA_QUICKSTART_STATE="$HOME/.local/state/pa-synthetic"
quickstart/kind.sh up --synthetic-history
quickstart/kind.sh active
quickstart/kind.sh status
quickstart/kind.sh stop
```

This option imports eight days of a generated daily request cycle (5–15 requests/s), continues that generated signal
and changes the PA query to `origin="synthetic"`. The HTTP workload still exists, but its request counter is not the
signal in this mode. The script runs the real trainer for two demonstration epochs, verifies a fresh forecast in
Recommend mode without changing replicas, then lets you opt into Active. Training can take several minutes; the
bounded training wait is 25 minutes and a forecast refresh can take up to seven minutes. The scheduled retraining
interval is six hours. Two epochs demonstrate the pipeline, not a production training recommendation.

Generated history proves installation, publication, authenticated inference and the scaling loop. It says nothing
about accuracy, latency, cost savings or real workload performance.

## Inspect and remove

The script prints the private kubeconfig path and unique cluster context. For manual inspection, pass both
explicitly; it never updates `~/.kube/config`. State defaults to
`$HOME/.local/state/predictive-autoscaler-quickstart` (or `$XDG_STATE_HOME/predictive-autoscaler-quickstart`). Use the
same `PA_QUICKSTART_STATE` for all commands on a run. `status` shows conditions, replica counts and last forecast.

```sh
quickstart/kind.sh down
```

This deliberately deletes **only the recorded local cluster**, including its metrics and model volume. The script
checks its saved Docker node identity; other clusters and images are kept. State and diagnostic logs remain private
in the state directory. Choose a new state directory for a new run. Existing state is never overwritten or adopted.
An installation failure leaves its cluster for inspection; inspect `last-check.log`, pod events and trainer logs.
No automatic broad Docker prune or cleanup runs. If kind fails before any node can be recorded, inspect the printed
unique name with `kind get clusters` and Docker before manually removing that failed run.

For your own workload and metrics, use [getting started](../docs/getting-started.md). For full security, persistence,
upgrade and uninstall checks, run `make smoke` on another disposable cluster; this quickstart is the shorter user path.
