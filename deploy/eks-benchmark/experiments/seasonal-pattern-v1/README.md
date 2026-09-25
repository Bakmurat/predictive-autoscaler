# S1: shared-input seasonal component experiment

This opt-in, pre-T0 experiment adds `demo/nginx-seasonal` alongside the three
existing workloads. It is a separate descriptive arm, not a change to their
algorithms or to the preregistered benchmark. There are no performance results
or efficacy claims in this declaration. Formal scoring requires a later human
T0 decision and an explicit protocol covering participating arms.

## Frozen behavior

Set the API's `SEASONAL_EXPERIMENT` environment variable to the JSON object in
`api-config.json`. Unset means the existing routes are unchanged. Invalid or
unknown configuration fails startup. Only the exact application, namespace and
`requests` metric match. Caller-supplied history is refused on this route.

S1 fetches **nginx-test's observed history in demo** and uses its currently loaded
checkpoint/scaler, with `pattern_weight_override=1.0`. It keeps the existing
seasonal calculation, masking, bounded input gap filling, adaptive percentile
and support-based confidence. Every requested step must have finite seasonal
support; otherwise the API refuses the whole forecast with HTTP 422. It must
never silently substitute a neural value. The neural computation can still run
for diagnostics; this experiment makes no compute-saving claim.

The source model wrapper is copied under its lock, and request state is assigned
only on that copy. S1 never triggers checkpoint reload; the normal source route
owns that lifecycle. The genuine checkpoint SHA remains `artifact_sha256`.
`model_version` carries the experiment id, canonical configuration SHA256,
source model key and full checkpoint SHA. The API also returns explicit
experiment metadata. The operator persists the version and checkpoint hash;
it does not persist the additional API metadata, so archive this declaration
and exact configuration with the deployment receipt.

S1's error tracker is keyed to nginx-seasonal but matures against **source
nginx-test actuals**, including its adaptive-percentile feedback. Its percentile
can therefore differ from the baseline's. The unchanged operator's reactive
rule measures **nginx-seasonal's own traffic** and remains a max() safeguard.
The same controller owns this fourth Deployment; do not add a second controller,
HPA or KEDA owner. Replica limits, lead window and per-replica target match the
existing predictive arm. The UTC k6 script is shared, with separate target tags.

## Observation plan and limitations

Freeze the code commit, API image digest, configuration hash, source artifact,
manifest diff, load-script hash, placement, validity mask and launch timestamps.
Preserve all pre-change records. API/node/workload changes restart qualification;
require two new complete clean hours and a new freeze before proposing T0.
Never backdate T0 or change an acceptance bar after examining results.

For each closed observation interval, report all six forecast steps by UTC
traffic phase, with sample counts, MAE and signed error, on both:

1. Common source nginx-test actuals, for a shared-input component comparison.
2. S1's own delivered traffic, for operational behavior of its workload.

Report forecast refusals, reactive fallbacks, missing observations and coverage
as outcomes. Also compare decision attribution, available/desired replicas,
replica time, request errors/latency, dropped k6 iterations and measured resource
use against the original predictive and reactive arms. Do not infer causality
from quiet-period success, one repeated synthetic trace, or unlike traffic.
No shortage/replica margin is adopted by this declaration.

All forecast consumers must select **application AND namespace before grouping
by artifact hash**: this experiment intentionally shares a checkpoint. The
public scorer already does this and has a mixed-arm regression. Historical
replay inputs/harnesses frozen before S1 must not be extended to a mixed log
without this selection and new provenance receipts.

## Capacity and deployment runbook

This arm requires three `t4g.medium` nodes in the personal Singapore cluster.
The experiment capacity override is `capacity.tfvars` (min 2, desired/max 3).
The existing node group's desired size may be ignored by Terraform lifecycle;
review the actual plan and reconcile the recorded EKS scaling update. Do not
blindly apply Terraform or change other resources. The approximate incremental
cost is $1.20/day including a 20 GiB root disk and public IPv4, before variable
charges. Record the user's cost notice before provisioning.

All four app Deployments use hostname spread, maxSkew 1, DoNotSchedule. At each
12-pod ceiling, each node has at most four pods per app. Each app pod requests
35m CPU/64Mi including the native Istio sidecar. The fourth generator requests
100m/160Mi and must run on the added node labelled
`predictive-bench/seasonal-generator=true`. Verify actual node allocatable,
daemon/system requests and training overlap before starting load. The inspected
budget covers one new-arm surge, not simultaneous rollouts of all four apps.
The original workloads can spread onto the new node; this is an explicit change
to the benchmark environment requiring requalification.

Apply in this order, recording bounded command results and before/after state:

1. Add the node, wait for Ready and label it. Recheck capacity including training.
2. Build only the API, using the deployed immutable image digest as the dependency
   base and overlaying the reviewed API source. Test that image, publish its
   digest, and preserve the previous Deployment for rollback. Trainer/operator
   images remain unchanged. Do not run the generic deployment script.
3. Render the live API Deployment with only image and `SEASONAL_EXPERIMENT`
   changes. Inspect `kubectl diff`; retain `strategy: Recreate`. Apply and check
   health plus the normal source route's forecasts.
4. Apply the fourth service and app, wait for Ready. Replace `VM_WRITE_URL` in the
   generator manifest with the existing generators' exact remote-write URL,
   then fail the pre-apply check if `VM_WRITE_URL` remains anywhere in the rendered
   manifest (`if rg -q VM_WRITE_URL rendered.yaml; then exit 1; fi`). Apply the
   generator and fourth PredictiveAutoscaler. Verify no duplicate
   owner, no extra generator, and ordinary controller-produced records for both
   applications over at least two forecast refreshes.
5. Preserve archive/forecast receipts, node placement and rollout times. Begin
   fresh qualification and the closed-interval observation plan above.

Include S1's model-age gauge in every hourly check: if the source autoscaler
stops calling, S1 deliberately does not reload and its checkpoint will age.

The dedicated label must be verified/reapplied if that node is replaced. A
missing label intentionally leaves the generator Pending; monitor that as a
failure rather than removing its placement constraint.

Rollback removes the fourth generator, PredictiveAutoscaler, app and service,
then restores the recorded previous API Deployment with Recreate. Remove only
the experiment label. Reduce capacity after checking that original workloads
and training fit, and record the resulting new qualification boundary. Never
delete the model or evidence PVCs, archived artifacts, or original workloads.
