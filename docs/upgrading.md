# Upgrading

## From the legacy API group (`autoscaler.example.com`) to `autoscaling.devkuban.com`

Before v0.1.0 the API group was the placeholder `autoscaler.example.com`. It is now `autoscaling.devkuban.com`. A
different group is a different CRD, so existing objects are **not** converted: each one is re-created in the new group,
and the hand-over is done in explicit steps with [`hack/migrate-api-group.py`](../hack/migrate-api-group.py).

**Why the order matters.** The legacy operator has no Recommend mode: it writes the replica count of every legacy
autoscaler it watches, on every reconcile. It knows nothing about the new group. The new operator, for its part, treats
every legacy autoscaler on its target as another replica writer and refuses to write next to it (see
[coexistence.md](coexistence.md)), so nothing is scaled by both. But stopping a legacy operator stops autoscaling for
**all** its targets at once. That is why every replacement is prepared and verified first, and why the tool works from
one inventory of all legacy autoscalers and all legacy operators.

### 1. Install the new version next to the legacy one

The two installs must not share any object. The legacy install used the names in `k8s-manifests/base`, in namespace
`ml-engine`: the Deployments `predictive-operator` and `ml-api`, the ClusterRole `predictive-operator-role` and its
binding. Applying `k8s-manifests/base` over it would replace the legacy operator and take away its permissions on the
legacy group before anything is migrated. **Do not do that.** Use the overlay `deploy/migration` instead:

```
kubectl kustomize deploy/migration | kubectl apply --server-side -f -
```

It installs into namespace `predictive-autoscaler`, prefixes every name with `pa-` (the ClusterRole and its binding
included), points the operator at its own forecasting service, and leaves cold start off. The two operators also elect
on different Leases (`predictive-autoscaler.autoscaling.devkuban.com`; the legacy one used
`predictive-autoscaler-leader`), so both run side by side. Set the training CronJob's `TRAINING_TARGET` to the
replacement you train first (step 4). Its RBAC can read every namespace, as the legacy one could.

### 2–7. The hand-over

| Step | Command | Changes |
|---|---|---|
| 2. Record the inventory | `hack/migrate-api-group.py plan --legacy-operator <ns>/<deployment> [...] --out inventory.json` | nothing (writes the file) |
| 3. Create the replacements | `hack/migrate-api-group.py prepare --inventory inventory.json` | creates new-group autoscalers in **Recommend** mode |
| 4. Train for the new autoscalers | the training CronJob with `TRAINING_TARGET=<namespace>/<name>`, once per replacement | models |
| 5. Check every replacement | `hack/migrate-api-group.py verify --inventory inventory.json` | nothing |
| 6. Stop the legacy operators, delete the legacy autoscalers | `hack/migrate-api-group.py cutover --inventory inventory.json` | scales every legacy operator to 0, then deletes the legacy autoscalers |
| 7. Activate, one autoscaler at a time | `hack/migrate-api-group.py activate --inventory inventory.json <ns>/<name>` | sets `mode: Active` |
| 8. Remove the legacy install and CRD | `kubectl delete` of the legacy install (namespace `ml-engine` and the cluster-scoped `predictive-operator-*` objects), then `kubectl delete crd predictiveautoscalers.autoscaler.example.com` | removes the legacy install |

What each step checks:
- **plan** records every legacy autoscaler and every legacy operator Deployment you name. Name **all** of them
  (`--legacy-operator` is repeatable), because cutover deletes every legacy autoscaler.
  - For each autoscaler it records the namespace, name, UID, generation and spec, plus the replacement it maps to.
    The replacement lives in the target's namespace, because the new operator requires the same namespace.
  - It refuses two legacy autoscalers that would map to one replacement (pass `--rename NS/NAME=NEWNAME`), and an
    existing new-group object holding a replacement's name that is not that replacement.
  - It prints GitOps ownership markers that cutover will ask about.
- **Every later step first checks that the cluster still matches the inventory.** That means no legacy autoscaler
  added, recreated (new UID) or edited (new generation), and the legacy operator Deployments unchanged. Otherwise it
  stops: run `plan` again.
- **prepare** copies each legacy spec with `mode: Recommend`, and records its provenance in the annotations
  `autoscaling.devkuban.com/migrated-from` (source namespace/name) and `autoscaling.devkuban.com/migrated-from-uid`.
  - It drops the server metadata, the status, `kubectl.kubernetes.io/last-applied-configuration`, and possible
    GitOps ownership: `argocd.argoproj.io/*` annotations, and the labels `argocd.argoproj.io/instance`,
    `kustomize.toolkit.fluxcd.io/name`, `helm.toolkit.fluxcd.io/name`, `app.kubernetes.io/instance` and
    `app.kubernetes.io/managed-by`.
  - It never changes a legacy object, and it refuses an existing object without that provenance.
- **Training is needed again.** Models are bound to the autoscaler's UID and compiled query, so the legacy models are
  not served for the new objects. Until the first training succeeds, a replacement reports "model unavailable" and
  the legacy operator keeps scaling.
- **verify** requires, for every replacement:
  - the provenance of its recorded source, the recorded target, and the same value for every field the legacy spec
    set;
  - `mode: Recommend` in both spec and status;
  - its current generation observed, with `Ready` and `TelemetryAvailable` True for that generation;
  - `ForecastAvailable` True as well (`--allow-reactive` waives only this one);
  - `ConflictDetected` either False, or naming only inventoried legacy autoscalers (from `status.conflicts`);
  - a recommendation in its status.
- **cutover** changes nothing until the inventory matches and every replacement verifies. It also refuses while the
  legacy autoscalers or the legacy operator Deployments carry possible GitOps ownership, which would restore them:
  Argo CD or Flux markers, or `app.kubernetes.io/instance` (Argo CD's label tracking uses it; so does Helm). Remove
  them from Git or suspend the sync, then pass `--gitops-handled`.
  - It scales each legacy operator to 0, bound to the resourceVersion it inspected, and waits until none of their pods
    remain (`--timeout`, default 300 s). Every read of a legacy operator Deployment must still show its recorded UID,
    so a Deployment recreated under the same name is never scaled or trusted.
  - Then it checks the inventory and every replacement **again**.
  - Before each delete it checks that no legacy operator pod has come back, that the legacy autoscaler is still the
    recorded one, and that its replacement still verifies. It deletes with UID and resourceVersion preconditions, so
    the API server refuses anything that changed after the check. It then waits until that object is actually gone:
    a finalizer can hold it, and then cutover stops before the next one.
  - The targets keep their current replica counts in between. Running cutover again after an interruption continues
    where it stopped.
- **activate** requires the replacement to be in the inventory and the cluster to match it, refuses while any legacy
  autoscaler still targets the Deployment, and checks the replacement as verify does, now with no conflict at all. A
  replacement that is already Active (activate run again) must still have its recorded provenance, target and
  configuration.
  - Its patch to `mode: Active` carries the inspected UID and resourceVersion. A concurrent change makes it check
    again, at most three times.
  - Then it waits until the operator reports `mode: Active` with no other replica writer, for the same object (UID)
    it patched, still with the recorded target and configuration.

Put the new autoscalers in Git (or wherever your manifests live) before activating them, so that a sync does not undo
the change.

Exit status of the tool: 0 done, 1 a check failed (nothing further was changed), 2 usage. It uses `$KUBECTL`
(default `kubectl`) and `--context`.

### History
The 2026-10 benchmark campaign on prodcluster ran on `autoscaler.example.com`. To reproduce a past run, check out its
pinned commit rather than the renamed tree.
