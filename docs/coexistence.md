# Coexistence with other autoscalers

A Deployment's replica count must have exactly **one owner**. Kubernetes does not arbitrate between controllers: if two
of them write `spec.replicas` (or the `/scale` subresource) for the same Deployment, each overwrites the other on its next
sync and the workload flaps between their answers.

## What the operator does (this version)
- **It refuses to share the replica count.** On every reconcile, and therefore before every write, the operator reads
  straight from the API server (no cache) every other object that can own the target's replica count:
  - a HorizontalPodAutoscaler whose `scaleTargetRef` is the Deployment, whatever its name (KEDA's generated HPA is
    `keda-hpa-<scaledobject>` unless the ScaledObject sets a custom name);
  - a KEDA ScaledObject on the Deployment, unless it is paused with `autoscaling.keda.sh/paused: "true"`.
    `autoscaling.keda.sh/paused-replicas` makes KEDA hold that count, so it is a writer; the directional pauses
    (`paused-scale-in`, `paused-scale-out`) are not a pause;
  - another PredictiveAutoscaler in `Active` mode on the same Deployment (both refuse; neither wins).

  In `Active` mode, while any of them exists the operator writes nothing. The decision is still computed and published;
  the status shows `ConflictDetected=True` (reason `ReplicaWriter`, the message names the objects) and
  `ScalingActive=False` (reason `Conflict`); one `ConflictDetected` warning event is emitted; and the decision ledger
  records `action: conflict_hold` with the list of conflicts. When the other writer is removed or paused, the next
  reconcile scales again and emits `ConflictResolved`. The scale-down stabilization window starts again after a conflict.
- **A failed check blocks like a conflict.** These all count as a failed check:
  - missing permissions;
  - an API discovery failure;
  - an API that is advertised but cannot be read (a 404 or a 503 included);
  - KEDA or the VPA served only in another version than the one the operator reads (`keda.sh/v1alpha1`,
    `autoscaling.k8s.io/v1`);
  - a check that does not finish within its deadline (10 s for the check, 5 s for the guard below).

  A failed check sets `ConflictDetected=Unknown` and `ScalingActive=False`, both with reason `CheckFailed`, and emits a
  `ConflictCheckFailed` warning. KEDA or the VPA not being installed is not a failure, but the operator accepts absence
  only when a fresh API discovery succeeds and shows the API is not served. KEDA installed later is noticed at the next
  reconcile, without restarting the operator.
- **VerticalPodAutoscaler:** a VPA on the target in any mode except `Off` is reported as `VPAInterference=True` with a
  warning event, and scaling continues. No mode means `Auto`. `Initial` also changes requests (on pods created by
  scale-ups), and the in-place modes change running pods without recreating them. A VPA does not own the replica count,
  but changing requests changes the per-pod capacity that `targetRPS` assumes. Tune the two together, or use `Off` to keep
  only the VPA's recommendations. When the check could not complete, the condition is `Unknown` (reason `CheckFailed`)
  rather than `False`.
- **Recommend mode** (the default) reports conflicts in the same conditions and writes nothing anyway. A
  PredictiveAutoscaler in Recommend mode can therefore run next to an existing HPA or KEDA object, so you can compare the
  two before a hand-over.
- **Last-moment guard:** immediately before a write, the operator re-reads the PredictiveAutoscaler and the Deployment
  (uncached). It aborts the write if the PredictiveAutoscaler was deleted, re-created, edited (a new generation) or
  switched out of `Active`, or if the Deployment was replaced. The abort is recorded as `action: guard_abort` and shown as
  `ScalingActive=False` with reason `GuardAborted`.
- **Same namespace only:** `targetDeployment.namespace` must equal the PredictiveAutoscaler's namespace. Otherwise the
  status shows `Ready=False` with reason `CrossNamespaceTarget`, and nothing is written.
- **Limits:**
  - The check is not atomic across objects. A scaler created between the check and the write (milliseconds) is caught at
    the next reconcile, not before that write.
  - HPAs and ScaledObjects are not watched yet, so a new conflict shows in the status at the next reconcile (one
    `updateIntervalSeconds`).
  - Writers of other kinds are not detected: a CI job, someone running `kubectl scale`, or a GitOps tool applying
    `replicas`.
- **GitOps:** the operator writes the replica count by updating the whole Deployment object. GitOps tools that own the
  Deployment manifest (Argo CD, Flux) will see drift on `spec.replicas`. Exclude that field from their comparison: for
  example, use Argo CD `ignoreDifferences` on `/spec/replicas`, or omit `replicas` from the managed manifest.
- **Leader election** exists but is off by default, so during a rolling update of the operator two instances can act at
  the same time. Run one replica with the `Recreate` strategy.

## Planned
- Replicas written only through the `/scale` subresource, re-authorized on a write conflict, and leader election on by
  default.
- Watching HPAs and ScaledObjects, so that a new conflict is reported at once.

## The paused-KEDA fallback pattern
Keep a KEDA ScaledObject for the same Deployment, **paused**, as a ready-made reactive fallback:
1. Create it with the annotation `autoscaling.keda.sh/paused: "true"`. While paused, KEDA does not scale the Deployment
   ([KEDA: pause autoscaling](https://keda.sh/docs/latest/concepts/scaling-deployments/#pause-autoscaling)).
2. Give it the same `minReplicaCount`/`maxReplicaCount` as the PredictiveAutoscaler and the same request-rate signal.
3. **Hand-over to KEDA:** first stop the PredictiveAutoscaler from writing:
   - set `mode: Recommend` (or delete the CR);
   - wait one reconcile interval;
   - check that `ScalingActive` shows `RecommendMode` and that the ledger shows no further writes.

   Then remove the annotation and confirm that KEDA's HPA (`keda-hpa-<scaledobject>`) exists and is active. Had you
   skipped the first step, the operator would have refused to scale once KEDA became active (`ConflictDetected`). It
   would not, however, have undone a write that was already in progress.
4. **Hand-back:** re-add the annotation, then verify the pause took effect:
   - the ScaledObject reports itself paused in its status conditions;
   - the ScaledObject has no `autoscaling.keda.sh/paused-replicas` annotation;
   - **no HPA of any name** targets the Deployment (`kubectl -n <ns> get hpa -o wide`, column REFERENCE). KEDA's HPA is
     `keda-hpa-<scaledobject>` unless the ScaledObject sets a custom name.

   Only then set `mode: Active` again. While such an HPA still exists, the operator refuses to scale, with
   `ConflictDetected=True`. A short period of unchanged replicas is not proof that KEDA stopped.

Never leave both active. The example in `examples/nginx-test/nginx-test-keda-fallback.yaml` follows this pattern; the
`myapptwo` example is a KEDA-only workload (no PredictiveAutoscaler) and is therefore active.

## Checklist before creating a PredictiveAutoscaler
```bash
NS=shop; DEPLOY=web-frontend
kubectl -n $NS get hpa -o wide | grep -w "$DEPLOY"                    # any HPA on it (KEDA creates one too)
kubectl -n $NS get scaledobjects.keda.sh -o yaml | grep -B2 -A8 "name: $DEPLOY"   # KEDA objects and their paused annotation
kubectl -n $NS get vpa -o wide 2>/dev/null | grep -w "$DEPLOY"         # VPA (requests/recreation)
kubectl get pa -A | grep -w "$DEPLOY"                                 # another PredictiveAutoscaler
```
After creating it, `kubectl -n $NS describe pa <name>` shows the operator's own view: the `ConflictDetected` and
`VPAInterference` conditions and their events.
