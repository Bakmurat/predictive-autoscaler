# Coexistence with other autoscalers

A Deployment's replica count must have exactly **one owner**. Kubernetes does not arbitrate between controllers: if two
of them write `spec.replicas` (or the `/scale` subresource) for the same Deployment, each overwrites the other on its next
sync and the workload flaps between their answers.

## Current behaviour (this version)
- The operator **does not detect** other scalers. It will scale a Deployment even if an HPA, an active KEDA
  ScaledObject or another PredictiveAutoscaler also targets it. Avoiding that is your responsibility today.
- It writes the replica count by updating the whole Deployment object. GitOps tools that own the Deployment manifest
  (Argo CD, Flux) will see drift on `spec.replicas`; exclude that field from their comparison (for example Argo CD
  `ignoreDifferences` on `/spec/replicas`, or omit `replicas` from the managed manifest).
- Leader election exists but is off by default, so during a rolling update of the operator two instances can act at the
  same time. Run one replica with the `Recreate` strategy.

## Planned behaviour (decided 2026-10-09, not yet implemented)
- **Refuse by default:** when another active replica writer targets the same Deployment (an HPA, a KEDA ScaledObject that
  is not paused, another PredictiveAutoscaler), the operator makes no change, sets a `ConflictDetected` condition and emits
  an event until the other scaler is removed or paused. Errors while checking (missing permissions, API discovery
  failures) also block scaling.
- A VerticalPodAutoscaler in `Auto`/`Recreate` mode changes resource requests and recreates pods rather than owning the
  replica count; it is reported as a warning, not a conflict.
- Replicas are written only through the `/scale` subresource, and leader election is on by default.
- `Recommend` mode (implemented, the default): the operator computes and publishes the replica count it would set
  without writing to the target or any scaler, so it can run next to an existing HPA or KEDA object. Use it to compare
  before handing a workload over (`mode: Active`).

## The paused-KEDA fallback pattern
Keep a KEDA ScaledObject for the same Deployment, **paused**, as a ready-made reactive fallback:
1. Create it with the annotation `autoscaling.keda.sh/paused: "true"`. While paused, KEDA does not scale the Deployment
   ([KEDA: pause autoscaling](https://keda.sh/docs/latest/concepts/scaling-deployments/#pause-autoscaling)).
2. Give it the same `minReplicaCount`/`maxReplicaCount` as the PredictiveAutoscaler and the same request-rate signal.
3. **Hand-over to KEDA:** first stop the PredictiveAutoscaler from writing — delete the CR (or scale the operator to 0)
   and wait until the operator has quiesced: deleting a CR does not cancel a reconcile already running, so wait at least one
   reconcile interval and check that the operator log shows no further decisions for the workload. Then remove the
   annotation and confirm KEDA's HPA (`keda-hpa-<scaledobject>`) exists and is active.
4. **Hand-back:** re-add the annotation, then verify the pause took effect — the ScaledObject reports itself paused in its
   status conditions and the generated HPA is gone (`kubectl -n <ns> get hpa keda-hpa-<scaledobject>` → NotFound). Only then
   re-create the CR. A short period of unchanged replicas is not proof that KEDA stopped.

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
