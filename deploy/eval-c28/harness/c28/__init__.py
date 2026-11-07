"""C2.8 evaluation harness: fixture manifests, load plans, run-lock, evidence collection, reduction and scoring
(EVAL-C28-MANIFEST). Offline code; the only network access is the evidence collector's read of a Prometheus-compatible
query API and the quota injector's guarded kubectl calls, both bound to a verified run-lock."""
