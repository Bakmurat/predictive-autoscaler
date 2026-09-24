# Shadow observations

`ShadowObserver` is an offline comparison adapter. It is not called by the live API or
controller, and it does not change replicas. Version 2 keeps the controller's recorded
`forecasts[].rpm` as `served_hybrid`, even when history or a model cannot be loaded.

Model components are reconstructed through the actual model's `predict()` method. The
adapter requires an exact, finite ten-minute input grid, matching the artifact's sequence
length, and validates the recorded and returned forecast targets. It transforms the input
with the artifact's fitted scaler, supplies explicit calendar timestamps and a timestamped
seasonal Series, and uses a shallow request-local model wrapper. The fitted scaler and
network are shared read-only for inference; request fields on the loader-owned wrapper are
not changed. The loader must return the artifact identified by the full hash in the record.

`history_lookup(start, end)` must supply **masked, genuinely observed** values, with UTC
meaning for naive timestamps. This adapter never fills gaps. Incomplete or ambiguous input
makes model components unavailable. It cannot reproduce an issuance that depended on filled
inputs; use the separate exact-runtime replay with the recorded preprocessing for that case.

The seasonal component also depends on contemporaneous error feedback. Supply
`replay_state_lookup(issuance)` returning, for example:

```python
{"mape_for_floor": 12.3, "source": "verified chronology record identifier"}
```

The value must be finite and nonnegative, and the source nonempty. Without it, `raw_network`
can still be reconstructed, but `seasonal_only` is unavailable. No state-dependent prediction
from an assumed default is presented as historical. The source and value are recorded in
`reconstruction`; a state supplied by chronological replay does not make this an independent
verification of that replay. `previous_day` remains the simple observed-yesterday reference,
not the state-dependent seasonal component.

A reconstruction is not automatically proof of a match to historical serving. Retain input,
artifact, state and transfer provenance, and use the exact-runtime replay/match gate before
attributing historical bias. `max_seconds` records an elapsed-time warning after work; it
is not an interrupting timeout.

## Version 1 limitation

Version 1 fetched the input window but did not put its scaled values into the model, and
passed seasonal pairs without their timestamp index. It also recomputed `served_hybrid`
instead of preserving recorded values. Treat its model-component and served-hybrid fields
as invalid for historical comparisons. Preserve old records; do not silently relabel them
as version 2. Persistence and previous-day fields require their own input-provenance checks.

The benchmark's separate exact-runtime replay does not use this module and is unaffected
by this defect.
