"""Shared builders for the demonstration-qualification tests: a reviewed qualification and the contract it names."""

import json
from datetime import datetime, timezone

from forecasting import demonstration as D
from forecasting import qualification as Q
from forecasting.contracts import sha256_hex

TREE = "a" * 40
DIGEST = "sha256:" + "d" * 64
EVAL = Q.BuildInfo(TREE, True)
NOW = int(datetime(2026, 11, 10, tzinfo=timezone.utc).timestamp())


def ts(seconds: int) -> str:
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def target(namespace="demo", name="web", uid="dep-uid", **over):
    return {"namespace": namespace, "name": name, "uid": uid, "min_replicas": 1, "max_replicas": 12,
            "capacity_per_replica": "5.5", **over}


def contract_doc(*, targets=None, families=("seasonalNaive",), expires_not_after="2026-11-28T00:00:00Z",
                 cadence=None, **over):
    checked = {"qualification": {"expires_not_after": expires_not_after}, "families": list(families),
               "cadence": cadence or {"grid_seconds": 60, "update_interval_seconds": 60},
               "targets": targets if targets is not None else [target()]}
    doc = {"schema": D.CONTRACT_SCHEMA, "checked": checked}
    doc.update(over)
    return doc


def contract(**kw) -> bytes:
    return json.dumps(contract_doc(**kw)).encode()


def qualification(contract_bytes: bytes, **over) -> bytes:
    """A qualification naming `contract_bytes`, with the scope of the default contract unless overridden."""
    d = {"schema": D.SCHEMA, "source_tree": TREE, "image_digest": DIGEST,
         "contract_sha256": sha256_hex(contract_bytes), "reviewers": ["executor", "reviewer"],
         "not_before": "2026-11-08T00:00:00Z", "expires": "2026-11-28T00:00:00Z", "families": ["seasonalNaive"],
         "scope": {"cluster_uid": "cluster-uid", "namespace": "demo",
                   "targets": [{"namespace": "demo", "name": "web", "uid": "dep-uid"}]}}
    d.update(over)
    return json.dumps(d).encode()


def pair(*, contract_kw=None, **over):
    """(qualification bytes, contract bytes): consistent unless an override says otherwise."""
    cd = contract(**(contract_kw or {}))
    return qualification(cd, **over), cd
